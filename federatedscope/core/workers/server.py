import logging
import copy
import os
import sys

import numpy as np
import pickle
import time

from federatedscope.core.monitors.early_stopper import EarlyStopper
from federatedscope.core.message import Message
from federatedscope.core.communication import StandaloneCommManager, \
    StandaloneDDPCommManager, gRPCCommManager
from federatedscope.core.auxiliaries.aggregator_builder import get_aggregator
from federatedscope.core.auxiliaries.sampler_builder import get_sampler
from federatedscope.core.auxiliaries.utils import merge_dict_of_results, \
    Timeout, merge_param_dict
from federatedscope.core.auxiliaries.trainer_builder import get_trainer
from federatedscope.core.secret_sharing import AdditiveSecretSharing
from federatedscope.core.workers.base_server import BaseServer
from federatedscope.core.workers.connection_handler_mixin import ConnectionHandlerMixin
from federatedscope.core.topology_manager import TopologyManager
from federatedscope.core.chunk_tracker import ChunkTracker, ChunkInfo

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Server(BaseServer, ConnectionHandlerMixin):
    """
    The Server class, which describes the behaviors of server in an FL \
    course. The behaviors are described by the handled functions (named as \
    ``callback_funcs_for_xxx``).

    Arguments:
        ID: The unique ID of the server, which is set to 0 by default
        state: The training round
        config: the configuration
        data: The data owned by the server (for global evaluation)
        model: The model used for aggregation
        client_num: The (expected) client num to start the FL course
        total_round_num: The total number of the training round
        device: The device to run local training and evaluation

    Attributes:
        ID: ID of worker
        state: the training round index
        model: the model maintained locally
        cfg: the configuration of FL course, \
            see ``federatedscope.core.configs``
        mode: the run mode for FL, ``distributed`` or ``standalone``
        monitor: monite FL course and record metrics, \
            see ``federatedscope.core.monitors.monitor.Monitor``
        trainer: instantiated trainer, see ``federatedscope.core.trainers``
        best_results: best results ever seen
        history_results: all evaluation results
        early_stopper: determine when to early stop, \
            see ``federatedscope.core.monitors.early_stopper.EarlyStopper``
        aggregators: a protocol for aggregate all clients' model(s), see \
            ``federatedscope.core.aggregators``
        sample_client_num: number of client aggregated in each round
        msg_buffer: dict buffer for storing message
        staled_msg_buffer: list buffer for storing staled message
        comm_manager: manager for communication, \
            see ``federatedscope.core.communication``
    """
    def __init__(self,
                 ID=-1,
                 state=0,
                 config=None,
                 data=None,
                 model=None,
                 client_num=5,
                 total_round_num=10,
                 device='cpu',
                 strategy=None,
                 unseen_clients_id=None,
                 **kwargs):
        super(Server, self).__init__(ID, state, config, model, strategy)
        # Initialize connection handler mixin
        ConnectionHandlerMixin.__init__(self)
        # Register message handlers
        self._register_default_handlers()

        # Un-configured worker
        if config is None:
            return

        self.data = data
        self.device = device
        self.best_results = dict()
        self.history_results = dict()
        self.early_stopper = EarlyStopper(
            self._cfg.early_stop.patience, self._cfg.early_stop.delta,
            self._cfg.early_stop.improve_indicator_mode,
            self._monitor.the_larger_the_better)

        if self._cfg.federate.share_local_model \
                and not self._cfg.federate.process_num > 1:
            # put the model to the specified device
            model.to(device)
        # Build aggregator
        self.aggregator = get_aggregator(self._cfg.federate.method,
                                         model=model,
                                         device=device,
                                         online=self._cfg.federate.online_aggr,
                                         config=self._cfg)
        if self._cfg.federate.restore_from != '':
            if not os.path.exists(self._cfg.federate.restore_from):
                logger.warning(f'Invalid `restore_from`:'
                               f' {self._cfg.federate.restore_from}.')
            else:
                _ = self.aggregator.load_model(self._cfg.federate.restore_from)
                logger.info("Restored the model from {}-th round's ckpt")

        if int(config.model.model_num_per_trainer) != \
                config.model.model_num_per_trainer or \
                config.model.model_num_per_trainer < 1:
            raise ValueError(
                f"model_num_per_trainer should be integer and >= 1, "
                f"got {config.model.model_num_per_trainer}.")
        self.model_num = config.model.model_num_per_trainer
        self.models = [self.model]
        self.aggregators = [self.aggregator]
        if self.model_num > 1:
            self.models.extend(
                [copy.deepcopy(self.model) for _ in range(self.model_num - 1)])
            self.aggregators.extend([
                copy.deepcopy(self.aggregator)
                for _ in range(self.model_num - 1)
            ])

        # function for recovering shared secret
        self.recover_fun = AdditiveSecretSharing(
            shared_party_num=int(self._cfg.federate.sample_client_num)
        ).fixedpoint2float if self._cfg.federate.use_ss else None

        if self._cfg.federate.make_global_eval:
            # set up a trainer for conducting evaluation in server
            assert self.models is not None
            assert self.data is not None

            if self._cfg.backend == 'torch':
                import torch.nn as nn
                # Set BN track_running_stats to False
                for name, module in model.named_modules():
                    if isinstance(module, nn.BatchNorm2d):
                        module.track_running_stats = False
            elif self._cfg.backend == 'tensorflow':
                # TODO: implement this
                pass
            else:
                raise ValueError(f'Unknown backend named {self._cfg.backend}.')

            self.trainer = get_trainer(
                model=self.models[0],
                data=self.data,
                device=self.device,
                config=self._cfg,
                only_for_eval=True,
                monitor=self._monitor
            )  # the trainer is only used for global evaluation
            self.trainers = [self.trainer]
            if self.model_num > 1:
                # By default, the evaluation is conducted by calling
                # trainer[i].eval over all internal models
                self.trainers.extend([
                    copy.deepcopy(self.trainer)
                    for _ in range(self.model_num - 1)
                ])

        # Initialize the number of joined-in clients
        self._client_num = client_num
        self._total_round_num = total_round_num
        self.sample_client_num = int(self._cfg.federate.sample_client_num)
        self.join_in_client_num = 0
        self.join_in_info = dict()
        
        # Initialize topology manager if enabled
        self.topology_manager = None
        if hasattr(self._cfg, 'topology') and self._cfg.topology.use:
            # Will be initialized with actual client list after all clients join
            self.topology_manager = TopologyManager(
                topology_type=self._cfg.topology.type,
                client_list=[],  # Will be updated when clients join
                connections=getattr(self._cfg.topology, 'connections', 2)
            )
            logger.info(f"Topology construction enabled: {self._cfg.topology.type}")
        
        # Initialize chunk tracker for BitTorrent-like chunk management
        self.chunk_tracker = ChunkTracker()
        logger.info("🗂️ Server: Initialize chunk tracker system")
    
        # the unseen clients indicate the ones that do not contribute to FL
        # process by training on their local data and uploading their local
        # model update. The splitting is useful to check participation
        # generalization gap in
        # [ICLR'22, What Do We Mean by Generalization in Federated Learning?]
        self.unseen_clients_id = [] if unseen_clients_id is None \
            else unseen_clients_id

        # Server state
        self.is_finish = False

        # Sampler
        if self._cfg.federate.sampler in ['uniform']:
            self.sampler = get_sampler(
                sample_strategy=self._cfg.federate.sampler,
                client_num=self.client_num,
                client_info=None)
        else:
            # Some type of sampler would be instantiated in trigger_for_start,
            # since they need more information
            self.sampler = None

        # Current Timestamp
        self.cur_timestamp = 0
        self.deadline_for_cur_round = 1

        # Staleness toleration
        self.staleness_toleration = self._cfg.asyn.staleness_toleration if \
            self._cfg.asyn.use else 0
        self.dropout_num = 0

        # Device information
        self.resource_info = kwargs['resource_info'] \
            if 'resource_info' in kwargs else None
        self.client_resource_info = kwargs['client_resource_info'] \
            if 'client_resource_info' in kwargs else None

        # Initialize communication manager and message buffer
        self.msg_buffer = {'train': dict(), 'eval': dict()}
        self.staled_msg_buffer = list()
        if self.mode == 'standalone':
            comm_queue = kwargs.get('shared_comm_queue', None)
            if self._cfg.federate.process_num > 1:
                id2comm = kwargs.get('id2comm', None)
                self.comm_manager = StandaloneDDPCommManager(
                    comm_queue=comm_queue,
                    monitor=self._monitor,
                    id2comm=id2comm)
            else:
                self.comm_manager = StandaloneCommManager(
                    comm_queue=comm_queue, monitor=self._monitor)
        elif self.mode == 'distributed':
            host = kwargs['host']
            port = kwargs['port']
            self.comm_manager = gRPCCommManager(host=host,
                                                port=port,
                                                client_num=client_num,
                                                cfg=self._cfg.distribute)
            logger.info('Server: Listen to {}:{}...'.format(host, port))

        # inject noise before broadcast
        self._noise_injector = None
    
    def _register_default_handlers(self):
        """
        Register default message handlers including chunk tracking and BitTorrent
        """
        # Call parent class to register standard handlers
        super(Server, self)._register_default_handlers()
        
        # Register chunk info handler
        self.register_handlers('chunk_info', self.callback_funcs_for_chunk_info, [])
        
        # Register BitTorrent completion handler
        self.register_handlers('bittorrent_complete', self.callback_funcs_for_bittorrent_complete, [None])
        
        # Register BitTorrent protocol message handlers
        self.register_handlers('bitfield', self.callback_funcs_for_bitfield, [])
        self.register_handlers('interested', self.callback_funcs_for_interested, [])
        self.register_handlers('unchoke', self.callback_funcs_for_unchoke, [])
        self.register_handlers('choke', self.callback_funcs_for_choke, [])
        self.register_handlers('have', self.callback_funcs_for_have, [])
        self.register_handlers('request', self.callback_funcs_for_request, [])
        self.register_handlers('piece', self.callback_funcs_for_piece, [])

    @property
    def client_num(self):
        return self._client_num

    @client_num.setter
    def client_num(self, value):
        self._client_num = value

    @property
    def total_round_num(self):
        return self._total_round_num

    @total_round_num.setter
    def total_round_num(self, value):
        self._total_round_num = value

    def register_noise_injector(self, func):
        self._noise_injector = func

    def run(self):
        """
        To start the FL course, listen and handle messages (for distributed \
        mode).
        """

        # Begin: Broadcast model parameters and start to FL train
        while self.join_in_client_num < self.client_num:
            msg = self.comm_manager.receive()
            # print(f"📥 SERVER: Received message type '{msg.msg_type}' from client {msg.sender}")
            self.msg_handlers[msg.msg_type](msg)

        # Topology Construction Phase moved to trigger_for_start()

        # Running: listen for message (updates from clients),
        # aggregate and broadcast feedbacks (aggregated model parameters)
        min_received_num = self._cfg.asyn.min_received_num \
            if self._cfg.asyn.use else self._cfg.federate.sample_client_num
        num_failure = 0
        time_budget = self._cfg.asyn.time_budget if self._cfg.asyn.use else -1
        with Timeout(time_budget) as time_counter:
            while self.state <= self.total_round_num:
                try:
                    msg = self.comm_manager.receive()
                    # print(f"📥 SERVER: Received message type '{msg.msg_type}' from client {msg.sender}")
                    move_on_flag = self.msg_handlers[msg.msg_type](msg)
                    if move_on_flag:
                        time_counter.reset()
                except TimeoutError:
                    logger.info('Time out at the training round #{}'.format(
                        self.state))
                    move_on_flag_eval = self.check_and_move_on(
                        min_received_num=min_received_num,
                        check_eval_result=True)
                    move_on_flag = self.check_and_move_on(
                        min_received_num=min_received_num)
                    if not move_on_flag and not move_on_flag_eval:
                        num_failure += 1
                        # Terminate the training if the number of failure
                        # exceeds the maximum number (default value: 10)
                        if time_counter.exceed_max_failure(num_failure):
                            logger.info(f'----------- Training fails at round '
                                        f'#{self.state}-------------')
                            break

                        # Time out, broadcast the model para and re-start
                        # the training round
                        logger.info(
                            f'----------- Re-starting the training round ('
                            f'Round #{self.state}) for {num_failure} time '
                            f'-------------')
                        # TODO: Clean the msg_buffer
                        if self.state in self.msg_buffer['train']:
                            self.msg_buffer['train'][self.state].clear()

                        self.broadcast_model_para(
                            msg_type='model_para',
                            sample_client_num=self.sample_client_num)
                    else:
                        num_failure = 0
                    time_counter.reset()

        self.terminate(msg_type='finish')

    def check_and_move_on(self,
                          check_eval_result=False,
                          min_received_num=None):
        """
        To check the message_buffer. When enough messages are receiving, \
        some events (such as perform aggregation, evaluation, and move to \
        the next training round) would be triggered.

        Arguments:
            check_eval_result (bool): If True, check the message buffer for \
                evaluation; and check the message buffer for training \
                otherwise.
            min_received_num: number of minimal received message, used for \
                async mode
        """
        if min_received_num is None:
            if self._cfg.asyn.use:
                min_received_num = self._cfg.asyn.min_received_num
            else:
                min_received_num = self._cfg.federate.sample_client_num
        assert min_received_num <= self.sample_client_num

        if check_eval_result and self._cfg.federate.mode.lower(
        ) == "standalone":
            # in evaluation stage and standalone simulation mode, we assume
            # strong synchronization that receives responses from all clients
            min_received_num = len(self.comm_manager.get_neighbors().keys())

        move_on_flag = True  # To record whether moving to a new training
        # round or finishing the evaluation
        if self.check_buffer(self.state, min_received_num, check_eval_result):
            if not check_eval_result:
                # Receiving enough feedback in the training process
                aggregated_num = self._perform_federated_aggregation()
                self.state += 1
                if self.state % self._cfg.eval.freq == 0 and self.state != \
                        self.total_round_num:
                    #  Evaluate
                    logger.info(f'Server: Starting evaluation at the end '
                                f'of round {self.state - 1}.')
                    self.eval()

                if self.state < self.total_round_num:
                    # Move to next round of training
                    logger.info(
                        f'----------- Starting a new training round (Round '
                        f'#{self.state}) -------------')
                    # Clean the msg_buffer
                    self.msg_buffer['train'][self.state - 1].clear()
                    self.msg_buffer['train'][self.state] = dict()
                    self.staled_msg_buffer.clear()
                    # Start a new training round
                    self._start_new_training_round(aggregated_num)
                else:
                    # Final Evaluate
                    logger.info('Server: Training is finished! Starting '
                                'evaluation.')
                    self.eval()

            else:
                # Receiving enough feedback in the evaluation process
                self._merge_and_format_eval_results()
                if self.state >= self.total_round_num:
                    self.is_finish = True

        else:
            move_on_flag = False

        return move_on_flag

    def _construct_network_topology(self):
        """
        Construct network topology between clients
        
        This method orchestrates the topology construction process:
        1. Initialize topology with current client list
        2. Compute required connections for each client
        3. Send topology instructions to clients
        4. Wait for all connections to be established
        """
        logger.info("🌐 Starting network topology construction...")
        
        # Get list of connected clients
        client_list = list(range(1, self.client_num + 1))
        
        # Update topology manager with actual client list
        self.topology_manager.client_list = client_list
        
        logger.info(f"🔧 Topology setup: client_num={self.client_num}, client_list={client_list}")
        
        # Validate that we have clients to work with
        if not client_list:
            logger.error(f"❌ Cannot construct topology: no clients available (client_num={self.client_num})")
            return
        
        # Compute the topology structure
        try:
            topology_graph = self.topology_manager.compute_topology()
            
            if not topology_graph:
                logger.warning("No topology computed, skipping topology construction")
                return
                
            logger.info(f"📋 Computed topology: {topology_graph}")
            
            # Send topology instructions to each client
            self._send_topology_instructions(topology_graph)
            
            # Wait for topology construction to complete
            self._wait_for_topology_completion()
            
            logger.info("🎉 Network topology construction completed successfully!")
            
        except Exception as e:
            logger.error(f"❌ Topology construction failed: {e}")
            if self._cfg.topology.require_full_topology:
                raise RuntimeError(f"Required topology construction failed: {e}")
            else:
                logger.warning("Continuing with incomplete topology as configured")

    def _send_topology_instructions(self, topology_graph):
        """
        Send topology connection instructions to each client
        
        Args:
            topology_graph: Dict[client_id] -> List[neighbor_ids]
        """
        logger.info("📤 Sending topology instructions to clients...")
        
        for client_id, neighbors in topology_graph.items():
            if neighbors:  # Only send if client has neighbors to connect to
                # 🔧 Critical fix: Add neighbor address information
                neighbor_addresses = {}
                for neighbor_id in neighbors:
                    if neighbor_id in self.comm_manager.neighbors:
                        neighbor_addr = self.comm_manager.neighbors[neighbor_id]
                        # Parse address string to dictionary format
                        if ':' in neighbor_addr:
                            host, port = neighbor_addr.split(':')
                            neighbor_addresses[neighbor_id] = {
                                'host': host,
                                'port': int(port)
                            }
                        else:
                            logger.warning(f"Invalid neighbor address format: {neighbor_addr}")
                    else:
                        logger.error(f"Neighbor {neighbor_id} address not found in comm_manager.neighbors")
                
                message = Message(
                    msg_type='topology_instruction',
                    sender=self.ID,
                    receiver=[client_id],
                    state=self.state,
                    timestamp=self.cur_timestamp,
                    content={
                        'neighbors_to_connect': neighbors,
                        'neighbor_addresses': neighbor_addresses,  # 🔧 Add real address information
                        'topology_type': self.topology_manager.topology_type.value,
                        'max_attempts': self._cfg.topology.max_connection_attempts,
                        'retry_delay': self._cfg.topology.connection_retry_delay
                    }
                )
                
                self.comm_manager.send(message)
                logger.info(f"📨 Sent topology instruction to Client {client_id}: connect to {neighbors}")
                logger.info(f"📍 With addresses: {neighbor_addresses}")
            else:
                logger.info(f"📭 Client {client_id} has no connections required")

    def _wait_for_topology_completion(self):
        """
        Wait for all required topology connections to be established
        """
        logger.info("⏳ Waiting for topology construction to complete...")
        
        start_time = time.time()
        timeout = self._cfg.topology.timeout
        check_interval = 2.0  # Check every 2 seconds
        
        logger.info(f"🕐 Topology waiting configuration: timeout={timeout}s, check_interval={check_interval}s")
        
        while True:
            # Check if topology is complete
            if self.topology_manager.is_topology_complete():
                construction_time = time.time() - start_time
                logger.info(f"✅ Topology construction completed in {construction_time:.2f} seconds")
                break
            
            # Check timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                if self._cfg.topology.require_full_topology:
                    self.topology_manager.print_topology_status()
                    raise TimeoutError(f"Topology construction timed out after {timeout} seconds")
                else:
                    logger.warning(f"⏰ Topology construction timed out after {timeout} seconds, "
                                 f"continuing with partial topology")
                    break
            
            # Print progress if verbose mode enabled
            if self._cfg.topology.verbose and int(elapsed_time) % 10 == 0:
                logger.info(f"🕰️ Topology construction progress at {elapsed_time:.1f}s...")
                self.topology_manager.print_topology_status()
            
            # Handle incoming messages during topology construction
            # Since gRPC receive() is blocking, we need to use a different approach
            # Check for messages in a non-blocking way by using the message queue directly
            try:
                # Check if there are messages in the queue (non-blocking)
                if hasattr(self.comm_manager, 'server_funcs') and \
                   hasattr(self.comm_manager.server_funcs, 'msg_queue') and \
                   len(self.comm_manager.server_funcs.msg_queue) > 0:
                    
                    # There are messages waiting, process them
                    msg = self.comm_manager.receive()
                    if msg:
                        logger.info(f"📥 SERVER: Received message type '{msg.msg_type}' from client {msg.sender} during topology wait")
                        
                        # Handle topology-related messages
                        if msg.msg_type in self.msg_handlers:
                            self.msg_handlers[msg.msg_type](msg)
                        else:
                            logger.warning(f"Unknown message type during topology construction: {msg.msg_type}")
                else:
                    # No messages in queue, sleep briefly before next check
                    logger.info(f"💤 No pending messages, sleeping {check_interval}s before next topology check")
                    time.sleep(check_interval)
                        
            except Exception as e:
                # Error in message handling, continue after brief sleep
                logger.debug(f"Message handling error during topology wait: {e}")
                time.sleep(check_interval)
        
        # Print final topology status
        if self._cfg.topology.verbose:
            self.topology_manager.print_topology_status()

    def check_and_save(self):
        """
        To save the results and save model after each evaluation, and check \
        whether to early stop.
        """

        # early stopping
        if "Results_weighted_avg" in self.history_results and \
                self._cfg.eval.best_res_update_round_wise_key in \
                self.history_results['Results_weighted_avg']:
            should_stop = self.early_stopper.track_and_check(
                self.history_results['Results_weighted_avg'][
                    self._cfg.eval.best_res_update_round_wise_key])
        elif "Results_avg" in self.history_results and \
                self._cfg.eval.best_res_update_round_wise_key in \
                self.history_results['Results_avg']:
            should_stop = self.early_stopper.track_and_check(
                self.history_results['Results_avg'][
                    self._cfg.eval.best_res_update_round_wise_key])
        else:
            should_stop = False

        if should_stop:
            self._monitor.global_converged()
            self.comm_manager.send(
                Message(
                    msg_type="converged",
                    sender=self.ID,
                    receiver=list(self.comm_manager.neighbors.keys()),
                    timestamp=self.cur_timestamp,
                    state=self.state,
                ))
            self.state = self.total_round_num + 1

        if should_stop or self.state == self.total_round_num:
            logger.info('Server: Final evaluation is finished! Starting '
                        'merging results.')
            # last round or early stopped
            self.save_best_results()
            if not self._cfg.federate.make_global_eval:
                self.save_client_eval_results()
            self.terminate(msg_type='finish')

        # Clean the clients evaluation msg buffer
        if not self._cfg.federate.make_global_eval:
            round = max(self.msg_buffer['eval'].keys())
            self.msg_buffer['eval'][round].clear()

        if self.state == self.total_round_num:
            # break out the loop for distributed mode
            self.state += 1

    def _perform_federated_aggregation(self):
        """
        Track client training completion for synchronization purposes in BitTorrent P2P mode.
        NOTE: Server no longer performs model aggregation - clients handle locally from P2P chunks.
        """
        train_msg_buffer = self.msg_buffer['train'][self.state]
        
        logger.info(f"[BT-FL] Server: Processing training completion signals - skipping server-side aggregation")
        
        # Track client completion information for synchronization
        completed_clients = []
        total_samples = 0
        
        for client_id, sync_info in train_msg_buffer.items():
            if isinstance(sync_info, dict) and sync_info.get('training_completed', False):
                completed_clients.append(client_id)
                total_samples += sync_info.get('sample_size', 0)
                logger.debug(f"[BT-FL] Server: Client {client_id} completed training with {sync_info.get('sample_size', 0)} samples")
        
        # Include stale message tracking for consistency
        for staled_message in self.staled_msg_buffer:
            state, client_id, sync_info = staled_message
            if isinstance(sync_info, dict) and sync_info.get('training_completed', False):
                completed_clients.append(client_id)
                total_samples += sync_info.get('sample_size', 0)
        
        aggregated_num = len(completed_clients)
        logger.info(f"[BT-FL] Server: Round {self.state} completion tracking - {aggregated_num} clients completed training with {total_samples} total samples")
        
        # No model parameter aggregation in BitTorrent mode
        # Clients aggregate locally from P2P chunks
        
        return aggregated_num

    def _start_new_training_round(self, aggregated_num=0):
        """
        The behaviors for starting a new training round
        """
        # 🔥 Key modification: Wait for BitTorrent completion before broadcasting new model
        if hasattr(self._cfg, 'bittorrent') and self._cfg.bittorrent.enable:
            self.trigger_bittorrent()
        
        if self._cfg.asyn.use:  # for asynchronous training
            if self._cfg.asyn.aggregator == "time_up":
                # Update the deadline according to the time budget
                self.deadline_for_cur_round = \
                    self.cur_timestamp + self._cfg.asyn.time_budget

            if self._cfg.asyn.broadcast_manner == \
                    'after_aggregating':
                if self._cfg.asyn.overselection:
                    sample_client_num = self.sample_client_num
                else:
                    sample_client_num = aggregated_num + \
                                        self.dropout_num

                self.broadcast_model_para(msg_type='model_para',
                                          sample_client_num=sample_client_num)
                self.dropout_num = 0
        else:  # for synchronous training
            self.broadcast_model_para(msg_type='model_para',
                                      sample_client_num=self.sample_client_num)

    def _merge_and_format_eval_results(self):
        """
        The behaviors of server when receiving enough evaluating results
        """
        # Get all the message & aggregate
        formatted_eval_res = \
            self.merge_eval_results_from_all_clients()
        self.history_results = merge_dict_of_results(self.history_results,
                                                     formatted_eval_res)
        if self.mode == 'standalone' and \
                self._monitor.wandb_online_track and \
                self._monitor.use_wandb:
            self._monitor.merge_system_metrics_simulation_mode(
                file_io=False, from_global_monitors=True)
        self.check_and_save()

    def save_best_results(self):
        """
        To Save the best evaluation results.
        """

        if self._cfg.federate.save_to != '':
            self.aggregator.save_model(self._cfg.federate.save_to, self.state)
        formatted_best_res = self._monitor.format_eval_res(
            results=self.best_results,
            rnd="Final",
            role='Server #',
            forms=["raw"],
            return_raw=True)
        logger.info(formatted_best_res)
        self._monitor.save_formatted_results(formatted_best_res)

    def save_client_eval_results(self):
        """
        save the evaluation results of each client when the fl course \
        early stopped or terminated
        """
        rnd = max(self.msg_buffer['eval'].keys())
        eval_msg_buffer = self.msg_buffer['eval'][rnd]

        with open(os.path.join(self._cfg.outdir, "eval_results.log"),
                  "a") as outfile:
            for client_id, client_eval_results in eval_msg_buffer.items():
                formatted_res = self._monitor.format_eval_res(
                    client_eval_results,
                    rnd=self.state,
                    role='Client #{}'.format(client_id),
                    return_raw=True)
                logger.info(formatted_res)
                outfile.write(str(formatted_res) + "\n")

    def merge_eval_results_from_all_clients(self):
        """
        Merge evaluation results from all clients, update best, \
        log the merged results and save them into eval_results.log

        Returns:
            the formatted merged results
        """
        round = max(self.msg_buffer['eval'].keys())
        eval_msg_buffer = self.msg_buffer['eval'][round]
        eval_res_participated_clients = []
        eval_res_unseen_clients = []
        for client_id in eval_msg_buffer:
            if eval_msg_buffer[client_id] is None:
                continue
            if client_id in self.unseen_clients_id:
                eval_res_unseen_clients.append(eval_msg_buffer[client_id])
            else:
                eval_res_participated_clients.append(
                    eval_msg_buffer[client_id])

        formatted_logs_all_set = dict()
        for merge_type, eval_res_set in [("participated",
                                          eval_res_participated_clients),
                                         ("unseen", eval_res_unseen_clients)]:
            if eval_res_set != []:
                metrics_all_clients = dict()
                for client_eval_results in eval_res_set:
                    for key in client_eval_results.keys():
                        if key not in metrics_all_clients:
                            metrics_all_clients[key] = list()
                        metrics_all_clients[key].append(
                            float(client_eval_results[key]))
                formatted_logs = self._monitor.format_eval_res(
                    metrics_all_clients,
                    rnd=round,
                    role='Server #',
                    forms=self._cfg.eval.report)
                if merge_type == "unseen":
                    for key, val in copy.deepcopy(formatted_logs).items():
                        if isinstance(val, dict):
                            # to avoid the overrides of results using the
                            # same name, we use new keys with postfix `unseen`:
                            # 'Results_weighted_avg' ->
                            # 'Results_weighted_avg_unseen'
                            formatted_logs[key + "_unseen"] = val
                            del formatted_logs[key]
                logger.info(formatted_logs)
                formatted_logs_all_set.update(formatted_logs)
                self._monitor.update_best_result(
                    self.best_results,
                    metrics_all_clients,
                    results_type="unseen_client_best_individual"
                    if merge_type == "unseen" else "client_best_individual")
                self._monitor.save_formatted_results(formatted_logs)
                for form in self._cfg.eval.report:
                    if form != "raw":
                        metric_name = form + "_unseen" if merge_type == \
                                                          "unseen" else form
                        self._monitor.update_best_result(
                            self.best_results,
                            formatted_logs[f"Results_{metric_name}"],
                            results_type=f"unseen_client_summarized_{form}"
                            if merge_type == "unseen" else
                            f"client_summarized_{form}")

        return formatted_logs_all_set

    def broadcast_model_para(self,
                             msg_type='model_para',
                             sample_client_num=-1,
                             filter_unseen_clients=True):
        """
        To broadcast the training signal to all clients or sampled clients.
        NOTE: Modified for BitTorrent integration - no longer sends model weights.
        Clients will aggregate from their local chunk databases instead.

        Arguments:
            msg_type: 'model_para' or other user defined msg_type
            sample_client_num: the number of sampled clients in the broadcast \
                behavior. And ``sample_client_num = -1`` denotes to \
                broadcast to all the clients.
            filter_unseen_clients: whether filter out the unseen clients that \
                do not contribute to FL process by training on their local \
                data and uploading their local model update.
        """
        if filter_unseen_clients:
            # to filter out the unseen clients when sampling
            self.sampler.change_state(self.unseen_clients_id, 'unseen')

        if sample_client_num > 0:
            receiver = self.sampler.sample(size=sample_client_num)
        else:
            # broadcast to all clients
            receiver = list(self.comm_manager.neighbors.keys())
            if msg_type == 'model_para':
                self.sampler.change_state(receiver, 'working')

        # Skip model weight processing for BitTorrent-enabled FL
        # Clients will aggregate from their chunk databases instead
        skip_broadcast = self._cfg.federate.method in ["local", "global"]
        
        # Prepare lightweight message content (no model weights)
        if msg_type == 'model_para':
            # Send training signal with metadata only
            message_content = {
                'round_num': self.state,
                'total_round_num': self.total_round_num,
                'aggregation_method': getattr(self._cfg.federate, 'method', 'FedAvg'),
                'sample_client_num': len(receiver) if 'receiver' in locals() else self.client_num,
                'skip_weights': True  # Signal that weights should come from chunk aggregation
            }
        else:
            # For non-model_para messages, use empty content
            message_content = {}

        # We define the evaluation happens at the end of an epoch
        rnd = self.state - 1 if msg_type == 'evaluate' else self.state

        self.comm_manager.send(
            Message(msg_type=msg_type,
                    sender=self.ID,
                    receiver=receiver,
                    state=min(rnd, self.total_round_num),
                    timestamp=self.cur_timestamp,
                    content=message_content))
        if self._cfg.federate.online_aggr:
            for idx in range(self.model_num):
                self.aggregators[idx].reset()

        if filter_unseen_clients:
            # restore the state of the unseen clients within sampler
            self.sampler.change_state(self.unseen_clients_id, 'seen')

    def broadcast_client_address(self):
        """
        To broadcast the communication addresses of clients (used for \
        additive secret sharing)
        """

        self.comm_manager.send(
            Message(msg_type='address',
                    sender=self.ID,
                    receiver=list(self.comm_manager.neighbors.keys()),
                    state=self.state,
                    timestamp=self.cur_timestamp,
                    content=self.comm_manager.get_neighbors()))

    def check_buffer(self,
                     cur_round,
                     min_received_num,
                     check_eval_result=False):
        """
        To check the message buffer

        Arguments:
            cur_round (int): The current round number
            min_received_num (int): The minimal number of the receiving \
                messages
            check_eval_result (bool): To check training results for \
                evaluation results

        Returns
            bool: Whether enough messages have been received or not
        """

        if check_eval_result:
            if 'eval' not in self.msg_buffer.keys() or len(
                    self.msg_buffer['eval'].keys()) == 0:
                return False

            buffer = self.msg_buffer['eval']
            cur_round = max(buffer.keys())
            cur_buffer = buffer[cur_round]
            return len(cur_buffer) >= min_received_num
        else:
            if cur_round not in self.msg_buffer['train']:
                cur_buffer = dict()
            else:
                cur_buffer = self.msg_buffer['train'][cur_round]
            if self._cfg.asyn.use and self._cfg.asyn.aggregator == 'time_up':
                if self.cur_timestamp >= self.deadline_for_cur_round and len(
                        cur_buffer) + len(self.staled_msg_buffer) == 0:
                    # When the time budget is run out but the server has not
                    # received any feedback
                    logger.warning(
                        f'The server has not received any feedback when the '
                        f'time budget has run out, therefore the server would '
                        f'wait for more {self._cfg.asyn.time_budget} seconds. '
                        f'Maybe you should carefully reset '
                        f'`cfg.asyn.time_budget` to a reasonable value.')
                    self.deadline_for_cur_round += self._cfg.asyn.time_budget
                    if self._cfg.asyn.broadcast_manner == \
                            'after_aggregating' and self.dropout_num != 0:
                        self.broadcast_model_para(
                            msg_type='model_para',
                            sample_client_num=self.dropout_num)
                        self.dropout_num = 0
                return self.cur_timestamp >= self.deadline_for_cur_round
            else:
                return len(cur_buffer)+len(self.staled_msg_buffer) >= \
                       min_received_num

    def check_client_join_in(self):
        """
        To check whether all the clients have joined in the FL course.
        """

        if len(self._cfg.federate.join_in_info) != 0:
            return len(self.join_in_info) == self.client_num
        else:
            return self.join_in_client_num == self.client_num

    def trigger_for_start(self):
        """
        To start the FL course when the expected number of clients have joined
        """

        if self.check_client_join_in():
            if self._cfg.federate.use_ss or self._cfg.vertical.use:
                self.broadcast_client_address()

            # Topology Construction Phase (if enabled) - BEFORE training starts
            if self.topology_manager is not None:
                logger.info("🚀 All clients joined! Starting topology construction before training...")
                self._construct_network_topology()

            # get sampler
            if 'client_resource' in self._cfg.federate.join_in_info:
                client_resource = [
                    self.join_in_info[client_index]['client_resource']
                    for client_index in np.arange(1, self.client_num + 1)
                ]
            else:
                if self._cfg.backend == 'torch':
                    model_size = sys.getsizeof(pickle.dumps(
                        self.models[0])) / 1024.0 * 8.
                else:
                    # TODO: calculate model size for TF Model
                    model_size = 1.0
                    logger.warning(f'The calculation of model size in backend:'
                                   f'{self._cfg.backend} is not provided.')

                client_resource = [
                    model_size / float(x['communication']) +
                    float(x['computation']) / 1000.
                    for x in self.client_resource_info
                ] if self.client_resource_info is not None else None

            if self.sampler is None:
                self.sampler = get_sampler(
                    sample_strategy=self._cfg.federate.sampler,
                    client_num=self.client_num,
                    client_info=client_resource)

            # change the deadline if the asyn.aggregator is `time up`
            if self._cfg.asyn.use and self._cfg.asyn.aggregator == 'time_up':
                self.deadline_for_cur_round = self.cur_timestamp + \
                                               self._cfg.asyn.time_budget

            # start feature engineering
            self.trigger_for_feat_engr(
                self.broadcast_model_para, {
                    'msg_type': 'model_para',
                    'sample_client_num': self.sample_client_num
                })

            logger.info(
                '----------- Starting training (Round #{:d}) -------------'.
                format(self.state))

    def trigger_for_feat_engr(self,
                              trigger_train_func,
                              kwargs_for_trigger_train_func={}):
        """
        Interface for feature engineering, the default operation is none
        """
        trigger_train_func(**kwargs_for_trigger_train_func)

    def trigger_for_time_up(self, check_timestamp=None):
        """
        The handler for time up: modify the currency timestamp \
        and check the trigger condition
        """
        if self.is_finish:
            return False

        if check_timestamp is not None and \
                check_timestamp < self.deadline_for_cur_round:
            return False

        self.cur_timestamp = self.deadline_for_cur_round
        self.check_and_move_on()
        return True

    def terminate(self, msg_type='finish'):
        """
        To terminate the FL course
        """
        self.is_finish = True
        if self.model_num > 1:
            model_para = [model.state_dict() for model in self.models]
        else:
            model_para = self.models[0].state_dict()

        self._monitor.finish_fl()

        self.comm_manager.send(
            Message(msg_type=msg_type,
                    sender=self.ID,
                    receiver=list(self.comm_manager.neighbors.keys()),
                    state=self.state,
                    timestamp=self.cur_timestamp,
                    content=model_para))

    def eval(self):
        """
        To conduct evaluation. When ``cfg.federate.make_global_eval=True``, \
        a global evaluation is conducted by the server.
        """

        if self._cfg.federate.make_global_eval:
            # By default, the evaluation is conducted one-by-one for all
            # internal models;
            # for other cases such as ensemble, override the eval function
            for i in range(self.model_num):
                trainer = self.trainers[i]
                # Preform evaluation in server
                metrics = {}
                for split in self._cfg.eval.split:
                    eval_metrics = trainer.evaluate(
                        target_data_split_name=split)
                    metrics.update(**eval_metrics)
                formatted_eval_res = self._monitor.format_eval_res(
                    metrics,
                    rnd=self.state,
                    role='Server #',
                    forms=self._cfg.eval.report,
                    return_raw=self._cfg.federate.make_global_eval)
                self._monitor.update_best_result(
                    self.best_results,
                    formatted_eval_res['Results_raw'],
                    results_type="server_global_eval")
                self.history_results = merge_dict_of_results(
                    self.history_results, formatted_eval_res)
                self._monitor.save_formatted_results(formatted_eval_res)
                logger.info(formatted_eval_res)
            self.check_and_save()
        else:
            # Preform evaluation in clients
            self.broadcast_model_para(msg_type='evaluate',
                                      filter_unseen_clients=False)

    def callback_funcs_model_para(self, message: Message):
        """
        The handling function for receiving training completion signals from clients.
        NOTE: Modified for BitTorrent integration - no longer processes model weights.
        Only tracks client training completion for synchronization.

        Arguments:
            message: The received message containing training completion signal.
        """
        if self.is_finish:
            return 'finish'

        round = message.state
        sender = message.sender
        timestamp = message.timestamp
        content = message.content
        self.sampler.change_state(sender, 'idle')

        # Check if this is a BitTorrent training completion signal
        if isinstance(content, dict) and content.get('skip_weights', False):
            logger.info(f"[BT-FL] Server: Received training completion signal from client {sender} for round {round}")
            
            # Extract synchronization information (no model weights)
            sync_info = {
                'client_id': content.get('client_id', sender),
                'sample_size': content.get('sample_size', 0),
                'training_completed': content.get('training_completed', True),
                'round_num': content.get('round_num', round)
            }
            
            logger.debug(f"[BT-FL] Server: Client {sender} completed training with {sync_info['sample_size']} samples")
        else:
            # Legacy mode: handle traditional model parameter messages
            logger.warning(f"[BT-FL] Server: Received legacy model parameters from client {sender}, processing for compatibility")
            
            # Legacy dequantization for backward compatibility
            if self._cfg.quantization.method == 'uniform':
                from federatedscope.core.compression import \
                    symmetric_uniform_dequantization
                if isinstance(content[1], list):  # multiple model
                    sample_size = content[0]
                    quant_model = [
                        symmetric_uniform_dequantization(x) for x in content[1]
                    ]
                else:
                    sample_size = content[0]
                    quant_model = symmetric_uniform_dequantization(content[1])
                content = (sample_size, quant_model)
            
            # For legacy compatibility, create sync_info from old format
            sync_info = {
                'client_id': sender,
                'sample_size': content[0] if isinstance(content, tuple) else 0,
                'training_completed': True,
                'round_num': round
            }

        # Update the currency timestamp according to the received message
        assert timestamp >= self.cur_timestamp  # for test
        self.cur_timestamp = timestamp

        # Store synchronization info instead of model weights
        if round == self.state:
            if round not in self.msg_buffer['train']:
                self.msg_buffer['train'][round] = dict()
            # Save the sync info in this round (no model weights)
            self.msg_buffer['train'][round][sender] = sync_info
        elif round >= self.state - self.staleness_toleration:
            # Save the staled sync messages
            self.staled_msg_buffer.append((round, sender, sync_info))
        else:
            # Drop the out-of-date messages
            logger.info(f'Drop a out-of-date message from round #{round}')
            self.dropout_num += 1

        # Skip online aggregation for BitTorrent mode (clients handle aggregation locally)
        if self._cfg.federate.online_aggr and not (isinstance(content, dict) and content.get('skip_weights', False)):
            logger.warning("[BT-FL] Server: Online aggregation disabled for BitTorrent mode")

        move_on_flag = self.check_and_move_on()
        if self._cfg.asyn.use and self._cfg.asyn.broadcast_manner == \
                'after_receiving':
            self.broadcast_model_para(msg_type='model_para',
                                      sample_client_num=1)

        return move_on_flag

    def callback_funcs_for_join_in(self, message: Message):
        """
        The handling function for receiving the join in information. The \
        server might request for some information (such as \
        ``num_of_samples``) if necessary, assign IDs for the servers. \
        If all the clients have joined in, the training process will be \
        triggered.

        Arguments:
            message: The received message
        """

        if 'info' in message.msg_type:
            sender, info = message.sender, message.content
            for key in self._cfg.federate.join_in_info:
                assert key in info
            self.join_in_info[sender] = info
            logger.info('Server: Client #{:d} has joined in !'.format(sender))
        else:
            self.join_in_client_num += 1
            sender, address = message.sender, message.content
            if int(sender) == -1:  # assign number to client
                sender = self.join_in_client_num
                self.comm_manager.add_neighbors(neighbor_id=sender,
                                                address=address)
                self.comm_manager.send(
                    Message(msg_type='assign_client_id',
                            sender=self.ID,
                            receiver=[sender],
                            state=self.state,
                            timestamp=self.cur_timestamp,
                            content=str(sender)))
            else:
                self.comm_manager.add_neighbors(neighbor_id=sender,
                                                address=address)

            if len(self._cfg.federate.join_in_info) != 0:
                self.comm_manager.send(
                    Message(msg_type='ask_for_join_in_info',
                            sender=self.ID,
                            receiver=[sender],
                            state=self.state,
                            timestamp=self.cur_timestamp,
                            content=self._cfg.federate.join_in_info.copy()))

        self.trigger_for_start()

    def callback_funcs_for_metrics(self, message: Message):
        """
        The handling function for receiving the evaluation results, \
        which triggers ``check_and_move_on`` (perform aggregation when \
        enough feedback has been received).

        Arguments:
            message: The received message
        """

        rnd = message.state
        sender = message.sender
        content = message.content

        if rnd not in self.msg_buffer['eval'].keys():
            self.msg_buffer['eval'][rnd] = dict()

        self.msg_buffer['eval'][rnd][sender] = content

        return self.check_and_move_on(check_eval_result=True)
    
    def callback_funcs_for_chunk_info(self, message: Message):
        """
        Handle chunk information reports from clients
        
        Arguments:
            message: Message containing chunk information
        """
        try:
            sender = message.sender
            chunk_info_dict = message.content
            
            # Debug: Print raw data types
            logger.debug(f"🔍 Raw chunk_info_dict types: {[(k, type(v), v) for k, v in chunk_info_dict.items()]}")
            
            # Safely convert data types, handle bytes data
            def safe_convert(value, target_type, field_name="unknown"):
                logger.debug(f"🔧 Converting {field_name}: {value} (type: {type(value)}) -> {target_type}")
                
                if isinstance(value, bytes):
                    # If bytes, try different processing approaches
                    if len(value) == 8:  # Possibly 64-bit integer bytes representation
                        import struct
                        try:
                            # Try to interpret 8 bytes as little-endian 64-bit integer
                            int_value = struct.unpack('<Q', value)[0] 
                            logger.debug(f"🔧 Decoded bytes as int64: {int_value}")
                            value = int_value
                        except:
                            # If failed, try decoding as string
                            try:
                                value = value.decode('utf-8').rstrip('\x00')  # Remove null characters
                                logger.debug(f"🔧 Decoded bytes as string: '{value}'")
                            except:
                                logger.debug(f"Failed to decode bytes: {value}")
                                return 0 if target_type == int else 0.0 if target_type == float else ""
                    else:
                        # Try to decode bytes of other lengths as string
                        try:
                            value = value.decode('utf-8').rstrip('\x00')
                        except:
                            logger.warning(f"⚠️ Failed to decode bytes: {value}")
                            return 0 if target_type == int else 0.0 if target_type == float else ""
                
                try:
                    if target_type == int:
                        return int(float(value)) if isinstance(value, str) and '.' in value else int(value)
                    elif target_type == float:
                        return float(value)
                    elif target_type == str:
                        return str(value)
                    return value
                except Exception as e:
                    logger.debug(f"Failed to convert {field_name} '{value}' to {target_type}: {e}")
                    return 0 if target_type == int else 0.0 if target_type == float else ""
            
            # Create ChunkInfo object from dictionary, ensure correct types
            chunk_info = ChunkInfo(
                client_id=safe_convert(chunk_info_dict['client_id'], int, 'client_id'),
                round_num=safe_convert(chunk_info_dict['round_num'], int, 'round_num'),
                chunk_id=safe_convert(chunk_info_dict['chunk_id'], int, 'chunk_id'),
                action=safe_convert(chunk_info_dict['action'], str, 'action'),
                chunk_hash=safe_convert(chunk_info_dict['chunk_hash'], str, 'chunk_hash'),
                chunk_size=safe_convert(chunk_info_dict['chunk_size'], int, 'chunk_size'),
                timestamp=safe_convert(chunk_info_dict['timestamp'], float, 'timestamp')
            )
            
            # Update chunk tracker
            success = self.chunk_tracker.update_chunk_info(chunk_info)
            
            if success:
                # Silently handle successful chunk information
                pass
            else:
                logger.debug(f"Server: Failed to process chunk information from client {sender}")
                
        except Exception as e:
            logger.error(f"❌ Server: Failed to process chunk information message: {e}")
    
    def get_chunk_tracker_stats(self) -> dict:
        """Get chunk tracker statistics"""
        return self.chunk_tracker.get_tracker_stats()
    
    def query_chunk_locations(self, round_num: int, chunk_id: int) -> list:
        """Query list of holders for specified chunk"""
        return self.chunk_tracker.get_chunk_locations(round_num, chunk_id)
    
    def get_client_chunks_list(self, client_id: int) -> list:
        """Get list of chunks held by specified client"""
        return self.chunk_tracker.get_client_chunks(client_id)
    
    def get_round_chunk_availability(self, round_num: int) -> dict:
        """Get availability statistics for all chunks in specified round"""
        return self.chunk_tracker.get_chunk_availability(round_num)

    # ==================== BitTorrent Support Methods ====================
    
    def trigger_bittorrent(self):
        """
        🔧 Modified: Synchronous blocking version
        Trigger BitTorrent chunk exchange after aggregation completion, wait for all clients to complete before continuing
        """
        logger.info("[BT] Server: Initiating BitTorrent chunk exchange phase")
        
        # Initialize BitTorrent state machine
        if not hasattr(self, 'bt_state'):
            self.bt_state = 'IDLE'
        
        # Set state to EXCHANGING
        self.bt_state = 'EXCHANGING'
        self.bt_round = self.state  # Record current FL round
        self.bittorrent_completion_status = {}
        self.bt_start_time = time.time()
        
        # Get configuration
        chunks_per_client = getattr(getattr(self._cfg, 'chunk', None), 'num_chunks', 10)
        self.bt_expected_chunks = self.client_num * chunks_per_client
        
        # Broadcast start BitTorrent message to all clients
        self.comm_manager.send(
            Message(msg_type='start_bittorrent',
                    sender=self.ID,
                    receiver=list(range(1, self.client_num + 1)),  # All client IDs
                    state=self.state,
                    content={
                        'round': self.state,
                        'expected_chunks': self.bt_expected_chunks
                    })
        )
        
        # Set timeout configuration
        timeout = getattr(getattr(self._cfg, 'bittorrent', None), 'timeout', 60.0)
        self.bt_timeout = timeout
        
        logger.info(f"[BT] Waiting for {self.client_num} clients to complete chunk exchange (timeout: {timeout}s)")
        
        # 🔥 Key modification: Synchronously wait for all clients to complete BitTorrent
        self._wait_for_all_bittorrent_completion()
        
        return True

    def _wait_for_all_bittorrent_completion(self):
        """
        🔥 Added: Synchronously wait for all clients to complete BitTorrent exchange
        """
        start_time = time.time()
        check_interval = 2.0  # Check every 2 seconds
        last_completed_clients = -1  # Track number of clients completed last time
        
        logger.info(f"[BT] Starting synchronous wait for all {self.client_num} clients to complete BitTorrent exchange")
        
        while True:
            # Check if all clients have completed
            completed_clients = len(self.bittorrent_completion_status)
            if completed_clients >= self.client_num:
                total_time = time.time() - start_time
                logger.info(f"[BT] ✅ All {self.client_num} clients completed BitTorrent exchange in {total_time:.2f}s")
                self.bt_state = 'COMPLETED'
                break
            
            # Check timeout
            elapsed_time = time.time() - start_time
            if elapsed_time > self.bt_timeout:
                logger.warning(f"[BT] ⏰ BitTorrent exchange timeout after {self.bt_timeout}s")
                logger.warning(f"[BT] Only {completed_clients}/{self.client_num} clients completed")
                self.bt_state = 'TIMEOUT'
                break
            
            # Only print progress when number of completed clients changes
            if completed_clients != last_completed_clients:
                logger.info(f"[BT] 🕰️ BitTorrent progress: {completed_clients}/{self.client_num} clients completed, {elapsed_time:.1f}s elapsed")
                last_completed_clients = completed_clients
            
            # Handle messages during waiting period
            try:
                # Use non-blocking way to check message queue
                if hasattr(self.comm_manager, 'server_funcs') and \
                   hasattr(self.comm_manager.server_funcs, 'msg_queue') and \
                   len(self.comm_manager.server_funcs.msg_queue) > 0:
                    
                    # Process messages
                    msg = self.comm_manager.receive()
                    if msg:
                        logger.debug(f"[BT] Processing message type '{msg.msg_type}' from client {msg.sender} during BitTorrent wait")
                        
                        # Handle BitTorrent-related messages
                        if msg.msg_type in self.msg_handlers:
                            self.msg_handlers[msg.msg_type](msg)
                        else:
                            logger.warning(f"[BT] Unknown message type during BitTorrent wait: {msg.msg_type}")
                else:
                    # No messages, sleep briefly
                    time.sleep(check_interval)
                        
            except Exception as e:
                logger.debug(f"[BT] Message handling error during BitTorrent wait: {e}")
                time.sleep(check_interval)
        
        # Record final result
        if self.bt_state == 'COMPLETED':
            total_chunks = sum(self.bittorrent_completion_status.values())
            logger.info(f"[BT] 🎉 BitTorrent exchange completed successfully! Total chunks collected: {total_chunks}")
        elif self.bt_state == 'TIMEOUT':
            logger.warning(f"[BT] ⚠️ BitTorrent exchange incomplete due to timeout")

    def callback_funcs_for_bittorrent_complete(self, message):
        """
        🔧 Modified: Simplified BitTorrent completion message handling (with synchronous wait mechanism)
        """
        sender_id = message.sender
        chunks_collected = message.content.get('chunks_collected', 0)
        exchange_time = message.content.get('exchange_time', 0)
        status = message.content.get('status', 'completed')
        
        # Handle byte counts (now sent as strings to avoid int32 overflow)
        bytes_downloaded_str = message.content.get('bytes_downloaded_str', '0')
        bytes_uploaded_str = message.content.get('bytes_uploaded_str', '0')
        try:
            bytes_downloaded = int(bytes_downloaded_str)
            bytes_uploaded = int(bytes_uploaded_str)
        except (ValueError, TypeError):
            bytes_downloaded = 0
            bytes_uploaded = 0
        
        # Record completion status
        if hasattr(self, 'bittorrent_completion_status'):
            self.bittorrent_completion_status[sender_id] = chunks_collected
            
            if status == 'failed':
                logger.error(f"[BT] ❌ Client {sender_id} failed BitTorrent exchange")
            else:
                # Display transfer stats in readable format
                mb_downloaded = bytes_downloaded / (1024 * 1024)
                mb_uploaded = bytes_uploaded / (1024 * 1024)
                logger.info(f"[BT] ✅ Client {sender_id} completed: {chunks_collected} chunks in {exchange_time:.2f}s - Downloaded: {bytes_downloaded} bytes, Uploaded: {bytes_uploaded} bytes")
                
            # Print current progress
            completed_count = len(self.bittorrent_completion_status)
            logger.info(f"[BT] Progress: {completed_count}/{self.client_num} clients completed BitTorrent exchange")
            
        else:
            logger.warning(f"[BT] Received unexpected bittorrent_complete from client {sender_id}")
            
        # No longer need check_bittorrent_state() as synchronous wait handles completion checking

    def check_bittorrent_state(self):
        """
        🆕 Added: Check BitTorrent state machine
        Called in message processing loop to avoid blocking
        """
        if not hasattr(self, 'bt_state') or self.bt_state != 'EXCHANGING':
            return
        
        # Check timeout
        if hasattr(self, 'bt_timeout') and self.bt_timeout > 0:
            if time.time() - self.bt_start_time > self.bt_timeout:
                logger.warning(f"[BT] BitTorrent exchange timeout after {self.bt_timeout}s")
                self._handle_bittorrent_timeout()
                return
        
        # Check if all clients completed
        if len(self.bittorrent_completion_status) >= self.client_num:
            self._handle_bittorrent_completion()
        elif len(self.bittorrent_completion_status) >= self.client_num * 0.8:
            # Optional: 80% completion can continue
            remaining_time = self.bt_timeout - (time.time() - self.bt_start_time)
            if remaining_time < 5.0:  # Only 5 seconds left
                logger.info("[BT] 80% clients completed, proceeding...")
                self._handle_bittorrent_completion()

    def _handle_bittorrent_completion(self):
        """
        🆕 Handle BitTorrent completion
        """
        self.bt_state = 'COMPLETED'
        
        # Calculate success rate
        success_count = sum(1 for status in self.bittorrent_completion_status.values() 
                           if status >= self.bt_expected_chunks)
        
        if success_count == self.client_num:
            logger.info("[BT] All clients successfully collected all chunks")
        else:
            logger.warning(f"[BT] {success_count}/{self.client_num} clients collected all chunks")
        
        # Clean up state
        self.bittorrent_completion_status.clear()
        
        logger.info("[BT] BitTorrent exchange phase completed successfully")

    def _handle_bittorrent_timeout(self):
        """
        🆕 Handle BitTorrent timeout
        """
        self.bt_state = 'TIMEOUT'
        
        completed = len(self.bittorrent_completion_status)
        logger.error(f"[BT] Timeout with {completed}/{self.client_num} clients completed")
        
        # Decide whether to continue based on completion status
        min_ratio = getattr(getattr(self._cfg, 'bittorrent', None), 'min_completion_ratio', 0.8)
        if completed >= self.client_num * min_ratio:
            logger.info(f"[BT] {completed} clients completed, continuing...")
            self._handle_bittorrent_completion()
        else:
            logger.error("[BT] Too few clients completed, aborting BitTorrent")
            self.bt_state = 'FAILED'
            logger.info("[BT] Proceeding with FL training despite BitTorrent failure")

    def check_bittorrent_completion(self):
        """Check if all clients have completed chunk collection"""
        if not hasattr(self, 'bittorrent_completion_status'):
            return False
            
        if len(self.bittorrent_completion_status) < self.client_num:
            return False
            
        # Check if each client has collected all n*m chunks
        expected_chunks = self.client_num * getattr(getattr(self._cfg, 'chunk', None), 'num_chunks', 10)
        for client_id, chunks_collected in self.bittorrent_completion_status.items():
            if chunks_collected < expected_chunks:
                return False
                
        return True

    # ================== BitTorrent Protocol Message Handlers ==================
    
    def callback_funcs_for_bitfield(self, message):
        """Handle BitTorrent bitfield messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received bitfield from client {message.sender}")
        return False  # Don't stop the message processing loop
    
    def callback_funcs_for_interested(self, message):
        """Handle BitTorrent interested messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received interested from client {message.sender}")
        return False
    
    def callback_funcs_for_unchoke(self, message):
        """Handle BitTorrent unchoke messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received unchoke from client {message.sender}")
        return False
    
    def callback_funcs_for_choke(self, message):
        """Handle BitTorrent choke messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received choke from client {message.sender}")
        return False
    
    def callback_funcs_for_have(self, message):
        """Handle BitTorrent have messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received have from client {message.sender}")
        return False
    
    def callback_funcs_for_request(self, message):
        """Handle BitTorrent request messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received request from client {message.sender}")
        return False
    
    def callback_funcs_for_piece(self, message):
        """Handle BitTorrent piece messages - server just logs and ignores"""
        logger.debug(f"[BT] Server received piece from client {message.sender}")
        return False

    @classmethod
    def get_msg_handler_dict(cls):
        return cls().msg_handlers_str
