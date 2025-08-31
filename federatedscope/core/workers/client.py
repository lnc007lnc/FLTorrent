import copy
import logging
import sys
import pickle
import time

from federatedscope.core.message import Message
from federatedscope.core.communication import StandaloneCommManager, \
    StandaloneDDPCommManager, gRPCCommManager
from federatedscope.core.connection_monitor import ConnectionMonitor, ConnectionEvent
from federatedscope.core.monitors.early_stopper import EarlyStopper
from federatedscope.core.auxiliaries.trainer_builder import get_trainer
from federatedscope.core.secret_sharing import AdditiveSecretSharing
from federatedscope.core.auxiliaries.utils import merge_dict_of_results, \
    calculate_time_cost
from federatedscope.core.workers.base_client import BaseClient
from federatedscope.core.chunk_tracker import ChunkInfo

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Client(BaseClient):
    """
    The Client class, which describes the behaviors of client in an FL \
    course. The behaviors are described by the handling functions (named as \
    ``callback_funcs_for_xxx``)

    Arguments:
        ID: The unique ID of the client, which is assigned by the server
        when joining the FL course
        server_id: (Default) 0
        state: The training round
        config: The configuration
        data: The data owned by the client
        model: The model maintained locally
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
        ss_manager: secret sharing manager
        msg_buffer: dict buffer for storing message
        comm_manager: manager for communication, \
            see ``federatedscope.core.communication``
    """
    def __init__(self,
                 ID=-1,
                 server_id=None,
                 state=-1,
                 config=None,
                 data=None,
                 model=None,
                 device='cpu',
                 strategy=None,
                 is_unseen_client=False,
                 *args,
                 **kwargs):
        super(Client, self).__init__(ID, state, config, model, strategy)

        self.data = data

        # Register message handlers
        self._register_default_handlers()
        
        # Initialize buffer for early BitTorrent messages
        self.bt_message_buffer = []

        # Un-configured worker
        if config is None:
            return

        # the unseen_client indicates that whether this client contributes to
        # FL process by training on its local data and uploading the local
        # model update, which is useful for check the participation
        # generalization gap in
        # [ICLR'22, What Do We Mean by Generalization in Federated Learning?]
        self.is_unseen_client = is_unseen_client

        # Parse the attack_id since we support both 'int' (for single attack)
        # and 'list' (for multiple attacks) for config.attack.attack_id
        parsed_attack_ids = list()
        if isinstance(config.attack.attacker_id, int):
            parsed_attack_ids.append(config.attack.attacker_id)
        elif isinstance(config.attack.attacker_id, list):
            parsed_attack_ids = config.attack.attacker_id
        else:
            raise TypeError(f"The expected types of config.attack.attack_id "
                            f"include 'int' and 'list', but we got "
                            f"{type(config.attack.attacker_id)}")

        # Attack only support the stand alone model;
        # Check if is a attacker; a client is a attacker if the
        # config.attack.attack_method is provided
        self.is_attacker = ID in parsed_attack_ids and \
            config.attack.attack_method != '' and \
            config.federate.mode == 'standalone'

        # Build Trainer
        # trainer might need configurations other than those of trainer node
        self.trainer = get_trainer(model=model,
                                   data=data,
                                   device=device,
                                   config=self._cfg,
                                   is_attacker=self.is_attacker,
                                   monitor=self._monitor)
        self.device = device

        # For client-side evaluation
        self.best_results = dict()
        self.history_results = dict()
        # in local or global training mode, we do use the early stopper.
        # Otherwise, we set patience=0 to deactivate the local early-stopper
        patience = self._cfg.early_stop.patience if \
            self._cfg.federate.method in [
                "local", "global"
            ] else 0
        self.early_stopper = EarlyStopper(
            patience, self._cfg.early_stop.delta,
            self._cfg.early_stop.improve_indicator_mode,
            self._monitor.the_larger_the_better)

        # Secret Sharing Manager and message buffer
        self.ss_manager = AdditiveSecretSharing(
            shared_party_num=int(self._cfg.federate.sample_client_num
                                 )) if self._cfg.federate.use_ss else None
        self.msg_buffer = {'train': dict(), 'eval': dict()}

        # Communication and communication ability
        if 'resource_info' in kwargs and kwargs['resource_info'] is not None:
            self.comp_speed = float(
                kwargs['resource_info']['computation']) / 1000.  # (s/sample)
            self.comm_bandwidth = float(
                kwargs['resource_info']['communication'])  # (kbit/s)
        else:
            self.comp_speed = None
            self.comm_bandwidth = None

        if self._cfg.backend == 'torch':
            self.model_size = sys.getsizeof(pickle.dumps(
                self.model)) / 1024.0 * 8.  # kbits
        else:
            # TODO: calculate model size for TF Model
            self.model_size = 1.0
            logger.warning(f'The calculation of model size in backend:'
                           f'{self._cfg.backend} is not provided.')

        # Initialize communication manager
        self.server_id = server_id
        if self.mode == 'standalone':
            comm_queue = kwargs['shared_comm_queue']
            if self._cfg.federate.process_num <= 1:
                self.comm_manager = StandaloneCommManager(
                    comm_queue=comm_queue, monitor=self._monitor)
            else:
                self.comm_manager = StandaloneDDPCommManager(
                    comm_queue=comm_queue, monitor=self._monitor)
            self.local_address = None
        elif self.mode == 'distributed':
            host = kwargs['host']
            port = kwargs['port']
            server_host = kwargs['server_host']
            server_port = kwargs['server_port']
            self.comm_manager = gRPCCommManager(
                host=host,
                port=port,
                client_num=self._cfg.federate.client_num,
                cfg=self._cfg.distribute)
            logger.info('Client: Listen to {}:{}...'.format(host, port))
            self.comm_manager.add_neighbors(neighbor_id=server_id,
                                            address={
                                                'host': server_host,
                                                'port': server_port
                                            })
            self.local_address = {
                'host': self.comm_manager.host,
                'port': self.comm_manager.port
            }
            
        # Initialize connection monitor
        self.connection_monitor = ConnectionMonitor(
            client_id=self.ID,
            comm_manager=self.comm_manager,
            server_id=self.server_id or 0
        )
        
        # Link connection monitor to communication manager
        if hasattr(self.comm_manager, 'connection_monitor'):
            self.comm_manager.connection_monitor = self.connection_monitor
            
        # ============ 🚀 gRPC Streaming Channel Manager ============
        self.topology_neighbors = []  # Store topology neighbor information
        self.neighbor_addresses = {}  # Store neighbor address information
        
        try:
            from federatedscope.core.streaming_channel_manager import StreamingChannelManager
            self._streaming_manager = StreamingChannelManager(self.ID, client_instance=self)
            logger.info(f"[StreamingManager] Initialized streaming channel manager for client {self.ID}")
        except Exception as e:
            logger.warning(f"[StreamingManager] Failed to initialize streaming manager: {e}")
            self._streaming_manager = None
        
        # Start connection monitoring for distributed mode
        if self.mode == 'distributed':
            self.connection_monitor.start_monitoring()
            # Report initial connection establishment
            self.connection_monitor.report_connection_established(
                peer_id=self.server_id or 0,
                details={
                    'client_address': self.local_address,
                    'server_address': {'host': server_host, 'port': server_port} if 'server_host' in locals() else None,
                    'initialization': True
                }
            )
            
    def cleanup(self):
        """Clean up client resources and report disconnection"""
        if hasattr(self, 'connection_monitor') and self.connection_monitor:
            # Report disconnection before cleanup
            self.connection_monitor.report_connection_lost(
                peer_id=self.server_id or 0,
                details={
                    'reason': 'client_shutdown',
                    'clean_shutdown': True
                }
            )
            # Stop monitoring
            self.connection_monitor.stop_monitoring()
            
    def __del__(self):
        """Destructor - ensure cleanup is called"""
        try:
            self.cleanup()
        except:
            pass  # Ignore errors during destruction

    def _gen_timestamp(self, init_timestamp, instance_number):
        if init_timestamp is None:
            return None

        comp_cost, comm_cost = calculate_time_cost(
            instance_number=instance_number,
            comm_size=self.model_size,
            comp_speed=self.comp_speed,
            comm_bandwidth=self.comm_bandwidth)
        return init_timestamp + comp_cost + comm_cost

    def _calculate_model_delta(self, init_model, updated_model):
        if not isinstance(init_model, list):
            init_model = [init_model]
            updated_model = [updated_model]

        model_deltas = list()
        for model_index in range(len(init_model)):
            model_delta = copy.deepcopy(updated_model[model_index])
            for key in init_model[model_index].keys():
                if key not in updated_model[model_index].keys():
                    continue
                model_delta[key] = updated_model[model_index][
                    key] - init_model[model_index][key]
            model_deltas.append(model_delta)

        if len(model_deltas) > 1:
            return model_deltas
        else:
            return model_deltas[0]

    def join_in(self):
        """
        To send ``join_in`` message to the server for joining in the FL course.
        """
        self.comm_manager.send(
            Message(msg_type='join_in',
                    sender=self.ID,
                    receiver=[self.server_id],
                    timestamp=0,
                    content=self.local_address))

    def run(self):
        """
        To listen to the message and handle them accordingly (used for \
        distributed mode)
        """
        while True:
            msg = self.comm_manager.receive()
            if self.state <= msg.state:
                self.msg_handlers[msg.msg_type](msg)

            if msg.msg_type == 'finish':
                break

    def run_standalone(self):
        """
        Run in standalone mode
        """
        self.join_in()
        self.run()

    def callback_funcs_for_model_para(self, message: Message):
        """
        The handling function for receiving training signals from server.
        Modified for BitTorrent integration: aggregates from chunk database
        instead of receiving model weights directly.

        Arguments:
            message: The received message (no longer contains model weights)
        """
        if 'ss' in message.msg_type:
            # A fragment of the shared secret
            state, content, timestamp = message.state, message.content, \
                                        message.timestamp
            self.msg_buffer['train'][state].append(content)

            if len(self.msg_buffer['train']
                   [state]) == self._cfg.federate.client_num:
                # Check whether the received fragments are enough
                model_list = self.msg_buffer['train'][state]
                sample_size, first_aggregate_model_para = model_list[0]
                single_model_case = True
                if isinstance(first_aggregate_model_para, list):
                    assert isinstance(first_aggregate_model_para[0], dict), \
                        "aggregate_model_para should a list of multiple " \
                        "state_dict for multiple models"
                    single_model_case = False
                else:
                    assert isinstance(first_aggregate_model_para, dict), \
                        "aggregate_model_para should " \
                        "a state_dict for single model case"
                    first_aggregate_model_para = [first_aggregate_model_para]
                    model_list = [[model] for model in model_list]

                for sub_model_idx, aggregate_single_model_para in enumerate(
                        first_aggregate_model_para):
                    for key in aggregate_single_model_para:
                        for i in range(1, len(model_list)):
                            aggregate_single_model_para[key] += model_list[i][
                                sub_model_idx][key]

                # Send training completion signal instead of model weights
                # BitTorrent P2P system handles model parameter exchange
                sync_message_content = {
                    'client_id': self.ID,
                    'round_num': self.state,
                    'sample_size': sample_size,
                    'training_completed': True,
                    'skip_weights': True  # Signal that weights are handled by BitTorrent
                }
                
                logger.info(f"[BT-FL] Client {self.ID}: Sending training completion signal for round {self.state} with {sample_size} samples")
                
                self.comm_manager.send(
                    Message(msg_type='model_para',
                            sender=self.ID,
                            receiver=[self.server_id],
                            state=self.state,
                            timestamp=timestamp,
                            content=sync_message_content))

        else:
            round = message.state
            sender = message.sender
            timestamp = message.timestamp
            content = message.content
            
            logger.info(f"[BT-FL] Client {self.ID}: Received training signal for round {round}")
            
            # 🔧 CRITICAL FIX: Stop BitTorrent exchange when receiving new model_para message
            # This prevents interference with next round BitTorrent exchange
            if hasattr(self, 'bt_manager') and self.bt_manager is not None:
                logger.info(f"[BT-FL] Client {self.ID}: Stopping previous BitTorrent exchange before processing new round {round}")
                self.bt_manager.stop_exchange()
                # 🆕 FIX: Don't immediately set to None - let exchange loop check is_stopped flag first
                # The exchange loop will exit gracefully when it sees bt_manager.is_stopped = True
                # We'll set bt_manager = None later in the loop cleanup
            
            # Check if this is a BitTorrent-enabled message
            if isinstance(content, dict) and content.get('skip_weights', False):
                logger.info(f"[BT-FL] Client {self.ID}: Aggregating from chunk database for round {round} (using previous round {round-1} data)")
                
                # For round 0, initialize with random/default model since no prior chunks exist
                if round == 0:
                    logger.info(f"[BT-FL] Client {self.ID}: Round 0 - using default model initialization, no aggregation needed")
                    # No model update needed, use current random initialization
                else:
                    # Aggregate model from chunk database for previous round (round - 1)
                    # This is because we aggregate the results from the previous training round
                    aggregated_model = self._aggregate_model_from_chunks(round - 1)
                    
                    if aggregated_model is not None:
                        # Update model with aggregated weights
                        self.trainer.update(aggregated_model,
                                            strict=self._cfg.federate.share_local_model)
                        logger.info(f"[BT-FL] Client {self.ID}: Successfully updated model from chunk aggregation")
                    else:
                        logger.warning(f"[BT-FL] Client {self.ID}: Failed to aggregate model from chunks, keeping current model")
                        # Continue with current model instead of failing
            else:
                # Legacy mode: handle traditional model parameter broadcast
                logger.warning(f"[BT-FL] Client {self.ID}: Received traditional model parameters, falling back to legacy mode")
                
                # dequantization
                if self._cfg.quantization.method == 'uniform':
                    from federatedscope.core.compression import \
                        symmetric_uniform_dequantization
                    if isinstance(content, list):  # multiple model
                        content = [
                            symmetric_uniform_dequantization(x) for x in content
                        ]
                    else:
                        content = symmetric_uniform_dequantization(content)

                # When clients share the local model, we must set strict=True
                if self._cfg.federate.process_num > 1:
                    for k, v in content.items():
                        content[k] = v.to(self.device)
                self.trainer.update(content,
                                    strict=self._cfg.federate.share_local_model)
            
            self.state = round
            skip_train_isolated_or_global_mode = \
                self.early_stopper.early_stopped and \
                self._cfg.federate.method in ["local", "global"]
            if self.is_unseen_client or skip_train_isolated_or_global_mode:
                # for these cases (1) unseen client (2) isolated_global_mode,
                # we do not local train and upload local model
                sample_size, model_para_all, results = \
                    0, self.trainer.get_model_para(), {}
                if skip_train_isolated_or_global_mode:
                    logger.info(
                        f"[Local/Global mode] Client #{self.ID} has been "
                        f"early stopped, we will skip the local training")
                    self._monitor.local_converged()
            else:
                if self.early_stopper.early_stopped and \
                        self._monitor.local_convergence_round == 0:
                    logger.info(
                        f"[Normal FL Mode] Client #{self.ID} has been locally "
                        f"early stopped. "
                        f"The next FL update may result in negative effect")
                    self._monitor.local_converged()
                sample_size, model_para_all, results = self.trainer.train()
                if self._cfg.federate.share_local_model and not \
                        self._cfg.federate.online_aggr:
                    model_para_all = copy.deepcopy(model_para_all)
                train_log_res = self._monitor.format_eval_res(
                    results,
                    rnd=self.state,
                    role='Client #{}'.format(self.ID),
                    return_raw=True)
                logger.info(train_log_res)
                if self._cfg.wandb.use and self._cfg.wandb.client_train_info:
                    self._monitor.save_formatted_results(train_log_res,
                                                         save_file_name="")

            # Return the feedbacks to the server after local update
            if self._cfg.federate.use_ss:
                assert not self.is_unseen_client, \
                    "Un-support using secret sharing for unseen clients." \
                    "i.e., you set cfg.federate.use_ss=True and " \
                    "cfg.federate.unseen_clients_rate in (0, 1)"
                single_model_case = True
                if isinstance(model_para_all, list):
                    assert isinstance(model_para_all[0], dict), \
                        "model_para should a list of " \
                        "multiple state_dict for multiple models"
                    single_model_case = False
                else:
                    assert isinstance(model_para_all, dict), \
                        "model_para should a state_dict for single model case"
                    model_para_all = [model_para_all]
                model_para_list_all = []
                for model_para in model_para_all:
                    for key in model_para:
                        model_para[key] = model_para[key] * sample_size
                    model_para_list = self.ss_manager.secret_split(model_para)
                    model_para_list_all.append(model_para_list)
                frame_idx = 0
                for neighbor in self.comm_manager.neighbors:
                    if neighbor != self.server_id:
                        content_frame = model_para_list_all[0][frame_idx] if \
                            single_model_case else \
                            [model_para_list[frame_idx] for model_para_list
                             in model_para_list_all]
                        self.comm_manager.send(
                            Message(msg_type='ss_model_para',
                                    sender=self.ID,
                                    receiver=[neighbor],
                                    state=self.state,
                                    timestamp=self._gen_timestamp(
                                        init_timestamp=timestamp,
                                        instance_number=sample_size),
                                    content=content_frame))
                        frame_idx += 1
                content_frame = model_para_list_all[0][frame_idx] if \
                    single_model_case else \
                    [model_para_list[frame_idx] for model_para_list in
                     model_para_list_all]
                self.msg_buffer['train'][self.state] = [(sample_size,
                                                         content_frame)]
            else:
                if self._cfg.asyn.use or self._cfg.aggregator.robust_rule in \
                        ['krum', 'normbounding', 'median', 'trimmedmean',
                         'bulyan']:
                    # Return the model delta when using asynchronous training
                    # protocol, because the staled updated might be discounted
                    # and cause that the sum of the aggregated weights might
                    # not be equal to 1
                    shared_model_para = self._calculate_model_delta(
                        init_model=content, updated_model=model_para_all)
                else:
                    shared_model_para = model_para_all

                # quantization
                if self._cfg.quantization.method == 'uniform':
                    from federatedscope.core.compression import \
                        symmetric_uniform_quantization
                    nbits = self._cfg.quantization.nbits
                    if isinstance(shared_model_para, list):
                        shared_model_para = [
                            symmetric_uniform_quantization(x, nbits)
                            for x in shared_model_para
                        ]
                    else:
                        shared_model_para = symmetric_uniform_quantization(
                            shared_model_para, nbits)
                
                # Save local model after training completion
                self._save_local_model_after_training()
                
                # Send training completion signal instead of model weights
                # BitTorrent P2P system handles model parameter exchange  
                sync_message_content = {
                    'client_id': self.ID,
                    'round_num': self.state,
                    'sample_size': sample_size,
                    'training_completed': True,
                    'skip_weights': True  # Signal that weights are handled by BitTorrent
                }
                
                logger.info(f"[BT-FL] Client {self.ID}: Sending training completion signal for round {self.state} with {sample_size} samples")
                
                self.comm_manager.send(
                    Message(msg_type='model_para',
                            sender=self.ID,
                            receiver=[sender],
                            state=self.state,
                            timestamp=self._gen_timestamp(
                                init_timestamp=timestamp,
                                instance_number=sample_size),
                            content=sync_message_content))

    def callback_funcs_for_assign_id(self, message: Message):
        """
        The handling function for receiving the client_ID assigned by the \
        server (during the joining process), which is used in the \
        distributed mode.

        Arguments:
            message: The received message
        """
        content = message.content
        self.ID = int(content)
        logger.info('Client (address {}:{}) is assigned with #{:d}.'.format(
            self.comm_manager.host, self.comm_manager.port, self.ID))
        
        # Update connection monitor with correct client ID
        if hasattr(self, 'connection_monitor') and self.connection_monitor:
            self.connection_monitor.client_id = self.ID
            
        # 🔧 CRITICAL FIX: Update streaming manager with correct client ID
        if hasattr(self, '_streaming_manager') and self._streaming_manager:
            self._streaming_manager.client_id = self.ID
            
        # 🚀 CRITICAL FIX: Set chunk_manager reference to gRPC server for chunk data access
        if hasattr(self, 'comm_manager') and hasattr(self.comm_manager, 'server_funcs'):
            # Make sure chunk_manager is initialized
            if not hasattr(self, 'chunk_manager'):
                from federatedscope.core.chunk_manager import ChunkManager
                self.chunk_manager = ChunkManager(
                    client_id=self.ID,
                    change_callback=self._send_chunk_info_to_server
                )
                logger.info(f"[Client {self.ID}] Initialized chunk_manager for chunk data access")
            
            # Set chunk_manager reference to gRPC server
            self.comm_manager.server_funcs.chunk_manager = self.chunk_manager
            logger.info(f"[Client {self.ID}] ✅ Set chunk_manager reference to gRPC server for chunk data access")
            # Also update client_id for all existing channels
            for peer_id, channel in self._streaming_manager.channels.items():
                channel.client_id = self.ID
            logger.info(f"[StreamingManager] Updated client_id from -1 to {self.ID} for streaming channels")
        
        # Send initial chunk information to server
        self.send_initial_chunk_info()

    def callback_funcs_for_join_in_info(self, message: Message):
        """
        The handling function for receiving the request of join in \
        information (such as ``batch_size``, ``num_of_samples``) during \
        the joining process.

        Arguments:
            message: The received message
        """
        requirements = message.content
        timestamp = message.timestamp
        join_in_info = dict()
        for requirement in requirements:
            if requirement.lower() == 'num_sample':
                if self._cfg.train.batch_or_epoch == 'batch':
                    num_sample = self._cfg.train.local_update_steps * \
                                 self._cfg.dataloader.batch_size
                else:
                    num_sample = self._cfg.train.local_update_steps * \
                                 len(self.trainer.data.train_data)
                join_in_info['num_sample'] = num_sample
                if self._cfg.trainer.type == 'nodefullbatch_trainer':
                    join_in_info['num_sample'] = \
                        self.trainer.data.train_data.x.shape[0]
            elif requirement.lower() == 'client_resource':
                assert self.comm_bandwidth is not None and self.comp_speed \
                       is not None, "The requirement join_in_info " \
                                    "'client_resource' does not exist."
                join_in_info['client_resource'] = self.model_size / \
                    self.comm_bandwidth + self.comp_speed
            else:
                raise ValueError(
                    'Fail to get the join in information with type {}'.format(
                        requirement))
        self.comm_manager.send(
            Message(msg_type='join_in_info',
                    sender=self.ID,
                    receiver=[self.server_id],
                    state=self.state,
                    timestamp=timestamp,
                    content=join_in_info))

    def callback_funcs_for_address(self, message: Message):
        """
        The handling function for receiving other clients' IP addresses, \
        which is used for constructing a complex topology

        Arguments:
            message: The received message
        """
        content = message.content
        for neighbor_id, address in content.items():
            if int(neighbor_id) != self.ID:
                self.comm_manager.add_neighbors(neighbor_id, address)

    def callback_funcs_for_evaluate(self, message: Message):
        """
        The handling function for receiving the request of evaluating

        Arguments:
            message: The received message
        """
        sender, timestamp = message.sender, message.timestamp
        self.state = message.state
        if message.content is not None:
            self.trainer.update(message.content,
                                strict=self._cfg.federate.share_local_model)
        if self.early_stopper.early_stopped and self._cfg.federate.method in [
                "local", "global"
        ]:
            metrics = list(self.best_results.values())[0]
        else:
            metrics = {}
            if self._cfg.finetune.before_eval:
                self.trainer.finetune()
            for split in self._cfg.eval.split:
                # TODO: The time cost of evaluation is not considered here
                eval_metrics = self.trainer.evaluate(
                    target_data_split_name=split)

                if self._cfg.federate.mode == 'distributed':
                    logger.info(
                        self._monitor.format_eval_res(eval_metrics,
                                                      rnd=self.state,
                                                      role='Client #{}'.format(
                                                          self.ID),
                                                      return_raw=True))

                metrics.update(**eval_metrics)

            formatted_eval_res = self._monitor.format_eval_res(
                metrics,
                rnd=self.state,
                role='Client #{}'.format(self.ID),
                forms=['raw'],
                return_raw=True)
            self._monitor.update_best_result(self.best_results,
                                             formatted_eval_res['Results_raw'],
                                             results_type=f"client #{self.ID}")
            self.history_results = merge_dict_of_results(
                self.history_results, formatted_eval_res['Results_raw'])
            self.early_stopper.track_and_check(self.history_results[
                self._cfg.eval.best_res_update_round_wise_key])

        self.comm_manager.send(
            Message(msg_type='metrics',
                    sender=self.ID,
                    receiver=[sender],
                    state=self.state,
                    timestamp=timestamp,
                    content=metrics))

    def callback_funcs_for_finish(self, message: Message):
        """
        The handling function for receiving the signal of finishing the FL \
        course.

        Arguments:
            message: The received message
        """
        logger.info(
            f"================= client {self.ID} received finish message "
            f"=================")

        if message.content is not None:
            self.trainer.update(message.content,
                                strict=self._cfg.federate.share_local_model)

        self._monitor.finish_fl()

    def callback_funcs_for_converged(self, message: Message):
        """
        The handling function for receiving the signal that the FL course \
        converged

        Arguments:
            message: The received message
        """
        self._monitor.global_converged()

    def callback_funcs_for_topology_instruction(self, message: Message):
        """
        The handling function for receiving topology construction instructions
        from the server, which tells the client which neighbors to connect to

        Arguments:
            message: The received message containing neighbor connection list
        """
        try:
            content = message.content
            neighbors_to_connect = content.get('neighbors_to_connect', [])
            neighbor_addresses = content.get('neighbor_addresses', {})  # 🔧 Get real address information
            topology_type = content.get('topology_type', 'unknown')
            max_attempts = content.get('max_attempts', 3)
            retry_delay = content.get('retry_delay', 2.0)
            
            logger.info(f"🌐 Client {self.ID}: Received topology instruction")
            logger.info(f"   Topology type: {topology_type}")
            logger.info(f"   Neighbors to connect: {neighbors_to_connect}")
            logger.info(f"   Neighbor addresses: {neighbor_addresses}")
            
            # 🔧 BitTorrent fix: Save topology neighbor information for BitTorrent use
            self.topology_neighbors = neighbors_to_connect
            self.neighbor_addresses = neighbor_addresses  # 🔧 Save address information
            logger.info(f"[BT] Client {self.ID}: Saved topology neighbors for BitTorrent: {self.topology_neighbors}")
            
            # 🚀 STREAMING OPTIMIZATION: Asynchronously create streaming channels, don't block topology construction
            if hasattr(self, '_streaming_manager') and self._streaming_manager:
                logger.info(f"[StreamingTopology] Client {self.ID}: Starting async streaming channel creation")
                
                # Build neighbor address mapping: {peer_id: (host, port)}
                streaming_addresses = {}
                for neighbor_id in neighbors_to_connect:
                    if neighbor_id in neighbor_addresses:
                        addr_info = neighbor_addresses[neighbor_id]
                        if isinstance(addr_info, dict) and 'host' in addr_info and 'port' in addr_info:
                            streaming_addresses[neighbor_id] = (addr_info['host'], addr_info['port'])
                        elif isinstance(addr_info, (list, tuple)) and len(addr_info) >= 2:
                            streaming_addresses[neighbor_id] = (addr_info[0], addr_info[1])
                
                if streaming_addresses:
                    # 🔧 Critical fix: Create streaming channels in separate thread, avoid blocking topology construction
                    import threading
                    def create_streaming_channels_async():
                        try:
                            logger.info(f"[StreamingTopology] Client {self.ID}: Creating streaming channels in background thread")
                            self._streaming_manager.create_channels_for_topology(streaming_addresses)
                            logger.info(f"[StreamingTopology] Client {self.ID}: Successfully created streaming channels for {len(streaming_addresses)} neighbors")
                        except Exception as e:
                            logger.warning(f"[StreamingTopology] Client {self.ID}: Streaming channel creation failed: {e}")
                    
                    streaming_thread = threading.Thread(
                        target=create_streaming_channels_async,
                        daemon=True,
                        name=f"StreamingSetup-{self.ID}"
                    )
                    streaming_thread.start()
                    logger.info(f"[StreamingTopology] Client {self.ID}: Started streaming setup in background, continuing with topology construction")
                else:
                    logger.warning(f"[StreamingTopology] Client {self.ID}: No valid neighbor addresses for streaming channels")
            else:
                logger.debug(f"[StreamingTopology] Client {self.ID}: Streaming manager not available")
            
            if not neighbors_to_connect:
                logger.info(f"   No neighbors to connect for Client {self.ID}")
                return
            
            # Establish connections to each neighbor using real addresses
            for neighbor_id in neighbors_to_connect:
                neighbor_addr = neighbor_addresses.get(neighbor_id)
                if neighbor_addr:
                    self._connect_to_neighbor_with_address(neighbor_id, neighbor_addr, max_attempts, retry_delay)
                else:
                    logger.error(f"❌ Client {self.ID}: No address provided for neighbor {neighbor_id}")
                    logger.error(f"❌ Client {self.ID}: Cannot establish connection without proper address")
                
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Error processing topology instruction: {e}")

    def callback_funcs_for_connection_ack(self, message: Message):
        """
        Handle connection acknowledgment messages from server
        Arguments:
            message: The received acknowledgment message
        """
        try:
            content = message.content
            ack_timestamp = content.get('ack_timestamp', 0)
            original_event = content.get('original_event', 'unknown')
            status = content.get('status', 'unknown')
            
            logger.debug(f"📨 Client {self.ID}: Received connection ack for {original_event} (status: {status})")
            
        except Exception as e:
            logger.warning(f"⚠️ Client {self.ID}: Error processing connection ack: {e}")

    def _connect_to_neighbor_with_address(self, neighbor_id, neighbor_address, max_attempts=3, retry_delay=2.0):
        """
        Attempt to connect to a specific neighbor client using provided address
        
        Args:
            neighbor_id: ID of the neighbor to connect to
            neighbor_address: Dict with 'host' and 'port' keys for the neighbor's address
            max_attempts: Maximum connection attempts
            retry_delay: Delay between attempts
        """
        import time
        
        logger.info(f"🔗 Client {self.ID}: Attempting to connect to Client {neighbor_id} at {neighbor_address}")
        
        for attempt in range(1, max_attempts + 1):
            try:
                # 🔧 Critical fix: Use real address provided by server instead of calculated address
                success = self._add_peer_to_comm_manager(neighbor_id, neighbor_address)
                
                if success:
                    # Send connection confirmation to server
                    self._send_connection_established_message(neighbor_id)
                    logger.info(f"✅ Client {self.ID}: Successfully connected to Client {neighbor_id} (attempt {attempt})")
                    return
                else:
                    logger.warning(f"⚠️ Client {self.ID}: Connection attempt {attempt} to Client {neighbor_id} failed")
                    
            except Exception as e:
                logger.error(f"❌ Client {self.ID}: Exception during connection attempt {attempt} to Client {neighbor_id}: {e}")
            
            if attempt < max_attempts:
                logger.info(f"🔄 Client {self.ID}: Retrying connection to Client {neighbor_id} in {retry_delay}s...")
                time.sleep(retry_delay)
        
        logger.error(f"❌ Client {self.ID}: Failed to connect to Client {neighbor_id} after {max_attempts} attempts")


    def _add_peer_to_comm_manager(self, peer_id, peer_address):
        """
        Add peer to communication manager using provided address
        
        Args:
            peer_id: ID of the peer to add
            peer_address: Dict with 'host' and 'port' keys
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # 🔧 Critical fix: Directly use real address provided by server
            self.comm_manager.add_neighbors(neighbor_id=peer_id, address=peer_address)
            
            logger.info(f"🔗 Client {self.ID}: Added peer {peer_id} to comm_manager (address: {peer_address})")
            logger.info(f"🔗 Client {self.ID}: Current neighbors: {list(self.comm_manager.neighbors.keys())}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Failed to add peer {peer_id} to comm_manager: {e}")
            return False

    def _send_connection_established_message(self, peer_id, peer_address=None):
        """
        Send connection established message to server for topology tracking
        
        Args:
            peer_id: ID of the connected peer
            peer_address: Address of the connected peer (optional)
        """
        try:
            # Send connection confirmation to server using correct format
            from federatedscope.core.connection_monitor import ConnectionEvent
            message = Message(
                msg_type='connect_msg',
                sender=self.ID,
                receiver=[0],  # Server ID
                state=self.state,
                timestamp=0,  # Use dummy timestamp
                content={
                    'event_type': ConnectionEvent.CONNECT.value,
                    'peer_id': peer_id,  # 🔧 Fix: Use correct field name
                    'source_client_id': self.ID,
                    'target_client_id': peer_id,
                    'details': {
                        'topology_connection': True,
                        'peer_address': f"{peer_address['host']}:{peer_address['port']}" if peer_address else f"peer_{peer_id}"
                    }
                }
            )
            self.comm_manager.send(message)
            logger.info(f"📡 Client {self.ID}: Sent connection established message for peer {peer_id}")
            
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Failed to send connection established message: {e}")


    def _save_local_model_after_training(self):
        """
        Save local model after training completion using chunked storage
        Stores model weights as chunks in local database with key format:
        (client_id, round_num, chunk_id) -> chunk_data
        """
        try:
            from federatedscope.core.chunk_manager import ChunkManager
            
            # Get the trained model
            model = None
            if hasattr(self.trainer, 'ctx') and hasattr(self.trainer.ctx, 'model'):
                model = self.trainer.ctx.model
            elif hasattr(self.trainer, 'model'):
                model = self.trainer.model
            
            if model is None:
                logger.warning(f"⚠️ Client {self.ID}: Cannot save model - no accessible model found")
                return
            
            # Initialize ChunkManager for this client
            if not hasattr(self, 'chunk_manager'):
                self.chunk_manager = ChunkManager(
                    client_id=self.ID,
                    change_callback=self._send_chunk_info_to_server
                )
            
            # Save model as chunks (default: 10 chunks)
            num_chunks = getattr(self._cfg, 'chunk_num', 10) if hasattr(self, '_cfg') else 10
            keep_rounds = getattr(self._cfg, 'chunk_keep_rounds', 2) if hasattr(self, '_cfg') else 2
            importance_method = getattr(self._cfg, 'chunk_importance_method', 'magnitude') if hasattr(self, '_cfg') else 'magnitude'
            
            logger.info(f"[Client {self.ID}] Saving model chunks with importance scoring using '{importance_method}' method")
            saved_hashes = self.chunk_manager.save_model_chunks(
                model=model,
                round_num=self.state,
                num_chunks=num_chunks,
                keep_rounds=keep_rounds,
                importance_method=importance_method
            )
            
            if saved_hashes:
                logger.info(f"✅ Client {self.ID}: Saved model as {len(saved_hashes)} chunks for round {self.state}")
                
                # Log storage statistics for current round
                stats = self.chunk_manager.get_storage_stats(round_num=self.state)
                logger.debug(f"📊 Client {self.ID}: Storage stats - "
                           f"Total chunks: {stats.get('unique_chunks', 0)}, "
                           f"Size: {stats.get('storage_size_mb', 0):.2f} MB")
            else:
                logger.error(f"❌ Client {self.ID}: Failed to save any chunks for round {self.state}")
                
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Failed to save model chunks: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
    
    def _aggregate_model_from_chunks(self, round_num):
        """
        Aggregate model parameters from chunk database
        Implement FedAvg aggregation algorithm: weighted average based on sample count
        
        Args:
            round_num: Target round
            
        Returns:
            dict: Aggregated model parameters, returns None if failed
        """
        try:
            if not hasattr(self, 'chunk_manager'):
                logger.warning(f"[BT-FL] Client {self.ID}: No chunk_manager available, initializing...")
                try:
                    from federatedscope.core.chunk_manager import ChunkManager
                    self.chunk_manager = ChunkManager(
                        client_id=self.ID,
                        change_callback=self._send_chunk_info_to_server
                    )
                    logger.info(f"[BT-FL] Client {self.ID}: Successfully initialized chunk_manager for aggregation")
                except Exception as e:
                    logger.error(f"[BT-FL] Client {self.ID}: Failed to initialize chunk_manager: {e}")
                    return None
                
            logger.info(f"[BT-FL] Client {self.ID}: Starting model aggregation from chunks for round {round_num}")
            
            # Get all available client model chunks
            available_clients = self.chunk_manager.get_available_clients_for_round(round_num)
            if not available_clients:
                logger.warning(f"[BT-FL] Client {self.ID}: No client models available in chunk database for round {round_num}")
                return None
                
            logger.info(f"[BT-FL] Client {self.ID}: Found models from {len(available_clients)} clients: {available_clients}")
            
            # Collect model parameters and sample counts from all clients
            client_models = []
            total_samples = 0
            
            for client_id in available_clients:
                try:
                    # Reconstruct model parameters from chunks
                    model_params = self.chunk_manager.reconstruct_model_from_client_chunks(client_id, round_num)
                    if model_params is None:
                        logger.warning(f"[BT-FL] Client {self.ID}: Failed to reconstruct model for client {client_id}")
                        continue
                        
                    # Get sample size (from chunk metadata or use default value)
                    sample_size = self.chunk_manager.get_client_sample_size(client_id, round_num)
                    if sample_size is None:
                        sample_size = 128  # Use default sample count
                        logger.warning(f"[BT-FL] Client {self.ID}: Using default sample size {sample_size} for client {client_id}")
                    
                    client_models.append((client_id, model_params, sample_size))
                    total_samples += sample_size
                    logger.debug(f"[BT-FL] Client {self.ID}: Loaded model from client {client_id} with {sample_size} samples")
                    
                except Exception as e:
                    logger.error(f"[BT-FL] Client {self.ID}: Failed to load model from client {client_id}: {e}")
                    continue
            
            if not client_models:
                logger.error(f"[BT-FL] Client {self.ID}: No valid client models found for aggregation")
                return None
                
            logger.info(f"[BT-FL] Client {self.ID}: Aggregating {len(client_models)} models with total {total_samples} samples")
            
            # Execute FedAvg weighted average aggregation
            aggregated_params = {}
            
            # Get model structure (use first available model)
            first_model = client_models[0][1]
            
            for param_name in first_model.keys():
                # Initialize parameters
                aggregated_params[param_name] = None
                
                # Weighted summation
                for client_id, model_params, sample_size in client_models:
                    if param_name not in model_params:
                        logger.warning(f"[BT-FL] Client {self.ID}: Parameter {param_name} missing in client {client_id} model")
                        continue
                        
                    weight = sample_size / total_samples
                    param_value = model_params[param_name]
                    
                    # Ensure parameters are on correct device
                    if hasattr(param_value, 'to'):
                        param_value = param_value.to(self.device)
                    
                    if aggregated_params[param_name] is None:
                        aggregated_params[param_name] = weight * param_value
                    else:
                        aggregated_params[param_name] += weight * param_value
            
            logger.info(f"[BT-FL] Client {self.ID}: Successfully aggregated model with {len(aggregated_params)} parameters")
            return aggregated_params
            
        except Exception as e:
            logger.error(f"[BT-FL] Client {self.ID}: Model aggregation failed: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _send_chunk_info_to_server(self, chunk_info: ChunkInfo):
        """
        Send chunk information to server for tracking
        
        Args:
            chunk_info: Chunk change information
        """
        try:
            # Construct message to send to server
            msg = Message(
                msg_type='chunk_info',
                sender=self.ID,
                receiver=[0],  # Send to server (ID=0)
                state=self.state,
                timestamp=chunk_info.timestamp,
                content=chunk_info.to_dict()
            )
            
            # Send message
            self.comm_manager.send(msg)
            logger.debug(f"📤 Client {self.ID}: Sent chunk info to server - "
                        f"Round {chunk_info.round_num}, chunk{chunk_info.chunk_id}, action {chunk_info.action}")
            
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Failed to send chunk info to server: {e}")
    
    def send_initial_chunk_info(self):
        """
        Send initial chunk information to server (after first connection or reconnection)
        """
        try:
            if not hasattr(self, 'chunk_manager'):
                return
                
            # Get all existing chunk information
            all_chunk_infos = self.chunk_manager.get_all_chunks_info()
            
            if all_chunk_infos:
                logger.info(f"📤 Client {self.ID}: Sending initial chunk info to server ({len(all_chunk_infos)} chunks)")
                
                for chunk_info in all_chunk_infos:
                    self._send_chunk_info_to_server(chunk_info)
            else:
                logger.debug(f"📤 Client {self.ID}: No chunk info needs to be sent")
                
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Failed to send initial chunk info: {e}")

    # ==================== BitTorrent Support Methods ====================
    
    def _register_default_handlers(self):
        """
        Register default message handlers including BitTorrent handlers
        """
        # Call parent class to register standard handlers
        super(Client, self)._register_default_handlers()
        
        # Register BitTorrent message handlers
        self.register_handlers('start_bittorrent', self.callback_funcs_for_start_bittorrent, ['bittorrent_complete'])
        self.register_handlers('bitfield', self.callback_funcs_for_bitfield, [None])
        self.register_handlers('have', self.callback_funcs_for_have, [None])
        self.register_handlers('interested', self.callback_funcs_for_interested, [None])
        self.register_handlers('choke', self.callback_funcs_for_choke, [None])
        self.register_handlers('unchoke', self.callback_funcs_for_unchoke, [None])
        self.register_handlers('request', self.callback_funcs_for_request, ['piece'])
        self.register_handlers('piece', self.callback_funcs_for_piece, [None])
        self.register_handlers('cancel', self.callback_funcs_for_cancel, [None])
    
    def callback_funcs_for_start_bittorrent(self, message):
        """
        Handle Server's start_bittorrent message, start chunk exchange
        """
        # 🔧 Fix: Check if Client ID has been assigned
        if self.ID <= 0:
            logger.error("[BT] Client ID not assigned yet, cannot start BitTorrent")
            self._report_bittorrent_completion_failure()
            return
        
        logger.info(f"[BT] Client {self.ID}: Received start_bittorrent signal")
        
        # 🐛 Bug fix 17: Record start time for statistics
        self.bt_start_time = time.time()
        
        # 1. Ensure model is saved as chunks (done when training completed)
        expected_chunks = message.content['expected_chunks']
        round_num = message.content['round']  # 🔴 Get current round
        
        # 🔧 Fix: BitTorrent needs to use topology neighbors, not communication manager neighbors
        # comm_manager.neighbors contains server ID, not the peer client IDs we want
        neighbors = []
        
        # Priority use topology information (if exists)
        if hasattr(self, 'topology_neighbors') and self.topology_neighbors:
            # Use stored topology neighbor list
            neighbors = list(self.topology_neighbors)
            logger.info(f"[BT] Client {self.ID}: Found neighbors from stored topology: {neighbors}")
        elif hasattr(self, 'topology_manager') and hasattr(self.topology_manager, 'topology'):
            # Get from topology manager
            topology = self.topology_manager.topology
            neighbors = topology.get(self.ID, [])
            logger.info(f"[BT] Client {self.ID}: Found neighbors from topology_manager: {neighbors}")
        else:
            # Fallback strategy: filter out server ID, use all other clients as neighbors
            logger.warning(f"[BT] Client {self.ID}: No topology info, using all other clients as neighbors")
            neighbors = [i for i in range(1, self._cfg.federate.client_num + 1) if i != self.ID]
            logger.info(f"[BT] Client {self.ID}: Fallback neighbors: {neighbors}")
        
        # 2. Start BitTorrent exchange
        # 🐛 Bug fix 19: Ensure chunk_manager exists, initialize if needed
        if not hasattr(self, 'chunk_manager'):
            logger.warning(f"[BT] Client {self.ID}: No chunk_manager found, initializing...")
            try:
                from federatedscope.core.chunk_manager import ChunkManager
                self.chunk_manager = ChunkManager(
                    client_id=self.ID,
                    change_callback=self._send_chunk_info_to_server
                )
                logger.info(f"[BT] Client {self.ID}: Successfully initialized chunk_manager")
            except Exception as e:
                logger.error(f"[BT] Client {self.ID}: Failed to initialize chunk_manager: {e}")
                self._report_bittorrent_completion_failure()
                return
            
        # 🔧 Critical fix: BitTorrent should exchange chunks from the round clients just finished training
        # Timing analysis: Client.state updates when receiving model, Server.state updates after aggregation +1
        # When Server sends start_bittorrent(state=N+1), clients only have chunks from round N
        bt_round = round_num - 1 if round_num > 0 else 0
        logger.info(f"[BT] Client {self.ID}: BitTorrent will exchange chunks from round {bt_round} (server state: {round_num})")
        
        # Pass corrected round to BitTorrent
        self._start_bittorrent_exchange(neighbors, bt_round)
        
        # 3. Start exchange loop (runs in background)
        logger.info(f"[BT] Client {self.ID}: Starting BitTorrent exchange loop with expected_chunks={expected_chunks}")
        import threading
        bt_thread = threading.Thread(target=self._run_bittorrent_exchange_loop, 
                                     args=(expected_chunks,), daemon=True)
        bt_thread.start()
        logger.info(f"[BT] Client {self.ID}: BitTorrent exchange thread started")

    def _start_bittorrent_exchange(self, neighbors, round_num):
        """
        Start BitTorrent chunk exchange after training completion
        🔴 Key modification: Added round_num parameter
        """
        # 🔧 Fix: Save current ID to chunk_manager
        if hasattr(self, 'chunk_manager'):
            self.chunk_manager.client_id = self.ID
            self.chunk_manager.current_round = round_num
        
        # Initialize BitTorrent manager
        try:
            from federatedscope.core.bittorrent_manager import BitTorrentManager
        except ImportError:
            logger.error("[BT] BitTorrentManager not found, using stub")
            # Fallback handling: Create a simple stub
            class BitTorrentManager:
                def __init__(self, *args, **kwargs):
                    self.round_num = round_num
                def start_exchange(self):
                    pass
        
        self.bt_manager = BitTorrentManager(
            self.ID,
            round_num,  # 🔴 Pass current round number
            self.chunk_manager,
            self.comm_manager,
            neighbors,
            self._cfg,  # 🆕 Pass configuration object
            self._streaming_manager  # 🚀 Pass streaming manager
        )
        
        # Directly start chunk exchange, no tracker needed
        self.bt_manager.start_exchange()
        
        # Process buffered BitTorrent messages
        self._process_buffered_bt_messages()
        
    def _process_buffered_bt_messages(self):
        """Process buffered BitTorrent messages after bt_manager is ready"""
        if not hasattr(self, 'bt_message_buffer') or len(self.bt_message_buffer) == 0:
            return
            
        logger.info(f"[BT] Client {self.ID}: Processing {len(self.bt_message_buffer)} buffered BitTorrent messages")
        
        buffered_messages = self.bt_message_buffer.copy()
        self.bt_message_buffer.clear()
        
        for message_type, message in buffered_messages:
            logger.info(f"[BT] Client {self.ID}: Processing buffered {message_type} from {message.sender}")
            
            if message_type == 'bitfield':
                self.callback_funcs_for_bitfield(message)
            elif message_type == 'have':
                self.callback_funcs_for_have(message)
            elif message_type == 'interested':
                self.callback_funcs_for_interested(message)
            elif message_type == 'choke':
                self.callback_funcs_for_choke(message)
            elif message_type == 'unchoke':
                self.callback_funcs_for_unchoke(message)
            elif message_type == 'request':
                self.callback_funcs_for_request(message)
            elif message_type == 'piece':
                self.callback_funcs_for_piece(message)
            elif message_type == 'cancel':
                self.callback_funcs_for_cancel(message)
            else:
                logger.warning(f"[BT] Client {self.ID}: Unknown buffered message type: {message_type}")
    
    def _run_bittorrent_exchange_loop(self, expected_chunks):
        """Run BitTorrent exchange main loop"""
        logger.info(f"[BT] Client {self.ID}: Exchange loop started, expected_chunks={expected_chunks}")
        try:
            import time
            # 🐛 Bug fix 20: Add safe loop termination condition
            max_iterations = 999999999999999  # Prevent infinite loop
            iteration = 0
            
            while not self._has_all_chunks(expected_chunks) and iteration < max_iterations:
                iteration += 1
                
                # 🔧 CRITICAL FIX: Check if BitTorrent exchange was stopped
                if hasattr(self.bt_manager, 'is_stopped') and self.bt_manager.is_stopped:
                    logger.info(f"[BT] Client {self.ID}: BitTorrent exchange was stopped, breaking from loop at iteration {iteration}")
                    break
                
                # Output progress every 100 iterations
                if iteration % 10 == 1:
                    # 🆕 FIX: Safe access to bt_manager - check if still exists and not stopped
                    if self.bt_manager and not self.bt_manager.is_stopped:
                        current_chunks = len(self.chunk_manager.get_global_bitfield(self.bt_manager.round_num))
                        peer_count = len(self.bt_manager.peer_bitfields)
                        logger.info(f"[BT] Client {self.ID}: Iteration {iteration}, current chunks: {current_chunks}/{expected_chunks}, peers: {peer_count}")
                    else:
                        logger.info(f"[BT] Client {self.ID}: BitTorrent manager stopped, skipping progress update at iteration {iteration}")
                        break
                
                # 🆕 Dual pool request management: Only fill when queue is empty, reduce selection program call frequency
                # 🆕 FIX: Check bt_manager is still valid before accessing
                if self.bt_manager and not self.bt_manager.is_stopped and len(self.bt_manager.pending_queue) == 0:
                    logger.debug(f"[BT] Client {self.ID}: Pending queue empty, filling with priority chunks...")
                    self.bt_manager._fill_pending_queue()
                elif not self.bt_manager or self.bt_manager.is_stopped:
                    logger.info(f"[BT] Client {self.ID}: BitTorrent manager stopped, exiting exchange loop")
                    break
                
                # Transfer requests from queue to active pool
                if (len(self.bt_manager.pending_requests) < self.bt_manager.MAX_ACTIVE_REQUESTS and
                    len(self.bt_manager.pending_queue) > 0):
                    logger.debug(f"[BT] Client {self.ID}: Transferring requests from queue to active pool...")
                    self.bt_manager._transfer_from_queue_to_active()
                else:
                    if iteration % 100 == 1:  # Reduce log frequency
                        active_count = len(self.bt_manager.pending_requests)
                        queue_count = len(self.bt_manager.pending_queue)
                        if active_count == 0 and queue_count == 0:
                            logger.info(f"[BT] Client {self.ID}: No chunks to request in iteration {iteration}")
                        else:
                            logger.debug(f"[BT] Client {self.ID}: Pool status - Active: {active_count}/{self.bt_manager.MAX_ACTIVE_REQUESTS}, Queue: {queue_count}/{self.bt_manager.MAX_PENDING_QUEUE}")
                        
                # Periodically update choke/unchoke (every 10 iterations)
                if iteration % 10 == 0:
                    self.bt_manager._regular_unchoke_algorithm()
                    
                # Check timeout
                self.bt_manager.check_timeouts()
                    
                # Brief sleep to avoid excessive CPU usage
                time.sleep(0.01)
                
            # Record completion reason
            final_chunks = len(self.chunk_manager.get_global_bitfield(self.bt_manager.round_num))
            logger.info(f"[BT] Client {self.ID}: Exchange loop completed after {iteration} iterations. Final chunks: {final_chunks}/{expected_chunks}")
            
            # 4. Report to Server after completion
            self._report_bittorrent_completion()
            
            # ✅ KEEP ALIVE: Keep bt_manager for serving other peers (round-level lifecycle management)
            logger.info(f"[BT] Client {self.ID}: BitTorrent download completed, but keeping manager alive for serving other peers")
            if hasattr(self, 'bt_manager'):
                self.bt_manager.is_download_complete = True  # Mark download complete, enter seeding mode
            
        except Exception as e:
            import traceback
            logger.error(f"[BT] Client {self.ID}: Exchange loop error: {e}")
            logger.error(f"[BT] Client {self.ID}: Exception type: {type(e)}")
            logger.error(f"[BT] Client {self.ID}: Exception traceback:")
            for line in traceback.format_exc().split('\n'):
                if line.strip():
                    logger.error(f"[BT] Client {self.ID}: {line}")
            self._report_bittorrent_completion_failure()
        finally:
            # ✅ KEEP ALIVE: Keep bt_manager for serving other peers in this round
            logger.info(f"[BT] Client {self.ID}: Exchange loop ended, but keeping BitTorrent manager for peer serving")
            # bt_manager will be naturally replaced when next round starts

    def callback_funcs_for_bitfield(self, message):
        """Handle bitfield message"""
        logger.info(f"[BT] Client {self.ID}: Received bitfield message from peer {message.sender}")
        
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.info(f"[BT] Client {self.ID}: bt_manager not ready, buffering bitfield message from {message.sender}")
            self.bt_message_buffer.append(('bitfield', message))
            return
            
        # 🔴 Validate round match
        if message.content['round_num'] != self.bt_manager.round_num:
            logger.warning(f"[BT] Client {self.ID}: Received bitfield from wrong round: {message.content['round_num']} vs {self.bt_manager.round_num}")
            return
        
        logger.info(f"[BT] Client {self.ID}: Processing bitfield from peer {message.sender}, round {message.content['round_num']}")
        
        # 🆕 Pass message content directly (including importance scores), let BitTorrent manager handle format conversion
        logger.info(f"[BT] Client {self.ID}: Passing bitfield message content from peer {message.sender}: {len(message.content.get('bitfield', []))} chunks")
        
        # 🔧 Debug: Check bitfield content
        if not message.content.get('bitfield'):
            logger.debug(f"[BT] Client {self.ID}: Received empty bitfield from peer {message.sender}")
            
        self.bt_manager.handle_bitfield(message.sender, message.content)
        
    def callback_funcs_for_have(self, message):
        """Handle have message"""
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.info(f"[BT] Client {self.ID}: bt_manager not ready, buffering have message from {message.sender}")
            # Buffer the have message for later processing
            self.bt_message_buffer.append(('have', message))
            return
            
        sender_id = message.sender
        # 🔴 Validate round match
        if message.content['round_num'] != self.bt_manager.round_num:
            logger.warning(f"[BT] Have message from wrong round: {message.content['round_num']}")
            return
            
        # 🆕 Get importance score (if available)
        importance_score = message.content.get('importance_score', 0.0)
        
        self.bt_manager.handle_have(sender_id, 
                                  message.content['round_num'],
                                  message.content['source_client_id'],
                                  message.content['chunk_id'],
                                  importance_score)
        
    def callback_funcs_for_interested(self, message):
        """Handle interested message"""
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.info(f"[BT] Client {self.ID}: bt_manager not ready, buffering interested message from {message.sender}")
            self.bt_message_buffer.append(('interested', message))
            return
        self.bt_manager.handle_interested(message.sender)
        
    def callback_funcs_for_choke(self, message):
        """Handle choke message"""
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.info(f"[BT] Client {self.ID}: bt_manager not ready, buffering choke message from {message.sender}")
            self.bt_message_buffer.append(('choke', message))
            return
        self.bt_manager.handle_choke(message.sender)
        
    def callback_funcs_for_unchoke(self, message):
        """Handle unchoke message"""
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.debug(f"[BT] Client {self.ID}: bt_manager not ready, buffering unchoke message from {message.sender}")
            self.bt_message_buffer.append(('unchoke', message))
            return
        self.bt_manager.handle_unchoke(message.sender)
        
    def callback_funcs_for_request(self, message):
        """Handle chunk request"""
        # 🔍 DEBUG LOG: Detailed receive log - extended display of complete message information
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}: RECEIVED chunk REQUEST from peer {message.sender}")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}: 📋 FULL MESSAGE DEBUG:")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}:   - msg_type: {message.msg_type}")  
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}:   - sender: {message.sender}")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}:   - receiver: {message.receiver}")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}:   - state: {message.state}")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}:   - timestamp: {getattr(message, 'timestamp', 'N/A')}")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}:   - content: {message.content}")
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}: Processing request for chunk ({message.content['round_num']}, {message.content['source_client_id']}, {message.content['chunk_id']})")
        
        logger.debug(f"[BT] Client {self.ID}: Received request from peer {message.sender} for chunk {message.content['source_client_id']}:{message.content['chunk_id']}")
        
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.debug(f"[BT] Client {self.ID}: bt_manager not ready, buffering request message from {message.sender}")
            self.bt_message_buffer.append(('request', message))
            return
        
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}: Calling bt_manager.handle_request for peer {message.sender}")
            
        # 🔴 Pass round_num to handle_request
        self.bt_manager.handle_request(
            message.sender,
            message.content['round_num'],
            message.content['source_client_id'],
            message.content['chunk_id']
        )
        
        logger.debug(f"🎯 [BT-REQ-RECV] Client {self.ID}: bt_manager.handle_request COMPLETED for peer {message.sender}")
        
    def callback_funcs_for_piece(self, message):
        """Handle chunk data"""
        # 🔍 DEBUG LOG: Detailed piece receive log
        logger.debug(f"📥 [BT-PIECE-RECV] Client {self.ID}: RECEIVED chunk PIECE from peer {message.sender}")
        logger.debug(f"📥 [BT-PIECE-RECV] Client {self.ID}: Piece content keys = {list(message.content.keys())}")
        logger.debug(f"📥 [BT-PIECE-RECV] Client {self.ID}: Processing piece for chunk ({message.content['round_num']}, {message.content['source_client_id']}, {message.content['chunk_id']})")
        
        logger.debug(f"[BT] Client {self.ID}: Received piece from peer {message.sender} for chunk {message.content['source_client_id']}:{message.content['chunk_id']}")
        
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.debug(f"[BT] Client {self.ID}: bt_manager not ready, buffering piece message from {message.sender}")
            self.bt_message_buffer.append(('piece', message))
            return
        
        logger.debug(f"📥 [BT-PIECE-RECV] Client {self.ID}: Calling bt_manager.handle_piece for peer {message.sender}")
            
        self.bt_manager.handle_piece(
            message.sender,
            message.content['round_num'],
            message.content['source_client_id'],
            message.content['chunk_id'],
            message.content['data'],
            message.content['checksum']
        )
        
        logger.debug(f"📥 [BT-PIECE-RECV] Client {self.ID}: bt_manager.handle_piece COMPLETED for peer {message.sender}")
        
    def callback_funcs_for_cancel(self, message):
        """Handle cancel message"""
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            logger.debug(f"[BT] Client {self.ID}: bt_manager not ready, buffering cancel message from {message.sender}")
            self.bt_message_buffer.append(('cancel', message))
            return
            
        # Simple handling: remove from pending requests
        chunk_key = (message.content['round_num'], 
                    message.content['source_client_id'], 
                    message.content['chunk_id'])
        if chunk_key in self.bt_manager.pending_requests:
            del self.bt_manager.pending_requests[chunk_key]

    def _has_all_chunks(self, expected_chunks):
        """🐛 Bug Fix 29: Check if all chunks have been collected"""
        if not hasattr(self, 'chunk_manager') or not hasattr(self, 'bt_manager'):
            return False
        
        # Get current number of chunks owned
        # 🔴 Pass round_num to limit checking only current round chunks
        current_chunks = len(self.chunk_manager.get_global_bitfield(self.bt_manager.round_num))
        return current_chunks >= expected_chunks
        
    def _report_bittorrent_completion(self):
        """
        Report BitTorrent exchange completion to Server
        """
        # 🐛 Bug Fix 31: Safely get statistics information
        # 🔴 Use current round to get chunk count
        chunks_collected = len(self.chunk_manager.get_global_bitfield(self.bt_manager.round_num)) if hasattr(self, 'chunk_manager') and hasattr(self, 'bt_manager') else 0
        
        # 🐛 Bug Fix 32: Safely get transfer statistics
        bytes_downloaded = getattr(self.bt_manager, 'total_downloaded', 0) if hasattr(self, 'bt_manager') else 0
        bytes_uploaded = getattr(self.bt_manager, 'total_uploaded', 0) if hasattr(self, 'bt_manager') else 0
        
        # 🐛 Bug Fix 33: Large integers are now handled automatically in message.py
        self.comm_manager.send(
            Message(msg_type='bittorrent_complete',
                    sender=self.ID,
                    receiver=[0],  # Server ID
                    content={
                        'chunks_collected': chunks_collected,
                        'exchange_time': time.time() - self.bt_start_time if hasattr(self, 'bt_start_time') else 0,
                        'bytes_downloaded_str': str(bytes_downloaded),  # Always send as string for consistency
                        'bytes_uploaded_str': str(bytes_uploaded)      # Always send as string for consistency
                    })
        )
        
        logger.debug(f"[BT] Client {self.ID}: Reported completion to server")
        
    def _report_bittorrent_completion_failure(self):
        """🐛 Bug Fix 33: Report BitTorrent failure"""
        self.comm_manager.send(
            Message(msg_type='bittorrent_complete',
                    sender=self.ID,
                    receiver=[0],
                    content={
                        'chunks_collected': 0,
                        'exchange_time': 0,
                        'bytes_downloaded_str': "0",
                        'bytes_uploaded_str': "0",
                        'status': 'failed'
                    })
        )
        logger.error(f"[BT] Client {self.ID}: Reported failure to server")

    @classmethod
    def get_msg_handler_dict(cls):
        return cls().msg_handlers_str
