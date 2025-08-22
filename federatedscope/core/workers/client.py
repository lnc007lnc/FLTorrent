import copy
import logging
import sys
import pickle

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
        The handling function for receiving model parameters, \
        which triggers the local training process. \
        This handling function is widely used in various FL courses.

        Arguments:
            message: The received message
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

                self.comm_manager.send(
                    Message(msg_type='model_para',
                            sender=self.ID,
                            receiver=[self.server_id],
                            state=self.state,
                            timestamp=timestamp,
                            content=(sample_size, first_aggregate_model_para[0]
                                     if single_model_case else
                                     first_aggregate_model_para)))

        else:
            round = message.state
            sender = message.sender
            timestamp = message.timestamp
            content = message.content

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

            # When clients share the local model, we must set strict=True to
            # ensure all the model params (which might be updated by other
            # clients in the previous local training process) are overwritten
            # and synchronized with the received model
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
                
                self.comm_manager.send(
                    Message(msg_type='model_para',
                            sender=self.ID,
                            receiver=[sender],
                            state=self.state,
                            timestamp=self._gen_timestamp(
                                init_timestamp=timestamp,
                                instance_number=sample_size),
                            content=(sample_size, shared_model_para)))

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
            topology_type = content.get('topology_type', 'unknown')
            max_attempts = content.get('max_attempts', 3)
            retry_delay = content.get('retry_delay', 2.0)
            
            logger.info(f"🌐 Client {self.ID}: Received topology instruction")
            logger.info(f"   Topology type: {topology_type}")
            logger.info(f"   Neighbors to connect: {neighbors_to_connect}")
            
            if not neighbors_to_connect:
                logger.info(f"   No neighbors to connect for Client {self.ID}")
                return
            
            # Establish connections to each neighbor
            for neighbor_id in neighbors_to_connect:
                self._connect_to_neighbor(neighbor_id, max_attempts, retry_delay)
                
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

    def _connect_to_neighbor(self, neighbor_id, max_attempts=3, retry_delay=2.0):
        """
        Attempt to connect to a specific neighbor client
        
        Args:
            neighbor_id: ID of the neighbor to connect to
            max_attempts: Maximum connection attempts
            retry_delay: Delay between attempts
        """
        import time
        
        logger.info(f"🔗 Client {self.ID}: Attempting to connect to Client {neighbor_id}")
        
        for attempt in range(1, max_attempts + 1):
            try:
                # For simulation purposes, we'll assume the connection is successful
                # In a real implementation, this would involve actual network connections
                
                # Simulate connection establishment
                success = self._simulate_connection_to_peer(neighbor_id)
                
                if success:
                    logger.info(f"✅ Client {self.ID}: Successfully connected to Client {neighbor_id}")
                    
                    # Report successful connection to server via connection monitor
                    if hasattr(self, 'connection_monitor') and self.connection_monitor:
                        self.connection_monitor.report_connection_established(
                            peer_id=neighbor_id,
                            details={
                                'topology_connection': True,
                                'attempt': attempt,
                                'peer_address': f'simulated_address_{neighbor_id}'
                            }
                        )
                    break
                else:
                    logger.warning(f"⚠️ Client {self.ID}: Connection to Client {neighbor_id} failed "
                                 f"(attempt {attempt}/{max_attempts})")
                    if attempt < max_attempts:
                        time.sleep(retry_delay)
                    
            except Exception as e:
                logger.error(f"❌ Client {self.ID}: Error connecting to Client {neighbor_id} "
                           f"(attempt {attempt}/{max_attempts}): {e}")
                if attempt < max_attempts:
                    time.sleep(retry_delay)
        
        # If all attempts failed
        if attempt == max_attempts:
            logger.error(f"❌ Client {self.ID}: Failed to connect to Client {neighbor_id} "
                        f"after {max_attempts} attempts")
            
            # Report failed connection to server
            if hasattr(self, 'connection_monitor') and self.connection_monitor:
                self.connection_monitor.report_connection_lost(
                    peer_id=neighbor_id,
                    details={
                        'topology_connection': True,
                        'error_message': f'Failed after {max_attempts} attempts',
                        'final_attempt': attempt
                    }
                )

    def _simulate_connection_to_peer(self, peer_id):
        """
        Simulate connection establishment to another peer client
        In a real implementation, this would involve actual network protocols
        
        Args:
            peer_id: ID of the peer to connect to
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        # For simulation purposes, randomly succeed most of the time
        import random
        success_rate = 0.8  # 80% success rate for simulation
        success = random.random() < success_rate
        
        if success:
            # In real implementation, this would:
            # 1. Establish TCP/gRPC connection to peer
            # 2. Exchange handshake messages
            # 3. Add peer to communication manager
            logger.debug(f"🔗 Simulated connection: Client {self.ID} -> Client {peer_id}")
        else:
            logger.debug(f"❌ Simulated connection failed: Client {self.ID} -> Client {peer_id}")
            
        return success

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
            saved_hashes = self.chunk_manager.save_model_chunks(
                model=model,
                round_num=self.state,
                num_chunks=num_chunks,
                keep_rounds=keep_rounds
            )
            
            if saved_hashes:
                logger.info(f"✅ Client {self.ID}: Saved model as {len(saved_hashes)} chunks for round {self.state}")
                
                # Log storage statistics
                stats = self.chunk_manager.get_storage_stats()
                logger.debug(f"📊 Client {self.ID}: Storage stats - "
                           f"Total chunks: {stats.get('unique_chunks', 0)}, "
                           f"Size: {stats.get('storage_size_mb', 0):.2f} MB")
            else:
                logger.error(f"❌ Client {self.ID}: Failed to save any chunks for round {self.state}")
                
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: Failed to save model chunks: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
    
    def _send_chunk_info_to_server(self, chunk_info: ChunkInfo):
        """
        发送chunk信息到服务器进行追踪
        
        Args:
            chunk_info: chunk变化信息
        """
        try:
            # 构造消息发送给服务器
            msg = Message(
                msg_type='chunk_info',
                sender=self.ID,
                receiver=[0],  # 发送给服务器 (ID=0)
                state=self.state,
                timestamp=chunk_info.timestamp,
                content=chunk_info.to_dict()
            )
            
            # 发送消息
            self.comm_manager.send(msg)
            logger.debug(f"📤 Client {self.ID}: 发送chunk信息到服务器 - "
                        f"轮次{chunk_info.round_num}, chunk{chunk_info.chunk_id}, 操作{chunk_info.action}")
            
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: 发送chunk信息到服务器失败: {e}")
    
    def send_initial_chunk_info(self):
        """
        发送初始的chunk信息到服务器（在首次连接或重连后）
        """
        try:
            if not hasattr(self, 'chunk_manager'):
                return
                
            # 获取所有已存在的chunk信息
            all_chunk_infos = self.chunk_manager.get_all_chunks_info()
            
            if all_chunk_infos:
                logger.info(f"📤 Client {self.ID}: 发送初始chunk信息到服务器 ({len(all_chunk_infos)}个chunks)")
                
                for chunk_info in all_chunk_infos:
                    self._send_chunk_info_to_server(chunk_info)
            else:
                logger.debug(f"📤 Client {self.ID}: 没有chunk信息需要发送")
                
        except Exception as e:
            logger.error(f"❌ Client {self.ID}: 发送初始chunk信息失败: {e}")

    # ==================== BitTorrent支持方法 ====================
    
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
        处理Server的start_bittorrent消息，开始chunk交换
        """
        # 🔧 修复：检查Client ID是否已分配
        if self.ID <= 0:
            logger.error("[BT] Client ID not assigned yet, cannot start BitTorrent")
            self._report_bittorrent_completion_failure()
            return
        
        logger.info(f"[BT] Client {self.ID}: Received start_bittorrent signal")
        
        # 🐛 Bug修复17: 记录开始时间用于统计
        self.bt_start_time = time.time()
        
        # 1. 确保模型已保存为chunks（在训练完成时已做）
        expected_chunks = message.content['expected_chunks']
        round_num = message.content['round']  # 🔴 获取当前轮次
        
        # 🔧 修复：正确获取邻居列表
        # FederatedScope中邻居信息存储在comm_manager.neighbors
        if hasattr(self.comm_manager, 'neighbors'):
            neighbors = list(self.comm_manager.neighbors.keys())
        elif hasattr(self, 'topology_manager') and hasattr(self.topology_manager, 'topology'):
            # 从拓扑结构中获取
            topology = self.topology_manager.topology
            neighbors = topology.get(self.ID, [])
        else:
            # 降级策略：使用所有clients作为邻居
            logger.warning(f"[BT] Client {self.ID}: Using all clients as neighbors")
            neighbors = [i for i in range(1, self._cfg.federate.client_num + 1) if i != self.ID]
        
        # 2. 启动BitTorrent交换
        # 🐛 Bug修复19: 确保chunk_manager存在
        if not hasattr(self, 'chunk_manager'):
            logger.error(f"[BT] Client {self.ID}: No chunk_manager found!")
            self._report_bittorrent_completion_failure()
            return
            
        # 🔴 传递round_num到start_bittorrent_exchange
        self._start_bittorrent_exchange(neighbors, round_num)
        
        # 3. 启动交换循环（在后台进行）
        import threading
        bt_thread = threading.Thread(target=self._run_bittorrent_exchange_loop, 
                                     args=(expected_chunks,), daemon=True)
        bt_thread.start()

    def _start_bittorrent_exchange(self, neighbors, round_num):
        """
        在训练完成后启动BitTorrent chunk交换
        🔴 关键修改：添加round_num参数
        """
        # 🔧 修复：保存当前ID到chunk_manager
        if hasattr(self, 'chunk_manager'):
            self.chunk_manager.client_id = self.ID
            self.chunk_manager.current_round = round_num
        
        # 初始化BitTorrent管理器
        try:
            from federatedscope.core.bittorrent_manager import BitTorrentManager
        except ImportError:
            logger.error("[BT] BitTorrentManager not found, using stub")
            # 降级处理：创建一个简单的stub
            class BitTorrentManager:
                def __init__(self, *args, **kwargs):
                    self.round_num = round_num
                def start_exchange(self):
                    pass
        
        self.bt_manager = BitTorrentManager(
            self.ID,
            round_num,  # 🔴 传递当前轮次
            self.chunk_manager,
            self.comm_manager,
            neighbors
        )
        
        # 直接启动chunk交换，无需tracker
        self.bt_manager.start_exchange()
        
    def _run_bittorrent_exchange_loop(self, expected_chunks):
        """运行BitTorrent交换主循环"""
        try:
            import time
            # 🐛 Bug修复20: 添加安全的循环终止条件
            max_iterations = 10000  # 防止无限循环
            iteration = 0
            
            while not self._has_all_chunks(expected_chunks) and iteration < max_iterations:
                iteration += 1
                
                # 选择要下载的chunk（Rarest First）
                target_chunk = self.bt_manager._rarest_first_selection()
                
                if target_chunk:
                    # 找到拥有该chunk的peer
                    peer_with_chunk = self.bt_manager._find_peer_with_chunk(target_chunk)
                    
                    if peer_with_chunk and peer_with_chunk not in self.bt_manager.choked_peers:
                        # 发送请求
                        round_num, source_id, chunk_id = target_chunk
                        self.bt_manager._send_request(peer_with_chunk, source_id, chunk_id)
                        
                # 定期更新choke/unchoke（每10次迭代）
                if iteration % 10 == 0:
                    self.bt_manager._regular_unchoke_algorithm()
                    
                # 检查超时
                self.bt_manager.check_timeouts()
                    
                # 短暂休眠避免CPU占用过高
                time.sleep(0.01)
                
            # 4. 完成后报告给Server
            self._report_bittorrent_completion()
            
        except Exception as e:
            logger.error(f"[BT] Client {self.ID}: Exchange loop error: {e}")
            self._report_bittorrent_completion_failure()

    def callback_funcs_for_bitfield(self, message):
        """处理bitfield消息"""
        if not hasattr(self, 'bt_manager'):
            return
            
        # 🔴 验证轮次匹配
        if message.content['round_num'] != self.bt_manager.round_num:
            logger.warning(f"[BT] Received bitfield from wrong round: {message.content['round_num']}")
            return
        
        # 🔧 修复：将列表转换回字典格式
        bitfield_dict = {}
        for item in message.content['bitfield']:
            key = (item['round'], item['source'], item['chunk'])
            bitfield_dict[key] = True
        
        self.bt_manager.handle_bitfield(message.sender, bitfield_dict)
        
    def callback_funcs_for_have(self, message):
        """处理have消息"""
        if not hasattr(self, 'bt_manager'):
            return
            
        sender_id = message.sender
        # 🔴 验证轮次匹配
        if message.content['round_num'] != self.bt_manager.round_num:
            logger.warning(f"[BT] Have message from wrong round: {message.content['round_num']}")
            return
            
        self.bt_manager.handle_have(sender_id, 
                                  message.content['round_num'],
                                  message.content['source_client_id'],
                                  message.content['chunk_id'])
        
    def callback_funcs_for_interested(self, message):
        """处理interested消息"""
        if hasattr(self, 'bt_manager'):
            self.bt_manager.handle_interested(message.sender)
        
    def callback_funcs_for_choke(self, message):
        """处理choke消息"""
        if hasattr(self, 'bt_manager'):
            self.bt_manager.handle_choke(message.sender)
        
    def callback_funcs_for_unchoke(self, message):
        """处理unchoke消息"""
        if hasattr(self, 'bt_manager'):
            self.bt_manager.handle_unchoke(message.sender)
        
    def callback_funcs_for_request(self, message):
        """处理chunk请求"""
        if hasattr(self, 'bt_manager'):
            # 🔴 传递round_num到handle_request
            self.bt_manager.handle_request(
                message.sender,
                message.content['round_num'],
                message.content['source_client_id'],
                message.content['chunk_id']
            )
        
    def callback_funcs_for_piece(self, message):
        """处理chunk数据"""
        if hasattr(self, 'bt_manager'):
            self.bt_manager.handle_piece(
                message.sender,
                message.content['round_num'],
                message.content['source_client_id'],
                message.content['chunk_id'],
                message.content['data'],
                message.content['checksum']
            )
        
    def callback_funcs_for_cancel(self, message):
        """处理cancel消息"""
        # 简单处理：从pending requests中移除
        if hasattr(self, 'bt_manager'):
            chunk_key = (message.content['round_num'], 
                        message.content['source_client_id'], 
                        message.content['chunk_id'])
            if chunk_key in self.bt_manager.pending_requests:
                del self.bt_manager.pending_requests[chunk_key]

    def _has_all_chunks(self, expected_chunks):
        """🐛 Bug修复29: 检查是否收集了所有chunks"""
        if not hasattr(self, 'chunk_manager') or not hasattr(self, 'bt_manager'):
            return False
        
        # 获取当前拥有的chunks数量
        # 🔴 传递round_num限制只检查当前轮次的chunks
        current_chunks = len(self.chunk_manager.get_global_bitfield(self.bt_manager.round_num))
        return current_chunks >= expected_chunks
        
    def _report_bittorrent_completion(self):
        """
        向Server报告BitTorrent交换完成
        """
        # 🐛 Bug修复31: 安全获取统计信息
        # 🔴 使用当前轮次获取chunks数量
        chunks_collected = len(self.chunk_manager.get_global_bitfield(self.bt_manager.round_num)) if hasattr(self, 'chunk_manager') and hasattr(self, 'bt_manager') else 0
        
        # 🐛 Bug修复32: 安全获取传输统计
        bytes_downloaded = getattr(self.bt_manager, 'total_downloaded', 0) if hasattr(self, 'bt_manager') else 0
        bytes_uploaded = getattr(self.bt_manager, 'total_uploaded', 0) if hasattr(self, 'bt_manager') else 0
        
        self.comm_manager.send(
            Message(msg_type='bittorrent_complete',
                    sender=self.ID,
                    receiver=[0],  # Server ID
                    content={
                        'chunks_collected': chunks_collected,
                        'exchange_time': time.time() - self.bt_start_time if hasattr(self, 'bt_start_time') else 0,
                        'bytes_downloaded': bytes_downloaded,
                        'bytes_uploaded': bytes_uploaded
                    })
        )
        
        logger.info(f"[BT] Client {self.ID}: Reported completion to server")
        
    def _report_bittorrent_completion_failure(self):
        """🐛 Bug修复33: 报告BitTorrent失败"""
        self.comm_manager.send(
            Message(msg_type='bittorrent_complete',
                    sender=self.ID,
                    receiver=[0],
                    content={
                        'chunks_collected': 0,
                        'exchange_time': 0,
                        'bytes_downloaded': 0,
                        'bytes_uploaded': 0,
                        'status': 'failed'
                    })
        )
        logger.error(f"[BT] Client {self.ID}: Reported failure to server")

    @classmethod
    def get_msg_handler_dict(cls):
        return cls().msg_handlers_str
