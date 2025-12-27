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
        
        # Register FL scheduler hook to restore LR after optimizer recreation
        self.trainer.register_hook_in_train(
            new_hook=self._hook_apply_fl_lr,
            trigger='on_fit_start',
            insert_pos=2  # After _hook_on_fit_start_init (pos=1) which recreates optimizer
        )
        
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
            # Log listen address (handle UDS mode)
            if str(host).startswith('unix://'):
                logger.info('Client: Listen to {} (UDS mode)...'.format(host))
            else:
                logger.info('Client: Listen to {}:{}...'.format(host, port))

            # Add server as neighbor (handle UDS mode for server address)
            if str(server_host).startswith('unix://'):
                self.comm_manager.add_neighbors(neighbor_id=server_id,
                                                address=server_host)  # UDS: pass full address
            else:
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

        # 🚀 GRACEFUL SHUTDOWN: Stop all components after FL completes
        self._cleanup_on_finish()

    def _cleanup_on_finish(self):
        """🚀 Clean up all resources after receiving finish message"""
        logger.info(f"[Client {self.ID}] Starting graceful shutdown...")

        # 1. Stop BitTorrent manager if exists
        if hasattr(self, 'bt_manager') and self.bt_manager is not None:
            try:
                self.bt_manager.stop_exchange()
                logger.info(f"[Client {self.ID}] BitTorrent manager stopped")
            except Exception as e:
                logger.warning(f"[Client {self.ID}] Error stopping BitTorrent: {e}")

        # 2. Stop streaming channel manager if exists
        if hasattr(self, 'streaming_channel_manager') and self.streaming_channel_manager is not None:
            try:
                self.streaming_channel_manager.close_all_channels()
                logger.info(f"[Client {self.ID}] Streaming channels closed")
            except Exception as e:
                logger.warning(f"[Client {self.ID}] Error closing streaming channels: {e}")

        # 3. Stop communication manager (gRPC server)
        if hasattr(self, 'comm_manager') and self.comm_manager is not None:
            try:
                if hasattr(self.comm_manager, 'stop'):
                    self.comm_manager.stop()
                logger.info(f"[Client {self.ID}] Communication manager stopped")
            except Exception as e:
                logger.warning(f"[Client {self.ID}] Error stopping comm_manager: {e}")

        logger.info(f"[Client {self.ID}] Graceful shutdown complete")

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
                    # Get aggregator type from config or use default
                    aggregator_type = getattr(self._cfg.federate, 'method', 'fedavg').lower()
                    aggregated_model = self._aggregate_model_from_chunks(round - 1, aggregator_type)
                    
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
                
                # FL-aware scheduler: step based on FL rounds, not local training
                if (hasattr(self.trainer, 'ctx') and hasattr(self.trainer.ctx, 'scheduler') 
                    and self.trainer.ctx.scheduler is not None):
                    old_lr = self.trainer.ctx.optimizer.param_groups[0]['lr']
                    
                    # Step scheduler based on FL round (self.state)
                    self._step_fl_scheduler()
                    
                    new_lr = self.trainer.ctx.optimizer.param_groups[0]['lr'] 
                    
                    # Store the new LR for next round
                    if not hasattr(self, '_fl_next_round_lr'):
                        self._fl_next_round_lr = {}
                    self._fl_next_round_lr[self.state + 1] = new_lr
                    
                    if old_lr != new_lr:
                        logger.info(f"[FL-Scheduler] Client {self.ID}: LR stepped for FL round {self.state} - {old_lr:.6e} → {new_lr:.6e}")
                        # logger.info(f"[FL-Scheduler] Client {self.ID}: Next round {self.state + 1} will use LR: {new_lr:.6e}")
                
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
                    change_callback=self._send_chunk_info_to_server,
                    cfg=self._cfg
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
                    change_callback=self._send_chunk_info_to_server,
                    cfg=self._cfg
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

                # 🚀 CRITICAL FIX: Register own chunks to memory set for fast lookup
                # This enables memory-based bitfield without DB queries
                if hasattr(self, 'bt_manager') and self.bt_manager is not None:
                    own_chunk_keys = [(self.state, self.ID, i) for i in range(len(saved_hashes))]
                    self.bt_manager.register_own_chunks(own_chunk_keys)

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
    
    def _hook_apply_fl_lr(self, ctx):
        """
        Hook to restore FL scheduler learning rate after optimizer recreation.
        This runs after _hook_on_fit_start_init which recreates the optimizer.
        """
        self._apply_stored_fl_lr()
    
    def _apply_stored_fl_lr(self):
        """
        Apply stored learning rate for current FL round before training starts
        This prevents LR from being reset to initial value each round
        """
        if not (hasattr(self.trainer, 'ctx') and hasattr(self.trainer.ctx, 'scheduler') 
                and self.trainer.ctx.scheduler is not None):
            logger.debug(f"[FL-Scheduler] Client {self.ID}: No scheduler available, skipping LR restoration")
            return
        
        optimizer = self.trainer.ctx.optimizer
        scheduler = self.trainer.ctx.scheduler
        current_lr = optimizer.param_groups[0]['lr']
        
        # Get initial LR - handle composite schedulers like SequentialLR
        initial_lr = self._get_scheduler_initial_lr(scheduler)
        
        logger.debug(f"[FL-Scheduler] Client {self.ID}: FL round {self.state}, current LR: {current_lr:.6e}, initial LR: {initial_lr:.6e}")
        
        # For composite schedulers (like SequentialLR), restore LR but don't use complex state restoration
        if self._is_composite_scheduler(scheduler):
            # Simply restore the stored LR if available
            if hasattr(self, '_fl_next_round_lr') and self.state in self._fl_next_round_lr:
                target_lr = self._fl_next_round_lr[self.state]
                optimizer.param_groups[0]['lr'] = target_lr
                logger.info(f"[FL-Scheduler] Client {self.ID}: FL round {self.state}, restored composite scheduler LR from {current_lr:.6e} to {target_lr:.6e}")
            else:
                logger.debug(f"[FL-Scheduler] Client {self.ID}: No stored LR for composite scheduler round {self.state}, keeping current: {current_lr:.6e}")
        else:
            # For simple schedulers, use LR restoration
            if hasattr(self, '_fl_next_round_lr') and self.state in self._fl_next_round_lr:
                target_lr = self._fl_next_round_lr[self.state]
                optimizer.param_groups[0]['lr'] = target_lr
                logger.info(f"[FL-Scheduler] Client {self.ID}: FL round {self.state}, restored LR from {current_lr:.6e} to {target_lr:.6e}")
            elif self.state == 0:
                # First round, set to the scheduler's intended initial LR
                if hasattr(scheduler, 'start_factor') and hasattr(scheduler, 'base_lrs'):
                    # For LinearLR, calculate initial LR
                    initial_lr = scheduler.base_lrs[0] * scheduler.start_factor
                    optimizer.param_groups[0]['lr'] = initial_lr
                    logger.info(f"[FL-Scheduler] Client {self.ID}: FL round 0, set LinearLR initial LR: {initial_lr:.6e}")
                else:
                    logger.info(f"[FL-Scheduler] Client {self.ID}: FL round 0, using current LR: {current_lr:.6e}")
            else:
                logger.warning(f"[FL-Scheduler] Client {self.ID}: No stored LR for round {self.state}, current: {current_lr:.6e}")
                if current_lr == initial_lr:
                    logger.warning(f"[FL-Scheduler] Client {self.ID}: LR appears to have been reset to initial value ({initial_lr:.6e}), this may cause scheduling issues")
    
    def _step_fl_scheduler(self):
        """
        Step learning rate scheduler based on FL rounds and save the new LR for next round
        This ensures learning rate decreases across FL rounds, not within local training
        """
        scheduler = self.trainer.ctx.scheduler
        if scheduler is None:
            return
        
        optimizer = self.trainer.ctx.optimizer
        
        # Initialize FL scheduler state if not exists
        if not hasattr(self, '_fl_scheduler_last_round'):
            self._fl_scheduler_last_round = -1
        if not hasattr(self, '_fl_scheduler_current_lr'):
            self._fl_scheduler_current_lr = optimizer.param_groups[0]['lr']
        
        # Only step scheduler once per FL round
        if self.state > self._fl_scheduler_last_round:
            # Handle composite schedulers (like SequentialLR) specially
            if self._is_composite_scheduler(scheduler):
                self._step_composite_scheduler(scheduler)
            else:
                # Set scheduler to correct FL round before stepping
                if hasattr(scheduler, 'last_epoch'):
                    scheduler.last_epoch = self.state - 1  # -1 because step() will increment it
                
                # Step the scheduler (suppress PyTorch warning for FL context)
                import warnings
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="Detected call of.*lr_scheduler.step.*before.*optimizer.step.*")
                    scheduler.step()
            
            # Save the new LR for next round
            new_lr = optimizer.param_groups[0]['lr']
            self._fl_scheduler_current_lr = new_lr
            self._fl_scheduler_last_round = self.state
            
            logger.debug(f"[FL-Scheduler] Client {self.ID}: Stepped scheduler to FL round {self.state}, saved LR: {new_lr:.6e}")
        else:
            logger.debug(f"[FL-Scheduler] Client {self.ID}: Scheduler already stepped for FL round {self.state}")
    
    def _get_scheduler_initial_lr(self, scheduler):
        """
        Get initial learning rate from scheduler, handling composite schedulers
        """
        import torch
        
        if hasattr(scheduler, 'base_lrs') and scheduler.base_lrs:
            return scheduler.base_lrs[0]
        elif hasattr(scheduler, '_schedulers') and scheduler._schedulers:
            # For SequentialLR, get from first sub-scheduler
            first_scheduler = scheduler._schedulers[0]
            if hasattr(first_scheduler, 'base_lrs') and first_scheduler.base_lrs:
                return first_scheduler.base_lrs[0]
        
        # Fallback
        return 0.1
    
    def _is_composite_scheduler(self, scheduler):
        """
        Check if scheduler is a composite scheduler (like SequentialLR)
        """
        import torch
        
        if hasattr(torch.optim.lr_scheduler, 'SequentialLR'):
            return isinstance(scheduler, torch.optim.lr_scheduler.SequentialLR)
        return False
    
    def _step_composite_scheduler(self, scheduler):
        """
        Step composite scheduler with proper epoch synchronization
        """
        import torch
        
        if isinstance(scheduler, torch.optim.lr_scheduler.SequentialLR):
            # For SequentialLR, manually synchronize internal scheduler states
            self._step_sequential_lr(scheduler)
        else:
            # For other composite schedulers, use default method
            if hasattr(scheduler, 'last_epoch'):
                scheduler.last_epoch = self.state - 1
            scheduler.step()
    
    def _step_sequential_lr(self, scheduler):
        """
        Properly step SequentialLR by synchronizing internal scheduler epochs
        """
        target_epoch = self.state
        
        # Set the main scheduler's last_epoch
        scheduler.last_epoch = target_epoch - 1
        
        # Manually synchronize internal scheduler epochs
        current_scheduler_idx = 0
        cumulative_epochs = 0
        
        # Find which sub-scheduler should be active
        for i, milestone in enumerate(scheduler._milestones):
            if target_epoch < milestone:  # Changed <= to < for correct milestone boundary
                current_scheduler_idx = i
                break
            cumulative_epochs = milestone
        else:
            # After all milestones, use last scheduler
            current_scheduler_idx = len(scheduler._schedulers) - 1
            if scheduler._milestones:
                cumulative_epochs = scheduler._milestones[-1]
        
        # Debug: log scheduler selection
        logger.info(f"[FL-Scheduler-Debug] Client {self.ID}: Round {target_epoch}, milestone check: {target_epoch} < {scheduler._milestones} → scheduler_idx={current_scheduler_idx}, cumulative_epochs={cumulative_epochs}")
        
        # Set epoch for the currently active scheduler
        if current_scheduler_idx < len(scheduler._schedulers):
            active_scheduler = scheduler._schedulers[current_scheduler_idx]
            relative_epoch = target_epoch - cumulative_epochs
            
            if hasattr(active_scheduler, 'last_epoch'):
                # Unified handling for all schedulers - use PyTorch's native step() mechanism
                # Special case: LinearLR already applies start_factor during creation (last_epoch=0)
                # So for round 0, we should NOT step it again
                scheduler_type = active_scheduler.__class__.__name__
                if scheduler_type == 'LinearLR' and relative_epoch == 0:
                    # LinearLR was just created and already at epoch 0 with factor applied
                    # Don't reset last_epoch or step for round 0
                    pass  # Will skip the stepping below
                else:
                    # For other cases, set last_epoch and step normally
                    active_scheduler.last_epoch = relative_epoch - 1
                
                # Debug: check scheduler state before step
                scheduler_type = 'LinearLR' if hasattr(active_scheduler, 'start_factor') else 'CosineAnnealingLR'
                if scheduler_type == 'LinearLR':
                    start_factor = getattr(active_scheduler, 'start_factor', 'N/A')
                    total_iters = getattr(active_scheduler, 'total_iters', 'N/A')
                    base_lrs = getattr(active_scheduler, 'base_lrs', 'N/A')
                    logger.info(f"[FL-Debug] Client {self.ID}: Round {target_epoch}, LinearLR config: start_factor={start_factor}, total_iters={total_iters}, base_lrs={base_lrs}")
                
                if hasattr(active_scheduler, 'base_lrs'):
                    logger.info(f"[FL-Debug] Client {self.ID}: base_lrs JUST BEFORE STEP: {active_scheduler.base_lrs}")
          
                
                # Pre-step debugging for LinearLR
                if scheduler_type == 'LinearLR':
                    optimizer_lr_before = self.trainer.ctx.optimizer.param_groups[0]['lr']
                    logger.info(f"[FL-Debug] Client {self.ID}: Optimizer LR BEFORE step: {optimizer_lr_before:.6e}")
                    
                    if hasattr(active_scheduler, 'get_lr'):
                        pytorch_lr_before = active_scheduler.get_lr()
                        logger.info(f"[FL-Debug] Client {self.ID}: PyTorch LinearLR.get_lr() BEFORE step: {pytorch_lr_before}")
                
                logger.info(f"[FL-Debug] Client {self.ID}: Round {target_epoch}, {scheduler_type} before step: last_epoch={getattr(active_scheduler, 'last_epoch', 'N/A')}, relative_epoch={relative_epoch}")
                
                # Step the scheduler to update LR (unless it's LinearLR at round 0)
                should_step = not (scheduler_type == 'LinearLR' and relative_epoch == 0)
                if should_step:
                    import warnings
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", message="Detected call of.*lr_scheduler.step.*before.*optimizer.step.*")
                        active_scheduler.step()
                
                current_lr = self.trainer.ctx.optimizer.param_groups[0]['lr']
                logger.info(f"[FL-Debug] Client {self.ID}: Round {target_epoch}, {scheduler_type} after step: last_epoch={getattr(active_scheduler, 'last_epoch', 'N/A')}, LR={current_lr:.6e}")
                
                # Post-step analysis for LinearLR
                if scheduler_type == 'LinearLR':
                    last_epoch = getattr(active_scheduler, 'last_epoch', 0)
                    expected_factor = start_factor + (1.0 - start_factor) * last_epoch / total_iters
                    expected_lr = base_lrs[0] * expected_factor if base_lrs != 'N/A' else 'Unknown'
                    
                    if hasattr(active_scheduler, 'get_lr'):
                        pytorch_lr_after = active_scheduler.get_lr()
                        logger.info(f"[FL-Debug] Client {self.ID}: PyTorch LinearLR.get_lr() AFTER step: {pytorch_lr_after}")
                    
                    logger.info(f"[FL-Debug] Client {self.ID}: LinearLR expected calculation: epoch={last_epoch}, factor={expected_factor:.6f}, expected_LR={expected_lr}")
        
        # Don't step the SequentialLR again - we've already handled the active sub-scheduler manually
        # The SequentialLR's main logic is just to select which sub-scheduler is active
        scheduler.last_epoch = target_epoch
        
        logger.debug(f"[FL-Scheduler] Client {self.ID}: SequentialLR synchronized to epoch {target_epoch}, active scheduler: {current_scheduler_idx}")
    
    def _restore_composite_scheduler_state(self, scheduler):
        """
        Restore composite scheduler state to current FL round
        """
        import torch
        
        if isinstance(scheduler, torch.optim.lr_scheduler.SequentialLR):
            # For SequentialLR, handle milestone transitions properly
            if self.state > 0:
                self._restore_sequential_lr_state(scheduler)
        else:
            # For other composite schedulers, use stored LR if available
            if hasattr(self, '_fl_next_round_lr') and self.state in self._fl_next_round_lr:
                target_lr = self._fl_next_round_lr[self.state]
                self.trainer.ctx.optimizer.param_groups[0]['lr'] = target_lr
                logger.info(f"[FL-Scheduler] Client {self.ID}: Restored composite scheduler LR to {target_lr:.6e}")
    
    def _restore_sequential_lr_state(self, scheduler):
        """
        Restore SequentialLR state with proper milestone handling
        """
        target_round = self.state
        
        # Determine which scheduler should be active and adjust initial LR accordingly
        current_scheduler_idx = 0
        cumulative_rounds = 0
        
        for i, milestone in enumerate(scheduler._milestones):
            if target_round < milestone:
                current_scheduler_idx = i
                break
            cumulative_rounds = milestone
        else:
            current_scheduler_idx = len(scheduler._schedulers) - 1
            if scheduler._milestones:
                cumulative_rounds = scheduler._milestones[-1]
        
        if current_scheduler_idx >= len(scheduler._schedulers):
            current_scheduler_idx = len(scheduler._schedulers) - 1
        
        # Special handling for phase transitions
        if current_scheduler_idx > 0:
            # We're in phase 2 or later - need to adjust initial LR
            # Calculate what LR should be at the start of this phase
            phase1_end_lr = self._calculate_phase_end_lr(scheduler, 0, scheduler._milestones[0])
            
            # Temporarily adjust optimizer's initial LR for the current phase
            original_lr = self.trainer.ctx.optimizer.param_groups[0]['lr']
            self.trainer.ctx.optimizer.param_groups[0]['lr'] = phase1_end_lr
            
            # Update the active scheduler's base_lrs to match
            active_scheduler = scheduler._schedulers[current_scheduler_idx]
            if hasattr(active_scheduler, 'base_lrs'):
                active_scheduler.base_lrs = [phase1_end_lr]
            
            logger.debug(f"[FL-Scheduler] Client {self.ID}: Adjusted phase {current_scheduler_idx+1} initial LR from {original_lr:.6e} to {phase1_end_lr:.6e}")
        
        # Don't call _step_sequential_lr again to avoid double stepping
        # The scheduler state should be maintained across rounds
        logger.debug(f"[FL-Scheduler] Client {self.ID}: Skipping restore step to maintain scheduler continuity")
        
        current_lr = self.trainer.ctx.optimizer.param_groups[0]['lr']
        logger.info(f"[FL-Scheduler] Client {self.ID}: Restored SequentialLR to FL round {target_round}, LR: {current_lr:.6e}, Phase: {current_scheduler_idx+1}")
    
    def _calculate_phase_end_lr(self, scheduler, phase_idx, phase_end_round):
        """
        Calculate the LR at the end of a specific phase
        """
        import torch
        import math
        
        if phase_idx >= len(scheduler._schedulers):
            return 0.1  # fallback
        
        phase_scheduler = scheduler._schedulers[phase_idx]
        
        if isinstance(phase_scheduler, torch.optim.lr_scheduler.CosineAnnealingLR):
            # Calculate end LR for CosineAnnealingLR
            T_max = phase_scheduler.T_max
            eta_min = phase_scheduler.eta_min
            
            # At the end of the phase (step = T_max), LR should be eta_min
            return eta_min
        else:
            # For other schedulers, fallback to empirical calculation
            # This would require stepping through the scheduler
            return 0.001  # reasonable fallback for phase 1 end
    
    def _reset_sequential_lr_to_initial_state(self, scheduler):
        """
        Reset SequentialLR and its sub-schedulers to initial state
        """
        # Reset main scheduler
        scheduler.last_epoch = 0
        
        # Reset all sub-schedulers
        for sub_scheduler in scheduler._schedulers:
            if hasattr(sub_scheduler, 'last_epoch'):
                sub_scheduler.last_epoch = 0
        
        # Reset optimizer learning rate to initial value
        initial_lr = self._get_scheduler_initial_lr(scheduler)
        self.trainer.ctx.optimizer.param_groups[0]['lr'] = initial_lr
    
    def _get_baseline_params_for_completion(self):
        """
        Get baseline parameters for parameter completion based on configuration
        
        Returns:
            Dict: Baseline parameters for completing missing chunks
        """
        import torch
        
        completion_strategy = getattr(self._cfg.bittorrent, 'parameter_completion', 'global_model')
        logger.info(f"[BT-FL] Client {self.ID}: Using parameter completion strategy: {completion_strategy}")
        
        # Get the reference model (usually the global model)
        # Use trainer's _param_filter to respect cfg.personalization.local_param (e.g., FedBN)
        reference_params = None
        if hasattr(self.trainer, 'ctx') and hasattr(self.trainer.ctx, 'model'):
            reference_params = self.trainer.ctx.model.state_dict()
        elif hasattr(self.trainer, 'model'):
            reference_params = self.trainer.model.state_dict()

        if reference_params is None:
            logger.error(f"[BT-FL] Client {self.ID}: No reference model available for parameter completion")
            return None

        # Filter parameters using trainer's built-in filter (respects cfg.personalization.local_param)
        filtered_reference = self.trainer._param_filter(reference_params)
        if not filtered_reference:
            logger.error(f"[BT-FL] Client {self.ID}: No valid parameters after BN filtering")
            return None
        
        # Select strategy based on configuration
        if completion_strategy == 'global_model':
            # Use global/reference model parameters (default behavior)
            baseline_params = {k: v.clone().detach() for k, v in filtered_reference.items()}
            logger.debug(f"[BT-FL] Client {self.ID}: Using global model for parameter completion")
            
        elif completion_strategy == 'zeros':
            # Use zero-initialized parameters
            baseline_params = {}
            for param_name, param_tensor in filtered_reference.items():
                if hasattr(param_tensor, 'shape'):
                    baseline_params[param_name] = torch.zeros_like(param_tensor, dtype=param_tensor.dtype, device=param_tensor.device)
                else:
                    baseline_params[param_name] = torch.zeros_like(param_tensor)
            logger.info(f"[BT-FL] Client {self.ID}: Using zero initialization for parameter completion")
            
        elif completion_strategy == 'local_model':
            # Use local model parameters (same as global model in most cases)
            # This strategy is useful when local model differs from global model
            baseline_params = {k: v.clone().detach() for k, v in filtered_reference.items()}
            logger.debug(f"[BT-FL] Client {self.ID}: Using local model for parameter completion")
            
        else:
            logger.error(f"[BT-FL] Client {self.ID}: Unknown completion strategy: {completion_strategy}")
            return None
        
        logger.debug(f"[BT-FL] Client {self.ID}: Prepared baseline with {len(baseline_params)} parameters")
        return baseline_params

    def _get_expected_client_sample_sizes(self, round_num):
        """
        Get expected sample sizes for all clients from various sources
        
        Args:
            round_num: Target round number
            
        Returns:
            dict: {client_id: sample_size} or None if unavailable
        """
        expected_sample_sizes = {}
        
        try:
            # Method 1: Try to get from chunk manager cache/history
            if hasattr(self, 'chunk_manager') and hasattr(self.chunk_manager, '_sample_sizes'):
                # Look for sample sizes from any available round (assuming stable data split)
                for past_round in range(max(0, round_num - 5), round_num + 1):  # Check recent rounds
                    if past_round in self.chunk_manager._sample_sizes:
                        expected_sample_sizes.update(self.chunk_manager._sample_sizes[past_round])
                        
            # Method 2: Try to get from local data if this client has data
            if hasattr(self, 'data') and self.data is not None:
                if hasattr(self.data, 'train') and self.data.train is not None:
                    local_sample_size = len(self.data.train)
                    expected_sample_sizes[self.ID] = local_sample_size
                    
            # Method 3: Try to get from config if available
            if hasattr(self._cfg, 'data') and hasattr(self._cfg.data, 'client_sample_sizes'):
                expected_sample_sizes.update(self._cfg.data.client_sample_sizes)
                
            if expected_sample_sizes:
                logger.debug(f"[BT-FL] Client {self.ID}: Found expected sample sizes for {len(expected_sample_sizes)} clients")
                return expected_sample_sizes
            else:
                logger.debug(f"[BT-FL] Client {self.ID}: No expected sample sizes found, will use average estimation")
                return None
                
        except Exception as e:
            logger.warning(f"[BT-FL] Client {self.ID}: Failed to get expected client sample sizes: {e}")
            return None

    def _apply_post_aggregation_compensation(self, aggregated_model, completed_client_models, baseline_params):
        """
        Apply post-aggregation parameter-level compensation to fix bias from non-uniform chunk selection
        
        Algorithm:
        1. Track participation rate ρ_k for each parameter k across all clients  
        2. Apply unbiased correction: compensated_k = (fed_k - base_k) / ρ_k + base_k
        3. Add stabilizers: EMA momentum and relative norm clipping
        
        Args:
            aggregated_model: Raw aggregated model from aggregator
            completed_client_models: List of (sample_size, model_params) tuples
            baseline_params: Baseline parameters for reference
            
        Returns:
            Dict: Compensated model parameters
        """
        if not completed_client_models or not baseline_params:
            return aggregated_model
            
        from collections import defaultdict
        import torch
        
        logger.debug(f"[BT-FL] Client {self.ID}: Applying post-aggregation compensation")
        
        # Configuration parameters with round-based dynamic adjustment
        eps = 1e-12
        alpha_cap = 1.0 
        rho_floor = 1e-13
        # Round-based parameter adjustment
        
        current_round = self.state
        if current_round < 9:  
            c_max = 3
            # clip_ratio = 0.16  # rel_clip
            # ema_beta = 0.50    # beta
            clip_ratio = 1.16  # rel_clip
            ema_beta = 0.0    # beta
            # alpha_cap=0.70
            rho_floor=0.12
        elif current_round < 20:  
            c_max = 2.5
            # clip_ratio = 0.12
            clip_ratio = 1.16  # rel_clip
            ema_beta = 0.00
        # elif current_round < 51:  
        #     c_max = 2.5
        #     clip_ratio = 0.08
        #     ema_beta = 0.80
        else:  
            c_max = 2.5
            # clip_ratio = 0.03
            # ema_beta = 0.90
            clip_ratio = 1.16  # rel_clip
            ema_beta = 0.0
            
        # c_max = 1.0
        # clip_ratio = 1000.16  
        # ema_beta = 0.0
        # # Step 1: Calculate participation weights for each parameter
        present_weight = defaultdict(float)  # k -> sum n_i * covered_len_i,k / total_len_k  
        total_weight = 0.0  # sum n_i
        
        # Extract real client models (exclude baseline entry if present)
        real_client_models = []
        baseline_weight = 0.0
        
        for sample_size, model_params, client_id, coverage_ratios in completed_client_models:
            # Check if this is a baseline entry (client_id = -1)
            is_baseline_entry = (client_id == -1)
            
            if not is_baseline_entry:
                real_client_models.append((sample_size, model_params, client_id, coverage_ratios))
                total_weight += sample_size
            else:
                baseline_weight += sample_size
                logger.debug(f"[BT-FL] Client {self.ID}: Detected baseline entry with weight {sample_size}")
        
        if not real_client_models:
            logger.warning(f"[BT-FL] Client {self.ID}: No real client models found for compensation")
            return aggregated_model
            
        # Calculate participation weights based on actual chunk coverage from chunk_manager
        
        for sample_size, model_params, client_id, coverage_ratios in real_client_models:
            for param_name in baseline_params.keys():
                if param_name in model_params and param_name in aggregated_model:
                    # 🔧 CRITICAL FIX: Use REAL coverage ratio from chunk reconstruction
                    coverage_ratio = coverage_ratios.get(param_name, 0.0)
                    
                    if coverage_ratio > eps:  # Has actual chunk coverage
                        present_weight[param_name] += sample_size * coverage_ratio
                        logger.debug(f"[BT-FL] Client {self.ID}: {param_name} REAL coverage={coverage_ratio:.3f} for client {client_id}")
        
        # Step 2: Apply parameter-level compensation
        compensated_model = {}
        
        # 🔧 CRITICAL FIX: Include absent client weights in denominator
        N_total = total_weight + baseline_weight  # Total expected sample weight
        
        for param_name, fed_param in aggregated_model.items():
            if param_name not in baseline_params:
                compensated_model[param_name] = fed_param
                continue
            
            # 🔧 BN filtering double protection
            if self._is_bn_key(param_name):
                compensated_model[param_name] = fed_param
                logger.debug(f"[BT-FL] Client {self.ID}: Skipping BN parameter {param_name}")
                continue
                
            # Get baseline reference
            base_param = baseline_params[param_name].detach().cpu().float()
            fed_param_cpu = fed_param.detach().cpu().float()
            
            # Calculate participation rate ρ_k with correct denominator
            param_present_weight = present_weight.get(param_name, 0.0)
            rho = param_present_weight / max(N_total, eps)
            
            if rho <= eps:
                # No participation detected, use baseline
                new_param = base_param
                logger.debug(f"[BT-FL] Client {self.ID}: {param_name}: rho=0, using baseline")
            else:
                # Apply compensation with capping
                alpha = min(alpha_cap, c_max * rho)  # Cap the compensation factor
                # if rho < rho_floor:
                #     alpha *= (rho / rho_floor)
                
                # Unbiased correction: (fed - base) / ρ  
                delta = (fed_param_cpu - base_param) / rho
                
                # 🔧 Apply EMA momentum (with persistent state)
                if not hasattr(self, '_postcomp_momentum'):
                    self._postcomp_momentum = {}
                
                momentum_key = f"{param_name}"
                if momentum_key not in self._postcomp_momentum:
                    # Initialize momentum with zeros
                    self._postcomp_momentum[momentum_key] = torch.zeros_like(delta)
                
                # EMA update: v = β*v + (1-β)*delta
                v = self._postcomp_momentum[momentum_key]
                v = v * ema_beta + delta * (1 - ema_beta)
                self._postcomp_momentum[momentum_key] = v
                
                # Use momentum-smoothed delta
                delta = v
                
                # Apply relative norm clipping for stability (after EMA smoothing)
                if clip_ratio > 0:
                    base_norm = torch.norm(base_param)
                    delta_norm = torch.norm(delta)
                    # 🔧 Improved clipping threshold with lower bound
                    max_delta_norm = clip_ratio * max(base_norm.item(), 1e-3)
                    
                    if delta_norm > max_delta_norm and max_delta_norm > eps:
                        clip_factor = max_delta_norm / delta_norm
                        delta = delta * clip_factor
                        logger.debug(f"[BT-FL] Client {self.ID}: {param_name}: Clipped delta by factor {clip_factor:.4f}")
                
                # Apply compensated update
                new_param = base_param + alpha * delta
                
                logger.debug(f"[BT-FL] Client {self.ID}: {param_name}: rho={rho:.4f}, alpha={alpha:.4f}")
            
            # Convert back to original device and dtype
            final_param = new_param.to(fed_param.device, dtype=fed_param.dtype)
            compensated_model[param_name] = final_param
        
        logger.info(f"[BT-FL] Client {self.ID}: Applied post-aggregation compensation to {len(compensated_model)} parameters")
        return compensated_model
    
    def _is_bn_key(self, param_name):
        """
        Check if a parameter key belongs to BatchNorm layers
        """
        bn_keywords = [
            'bn', 'batch_norm', 'batchnorm', 'norm',
            'running_mean', 'running_var', 'num_batches_tracked'
        ]
        param_lower = param_name.lower()
        
        # Check for common BN patterns
        for keyword in bn_keywords:
            if keyword in param_lower:
                return True
                
        # Check for numbered BN layers (e.g., bn1, bn2, etc.)
        import re
        if re.search(r'\bbn\d+\b', param_lower):
            return True
            
        return False
    
    def _is_baseline_entry(self, model_params, baseline_params):
        """
        Check if a model_params entry is actually the baseline entry by comparing values
        """
        import torch
        
        if len(model_params) != len(baseline_params):
            return False
            
        # Sample a few parameters to check similarity
        check_count = 0
        similar_count = 0
        eps = 1e-6
        
        for param_name in list(baseline_params.keys())[:3]:  # Check first 3 parameters
            if param_name in model_params:
                base_param = baseline_params[param_name].detach().cpu().float()
                model_param = model_params[param_name].detach().cpu().float()
                
                # Check if they're very similar (within numerical precision)
                diff = torch.abs(base_param - model_param)
                relative_diff = torch.mean(diff) / (torch.mean(torch.abs(base_param)) + eps)
                
                if relative_diff < eps:
                    similar_count += 1
                check_count += 1
        
        # If most checked parameters are very similar, it's likely a baseline entry
        return similar_count >= check_count * 0.8 if check_count > 0 else False

    def _aggregate_model_from_chunks(self, round_num, aggregator_type='fedavg'):
        """
        Aggregate model parameters from chunk database using built-in aggregators
        with parameter completion for missing client contributions
        
        Args:
            round_num: Target round
            aggregator_type: Type of aggregator to use ('fedavg', 'krum', 'median', etc.)
            
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
                        change_callback=self._send_chunk_info_to_server,
                        cfg=self._cfg
                    )
                    logger.info(f"[BT-FL] Client {self.ID}: Successfully initialized chunk_manager for aggregation")
                except Exception as e:
                    logger.error(f"[BT-FL] Client {self.ID}: Failed to initialize chunk_manager: {e}")
                    return None
                
            logger.info(f"[BT-FL] Client {self.ID}: Starting model aggregation using {aggregator_type} aggregator for round {round_num}")
            
            # Get all available client model chunks
            available_clients = self.chunk_manager.get_available_clients_for_round(round_num)
            if not available_clients:
                logger.warning(f"[BT-FL] Client {self.ID}: No client models available in chunk database for round {round_num}")
                return None
                
            logger.info(f"[BT-FL] Client {self.ID}: Found models from {len(available_clients)} clients: {available_clients}")
            
            # Prepare baseline parameters based on completion strategy
            baseline_params = self._get_baseline_params_for_completion()
            if baseline_params is None:
                logger.error(f"[BT-FL] Client {self.ID}: Failed to prepare baseline parameters for completion")
                return None
            
            # Collect and complete model parameters from all clients
            completed_client_models = []
            
            # Get expected sample sizes for all clients (real data split info)
            expected_sample_sizes = self._get_expected_client_sample_sizes(round_num)
            
            # Calculate available clients' actual sample sizes
            available_sample_sizes = {}
            total_available_samples = 0
            
            for client_id in available_clients:
                try:
                    sample_size = self.chunk_manager.get_client_sample_size(client_id, round_num)
                    if sample_size is not None:
                        available_sample_sizes[client_id] = sample_size
                        total_available_samples += sample_size
                except Exception as e:
                    logger.warning(f"[BT-FL] Client {self.ID}: Failed to get sample size for client {client_id}: {e}")
            
            if not available_sample_sizes:
                logger.error(f"[BT-FL] Client {self.ID}: No valid sample sizes found")
                return None
            
            # Expected total clients (from config or estimated from expected_sample_sizes)
            if expected_sample_sizes:
                expected_total_clients = len(expected_sample_sizes)
                logger.info(f"[BT-FL] Client {self.ID}: Using real data split info for {expected_total_clients} expected clients")
            else:
                expected_total_clients = getattr(self._cfg.federate, 'sample_client_num', len(available_clients))
                avg_sample_size = total_available_samples / len(available_sample_sizes)
                logger.info(f"[BT-FL] Client {self.ID}: Using average sample size estimation: {avg_sample_size:.1f}")
            
            # Process available clients with parameter completion
            for client_id in available_clients:
                try:
                    # 🔧 CRITICAL FIX: Get model params WITH coverage information
                    result = self.chunk_manager.reconstruct_with_coverage_info(client_id, round_num, baseline_params)
                    sample_size = available_sample_sizes.get(client_id)
                    
                    if result is not None and sample_size is not None:
                        model_params, coverage_ratios = result
                        
                        # Parameter completion and BN filtering now handled in chunk_manager
                        # Just ensure device/dtype consistency for aggregator
                        normalized_params = {}
                        for param_name, param_value in model_params.items():
                            if hasattr(param_value, 'cpu') and hasattr(param_value, 'float'):
                                normalized_params[param_name] = param_value.cpu().float()
                            else:
                                normalized_params[param_name] = param_value
                        
                        # 🔧 CRITICAL FIX: Preserve client_id AND coverage_ratios for real compensation
                        completed_client_models.append((sample_size, normalized_params, client_id, coverage_ratios))
                        logger.debug(f"[BT-FL] Client {self.ID}: Completed model for client {client_id} with {sample_size} samples and coverage info")
                    else:
                        logger.warning(f"[BT-FL] Client {self.ID}: Failed to reconstruct model for available client {client_id}")
                        
                except Exception as e:
                    logger.error(f"[BT-FL] Client {self.ID}: Failed to process client {client_id}: {e}")
            
            # Calculate absent clients and merge their baseline contribution (only for FedAvg)
            if expected_sample_sizes:
                # Use real expected client IDs and their true sample sizes
                expected_client_ids = set(expected_sample_sizes.keys())
                available_client_ids = set(available_clients)
                absent_client_ids = expected_client_ids - available_client_ids
                
                if absent_client_ids and aggregator_type.lower() == 'fedavg':
                    # Calculate true total sample weight for absent clients
                    total_absent_sample_weight = sum(expected_sample_sizes[cid] for cid in absent_client_ids)
                    # Normalize baseline parameters to CPU float32 for aggregator
                    baseline_params_copy = {}
                    for k, v in baseline_params.items():
                        if hasattr(v, 'cpu') and hasattr(v, 'float'):
                            baseline_params_copy[k] = v.clone().detach().cpu().float()
                        else:
                            baseline_params_copy[k] = v.clone().detach()
                    # Create dummy coverage for baseline (0.0 for all params since absent clients contribute nothing)
                    baseline_coverage = {param_name: 0.0 for param_name in baseline_params_copy.keys()}
                    completed_client_models.append((total_absent_sample_weight, baseline_params_copy, -1, baseline_coverage))
                    logger.info(f"[BT-FL] Client {self.ID}: Added baseline entry for {len(absent_client_ids)} absent clients with true total weight {total_absent_sample_weight}")
                    logger.debug(f"[BT-FL] Client {self.ID}: Absent clients: {sorted(absent_client_ids)}")
                elif absent_client_ids:
                    logger.info(f"[BT-FL] Client {self.ID}: Skipping baseline injection for {aggregator_type} aggregator ({len(absent_client_ids)} absent clients)")
                    logger.info(f"[BT-FL] Client {self.ID}: Robust aggregators work with available clients only to preserve geometric properties")
            else:
                # Fallback to estimation when real sample sizes unavailable
                num_absent_clients = expected_total_clients - len(available_clients)
                if num_absent_clients > 0 and aggregator_type.lower() == 'fedavg':
                    # Use average estimation as fallback
                    total_absent_sample_weight = num_absent_clients * avg_sample_size
                    # Normalize baseline parameters to CPU float32 for aggregator
                    baseline_params_copy = {}
                    for k, v in baseline_params.items():
                        if hasattr(v, 'cpu') and hasattr(v, 'float'):
                            baseline_params_copy[k] = v.clone().detach().cpu().float()
                        else:
                            baseline_params_copy[k] = v.clone().detach()
                    # Create dummy coverage for baseline (0.0 for all params since absent clients contribute nothing)
                    baseline_coverage = {param_name: 0.0 for param_name in baseline_params_copy.keys()}
                    completed_client_models.append((total_absent_sample_weight, baseline_params_copy, -1, baseline_coverage))
                    logger.info(f"[BT-FL] Client {self.ID}: Added baseline entry for {num_absent_clients} absent clients with estimated total weight {total_absent_sample_weight:.1f}")
                    logger.warning(f"[BT-FL] Client {self.ID}: Using average sample size estimation (real data split info unavailable)")
                elif num_absent_clients > 0:
                    logger.info(f"[BT-FL] Client {self.ID}: Skipping baseline injection for {aggregator_type} aggregator ({num_absent_clients} absent clients)")
                    logger.info(f"[BT-FL] Client {self.ID}: Robust aggregators work with available clients only to preserve geometric properties")
            
            if not completed_client_models:
                logger.error(f"[BT-FL] Client {self.ID}: No valid completed client models for aggregation")
                return None
                
            if aggregator_type.lower() == 'fedavg':
                logger.info(f"[BT-FL] Client {self.ID}: Prepared {len(completed_client_models)} models ({len(available_clients)} real + baseline) for {aggregator_type} aggregation")
            else:
                logger.info(f"[BT-FL] Client {self.ID}: Prepared {len(completed_client_models)} available client models for {aggregator_type} aggregation (parameter-completed)")
            
            # Create and use built-in aggregator
            from federatedscope.core.auxiliaries.aggregator_builder import get_aggregator
            
            # Get a reference model for aggregator initialization
            reference_model = None
            if hasattr(self.trainer, 'ctx') and hasattr(self.trainer.ctx, 'model'):
                reference_model = self.trainer.ctx.model
            elif hasattr(self.trainer, 'model'):
                reference_model = self.trainer.model
            
            aggregator = get_aggregator(
                method=aggregator_type,
                model=reference_model,
                device=self.device,
                config=self._cfg
            )
            
            # Prepare aggregation info (strip client_id and coverage for aggregator)
            agg_info = {
                "client_feedback": [(sample_size, model_params) for sample_size, model_params, _, _ in completed_client_models]
            }
            
            # Perform aggregation (on CPU with float32)
            aggregated_model = aggregator.aggregate(agg_info)

            # 🔧 Post-aggregation parameter-level compensation (only if enabled in config)
            enable_compensation = self._cfg.bittorrent.enable_compensation if hasattr(self._cfg, 'bittorrent') and hasattr(self._cfg.bittorrent, 'enable_compensation') else True
            if enable_compensation and aggregated_model is not None and isinstance(aggregated_model, dict):
                logger.debug(f"[BT-FL] Client {self.ID}: Post-aggregation compensation enabled, applying correction")
                aggregated_model = self._apply_post_aggregation_compensation(
                    aggregated_model, completed_client_models, baseline_params
                )
            elif not enable_compensation:
                logger.debug(f"[BT-FL] Client {self.ID}: Post-aggregation compensation disabled, skipping correction")
            
            # Move aggregated results back to target device and original dtype if needed
            if aggregated_model is not None and isinstance(aggregated_model, dict):
                target_device = self.device
                # Get original dtypes from baseline_params for reference
                final_model = {}
                for param_name, param_value in aggregated_model.items():
                    if hasattr(param_value, 'to'):
                        # Get target dtype from baseline if available
                        target_dtype = None
                        if param_name in baseline_params and hasattr(baseline_params[param_name], 'dtype'):
                            target_dtype = baseline_params[param_name].dtype
                        
                        # Move to target device and dtype
                        final_param = param_value.to(target_device)
                        if target_dtype is not None and final_param.dtype != target_dtype:
                            final_param = final_param.to(target_dtype)
                        final_model[param_name] = final_param
                    else:
                        final_model[param_name] = param_value
                aggregated_model = final_model
            
            logger.info(f"[BT-FL] Client {self.ID}: Successfully aggregated model using {aggregator_type} with {len(completed_client_models)} clients")
            return aggregated_model
            
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
                    change_callback=self._send_chunk_info_to_server,
                    cfg=self._cfg
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

        # 🚀 CRITICAL FIX: Register own chunks to memory AFTER bt_manager is created
        # Query chunk_manager for own chunks and register them to bt_manager
        chunks_per_client = self._cfg.chunk.num_chunks if hasattr(self._cfg, 'chunk') else 16
        own_chunk_keys = [(round_num, self.ID, i) for i in range(chunks_per_client)]
        self.bt_manager.register_own_chunks(own_chunk_keys)

        # Initialize memory-based chunk counters
        own_chunks_count = len(own_chunk_keys)
        total_clients = self._cfg.federate.client_num if hasattr(self._cfg, 'federate') else 50
        expected_total = total_clients * chunks_per_client

        self.bt_manager.set_own_chunks_count(own_chunks_count)
        self.bt_manager.set_expected_total_chunks(expected_total)
        logger.info(f"[BT] Client {self.ID}: Memory counter initialized - own={own_chunks_count}, expected_total={expected_total}")

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
        """
        🚀 EVENT-DRIVEN: BitTorrent exchange main loop

        This loop is driven by events:
        1. BITFIELD/HAVE messages trigger request queue entries
        2. Main loop processes request queue and sends REQUESTs
        3. PIECE reception triggers HAVE broadcast (in callback)
        4. Timeout mechanism retries failed requests

        No more busy-polling or active request filling.
        """
        logger.info(f"[BT] Client {self.ID}: Event-driven exchange loop started, expected_chunks={expected_chunks}")
        try:
            import time
            last_progress_time = time.time()
            last_unchoke_time = time.time()
            last_bitfield_refresh = 0

            # Use memory-based counter instead of DB queries to avoid SQLite lock contention
            BITFIELD_REFRESH_INTERVAL = 5.0
            cached_bitfield = {}

            # 🚀 CRITICAL FIX: Initialize final_chunks to track early capture
            # When is_stopped is detected, we capture the count immediately before it's cleared
            final_chunks = None

            while True:
                current_time = time.time()

                # Use memory-based counter (instant, no DB query)
                current_chunks = self.bt_manager.get_memory_chunk_count()
                has_all = current_chunks >= expected_chunks

                # 🚀 CRITICAL FIX: Use memory bitfield instead of DB query to avoid SQLite lock
                # This prevents process freeze caused by SQLite lock contention
                if current_time - last_bitfield_refresh >= BITFIELD_REFRESH_INTERVAL:
                    try:
                        # Use memory-based bitfield (instant, no DB query)
                        cached_bitfield = self.bt_manager.get_memory_bitfield()
                        last_bitfield_refresh = time.time()
                    except Exception as e:
                        logger.debug(f"[BT] Client {self.ID}: Memory bitfield failed: {e}")

                if has_all:
                    break

                # Check if exchange was stopped
                if hasattr(self.bt_manager, 'is_stopped') and self.bt_manager.is_stopped:
                    # 🚀 CRITICAL FIX: Use final_chunk_count saved by stop_exchange() BEFORE clearing
                    # stop_exchange() saves the count to final_chunk_count before setting is_stopped=True
                    # and before clearing the counters, so this is guaranteed to be the correct value
                    final_chunks = getattr(self.bt_manager, 'final_chunk_count', 0)
                    logger.info(f"[BT] Client {self.ID}: BitTorrent exchange stopped, using saved final_chunks={final_chunks}")
                    break

                if not self.bt_manager:
                    final_chunks = 0
                    logger.info(f"[BT] Client {self.ID}: BitTorrent manager gone, exiting")
                    break

                current_time = time.time()

                # Process request triggers using cached bitfield
                requests_sent = self.bt_manager.process_request_triggers(max_requests=20, cached_bitfield=cached_bitfield)

                # Check timeouts (handles retries)
                self.bt_manager.check_timeouts()

                # Periodic unchoke (every 10 seconds)
                if current_time - last_unchoke_time >= 10.0:
                    self.bt_manager._regular_unchoke_algorithm()
                    last_unchoke_time = current_time

                # Progress logging (every 5 seconds)
                if current_time - last_progress_time >= 5.0:
                    pending_count = len(self.bt_manager.pending_requests)
                    queue_size = self.bt_manager._request_trigger_queue.qsize()
                    logger.info(f"[BT] Client {self.ID}: Progress {current_chunks}/{expected_chunks}, pending={pending_count}, queue={queue_size}")
                    last_progress_time = current_time

                # Wait for events instead of busy-polling
                if requests_sent == 0 and self.bt_manager._request_trigger_queue.empty():
                    time.sleep(0.5)
                else:
                    time.sleep(0.01)
                
            # 🚀 CRITICAL FIX: Record chunk count IMMEDIATELY before any other operation
            # This prevents race condition where main thread calls stop_exchange() and clears counters
            # Note: final_chunks may have been set in the loop when is_stopped was detected
            if final_chunks is None:
                final_chunks = self.bt_manager.get_memory_chunk_count()
            final_downloaded = getattr(self.bt_manager, 'total_downloaded', 0)
            final_uploaded = getattr(self.bt_manager, 'total_uploaded', 0)
            logger.info(f"[BT] Client {self.ID}: Exchange loop completed. Final chunks: {final_chunks}/{expected_chunks}")

            #Clear old file files
            keep_rounds = getattr(self._cfg, 'chunk_keep_rounds', 2) if hasattr(self, '_cfg') else 2

            self.chunk_manager.cleanup_old_rounds(keep_rounds=keep_rounds, current_round=self.bt_manager.round_num)

            # 4. Report to Server after completion (pass saved values to avoid race condition)
            self._report_bittorrent_completion(chunks_collected=final_chunks)
            
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
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            self.bt_message_buffer.append(('request', message))
            return

        self.bt_manager.handle_request(
            message.sender,
            message.content['round_num'],
            message.content['source_client_id'],
            message.content['chunk_id']
        )

    def callback_funcs_for_piece(self, message):
        """Handle chunk data"""
        if not hasattr(self, 'bt_manager') or self.bt_manager is None:
            self.bt_message_buffer.append(('piece', message))
            return

        self.bt_manager.handle_piece(
            message.sender,
            message.content['round_num'],
            message.content['source_client_id'],
            message.content['chunk_id'],
            message.content['data'],
            message.content['checksum']
        )
        
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
        """Check if all chunks have been collected"""
        if not hasattr(self, 'bt_manager'):
            return False

        # 🚀 CRITICAL FIX: Use memory counter (NO DB QUERY!)
        current_chunks = self.bt_manager.get_memory_chunk_count()
        return current_chunks >= expected_chunks

    def _report_bittorrent_completion(self, chunks_collected: int = None):
        """
        Report BitTorrent exchange completion to Server

        🚀 CRITICAL FIX: Accept chunks_collected as parameter to avoid race condition.
        The main thread may call stop_exchange() which clears the counter before
        this function reads it. By passing the value from the caller (who saved it
        before stop_exchange could be called), we ensure correct reporting.
        """
        # 🐛 Bug Fix 31: Safely get statistics information
        # 🚀 CRITICAL FIX: Use passed parameter if available, otherwise fallback to counter
        if chunks_collected is None:
            chunks_collected = self.bt_manager.get_memory_chunk_count() if hasattr(self, 'bt_manager') else 0

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
