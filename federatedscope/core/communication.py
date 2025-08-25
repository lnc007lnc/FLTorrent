import grpc
from concurrent import futures
import logging
import torch.distributed as dist

from collections import deque

from federatedscope.core.proto import gRPC_comm_manager_pb2, \
    gRPC_comm_manager_pb2_grpc
from federatedscope.core.gRPC_server import gRPCComServeFunc
from federatedscope.core.message import Message

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class StandaloneCommManager(object):
    """
    The communicator used for standalone mode
    """
    def __init__(self, comm_queue, monitor=None):
        self.comm_queue = comm_queue
        self.neighbors = dict()
        self.monitor = monitor  # used to track the communication related
        # metrics

    def receive(self):
        # we don't need receive() in standalone
        pass

    def add_neighbors(self, neighbor_id, address=None):
        self.neighbors[neighbor_id] = address

    def get_neighbors(self, neighbor_id=None):
        address = dict()
        if neighbor_id:
            if isinstance(neighbor_id, list):
                for each_neighbor in neighbor_id:
                    address[each_neighbor] = self.get_neighbors(each_neighbor)
                return address
            else:
                return self.neighbors[neighbor_id]
        else:
            # Get all neighbors
            return self.neighbors

    def send(self, message):
        # All the workers share one comm_queue
        self.comm_queue.append(message)


class StandaloneDDPCommManager(StandaloneCommManager):
    """
    The communicator used for standalone mode with multigpu
    """
    def __init__(self, comm_queue, monitor=None, id2comm=None):
        super().__init__(comm_queue, monitor)
        self.id2comm = id2comm
        self.device = "cuda:{}".format(dist.get_rank())

    def _send_model_para(self, model_para, dst_rank):
        for v in model_para.values():
            t = v.to(self.device)
            dist.send(tensor=t, dst=dst_rank)

    def send(self, message):
        is_model_para = message.msg_type == 'model_para'
        is_evaluate = message.msg_type == 'evaluate'
        if self.id2comm is None:
            # client to server
            if is_model_para:
                model_para = message.content[1]
                message.content = (message.content[0], {})
                self.comm_queue.append(message) if isinstance(
                    self.comm_queue, deque) else self.comm_queue.put(message)
                self._send_model_para(model_para, 0)
            else:
                self.comm_queue.append(message) if isinstance(
                    self.comm_queue, deque) else self.comm_queue.put(message)
        else:
            receiver = message.receiver
            if not isinstance(receiver, list):
                receiver = [receiver]
            if is_model_para or is_evaluate:
                model_para = message.content
                message.content = {}
            for idx, each_comm in enumerate(self.comm_queue):
                for each_receiver in receiver:
                    if each_receiver in self.neighbors and \
                            self.id2comm[each_receiver] == idx:
                        each_comm.put(message)
                        break
                if is_model_para or is_evaluate:
                    for each_receiver in receiver:
                        if each_receiver in self.neighbors and \
                                self.id2comm[each_receiver] == idx:
                            self._send_model_para(model_para, idx + 1)
                            break
        download_bytes, upload_bytes = message.count_bytes()
        self.monitor.track_upload_bytes(upload_bytes)


class gRPCCommManager(object):
    """
        The implementation of gRPCCommManager is referred to the tutorial on
        https://grpc.io/docs/languages/python/
    """
    def __init__(self, host='0.0.0.0', port='50050', client_num=2, cfg=None):
        # Docker three-segment address support: bind IP|report IP|report port
        logger.info(f"🔍 gRPCCommManager initialize - original host: '{host}', port: {port}")
        
        # Handle host address format
        if '|' in host:
            # Docker three-segment format: bind IP|report IP|report port
            parts = host.split('|')
            bind_host, report_host, report_port_str = parts
            self.bind_host = bind_host    # Actual bind address
            self.report_host = report_host  # Address reported to other entities
            self.host = report_host  # local_address uses report address
            self.report_port = int(report_port_str)  # Port reported to other entities
            self.port = self.report_port  # local_address uses report port
            logger.info(f"🐳 Docker three-segment mode - bind: '{bind_host}:{port}', report: '{report_host}:{self.report_port}'")
        else:
            # Non-Docker single address mode
            self.bind_host = host
            self.report_host = host
            self.host = host
            self.report_port = int(port)
            self.port = int(port)
            logger.info(f"📡 Non-Docker mode - address: '{host}:{port}'")
        
        # Bind port always uses port parameter from configuration
        self.bind_port = int(port)
        logger.info(f"✅ Final settings - bind: '{self.bind_host}:{self.bind_port}', report: '{self.host}:{self.port}'")
        options = [
            ("grpc.max_send_message_length", cfg.grpc_max_send_message_length),
            ("grpc.max_receive_message_length",
             cfg.grpc_max_receive_message_length),
            ("grpc.enable_http_proxy", cfg.grpc_enable_http_proxy),
        ]

        if cfg.grpc_compression.lower() == 'deflate':
            self.comp_method = grpc.Compression.Deflate
        elif cfg.grpc_compression.lower() == 'gzip':
            self.comp_method = grpc.Compression.Gzip
        else:
            self.comp_method = grpc.Compression.NoCompression

        self.server_funcs = gRPCComServeFunc()
        self.grpc_server = self.serve(max_workers=client_num,
                                      host=self.bind_host,  # Use bind address to start server
                                      port=self.bind_port,  # Use bind port to start server
                                      options=options)
        self.neighbors = dict()
        self.monitor = None  # used to track the communication related metrics
        self.connection_monitor = None  # Will be set by the client

    def serve(self, max_workers, host, port, options):
        """
        This function is referred to
        https://grpc.io/docs/languages/python/basics/#starting-the-server
        """
        logger.info(f"🚀 gRPC server startup - bind address: {host}:{port}")
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=max_workers),
            compression=self.comp_method,
            options=options)
        gRPC_comm_manager_pb2_grpc.add_gRPCComServeFuncServicer_to_server(
            self.server_funcs, server)
        # Ensure host does not contain port number
        if ':' in host:
            logger.error(f"❌ Host address contains port number: {host}")
            # If host already contains port number, use directly
            bind_address = host
        else:
            bind_address = "{}:{}".format(host, port)
        logger.info(f"📍 gRPC attempting to bind to: {bind_address}")
        server.add_insecure_port(bind_address)
        server.start()

        return server

    def add_neighbors(self, neighbor_id, address):
        if isinstance(address, dict):
            self.neighbors[neighbor_id] = '{}:{}'.format(
                address['host'], address['port'])
        elif isinstance(address, str):
            self.neighbors[neighbor_id] = address
        else:
            raise TypeError(f"The type of address ({type(address)}) is not "
                            "supported yet")

    def get_neighbors(self, neighbor_id=None):
        address = dict()
        if neighbor_id:
            if isinstance(neighbor_id, list):
                for each_neighbor in neighbor_id:
                    address[each_neighbor] = self.get_neighbors(each_neighbor)
                return address
            else:
                return self.neighbors[neighbor_id]
        else:
            # Get all neighbors
            return self.neighbors

    def _send(self, receiver_address, message):
        def _create_stub(receiver_address):
            """
            This part is referred to
            https://grpc.io/docs/languages/python/basics/#creating-a-stub
            """
            channel = grpc.insecure_channel(receiver_address,
                                            compression=self.comp_method,
                                            options=(('grpc.enable_http_proxy',
                                                      0), ))
            stub = gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub(channel)
            return stub, channel

        stub, channel = _create_stub(receiver_address)
        request = message.transform(to_list=True)
        try:
            stub.sendMessage(request)
            # Notify connection monitor about successful send (if available)
            if hasattr(self, 'connection_monitor') and self.connection_monitor:
                # This indicates connection is active
                pass
        except grpc._channel._InactiveRpcError as error:
            logger.warning(f"Connection error to {receiver_address}: {error}")
            # Notify connection monitor about connection failure
            if hasattr(self, 'connection_monitor') and self.connection_monitor:
                from federatedscope.core.connection_monitor import ConnectionEvent
                self.connection_monitor.report_connection_lost(
                    peer_id=message.receiver,
                    details={
                        'error_type': type(error).__name__,
                        'error_message': str(error),
                        'receiver_address': receiver_address,
                        'message_type': message.msg_type
                    }
                )
            pass
        channel.close()

    def send(self, message):
        receiver = message.receiver
        if receiver is not None:
            if not isinstance(receiver, list):
                receiver = [receiver]
            for each_receiver in receiver:
                if each_receiver in self.neighbors:
                    receiver_address = self.neighbors[each_receiver]
                    self._send(receiver_address, message)
        else:
            for each_receiver in self.neighbors:
                receiver_address = self.neighbors[each_receiver]
                self._send(receiver_address, message)

    def receive(self):
        received_msg = self.server_funcs.receive()
        message = Message()
        message.parse(received_msg.msg)
        return message
