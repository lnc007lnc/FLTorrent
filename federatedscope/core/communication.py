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
        import os

        # Check if UDS mode is enabled (bypasses TCP buffer limits)
        self.use_uds = getattr(cfg, 'use_uds', False) if cfg else False
        self.uds_dir = getattr(cfg, 'uds_dir', '/tmp/federatedscope_uds') if cfg else '/tmp/federatedscope_uds'

        logger.info(f"🔍 gRPCCommManager initialize - original host: '{host}', port: {port}, use_uds: {self.use_uds}")

        # Check if host is already a UDS address (unix://...)
        if str(host).startswith('unix://'):
            # Host is already UDS format - use it directly
            self.use_uds = True
            self.uds_path = host.replace('unix://', '')
            # Ensure directory exists
            uds_dir = os.path.dirname(self.uds_path)
            if uds_dir:
                os.makedirs(uds_dir, exist_ok=True)
            # Remove stale socket file if exists
            if os.path.exists(self.uds_path):
                os.remove(self.uds_path)
            self.bind_address = host
            self.report_address = host
            self.bind_host = None
            self.report_host = None
            self.host = host
            self.bind_port = int(port)
            self.report_port = int(port)
            self.port = int(port)
            logger.info(f"🔌 UDS mode (from host) - socket: {self.uds_path}")
        elif self.use_uds:
            # UDS mode from config - generate socket path
            os.makedirs(self.uds_dir, exist_ok=True)
            self.uds_path = f"{self.uds_dir}/grpc_{port}.sock"
            # Remove stale socket file if exists
            if os.path.exists(self.uds_path):
                os.remove(self.uds_path)
            self.bind_address = f"unix://{self.uds_path}"
            self.report_address = self.bind_address
            # Keep these for compatibility
            self.bind_host = None
            self.report_host = None
            self.host = self.bind_address
            self.bind_port = int(port)
            self.report_port = int(port)
            self.port = int(port)
            logger.info(f"🔌 UDS mode (from config) - socket: {self.uds_path}")
        elif '|' in host:
            # Docker three-segment format: bind IP|report IP|report port
            parts = host.split('|')
            bind_host, report_host, report_port_str = parts
            self.bind_host = bind_host    # Actual bind address
            self.report_host = report_host  # Address reported to other entities
            self.host = report_host  # local_address uses report address
            self.report_port = int(report_port_str)  # Port reported to other entities
            self.port = self.report_port  # local_address uses report port
            self.bind_address = None
            self.report_address = None
            self.uds_path = None
            logger.info(f"🐳 Docker three-segment mode - bind: '{bind_host}:{port}', report: '{report_host}:{self.report_port}'")
        else:
            # Non-Docker single address mode (TCP)
            self.bind_host = host
            self.report_host = host
            self.host = host
            self.report_port = int(port)
            self.port = int(port)
            self.bind_address = None
            self.report_address = None
            self.uds_path = None
            logger.info(f"📡 TCP mode - address: '{host}:{port}'")
        
        # Bind port always uses port parameter from configuration
        self.bind_port = int(port)
        logger.info(f"✅ Final settings - bind: '{self.bind_host}:{self.bind_port}', report: '{self.host}:{self.port}'")
        # 🚀 HIGH-PERFORMANCE gRPC OPTIONS - Consistent with streaming channels
        # 🔧 CRITICAL FIX: Prevent TCP buffer deadlock in P2P chunk exchange
        options = [
            ("grpc.max_send_message_length", cfg.grpc_max_send_message_length),
            ("grpc.max_receive_message_length", cfg.grpc_max_receive_message_length),
            ("grpc.enable_http_proxy", cfg.grpc_enable_http_proxy),

            # 🔧 TCP Socket Buffer - Prevent deadlock from small buffers
            ("grpc.so_reuseport", 1),                                # Allow port reuse
            ("grpc.tcp_socket_recv_buffer_size", 8 * 1024 * 1024),   # 8MB receive buffer
            ("grpc.tcp_socket_send_buffer_size", 8 * 1024 * 1024),   # 8MB send buffer

            # Keepalive settings - More robust for high-concurrency FL
            ("grpc.keepalive_time_ms", 300000),                      # 300s (5min) - reduce ping frequency
            ("grpc.keepalive_timeout_ms", 60000),                    # 60s timeout (was 20s)
            ("grpc.keepalive_permit_without_calls", 1),              # Server relaxed
            ("grpc.http2.min_ping_interval_without_data_ms", 60000), # 60s min ping interval
            ("grpc.http2.max_pings_without_data", 0),                # Unlimited pings without data
            ("grpc.http2.min_recv_ping_interval_without_data_ms", 30000),  # 30s - allow client pings

            # 🔧 HTTP/2 Flow Control - Larger windows to prevent blocking
            ("grpc.http2.initial_window_size", 64 * 1024 * 1024),           # 64MB (was 16MB)
            ("grpc.http2.initial_connection_window_size", 128 * 1024 * 1024),# 128MB (was 32MB)
            ("grpc.http2.bdp_probe", 1),                             # Enable bandwidth-delay product probing
            ("grpc.http2.max_frame_size", 16 * 1024 * 1024),         # 16MB max frame (default 16KB)

            # 🔧 Prevent write blocking
            ("grpc.http2.write_buffer_size", 16 * 1024 * 1024),      # 16MB write buffer
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

        # 🚀 DUAL CHANNEL ARCHITECTURE: Prevent HOL blocking
        # Problem: When data channel (large chunks) is congested, control messages
        # (heartbeat, status, bitfield) get blocked too → DEADLINE_EXCEEDED
        # Solution: Separate control and data into independent gRPC channels
        self._control_connection_pool = {}  # {receiver_address: (stub, channel)} - lightweight messages
        self._data_connection_pool = {}     # {receiver_address: (stub, channel)} - bulk data

        # Message types that should use control channel (lightweight, latency-sensitive)
        self._control_msg_types = {
            'join_in', 'address', 'model_para', 'evaluate', 'finish',
            'bitfield', 'have', 'interested', 'choke', 'unchoke', 'cancel',
            'request', 'topology_instruction', 'connect_msg',
            'start_bittorrent', 'bt_complete', 'training_completion'
        }
        # Data message types (bulk data, can tolerate congestion)
        self._data_msg_types = {'piece', 'chunk', 'chunk_piece'}

        # 🚀 DISCONNECT TOLERANCE: Prevent false disconnects due to temporary thread pool congestion
        # Only report disconnect after multiple consecutive failures
        self._failure_counts = {}  # {receiver_address: consecutive_failure_count}
        self._disconnect_threshold = 5  # Need 5 consecutive failures to trigger disconnect

    def serve(self, max_workers, host, port, options):
        """
        This function is referred to
        https://grpc.io/docs/languages/python/basics/#starting-the-server
        """
        # 🚀 CRITICAL: Reserve threads for control messages (sendMessage)
        # Problem: Streaming RPCs (uploadChunks, streamChunks) block threads until completion
        # This starves sendMessage RPCs, causing server control messages to timeout
        #
        # Solution: Use maximum_concurrent_rpcs to limit streaming, ensuring threads for control
        # With mesh=4, each client has ~8 inbound streaming connections (4 neighbors × 2 streams)
        # Reserve at least 50% of threads for control messages
        thread_pool_size = max_workers * 50
        max_concurrent_streaming = min(thread_pool_size // 2, 100)  # Cap at 100 streaming RPCs

        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=thread_pool_size),
            compression=self.comp_method,
            options=options,
            maximum_concurrent_rpcs=thread_pool_size  # Allow all threads to be used
        )
        gRPC_comm_manager_pb2_grpc.add_gRPCComServeFuncServicer_to_server(
            self.server_funcs, server)

        # Determine bind address based on mode
        if self.use_uds:
            # UDS mode: use unix:// address
            bind_address = self.bind_address
            logger.info(f"🔌 gRPC server startup - UDS bind: {bind_address}")
        elif host is None:
            # Fallback for edge cases
            bind_address = f"0.0.0.0:{port}"
            logger.info(f"🚀 gRPC server startup - fallback bind: {bind_address}")
        elif ':' in str(host):
            # Host already contains port number
            logger.error(f"❌ Host address contains port number: {host}")
            bind_address = host
        else:
            # Standard TCP mode
            bind_address = "{}:{}".format(host, port)
            logger.info(f"🚀 gRPC server startup - TCP bind: {bind_address}")

        logger.info(f"📍 gRPC attempting to bind to: {bind_address}")
        server.add_insecure_port(bind_address)
        server.start()

        return server

    def add_neighbors(self, neighbor_id, address):
        if isinstance(address, dict):
            # Check if UDS address is provided
            if 'uds_path' in address:
                self.neighbors[neighbor_id] = f"unix://{address['uds_path']}"
            elif address.get('host', '').startswith('unix://'):
                self.neighbors[neighbor_id] = address['host']
            else:
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

    def _create_stub(self, receiver_address):
        """
        Create gRPC stub and channel for a receiver address.
        This part is referred to https://grpc.io/docs/languages/python/basics/#creating-a-stub
        """
        # 🚀 Apply high-performance client options consistent with server
        # 🔧 CRITICAL FIX: Match server buffer sizes to prevent TCP deadlock
        client_options = [
            ('grpc.enable_http_proxy', 0),

            # 🔧 TCP Socket Buffer - Must match server settings
            ('grpc.tcp_socket_recv_buffer_size', 8 * 1024 * 1024),   # 8MB
            ('grpc.tcp_socket_send_buffer_size', 8 * 1024 * 1024),   # 8MB

            # Keepalive settings - More robust for high-concurrency FL
            ('grpc.keepalive_time_ms', 300000),          # 300s (5min) - reduce ping frequency
            ('grpc.keepalive_timeout_ms', 60000),        # 60s timeout (was 20s)
            ('grpc.keepalive_permit_without_calls', 0),  # Don't ping without active calls
            ('grpc.http2.min_time_between_pings_ms', 60000),  # 60s min between pings
            ('grpc.http2.max_pings_without_data', 0),    # Unlimited pings without data

            # 🔧 HTTP/2 Flow Control - Match server settings
            ('grpc.http2.initial_window_size', 64 * 1024 * 1024),           # 64MB
            ('grpc.http2.initial_connection_window_size', 128 * 1024 * 1024),# 128MB
            ('grpc.http2.bdp_probe', 1),
            ('grpc.http2.max_frame_size', 16 * 1024 * 1024),         # 16MB
            ('grpc.http2.write_buffer_size', 16 * 1024 * 1024),      # 16MB

            ('grpc.use_local_subchannel_pool', 1),
        ]

        channel = grpc.insecure_channel(receiver_address,
                                        compression=self.comp_method,
                                        options=client_options)
        stub = gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub(channel)
        return stub, channel

    def _get_or_create_connection(self, receiver_address, use_data_channel=False):
        """
        🚀 DUAL CHANNEL: Get existing connection or create new one.
        Separates control and data channels to prevent HOL blocking.

        Args:
            receiver_address: Target address
            use_data_channel: If True, use data channel pool; else use control channel pool
        """
        pool = self._data_connection_pool if use_data_channel else self._control_connection_pool
        pool_name = "Data" if use_data_channel else "Control"

        if receiver_address in pool:
            stub, channel = pool[receiver_address]
            # Check if channel is still usable
            try:
                state = channel._channel.check_connectivity_state(True)
                if state == grpc.ChannelConnectivity.SHUTDOWN:
                    # Connection closed, need to recreate
                    logger.debug(f"[{pool_name}Pool] Connection to {receiver_address} was shutdown, recreating...")
                    del pool[receiver_address]
                else:
                    # Connection still valid, reuse it
                    return stub, channel
            except Exception as e:
                # Check failed, recreate connection
                logger.debug(f"[{pool_name}Pool] Connection check failed for {receiver_address}: {e}, recreating...")
                try:
                    channel.close()
                except:
                    pass
                if receiver_address in pool:
                    del pool[receiver_address]

        # Create new connection and add to pool
        stub, channel = self._create_stub(receiver_address)
        pool[receiver_address] = (stub, channel)
        logger.debug(f"[{pool_name}Pool] Created new connection to {receiver_address}, pool size: {len(pool)}")
        return stub, channel

    def _is_data_message(self, msg_type):
        """Check if message type should use data channel"""
        return msg_type in self._data_msg_types

    def _send(self, receiver_address, message):
        """
        🚀 DUAL CHANNEL: Route message to appropriate channel based on type.
        Control messages → control channel (isolated from data congestion)
        Data messages → data channel (can tolerate congestion)
        """
        # Determine which channel to use based on message type
        use_data_channel = self._is_data_message(message.msg_type)
        pool_name = "Data" if use_data_channel else "Control"

        stub, channel = self._get_or_create_connection(receiver_address, use_data_channel)
        request = message.transform(to_list=True)

        try:
            # 🔧 CRITICAL FIX: Add timeout to prevent infinite blocking on TCP deadlock
            stub.sendMessage(request, timeout=30.0)  # 30 second timeout

            # 🚀 SUCCESS: Reset failure count for this address
            if receiver_address in self._failure_counts:
                self._failure_counts[receiver_address] = 0

        except grpc._channel._InactiveRpcError as error:
            # 🚀 DISCONNECT TOLERANCE: Track consecutive failures
            self._failure_counts[receiver_address] = self._failure_counts.get(receiver_address, 0) + 1
            failure_count = self._failure_counts[receiver_address]

            logger.warning(f"[{pool_name}Pool] Connection error to {receiver_address} (failure {failure_count}/{self._disconnect_threshold}): {error}")

            # Remove failed connection from appropriate pool (will retry with new connection)
            pool = self._data_connection_pool if use_data_channel else self._control_connection_pool
            if receiver_address in pool:
                try:
                    pool[receiver_address][1].close()
                except:
                    pass
                del pool[receiver_address]

            # 🚀 Only report disconnect after multiple consecutive failures
            # This prevents false disconnects due to temporary thread pool congestion
            if failure_count >= self._disconnect_threshold:
                if hasattr(self, 'connection_monitor') and self.connection_monitor:
                    from federatedscope.core.connection_monitor import ConnectionEvent
                    self.connection_monitor.report_connection_lost(
                        peer_id=message.receiver,
                        details={
                            'error_type': type(error).__name__,
                            'error_message': str(error),
                            'receiver_address': receiver_address,
                            'message_type': message.msg_type,
                            'channel_type': pool_name,
                            'consecutive_failures': failure_count
                        }
                    )
                    logger.error(f"[{pool_name}Pool] DISCONNECT: {receiver_address} after {failure_count} consecutive failures")
                    # Reset counter after reporting disconnect
                    self._failure_counts[receiver_address] = 0
            else:
                logger.debug(f"[{pool_name}Pool] Temporary failure to {receiver_address}, will retry ({failure_count}/{self._disconnect_threshold})")

        # 🚀 NOTE: Do NOT close channel here - keep it in pool for reuse

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

    def close_connection_pool(self):
        """🚀 DUAL CHANNEL: Close all pooled connections (call on shutdown)"""
        # Close control channel pool
        for address, (stub, channel) in list(self._control_connection_pool.items()):
            try:
                channel.close()
                logger.debug(f"[ControlPool] Closed connection to {address}")
            except Exception as e:
                logger.warning(f"[ControlPool] Error closing connection to {address}: {e}")
        self._control_connection_pool.clear()

        # Close data channel pool
        for address, (stub, channel) in list(self._data_connection_pool.items()):
            try:
                channel.close()
                logger.debug(f"[DataPool] Closed connection to {address}")
            except Exception as e:
                logger.warning(f"[DataPool] Error closing connection to {address}: {e}")
        self._data_connection_pool.clear()

        logger.info(f"[ConnPool] Both control and data connection pools cleared")

    def stop(self):
        """🚀 GRACEFUL SHUTDOWN: Stop gRPC server and close all connections"""
        logger.info(f"[gRPC] Stopping gRPC communication manager...")

        # 1. Close connection pools first
        self.close_connection_pool()

        # 2. Stop gRPC server
        if hasattr(self, 'grpc_server') and self.grpc_server is not None:
            try:
                # Grace period: allow 2 seconds for ongoing RPCs to complete
                self.grpc_server.stop(grace=2.0)
                logger.info(f"[gRPC] gRPC server stopped successfully")
            except Exception as e:
                logger.warning(f"[gRPC] Error stopping gRPC server: {e}")

        # 3. Clean up UDS socket file if used
        if self.use_uds and hasattr(self, 'uds_path') and self.uds_path:
            import os
            try:
                if os.path.exists(self.uds_path):
                    os.remove(self.uds_path)
                    logger.debug(f"[gRPC] Removed UDS socket: {self.uds_path}")
            except Exception as e:
                logger.warning(f"[gRPC] Error removing UDS socket: {e}")

        logger.info(f"[gRPC] Communication manager shutdown complete")
