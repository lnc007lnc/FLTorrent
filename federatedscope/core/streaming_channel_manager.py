"""
🚀 gRPC Streaming Channel Manager for BitTorrent
在拓扑构建时创建专用streaming通道，提供高效的chunk传输
"""

import grpc
import threading
import time
import logging
from typing import Dict, Optional, List, Tuple
from collections import defaultdict
import queue

from federatedscope.core.proto import gRPC_comm_manager_pb2_grpc
from federatedscope.core.proto import gRPC_comm_manager_pb2

logger = logging.getLogger(__name__)


class StreamingChannel:
    """单个streaming通道封装"""
    
    def __init__(self, peer_id: int, channel: grpc.Channel, stub, stream_type='bidirectional', client_id: int = -1, client_instance=None):
        self.peer_id = peer_id
        self.channel = channel
        self.stub = stub
        self.stream_type = stream_type
        self.client_id = client_id  # 添加client_id支持
        self.client_instance = client_instance  # 添加客户端实例引用，用于调用回调函数
        
        # Streaming对象
        self.request_stream = None
        self.response_stream = None
        self.request_queue = queue.Queue()
        
        # 状态管理
        self.is_active = False
        self.last_activity = time.time()
        self.bytes_sent = 0
        self.bytes_received = 0
        self.chunks_sent = 0
        self.chunks_received = 0
        
        # 线程管理
        self.sender_thread = None
        self.receiver_thread = None
        self.upload_sender_thread = None
        
    def start_streaming(self):
        """启动streaming（自动回退兼容）"""
        if self.is_active:
            return
            
        try:
            # 🚀 尝试启动多流streaming
            self._start_multi_streaming()
            
        except Exception as e:
            # 🔧 Streaming失败，标记为不可用，但不抛出错误
            logger.warning(f"[StreamChannel] Multi-streaming not supported for peer {self.peer_id}, will use traditional messaging: {e}")
            self.is_active = False  # 标记streaming不可用
            # 不抛出异常，让上层代码继续使用传统方式
            
    def _start_multi_streaming(self):
        """内部方法：启动多流并发streaming"""
        # 🚀 多流并发优化：创建专用流
        
        # 🔧 CRITICAL FIX: 先设置is_active=True，再创建generators
        self.is_active = True
        logger.info(f"[StreamChannel] Activating streaming channels to peer {self.peer_id}")
        
        # 1. 双向通用streaming (用于控制消息)
        self.control_stream = self.stub.streamChunks(self._control_request_generator())
        
        # 2. 专用上传流 (chunk发送) - 客户端流，返回单个响应
        self.upload_request_queue = queue.Queue()
        # 不在主线程创建upload_stream，改为在专用线程中创建
        
        # 3. 专用下载流 (chunk请求)
        self.download_request_queue = queue.Queue()
        
        # 启动接收线程 (注意：uploadChunks返回单个响应，不需要响应处理线程)
        self.control_receiver_thread = threading.Thread(
            target=self._control_response_handler,
            daemon=True,
            name=f"ControlReceiver-{self.peer_id}"
        )
        
        self.download_sender_thread = threading.Thread(
            target=self._download_request_sender,
            daemon=True,
            name=f"DownloadSender-{self.peer_id}"
        )
        
        self.upload_sender_thread = threading.Thread(
            target=self._run_upload_stream,
            daemon=True,
            name=f"UploadSender-{self.peer_id}"
        )
        
        # 启动线程
        self.control_receiver_thread.start()
        self.download_sender_thread.start()
        self.upload_sender_thread.start()
        
        logger.info(f"[StreamChannel] Started multi-stream channels to peer {self.peer_id}")
            
    def _control_request_generator(self):
        """控制消息流生成器"""
        while self.is_active:
            try:
                request = self.request_queue.get(timeout=1.0)
                if request is None:  # Sentinel for shutdown
                    break
                # 只处理控制消息 (HAVE, BITFIELD, INTERESTED等)
                if request.chunk_type in [gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE, 
                                         gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD]:
                    yield request
                    self.request_queue.task_done()
                else:
                    # 非控制消息放回队列或转发到专用流
                    self.request_queue.put(request)
                    break
            except queue.Empty:
                continue
                
    def _upload_request_generator(self):
        """🚀 专用上传流：只处理chunk数据传输 - 修复Generator过早终止问题"""
        logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Upload stream generator started for peer {self.peer_id}")
        
        # 🔧 FIX: 添加等待机制参数
        empty_queue_wait = 0.01  # 10ms等待时间
        max_idle_time = 1.0     # 最大空闲时间1秒
        last_activity = time.time()
        
        while self.is_active:
            try:
                # 🔧 CRITICAL FIX: 使用带超时的get替代get_nowait，并继续循环而不是return
                try:
                    request = self.upload_request_queue.get(timeout=empty_queue_wait)
                    last_activity = time.time()  # 更新活动时间
                    
                except queue.Empty:
                    # 🚀 FIX: 队列为空时继续等待，而不是终止generator
                    current_time = time.time()
                    idle_time = current_time - last_activity
                    
                    # 定期输出调试信息，但不要太频繁
                    if idle_time > max_idle_time and int(current_time) % 5 == 0:  # 每5秒输出一次
                        logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Waiting for data, idle for {idle_time:.1f}s")
                        last_activity = current_time  # 重置计时器避免重复日志
                    
                    continue  # 🚀 关键修复：继续循环而不是return终止generator
                    
                if request is None:  # Sentinel for shutdown
                    logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Received shutdown sentinel for peer {self.peer_id}")
                    break
                
                # 🚀 处理正常的chunk请求
                logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Yielding chunk request for peer {self.peer_id}")
                logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Request details - round={request.round_num}, source={request.source_client_id}, chunk={request.chunk_id}, size={len(request.chunk_data)}")
                
                yield request
                self.upload_request_queue.task_done()
                
                logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Successfully yielded chunk request for peer {self.peer_id}")
                
            except Exception as e:
                logger.error(f"[🚀 UploadGenerator] Client {self.client_id}: Unexpected error: {e}")
                if not self.is_active:
                    break
                # 🔧 FIX: 遇到异常时短暂等待后继续，避免紧密循环
                time.sleep(0.1)
                
        logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Upload stream generator ended for peer {self.peer_id}")
    
    def _run_upload_stream(self):
        """🔧 FIX: 在专用线程中运行上传流，并正确消费响应"""
        try:
            logger.info(f"[🚀 UploadStream] Client {self.client_id}: Starting upload stream to peer {self.peer_id}")
            
            # 创建上传流并消费最终响应
            upload_response = self.stub.uploadChunks(self._upload_request_generator())
            
            # 🔧 CRITICAL FIX: 必须消费响应才能完成gRPC客户端流
            logger.info(f"[🚀 UploadStream] Client {self.client_id}: Upload stream response: {upload_response}")
            logger.info(f"[🚀 UploadStream] Client {self.client_id}: Upload stream completed for peer {self.peer_id}")
            
        except Exception as e:
            logger.error(f"[🚀 UploadStream] Client {self.client_id}: Upload stream error for peer {self.peer_id}: {e}")
            self.is_active = False
                
    def _control_response_handler(self):
        """控制消息响应处理器"""
        try:
            for response in self.control_stream:
                if not self.is_active:
                    break
                self._handle_control_response(response)
                self.last_activity = time.time()
        except Exception as e:
            logger.error(f"[StreamChannel] Control response handler error for peer {self.peer_id}: {e}")
            
    def _download_request_sender(self):
        """🚀 下载请求发送器：批量处理chunk请求"""
        batch_requests = []
        batch_timeout = 0.1  # 100ms批处理超时
        
        while self.is_active:
            try:
                # 收集批量请求
                end_time = time.time() + batch_timeout
                while time.time() < end_time and len(batch_requests) < 10:  # 最多10个一批
                    try:
                        request = self.download_request_queue.get(timeout=0.01)
                        if request is None:  # Shutdown signal
                            break
                        batch_requests.append(request)
                    except queue.Empty:
                        break
                
                # 发送批量请求
                if batch_requests:
                    batch_request = gRPC_comm_manager_pb2.ChunkBatchRequest(
                        client_id=self.peer_id,
                        round_num=batch_requests[0].round_num,
                        chunk_requests=[
                            gRPC_comm_manager_pb2.ChunkRequest(
                                source_client_id=req.source_client_id,
                                chunk_id=req.chunk_id,
                                importance_score=req.importance_score
                            ) for req in batch_requests
                        ]
                    )
                    
                    # 发送批量下载请求
                    download_stream = self.stub.downloadChunks(batch_request)
                    for chunk_response in download_stream:
                        self._handle_download_response(chunk_response)
                    
                    logger.info(f"[StreamChannel] Sent batch download request with {len(batch_requests)} chunks to peer {self.peer_id}")
                    batch_requests.clear()
                    
            except Exception as e:
                logger.error(f"[StreamChannel] Download request sender error for peer {self.peer_id}: {e}")
                batch_requests.clear()
            
    def _handle_control_response(self, response):
        """处理控制消息响应 (HAVE, BITFIELD等)"""
        logger.info(f"[StreamChannel] Received control response from peer {self.peer_id}: {response.response_type}")
        # TODO: 集成到BitTorrentManager的bitfield更新
        
    def _handle_download_response(self, response):
        """🚀 处理chunk下载响应"""
        self.chunks_received += 1
        self.bytes_received += len(response.response_data) if response.response_data else 0
        logger.info(f"[StreamChannel] Received chunk download from peer {self.peer_id}, chunk_id: {response.chunk_id}")
        # TODO: 集成到BitTorrentManager的chunk处理
        
    def _handle_chunk_response(self, response):
        """兼容性方法：处理收到的chunk响应"""
        self.chunks_received += 1
        self.bytes_received += len(response.response_data) if response.response_data else 0
        logger.info(f"[StreamChannel] Received chunk response from peer {self.peer_id}")
        
    def send_chunk_request(self, round_num: int, source_client_id: int, chunk_id: int, 
                          importance_score: float = 0.0):
        """🚀 发送chunk请求 - 使用批量下载队列"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
            
        request = gRPC_comm_manager_pb2.ChunkStreamRequest(
            sender_id=self.client_id,  # 🔧 FIX: 正确的发送方ID
            receiver_id=self.peer_id,  # 🔧 FIX: 正确的接收方ID
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
            importance_score=importance_score,
            timestamp=int(time.time() * 1000)
        )
        
        try:
            # 🚀 使用专用下载请求队列，支持批量处理
            self.download_request_queue.put(request, timeout=1.0)
            logger.info(f"[StreamChannel] Queued chunk request {source_client_id}:{chunk_id} to peer {self.peer_id} for batch download")
            return True
        except queue.Full:
            logger.error(f"[StreamChannel] Download request queue full for peer {self.peer_id}")
            return False
            
    def send_chunk_data(self, round_num: int, source_client_id: int, chunk_id: int,
                       chunk_data: bytes, checksum: str, importance_score: float = 0.0):
        """🚀 发送chunk数据 - 使用专用上传流"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
            
        # 🔧 CRITICAL FIX: 正确设置sender_id和receiver_id
        # sender_id应该是发送方的client_id，receiver_id应该是接收方的peer_id
        request = gRPC_comm_manager_pb2.ChunkStreamRequest(
            sender_id=self.client_id,  # 发送方client_id
            receiver_id=self.peer_id,   # 接收方peer_id
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            chunk_data=chunk_data,
            checksum=checksum,
            chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE,
            importance_score=importance_score,
            timestamp=int(time.time() * 1000)
        )
        
        try:
            # 🚀 使用专用上传队列，提高chunk数据传输效率
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Queueing chunk data {source_client_id}:{chunk_id} to peer {self.peer_id}")
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Chunk data size: {len(chunk_data)}, queue size before: {self.upload_request_queue.qsize()}")
            
            self.upload_request_queue.put(request, timeout=1.0)
            self.chunks_sent += 1
            self.bytes_sent += len(chunk_data)
            
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Successfully queued chunk data {source_client_id}:{chunk_id} to peer {self.peer_id}")
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Queue size after: {self.upload_request_queue.qsize()}")
            return True
        except queue.Full:
            logger.error(f"[StreamChannel] Upload queue full for peer {self.peer_id}")
            return False
    
    def send_control_message(self, msg_type: str, round_num: int, **kwargs) -> bool:
        """🚀 发送BitTorrent控制消息 (bitfield, have, interested, cancel)"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
        
        # 映射消息类型到protobuf ChunkType
        chunk_type_map = {
            'bitfield': gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
            'have': gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            'cancel': gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL,
            'request': gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
            'piece': gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE
        }
        
        if msg_type not in chunk_type_map:
            logger.error(f"[StreamChannel] Unsupported control message type: {msg_type}")
            return False
        
        # 构建控制消息请求
        request = gRPC_comm_manager_pb2.ChunkStreamRequest(
            sender_id=self.client_id,
            receiver_id=self.peer_id,
            round_num=round_num,
            chunk_type=chunk_type_map[msg_type],
            timestamp=int(time.time() * 1000)
        )
        
        # 根据消息类型设置特定字段
        if msg_type == 'bitfield':
            # bitfield消息：使用chunk_data传输bitfield数据
            import json
            bitfield_data = kwargs.get('bitfield', [])
            request.chunk_data = json.dumps(bitfield_data).encode('utf-8')
            logger.info(f"[StreamChannel] Client {self.client_id}: Sending BITFIELD to peer {self.peer_id}, chunks: {len(bitfield_data)}")
            
        elif msg_type == 'have':
            # have消息：标识拥有的chunk
            request.source_client_id = kwargs.get('source_client_id', 0)
            request.chunk_id = kwargs.get('chunk_id', 0)
            request.importance_score = kwargs.get('importance_score', 0.0)
            logger.info(f"[StreamChannel] Client {self.client_id}: Sending HAVE to peer {self.peer_id}, chunk: {request.source_client_id}:{request.chunk_id}")
            
        elif msg_type == 'cancel':
            # cancel消息：取消chunk请求
            request.source_client_id = kwargs.get('source_client_id', 0)
            request.chunk_id = kwargs.get('chunk_id', 0)
            logger.info(f"[StreamChannel] Client {self.client_id}: Sending CANCEL to peer {self.peer_id}, chunk: {request.source_client_id}:{request.chunk_id}")
        
        try:
            # 使用控制流队列发送
            self.request_queue.put(request, timeout=1.0)
            logger.info(f"[StreamChannel] Client {self.client_id}: Successfully queued {msg_type.upper()} message to peer {self.peer_id}")
            return True
        except queue.Full:
            logger.error(f"[StreamChannel] Control queue full for peer {self.peer_id}")
            return False
            
    def close(self):
        """关闭streaming通道"""
        if not self.is_active:
            return
            
        self.is_active = False
        
        # 发送关闭信号到各个队列
        try:
            self.request_queue.put(None)
            self.upload_request_queue.put(None)
            self.download_request_queue.put(None)
        except:
            pass
        
        # 等待线程结束
        threads = [
            self.control_receiver_thread,
            self.download_sender_thread,
            self.upload_sender_thread,
        ]
        
        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=1.0)
            
        # 关闭gRPC通道
        try:
            self.channel.close()
            logger.info(f"[StreamChannel] Closed streaming to peer {self.peer_id}")
        except:
            pass
            
    def get_stats(self):
        """获取通道统计信息"""
        return {
            'peer_id': self.peer_id,
            'is_active': self.is_active,
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received,
            'chunks_sent': self.chunks_sent,
            'chunks_received': self.chunks_received,
            'last_activity': self.last_activity
        }


class StreamingChannelManager:
    """🚀 gRPC Streaming通道管理器 - 在拓扑构建时创建专用通道"""
    
    def __init__(self, client_id: int):
        self.client_id = client_id
        self.channels: Dict[int, StreamingChannel] = {}  # peer_id -> StreamingChannel
        self.server_stubs: Dict[str, gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub] = {}
        
        # 性能统计
        self.total_bytes_sent = 0
        self.total_bytes_received = 0
        self.total_chunks_sent = 0
        self.total_chunks_received = 0
        
        logger.info(f"[StreamingManager] Initialized for client {client_id}")
        
    def create_channels_for_topology(self, neighbor_addresses: Dict[int, Tuple[str, int]]):
        """
        🚀 在拓扑构建时为所有邻居创建专用streaming通道
        Args:
            neighbor_addresses: {peer_id: (host, port)}
        """
        logger.info(f"[StreamingManager] Client {self.client_id}: Creating streaming channels for topology")
        logger.info(f"[StreamingManager] Neighbor addresses: {neighbor_addresses}")
        
        for peer_id, (host, port) in neighbor_addresses.items():
            if peer_id == self.client_id:
                continue  # 跳过自己
                
            try:
                # 创建gRPC通道
                address = f"{host}:{port}"
                channel = grpc.insecure_channel(address)
                
                # 创建stub
                stub = gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub(channel)
                
                # 创建streaming通道封装
                stream_channel = StreamingChannel(peer_id, channel, stub, client_id=self.client_id)
                
                # 启动streaming (如果失败会自动回退)
                stream_channel.start_streaming()
                
                # 总是保存通道，即使streaming失败也可以用于传统消息
                self.channels[peer_id] = stream_channel
                self.server_stubs[address] = stub
                
                if stream_channel.is_active:
                    logger.info(f"[StreamingManager] ✅ Created multi-stream channel: Client {self.client_id} -> Peer {peer_id} ({address})")
                else:
                    logger.info(f"[StreamingManager] 🔧 Created traditional channel: Client {self.client_id} -> Peer {peer_id} ({address}) - streaming not available")
                
            except Exception as e:
                logger.error(f"[StreamingManager] Failed to create any channel to peer {peer_id} at {host}:{port}: {e}")
                
        logger.info(f"[StreamingManager] Client {self.client_id}: Created {len(self.channels)} streaming channels")
        logger.info(f"[StreamingManager] 🚀 STREAMING OPTIMIZATION: BitTorrent messages now support high-performance streaming:")
        logger.info(f"[StreamingManager]   📦 CHUNK_PIECE: Large chunk data via dedicated upload streams") 
        logger.info(f"[StreamingManager]   📤 CHUNK_REQUEST: Chunk requests via control streams")
        logger.info(f"[StreamingManager]   🎯 CHUNK_HAVE: Have notifications via control streams") 
        logger.info(f"[StreamingManager]   📋 CHUNK_BITFIELD: Bitfield updates via control streams")
        logger.info(f"[StreamingManager]   ❌ CHUNK_CANCEL: Cancel requests via control streams")
        logger.info(f"[StreamingManager]   🔄 Auto fallback to traditional messaging if streaming fails")
        
    def send_chunk_request(self, peer_id: int, round_num: int, source_client_id: int, 
                          chunk_id: int, importance_score: float = 0.0) -> bool:
        """通过streaming通道发送chunk请求"""
        if peer_id not in self.channels:
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id}")
            return False
            
        return self.channels[peer_id].send_chunk_request(
            round_num, source_client_id, chunk_id, importance_score)
            
    def send_bittorrent_message(self, peer_id: int, msg_type: str, **kwargs) -> bool:
        """🚀 统一的BitTorrent消息发送接口 - 支持所有消息类型"""
        if peer_id not in self.channels:
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id}")
            return False
        
        # 根据消息类型构建不同的请求
        if msg_type == 'piece':
            return self.channels[peer_id].send_chunk_data(**kwargs)
        elif msg_type == 'request':
            return self.channels[peer_id].send_chunk_request(**kwargs)
        elif msg_type in ['bitfield', 'have', 'cancel']:
            return self.channels[peer_id].send_control_message(msg_type, **kwargs)
        else:
            logger.error(f"[StreamingManager] Unknown BitTorrent message type: {msg_type}")
            return False
    
    def send_chunk_data(self, peer_id: int, round_num: int, source_client_id: int,
                       chunk_id: int, chunk_data: bytes, checksum: str, 
                       importance_score: float = 0.0) -> bool:
        """通过streaming通道发送chunk数据"""
        if peer_id not in self.channels:
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id}")
            return False
            
        success = self.channels[peer_id].send_chunk_data(
            round_num, source_client_id, chunk_id, chunk_data, checksum, importance_score)
            
        if success:
            self.total_chunks_sent += 1
            self.total_bytes_sent += len(chunk_data)
            
        return success
        
    def get_active_peers(self) -> List[int]:
        """获取所有活跃的peer列表"""
        return [peer_id for peer_id, channel in self.channels.items() if channel.is_active]
        
    def close_all_channels(self):
        """关闭所有streaming通道"""
        logger.info(f"[StreamingManager] Client {self.client_id}: Closing all streaming channels")
        
        for peer_id, channel in self.channels.items():
            channel.close()
            
        self.channels.clear()
        self.server_stubs.clear()
        
        logger.info(f"[StreamingManager] Client {self.client_id}: All streaming channels closed")
        
    def get_channel_stats(self):
        """获取所有通道的统计信息"""
        stats = {
            'client_id': self.client_id,
            'total_channels': len(self.channels),
            'active_channels': len([c for c in self.channels.values() if c.is_active]),
            'total_bytes_sent': self.total_bytes_sent,
            'total_bytes_received': self.total_bytes_received,
            'total_chunks_sent': self.total_chunks_sent,
            'total_chunks_received': self.total_chunks_received,
            'channels': {peer_id: channel.get_stats() for peer_id, channel in self.channels.items()}
        }
        return stats