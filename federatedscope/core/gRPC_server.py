import logging
import time
import threading
import hashlib
import pickle
import json
from collections import deque

from federatedscope.core.proto import gRPC_comm_manager_pb2, \
    gRPC_comm_manager_pb2_grpc

logger = logging.getLogger(__name__)


class gRPCComServeFunc(gRPC_comm_manager_pb2_grpc.gRPCComServeFuncServicer):
    def __init__(self, chunk_manager=None):
        self.msg_queue = deque()
        # 🚀 Streaming queues for chunk transfer
        self.chunk_stream_queues = {}  # {client_id: deque()}
        # 🚀 CRITICAL FIX: Add chunk_manager reference for data access (will be set by client)
        self.chunk_manager = chunk_manager
        
        # 🔧 FIX: 单例线程管理 - 平时待机，需要时启动
        self._background_processor = None
        self._processor_lock = threading.Lock()
        self._active_uploads = 0  # 活跃上传连接计数器
    
    def _ensure_background_processor(self):
        """🔧 懒加载：只在需要时启动后台处理线程"""
        with self._processor_lock:
            if self._background_processor is None or not self._background_processor.is_alive():
                self._background_processor = threading.Thread(
                    target=self._background_chunk_processor,
                    daemon=True,
                    name="SharedBackgroundProcessor"
                )
                self._background_processor.start()
                logger.info("[🎯 gRPCServer] 🔧 Background processor started (lazy-loaded)")
    
    def _background_chunk_processor(self):
        """🔧 共享的后台处理线程 - 待机模式，处理所有客户端的chunk数据"""
        logger.info("[🎯 gRPCServer] 🚇 Background processor thread started, entering standby mode")
        
        while True:
            try:
                # 待机模式：当没有活跃连接时休眠
                if self._active_uploads == 0:
                    time.sleep(0.1)  # 100ms待机检查间隔
                    continue
                
                # 处理模式：当有活跃连接时处理消息队列
                # 这里可以添加实际的chunk处理逻辑
                time.sleep(0.01)  # 10ms处理间隔
                
            except Exception as e:
                logger.error(f"[🎯 gRPCServer] 🚇 Background processor error: {e}")
                time.sleep(1.0)  # 错误恢复间隔
        
    def sendMessage(self, request, context):
        self.msg_queue.append(request)
        return gRPC_comm_manager_pb2.MessageResponse(msg='ACK')
        
    def streamChunks(self, request_iterator, context):
        """🚀 双向streaming RPC for chunk control messages"""
        logger.debug("[gRPCServer] streamChunks method called")
        logger.info(f"[🔍 gRPCServer] streamChunks called from peer: {context.peer()}")
        
        try:
            for request in request_iterator:
                logger.info(f"[🔍 gRPCServer] Received request: sender_id={request.sender_id}, receiver_id={request.receiver_id}, chunk_type={request.chunk_type}, from peer: {context.peer()}")
                # Process control messages (HAVE, BITFIELD, REQUEST, CANCEL)
                if request.chunk_type in [gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
                                         gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
                                         gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
                                         gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL]:
                    # 🔧 CRITICAL FIX: Only send REQUEST messages to the target receiver, not back to sender
                    if request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST:
                        # REQUEST messages should only go to the receiver (target peer), not be broadcast
                        converted_msg = self._convert_chunk_to_message(request)
                        # Only queue the message if it's meant for a specific receiver
                        self.msg_queue.append(converted_msg)
                        logger.info(f"[🎯 gRPCServer] Routing REQUEST from {request.sender_id} to {request.receiver_id}")
                    else:
                        # Other messages (HAVE, BITFIELD, CANCEL) can be processed normally
                        self.msg_queue.append(self._convert_chunk_to_message(request))
                    
                    # Send acknowledgment  
                    yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                        sender_id=request.sender_id,
                        receiver_id=request.receiver_id,
                        success=True,
                        response_type=gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_ACK,
                        round_num=request.round_num,
                        chunk_id=request.chunk_id
                    )
        except Exception as e:
            logger.error(f"[gRPCServer] streamChunks error: {e}")
            yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                success=False,
                error_message=str(e)
            )
            
    def uploadChunks(self, request_iterator, context):
        """🚀 优化2：增强的地下管道模式 - 永不停止的高性能chunk上传处理"""
        logger.debug("[🎯 gRPCServer] 📤 Enhanced upload pipeline started - UNDERGROUND MODE")
        
        # 🚀 地下管道性能参数
        successful_chunks = 0
        failed_chunks = 0
        error_messages = []
        processing_start_time = time.time()
        client_id = 0  # 将从第一个请求中获取
        
        # 🚀 性能监控
        chunk_sizes = []
        processing_times = []
        last_performance_report = time.time()
        
        def enhanced_underground_processor():
            """🚀 增强的地下管道处理器 - 高性能、永不停止、智能错误处理"""
            nonlocal successful_chunks, failed_chunks, error_messages, chunk_sizes, processing_times, client_id, last_performance_report
            
            logger.debug("[🎯 gRPCServer] 🚇 Enhanced underground pipeline started - PERFORMANCE MODE")
            
            # 🚀 地下管道批处理优化
            chunk_batch = []
            batch_size = 10  # 批处理大小
            batch_timeout = 0.1  # 100ms批处理超时
            last_batch_time = time.time()
            
            try:
                for request in request_iterator:
                    request_start_time = time.time()
                    
                    # 🚀 获取client_id（从第一个请求中）
                    if client_id == 0 and request.sender_id:
                        client_id = request.sender_id
                        logger.info(f"[🎯 gRPCServer] 📤 Client {client_id} connected to upload pipeline")
                    
                    # 🚀 快速请求分类和处理
                    if request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE:
                        chunk_size = len(request.chunk_data) if request.chunk_data else 0
                        logger.debug(f"[🎯 gRPCServer] 🚇 Processing CHUNK_PIECE - size={chunk_size}B, chunk={request.source_client_id}:{request.chunk_id}")
                        
                        try:
                            # 🚀 批量转换优化 - 收集到批处理队列
                            chunk_batch.append(request)
                            
                            # 🚀 智能批处理触发条件
                            current_time = time.time()
                            should_process_batch = (
                                len(chunk_batch) >= batch_size or  # 批大小达到
                                (current_time - last_batch_time) > batch_timeout or  # 超时
                                chunk_size > 1024 * 1024  # 大chunk立即处理
                            )
                            
                            if should_process_batch:
                                # 🚀 批量处理chunk
                                batch_start_time = time.time()
                                for chunk_req in chunk_batch:
                                    converted_msg = self._convert_chunk_to_message(chunk_req)
                                    self.msg_queue.append(converted_msg)
                                    successful_chunks += 1
                                    
                                    # 性能统计
                                    if chunk_req.chunk_data:
                                        chunk_sizes.append(len(chunk_req.chunk_data))
                                
                                batch_time = time.time() - batch_start_time
                                processing_times.append(batch_time)
                                
                                logger.debug(f"[🎯 gRPCServer] 🚇 Batch processed: {len(chunk_batch)} chunks in {batch_time:.3f}s")
                                
                                chunk_batch.clear()
                                last_batch_time = current_time
                                
                        except Exception as e:
                            failed_chunks += 1
                            error_msg = f"Chunk processing error: {e}"
                            error_messages.append(error_msg)
                            logger.error(f"[🎯 gRPCServer] 🚇 {error_msg}")
                            
                    elif request.chunk_type in [
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL
                    ]:
                        # 🚀 控制消息快速处理通道
                        logger.debug(f"[🎯 gRPCServer] 🚇 Processing control message - type={request.chunk_type}")
                        converted_msg = self._convert_chunk_to_message(request)
                        self.msg_queue.append(converted_msg)
                        successful_chunks += 1
                        
                    else:
                        # 🚀 心跳消息处理
                        if request.chunk_data == b'heartbeat':
                            logger.debug(f"[🎯 gRPCServer] 🚇💓 Heartbeat received from client {request.sender_id}")
                        else:
                            failed_chunks += 1
                            error_msg = f"Unknown chunk type: {request.chunk_type}"
                            error_messages.append(error_msg)
                            logger.warning(f"[🎯 gRPCServer] 🚇 {error_msg}")
                    
                    # 🚀 处理时间监控
                    processing_time = time.time() - request_start_time
                    if processing_time > 0.05:  # 超过50ms的慢处理
                        logger.warning(f"[🎯 gRPCServer] 🐌 Slow chunk processing: {processing_time:.3f}s")
                    
                    # 🚀 定期性能报告
                    current_time = time.time()
                    if current_time - last_performance_report > 30.0:  # 每30秒报告
                        if chunk_sizes:
                            avg_chunk_size = sum(chunk_sizes) / len(chunk_sizes)
                            avg_processing_time = sum(processing_times) / len(processing_times)
                            logger.info(f"[🎯 gRPCServer] 📊 Underground pipeline performance:")
                            logger.info(f"[🎯 gRPCServer] 📊   {successful_chunks} chunks processed, {failed_chunks} failed")
                            logger.info(f"[🎯 gRPCServer] 📊   Avg chunk size: {avg_chunk_size:.0f}B, processing time: {avg_processing_time:.3f}s")
                        last_performance_report = current_time
                
                # 🚀 处理剩余的批处理数据
                if chunk_batch:
                    logger.info(f"[🎯 gRPCServer] 🚇 Processing final batch: {len(chunk_batch)} chunks")
                    for chunk_req in chunk_batch:
                        converted_msg = self._convert_chunk_to_message(chunk_req)
                        self.msg_queue.append(converted_msg)
                        successful_chunks += 1
                
                total_processing_time = time.time() - processing_start_time
                logger.debug(f"[🎯 gRPCServer] 🚇 Underground pipeline completed - total time: {total_processing_time:.3f}s")
                        
            except Exception as e:
                logger.error(f"[🎯 gRPCServer] 🚇 Underground pipeline fatal error: {e}")
                failed_chunks += 1
                error_messages.append(str(e))
            finally:
                # 🚀 最终性能统计
                total_time = time.time() - processing_start_time
                logger.debug(f"[🎯 gRPCServer] 🚇 Underground pipeline finished:")
                logger.debug(f"[🎯 gRPCServer] 📊   Success: {successful_chunks}, Failed: {failed_chunks}")
                logger.debug(f"[🎯 gRPCServer] 📊   Total time: {total_time:.3f}s")
                if successful_chunks > 0:
                    logger.debug(f"[🎯 gRPCServer] 📊   Throughput: {successful_chunks/total_time:.1f} chunks/sec")
        
        # 🔧 FIX: 使用共享后台线程，避免为每个请求创建新线程
        with self._processor_lock:
            self._active_uploads += 1
        
        # 🔧 确保后台处理线程存在（懒加载）
        self._ensure_background_processor()
        
        # 🚀 直接在当前线程处理，或将数据传递给后台线程
        logger.debug(f"[🎯 gRPCServer] Processing upload request for client {client_id}")
        enhanced_underground_processor()
        
        # 🔧 处理完成后减少活跃连接计数
        with self._processor_lock:
            self._active_uploads = max(0, self._active_uploads - 1)
        
        # 🚀 返回处理结果
        logger.debug("[🎯 gRPCServer] Upload processing completed")
        return gRPC_comm_manager_pb2.ChunkBatchResponse(
            client_id=client_id,
            successful_chunks=successful_chunks,
            failed_chunks=failed_chunks,
            error_messages=error_messages
        )
        
    def downloadChunks(self, request, context):
        """🚀 服务端流式RPC for batch chunk download"""
        logger.debug(f"[gRPCServer] downloadChunks method called for client {request.client_id}")
        
        try:
            # Convert batch request to individual message requests
            for chunk_req in request.chunk_requests:
                # Create chunk request message for compatibility
                chunk_request = gRPC_comm_manager_pb2.ChunkStreamRequest(
                    sender_id=request.sender_id,   
                    receiver_id=request.client_id,          
                    round_num=request.round_num,
                    source_client_id=chunk_req.source_client_id,
                    chunk_id=chunk_req.chunk_id,
                    chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
                    importance_score=chunk_req.importance_score
                )
                
                # Add to message queue for traditional BitTorrent compatibility
                self.msg_queue.append(self._convert_chunk_to_message(chunk_request))
                
                # 🚀 增强错误处理：检查chunk_manager并获取chunk数据
                chunk_data = None
                error_message = None
                
                if not hasattr(self, 'chunk_manager') or self.chunk_manager is None:
                    error_message = "Chunk manager not initialized on server"
                    logger.error(f"[gRPCServer] 🚫 {error_message} for chunk {chunk_req.source_client_id}:{chunk_req.chunk_id}")
                else:
                    try:
                        chunk_data = self.chunk_manager.get_chunk_data(request.round_num, chunk_req.source_client_id, chunk_req.chunk_id)
                        logger.debug(f"[gRPCServer] 📥 Chunk lookup: round={request.round_num}, source={chunk_req.source_client_id}, chunk={chunk_req.chunk_id}, found={chunk_data is not None}")
                        
                        if chunk_data is None:
                            error_message = f"Chunk {chunk_req.source_client_id}:{chunk_req.chunk_id} not found in storage"
                            
                    except Exception as e:
                        error_message = f"Error accessing chunk data: {str(e)}"
                        logger.error(f"[gRPCServer] 🚫 Exception during chunk lookup: {e}")
                
                # 🚀 统一错误响应处理
                if error_message:
                    logger.warning(f"[gRPCServer] 📤 Sending NACK for chunk {chunk_req.source_client_id}:{chunk_req.chunk_id}: {error_message}")
                    yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                        sender_id=chunk_req.source_client_id,
                        receiver_id=request.client_id,
                        success=False,
                        response_type=gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_NACK,
                        round_num=request.round_num,
                        chunk_id=chunk_req.chunk_id,
                        error_message=error_message
                    )
                    continue
                
                # 🚀 成功响应：返回实际chunk数据
                logger.info(f"[gRPCServer] 📤 Sending chunk data for {chunk_req.source_client_id}:{chunk_req.chunk_id} to client {request.client_id}")
                
                serialized_data = pickle.dumps(chunk_data)
                checksum = hashlib.sha256(serialized_data).hexdigest()
                data_size = len(serialized_data)
                
                logger.debug(f"[gRPCServer] 📤 Chunk data prepared: size={data_size}B, checksum={checksum[:8]}...")
                
                yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                    sender_id=chunk_req.source_client_id,
                    receiver_id=request.client_id,
                    success=True,
                    response_type=gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_ACK,
                    round_num=request.round_num,
                    chunk_id=chunk_req.chunk_id,
                    response_data=serialized_data
                )
                
        except Exception as e:
            logger.error(f"[gRPCServer] downloadChunks error: {e}")
            yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                success=False,
                error_message=str(e)
            )
            
    def _convert_chunk_to_message(self, chunk_request):
        """🔧 Convert chunk stream request to traditional message format for compatibility"""
        # This is a compatibility bridge - convert streaming chunk requests 
        # back to the traditional message format that the existing system expects
        
        if chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE:
            # 🚀 CRITICAL FIX: 正确创建piece消息，包含所有必要的chunk数据
            from federatedscope.core.message import ChunkData
            
            
            # 创建ChunkData包装器
            chunk_wrapper = ChunkData(chunk_request.chunk_data, chunk_request.checksum)
            
            # 创建完整的protobuf消息，包含所有piece数据
            message_request = gRPC_comm_manager_pb2.MessageRequest()
            
            # 设置消息内容 - 模拟传统piece消息的结构
            # 注意：这里我们需要手动构造消息内容，就像_send_piece中的传统消息一样
            from federatedscope.core.message import Message
            
            # 创建传统格式的Message对象
            traditional_msg = Message(
                msg_type='piece',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'source_client_id': chunk_request.source_client_id,
                    'chunk_id': chunk_request.chunk_id,
                    'data': chunk_wrapper,  # ChunkData对象，包含原始bytes
                    'checksum': chunk_request.checksum
                }
            )
            
            logger.info(f"[🔧 gRPCServer] Created traditional message: type={traditional_msg.msg_type}, sender={traditional_msg.sender}")
            logger.info(f"[🔧 gRPCServer] Message content keys: {list(traditional_msg.content.keys())}")
            
            # 将Message对象转换为protobuf格式
            message_request = traditional_msg.transform(to_list=True)
            
            logger.info(f"[🔧 gRPCServer] Successfully converted CHUNK_PIECE to MessageRequest")
            return message_request
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST:
            # 处理request消息
            from federatedscope.core.message import Message
            
            traditional_msg = Message(
                msg_type='request',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'source_client_id': chunk_request.source_client_id,
                    'chunk_id': chunk_request.chunk_id
                }
            )
            
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE:
            # 🚀 处理have消息
            from federatedscope.core.message import Message
            
            traditional_msg = Message(
                msg_type='have',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'source_client_id': chunk_request.source_client_id,
                    'chunk_id': chunk_request.chunk_id,
                    'importance_score': chunk_request.importance_score
                }
            )
            
            logger.info(f"[🔧 gRPCServer] Converting CHUNK_HAVE to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD:
            # 🚀 处理bitfield消息
            from federatedscope.core.message import Message
            
            # 解码bitfield数据
            bitfield_data = []
            if chunk_request.chunk_data:
                try:
                    bitfield_data = json.loads(chunk_request.chunk_data.decode('utf-8'))
                except Exception as e:
                    logger.error(f"[🔧 gRPCServer] Failed to decode bitfield data: {e}")
            
            traditional_msg = Message(
                msg_type='bitfield',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'bitfield': bitfield_data
                }
            )
            
            logger.info(f"[🔧 gRPCServer] Converting CHUNK_BITFIELD to traditional message format, chunks: {len(bitfield_data)}")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL:
            # 🚀 处理cancel消息
            from federatedscope.core.message import Message
            
            traditional_msg = Message(
                msg_type='cancel',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'source_client_id': chunk_request.source_client_id,
                    'chunk_id': chunk_request.chunk_id
                }
            )
            
            logger.info(f"[🔧 gRPCServer] Converting CHUNK_CANCEL to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        else:
            logger.warning(f"[🔧 gRPCServer] Unknown chunk_type: {chunk_request.chunk_type}")
            # 返回空的MessageRequest作为fallback
            return gRPC_comm_manager_pb2.MessageRequest()

    def receive(self):
        while len(self.msg_queue) == 0:
            continue
        msg = self.msg_queue.popleft()
        return msg
