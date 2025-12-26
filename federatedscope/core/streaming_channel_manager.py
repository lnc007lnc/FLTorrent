"""
🚀 gRPC Streaming Channel Manager for BitTorrent
Creates dedicated streaming channels during topology construction, providing efficient chunk transmission
"""

import grpc
import threading
import time
import logging
from typing import Dict, Optional, List, Tuple
from collections import defaultdict
import queue
import json

from federatedscope.core.proto import gRPC_comm_manager_pb2_grpc
from federatedscope.core.proto import gRPC_comm_manager_pb2

# 🚀 FIX: Import at module level to avoid import lock contention in multi-threaded environment
from federatedscope.core.message import Message, ChunkData

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class StreamingChannel:
    """Single streaming channel encapsulation"""
    
    def __init__(self, peer_id, channel, stub, stream_type='bidirectional', client_id=-1, client_instance=None):
        self.peer_id = peer_id
        self.channel = channel
        self.stub = stub
        self.stream_type = stream_type
        self.client_id = client_id  # Add client_id support
        self.client_instance = client_instance  # Add client instance reference for calling callback functions
        
        # Streaming objects
        self.request_stream = None
        self.response_stream = None
        self.request_queue = queue.Queue()
        
        # State management
        self.is_active = False
        self.last_activity = time.time()
        self.bytes_sent = 0
        self.bytes_received = 0
        self.chunks_sent = 0
        self.chunks_received = 0
        
        # Thread management
        self.sender_thread = None
        self.receiver_thread = None
        self.upload_sender_thread = None
        
        self.send_retries = 0
        
    def start_streaming(self):
        """Start streaming (automatic fallback compatibility)"""
        if self.is_active:
            return
            
        try:
            # 🚀 Try to start multi-stream streaming
            self._start_multi_streaming()
            
        except Exception as e:
            # 🔧 Streaming failed, mark as unavailable, but don't throw error
            logger.warning(f"[StreamChannel] Multi-streaming not supported for peer {self.peer_id}, will use traditional messaging: {e}")
            self.is_active = False  # Mark streaming as unavailable
            # Don't throw exception, let upper layer code continue using traditional methods
            
    def _start_multi_streaming(self):
        """Internal method: Start multi-stream concurrent streaming - 🚀 Multi-optimization design"""
        # 🚀 Multi-stream concurrent optimization: Create dedicated stream pipelines
        
        # 🔧 CRITICAL FIX: Set is_active=True first, then create generators
        self.is_active = True
        logger.debug(f"[StreamChannel] 🚀 Activating multi-pipeline streaming to peer {self.peer_id}")
        
        # 🚀 Optimization 1: Stream pipeline reuse - Create dedicated queues and buffers
        self.control_request_queue = queue.Queue(maxsize=500)  # Control message queue
        self.upload_request_queue = queue.PriorityQueue(maxsize=500)   # 🎯 Upload priority queue (importance+rarity sorting)
        self.download_request_queue = queue.PriorityQueue(maxsize=500) # 🎯 Download priority queue (importance+rarity sorting)
        
        # 🚀 Optimization 2: Performance monitoring and statistics
        self.control_msg_count = 0
        self.upload_chunk_count = 0
        self.download_request_count = 0
        self.last_activity_time = time.time()
        
        # 🚀 Optimization 3: Smart stream allocation
        # 1. Control stream: Dedicated handling of lightweight control messages (HAVE, BITFIELD, INTERESTED, etc.)
        self.control_stream = self.stub.streamChunks(self._enhanced_control_generator())
        logger.debug(f"[StreamChannel] ✅ Control pipeline established for peer {self.peer_id}")
        
        # 2. Upload stream: Dedicated transmission of large chunk data (managed in dedicated threads)
        # 3. Download stream: Batch processing of chunk requests (managed in dedicated threads)
        
        # 🚀 Optimization 4: Dedicated thread pool for improved concurrency performance
        self.control_receiver_thread = threading.Thread(
            target=self._enhanced_control_response_handler,
            daemon=True,
            name=f"ControlPipeline-{self.peer_id}"
        )

        self.download_sender_thread = threading.Thread(
            target=self._enhanced_download_batch_processor,
            daemon=True,
            name=f"DownloadPipeline-{self.peer_id}"
        )

        self.upload_sender_thread = threading.Thread(
            target=self._enhanced_upload_stream_processor,
            daemon=True,
            name=f"UploadPipeline-{self.peer_id}"
        )

        # 🚀 DEADLOCK FIX: Decouple network read from callback processing
        # Download response queue: capacity must >= max_batch_size to ensure single batch responses
        # can be enqueued without blocking the read thread
        self.max_download_batch_size = 8  # Conservative for small buffer environments
        self.download_response_queue = queue.Queue(maxsize=self.max_download_batch_size)

        self.download_response_worker_thread = threading.Thread(
            target=self._download_response_worker,
            daemon=True,
            name=f"DownloadRespWorker-{self.peer_id}"
        )

        # Start optimized streaming pipeline processors
        self.control_receiver_thread.start()
        self.download_sender_thread.start()
        self.upload_sender_thread.start()
        self.download_response_worker_thread.start()

        logger.debug(f"[StreamChannel] 🚀 Download response worker started for peer {self.peer_id}")
        
        logger.debug(f"[StreamChannel] 🚀 Multi-pipeline streaming ACTIVE for peer {self.peer_id}")
        logger.debug(f"[StreamChannel] 📊 Performance monitoring enabled - Control/Upload/Download pipelines ready")
            
    def _enhanced_control_generator(self):
        """🚀 Optimization 1: Enhanced control message flow generator - specialized for lightweight control messages"""
        logger.debug(f"[StreamChannel] 🎛️ Control pipeline generator started for peer {self.peer_id}")
        
        # 🚀 Performance optimization parameters
        # heartbeat_interval = 30.0  # heartbeat interval
        last_heartbeat = time.time()
        processed_count = 0
        
        while self.is_active:
            try:
                # 🚀 Optimization: Use dedicated control queue to avoid message confusion
                try:
                    request = self.control_request_queue.get(timeout=0.2)  # Optimize response time and reduce CPU idle spinning
                except queue.Empty:
                    # 🚀 Optimization: Send heartbeat to keep connection active
                    # current_time = time.time()
                    # if current_time - last_heartbeat > heartbeat_interval:
                    #     # Send lightweight heartbeat message to maintain connection
                    #     heartbeat_request = self._create_heartbeat_message()
                    #     if heartbeat_request:
                    #         yield heartbeat_request
                    #         last_heartbeat = current_time
                    #         logger.debug(f"[StreamChannel] 💓 Heartbeat sent to peer {self.peer_id}")
                    continue
                
                if request is None:  # Sentinel for shutdown
                    logger.debug(f"[StreamChannel] 🎛️ Control pipeline shutdown for peer {self.peer_id}")
                    break
                
                # 🚀 Optimization: Intelligent message routing - only process control messages
                if self._is_control_message(request):
                    yield request
                    processed_count += 1
                    self.control_msg_count += 1
                    self.last_activity_time = time.time()
                    
                    # 🚀 Performance monitoring
                    if processed_count % 50 == 0:
                        logger.debug(f"[StreamChannel] 🎛️ Control pipeline processed {processed_count} messages for peer {self.peer_id}")
                else:
                    # Re-route non-control messages to correct queue
                    self._route_message_to_correct_pipeline(request)
                    
                self.control_request_queue.task_done()
                
            except Exception as e:
                logger.error(f"[StreamChannel] 🎛️ Control generator error for peer {self.peer_id}: {e}")
                time.sleep(0.1)  # Avoid error loops
                
        logger.debug(f"[StreamChannel] 🎛️ Control pipeline generator ended for peer {self.peer_id}, processed {processed_count} messages")
        
    def _is_control_message(self, request) -> bool:
        """Determine if it is a control message"""
        control_types = [
            gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
            gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL,
            # 🔧 添加新的BitTorrent控制消息类型
            gRPC_comm_manager_pb2.ChunkType.CHUNK_INTERESTED_REQ,
            gRPC_comm_manager_pb2.ChunkType.CHUNK_UNCHOKE_REQ,
            gRPC_comm_manager_pb2.ChunkType.CHUNK_CHOKE_REQ,
        ]
        return request.chunk_type in control_types
        
    # def _create_heartbeat_message(self):
    #     """创建心跳消息维持连接"""
    #     try:
    #         return gRPC_comm_manager_pb2.ChunkStreamRequest(
    #             sender_id=self.client_id,
    #             receiver_id=self.peer_id,
    #             round_num=0,  # 心跳消息不需要具体轮次
    #             chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
    #             timestamp=int(time.time() * 1000)
    #         )
    #     except Exception:
    #         return None
            
    def _route_message_to_correct_pipeline(self, request):
        """将消息路由到正确的流管道"""
        try:
            if request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE:
                # chunk数据 -> 上传流 (CHUNK_PIECE按importance+rare排序)
                importance_score = getattr(request, 'importance_score', 0.0)
                rarity_score = 0  # 路由时暂时使用默认值
                priority = self._calculate_request_priority(importance_score, rarity_score)
                self.upload_request_queue.put((priority, request), timeout=0.1)
                logger.debug(f"[StreamChannel] 🔄 Routed CHUNK_PIECE to upload pipeline for peer {self.peer_id}")
            elif request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST:
                # chunk请求 -> 下载流 (CHUNK_REQUEST按importance+rare排序)
                importance_score = getattr(request, 'importance_score', 0.0)
                rarity_score = 0  # 路由时暂时使用默认值
                priority = self._calculate_request_priority(importance_score, rarity_score)
                self.download_request_queue.put((priority, request), timeout=0.1)
                logger.debug(f"[StreamChannel] 🔄 Routed CHUNK_REQUEST to download pipeline for peer {self.peer_id}")
            else:
                logger.warning(f"[StreamChannel] 🔄 Unknown message type for routing: {request.chunk_type}")
        except queue.Full:
            logger.warning(f"[StreamChannel] 🔄 Target pipeline queue full, dropping message for peer {self.peer_id}")
        except Exception as e:
            logger.error(f"[StreamChannel] 🔄 Message routing error for peer {self.peer_id}: {e}")
                
    def _upload_request_generator(self):
        """🚀 专用上传流：只处理chunk数据传输 - 修复Generator过早终止问题"""
        logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Upload stream generator started for peer {self.peer_id}")
        
        # 🚀 性能优化：调整等待参数减少CPU空转
        empty_queue_wait = 0.1   # 100ms等待时间 - 减少CPU空转
        max_idle_time = 5.0      # 最大空闲时间5秒
        last_activity = time.time()
        
        while self.is_active:
            try:
                # 🔧 CRITICAL FIX: 使用带超时的get替代get_nowait，并继续循环而不是return
                try:
                    priority_item = self.upload_request_queue.get(timeout=empty_queue_wait)
                    last_activity = time.time()  # 更新活动时间
                    # 🎯 从优先队列中解包：(priority, request)
                    priority, request = priority_item
                    
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
                    logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Received shutdown sentinel for peer {self.peer_id}")
                    break
                
                # 🚀 处理正常的chunk请求
                logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Yielding chunk request for peer {self.peer_id}")
                logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Request details - round={request.round_num}, source={request.source_client_id}, chunk={request.chunk_id}, size={len(request.chunk_data)}")
                
                yield request
                self.upload_request_queue.task_done()
                
                logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Successfully yielded chunk request for peer {self.peer_id}")
                
            except Exception as e:
                logger.error(f"[🚀 UploadGenerator] Client {self.client_id}: Unexpected error: {e}")
                if not self.is_active:
                    break
                # 🔧 FIX: 遇到异常时短暂等待后继续，避免紧密循环
                time.sleep(0.1)
                
        logger.debug(f"[🚀 UploadGenerator] Client {self.client_id}: Upload stream generator ended for peer {self.peer_id}")
    
    def _enhanced_upload_stream_processor(self):
        """🚀 优化2：增强的上传流处理器 - 地下管道模式与批量传输"""
        logger.debug(f"[StreamChannel] 📤 Upload pipeline processor started for peer {self.peer_id}")
        
        # 🚀 地下管道性能参数
        total_chunks_uploaded = 0
        total_upload_time = 0.0
        last_performance_check = time.time()
        connection_retries = 0
        max_retries = 3
        
        # 🔧 CRITICAL FIX: 持续运行的upload pipeline，不要过早退出
        while self.is_active:
            try:
                logger.debug(f"[StreamChannel] 📤 Starting persistent upload pipeline to peer {self.peer_id} (attempt {connection_retries + 1})")
                
                # 🚀 地下管道模式：持续运行的上传流
                upload_start_time = time.time()
                upload_response = self.stub.uploadChunks(self._enhanced_upload_generator())
                
                # 🚀 关键：消费最终响应以完成客户端流
                if upload_response:
                    upload_duration = time.time() - upload_start_time
                    logger.debug(f"[StreamChannel] 📤 Upload pipeline batch completed for peer {self.peer_id}")
                    logger.debug(f"[StreamChannel] 📤 Response: success={upload_response.successful_chunks}, failed={upload_response.failed_chunks}")
                    logger.debug(f"[StreamChannel] 📤 Batch duration: {upload_duration:.3f}s")
                    
                    # 🚀 性能统计更新
                    total_chunks_uploaded += upload_response.successful_chunks
                    total_upload_time += upload_duration
                    
                    # 🔧 FIX: 重置连接计数器，继续运行而不是退出
                    connection_retries = 0
                    logger.debug(f"[StreamChannel] 📤 Upload pipeline continuing for peer {self.peer_id}...")
                    continue  # 继续运行，而不是break
                
            except Exception as e:
                connection_retries += 1
                error_msg = str(e)
                
                if "UNAVAILABLE" in error_msg or "DEADLINE_EXCEEDED" in error_msg:
                    # 网络连接问题，可以重试
                    logger.warning(f"[StreamChannel] 📤 Upload pipeline connection error for peer {self.peer_id} (attempt {connection_retries}): {e}")
                    if connection_retries < max_retries:
                        retry_delay = min(2.0 ** connection_retries, 10.0)  # 指数退避
                        logger.debug(f"[StreamChannel] 📤 Retrying upload pipeline in {retry_delay}s...")
                        time.sleep(retry_delay)
                        continue
                else:
                    # 其他严重错误，短暂等待后重试
                    logger.error(f"[StreamChannel] 📤 Upload pipeline error for peer {self.peer_id}: {e}")
                    if connection_retries >= max_retries:
                        logger.warning(f"[StreamChannel] 📤 Too many errors, waiting 60s before restart for peer {self.peer_id}")
                        time.sleep(60.0)
                        connection_retries = 0  # 重置计数器
                    else:
                        time.sleep(5.0)  # 短暂等待后继续
                    continue
        
        # 🔧 这个分支永远不会到达，因为while循环只在is_active=False时退出
        logger.debug(f"[StreamChannel] 📤 Upload pipeline processor ended for peer {self.peer_id} (is_active={self.is_active})")
        
        # 🚀 最终性能报告
        if total_chunks_uploaded > 0:
            avg_upload_time = total_upload_time / max(total_chunks_uploaded, 1)
            logger.debug(f"[StreamChannel] 📊 Upload pipeline final stats for peer {self.peer_id}:")
            logger.debug(f"[StreamChannel] 📊   {total_chunks_uploaded} chunks uploaded in {total_upload_time:.3f}s")
            logger.debug(f"[StreamChannel] 📊   Average: {avg_upload_time:.3f}s per chunk")
        
        logger.debug(f"[StreamChannel] 📤 Upload pipeline processor ended for peer {self.peer_id}")
        
    def _enhanced_upload_generator(self):
        """🚀 优化2：增强的上传流生成器 - 永不停止的地下管道"""
        logger.debug(f"[StreamChannel] 📤 Upload pipeline generator started for peer {self.peer_id}")
        
        # 🚀 地下管道优化参数
        empty_queue_wait = 0.1         # 100ms等待时间 - 减少CPU空转
        processed_chunks = 0
        consecutive_empty_checks = 0
        max_empty_checks = 10          # 最大空检查次数 (1秒)
        
        while self.is_active:
            try:
                # 🚀 高效队列处理
                try:
                    priority_item = self.upload_request_queue.get(timeout=empty_queue_wait)
                    consecutive_empty_checks = 0  # 重置计数器
                    # 🎯 从优先队列中解包：(priority, request)
                    priority, request = priority_item
                    
                except queue.Empty:
                    consecutive_empty_checks += 1
                    
                    # 🚀 优化：自适应等待策略
                    if consecutive_empty_checks >= max_empty_checks:
                        # 队列长时间为空，发送心跳维持连接
                        # current_time = time.time()
                        # if current_time - last_heartbeat > heartbeat_interval:
                        #     heartbeat_msg = self._create_upload_heartbeat()
                        #     if heartbeat_msg:
                        #         yield heartbeat_msg
                        #         last_heartbeat = current_time
                        #         logger.debug(f"[StreamChannel] 📤💓 Upload heartbeat sent to peer {self.peer_id}")
                        consecutive_empty_checks = 0  # 重置计数器
                    continue
                
                if request is None:  # Sentinel for shutdown
                    logger.debug(f"[StreamChannel] 📤 Upload generator shutdown for peer {self.peer_id}")
                    break
                
                # 🚀 处理chunk数据传输
                logger.debug(f"[StreamChannel] 📤 Yielding chunk {request.source_client_id}:{request.chunk_id} to peer {self.peer_id}")
                
                yield request
                processed_chunks += 1
                self.upload_chunk_count += 1
                self.last_activity_time = time.time()
                
                self.upload_request_queue.task_done()
                
                # 🚀 性能监控
                if processed_chunks % 100 == 0:
                    logger.debug(f"[StreamChannel] 📤 Upload generator processed {processed_chunks} chunks for peer {self.peer_id}")
                
            except Exception as e:
                logger.error(f"[StreamChannel] 📤 Upload generator error for peer {self.peer_id}: {e}")
                if not self.is_active:
                    break
                time.sleep(0.1)  # 避免错误循环
                
        logger.debug(f"[StreamChannel] 📤 Upload generator ended for peer {self.peer_id}, processed {processed_chunks} chunks")
        
    # def _create_upload_heartbeat(self):
    #     """创建上传心跳消息"""
    #     try:
    #         return gRPC_comm_manager_pb2.ChunkStreamRequest(
    #             sender_id=self.client_id,
    #             receiver_id=self.peer_id,
    #             round_num=0,
    #             chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,  # 轻量级心跳
    #             timestamp=int(time.time() * 1000),
    #             chunk_data=b'heartbeat'  # 最小化数据
    #         )
    #     except Exception:
    #         return None
                
    def _enhanced_control_response_handler(self):
        """🚀 优化1：增强的控制消息响应处理器 - 高性能响应处理"""
        logger.debug(f"[StreamChannel] 🎛️ Control pipeline response handler started for peer {self.peer_id}")
        
        processed_responses = 0
        error_count = 0
        last_stats_report = time.time()
        
        try:
            for response in self.control_stream:
                if not self.is_active:
                    logger.debug(f"[StreamChannel] 🎛️ Control response handler stopping for peer {self.peer_id}")
                    break
                
                # 🚀 Optimization: Fast response processing
                start_time = time.time()
                try:
                    self._handle_control_response(response)
                    processed_responses += 1
                    self.last_activity_time = time.time()
                    
                    # 🚀 Performance monitoring
                    processing_time = time.time() - start_time
                    if processing_time > 0.1:  # Slow response over 100ms
                        logger.warning(f"[StreamChannel] 🐌 Slow control response processing: {processing_time:.3f}s for peer {self.peer_id}")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"[StreamChannel] 🎛️ Control response processing error for peer {self.peer_id}: {e}")
                    if error_count > 10:  # Too many errors, disconnect
                        logger.error(f"[StreamChannel] 🎛️ Too many errors ({error_count}), stopping control pipeline for peer {self.peer_id}")
                        break
                
                # 🚀 Regular performance reports
                current_time = time.time()
                if current_time - last_stats_report > 60.0:  # Report every minute
                    logger.debug(f"[StreamChannel] 📊 Control pipeline stats for peer {self.peer_id}: {processed_responses} responses, {error_count} errors")
                    last_stats_report = current_time
                    
        except Exception as e:
            logger.error(f"[StreamChannel] 🎛️ Control response handler fatal error for peer {self.peer_id}: {e}")
        finally:
            logger.debug(f"[StreamChannel] 🎛️ Control response handler ended for peer {self.peer_id}: {processed_responses} responses processed, {error_count} errors")
            
    def _download_response_worker(self):
        """🚀 DEADLOCK FIX: Dedicated worker for callback processing (hash/callback).
        This thread NEVER participates in gRPC stream reading, ensuring network reads
        are completely decoupled from potentially blocking callback operations.
        """
        logger.debug(f"[StreamChannel] 📥 Download response worker started for peer {self.peer_id}")
        processed_count = 0
        error_count = 0

        while self.is_active:
            try:
                # Block waiting for response from queue
                resp = self.download_response_queue.get(timeout=1.0)
                if resp is None:
                    # Shutdown signal
                    logger.debug(f"[StreamChannel] 📥 Download response worker shutdown signal for peer {self.peer_id}")
                    break

                try:
                    # Process response (hash + callback) - may block but won't affect network reads
                    self._handle_download_response(resp)
                    processed_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"[StreamChannel] 📥 Download response worker error for peer {self.peer_id}: {e}")
                    if error_count > 50:
                        logger.error(f"[StreamChannel] 📥 Too many errors ({error_count}), stopping worker for peer {self.peer_id}")
                        break

            except queue.Empty:
                # Timeout, continue checking is_active
                continue
            except Exception as e:
                logger.error(f"[StreamChannel] 📥 Download response worker fatal error for peer {self.peer_id}: {e}")
                break

        logger.debug(f"[StreamChannel] 📥 Download response worker ended for peer {self.peer_id}: "
                    f"{processed_count} processed, {error_count} errors")

    def _enhanced_download_batch_processor(self):
        """🚀 Optimization 3: Enhanced download batch processor - Intelligent batch processing of chunk requests
        🔧 DEADLOCK FIX: Read stream only enqueues to download_response_queue, never calls callbacks directly.
        """
        logger.debug(f"[StreamChannel] 📥 Download pipeline processor started for peer {self.peer_id}")

        # 🚀 DEADLOCK FIX: Limit batch size to queue capacity to ensure responses can be enqueued
        batch_requests = []
        adaptive_batch_size = 2   # Start small for safety
        max_batch_size = getattr(self, "max_download_batch_size", 8)  # Must not exceed queue capacity
        min_batch_timeout = 0.02
        max_batch_timeout = 0.1
        current_batch_timeout = min_batch_timeout

        # 🚀 Performance monitoring
        processed_batches = 0
        total_chunks_downloaded = 0
        total_download_time = 0.0
        last_performance_check = time.time()

        while self.is_active:
            batch_start_time = time.time()

            try:
                # 🚀 Optimization: Adaptive batch collection
                end_time = time.time() + current_batch_timeout
                while time.time() < end_time and len(batch_requests) < adaptive_batch_size:
                    try:
                        priority_item = self.download_request_queue.get(timeout=0.05)
                        if priority_item is None:  # Compatible with old format shutdown signal
                            logger.debug(f"[StreamChannel] 📥 Download pipeline shutdown signal for peer {self.peer_id}")
                            break
                        # 🎯 Unpack from priority queue: (priority, request)
                        priority, request = priority_item
                        if request is None:  # New format shutdown signal
                            logger.debug(f"[StreamChannel] 📥 Download pipeline shutdown signal for peer {self.peer_id}")
                            break
                        batch_requests.append(request)
                    except queue.Empty:
                        break

                # 🚀 Optimization: Intelligent batch sending
                if batch_requests:
                    batch_size = len(batch_requests)
                    logger.debug(f"[StreamChannel] 📥 Sending batch of {batch_size} download requests to peer {self.peer_id}")

                    # 🚀 DEADLOCK FIX: Wait for queue to have enough space BEFORE initiating gRPC stream
                    # This ensures the read thread can drain the entire batch without blocking
                    expected = batch_size
                    wait_start = time.time()
                    queue_wait_timeout = 60.0  # 60s timeout for queue space

                    while self.is_active:
                        free = self.download_response_queue.maxsize - self.download_response_queue.qsize()
                        if free >= expected:
                            break
                        if time.time() - wait_start > queue_wait_timeout:
                            logger.error(f"[StreamChannel] 📥 Queue blocked for {queue_wait_timeout}s, breaking to recover for peer {self.peer_id}")
                            break
                        time.sleep(0.005)  # Small sleep; gRPC stream not yet started, won't block server

                    batch_request = gRPC_comm_manager_pb2.ChunkBatchRequest(
                        client_id=self.peer_id,
                        sender_id=self.client_id,
                        round_num=batch_requests[0].round_num,
                        chunk_requests=[
                            gRPC_comm_manager_pb2.ChunkRequest(
                                source_client_id=req.source_client_id,
                                chunk_id=req.chunk_id,
                                importance_score=req.importance_score
                            ) for req in batch_requests
                        ]
                    )

                    # 🚀 DEADLOCK FIX: Only enqueue responses, never call callbacks directly
                    download_start = time.time()
                    try:
                        download_stream = self.stub.downloadChunks(batch_request)
                        chunks_received = 0

                        for chunk_response in download_stream:
                            # 🚀 DEADLOCK FIX: Only enqueue, no callback processing here!
                            # This ensures the read thread drains the stream completely
                            self.download_response_queue.put(chunk_response)
                            chunks_received += 1
                            self.download_request_count += 1

                        download_time = time.time() - download_start
                        total_download_time += download_time
                        total_chunks_downloaded += chunks_received
                        processed_batches += 1

                        # 🚀 Adaptive optimization: Adjust batch processing parameters based on performance
                        avg_time_per_chunk = download_time / max(chunks_received, 1)
                        if avg_time_per_chunk < 0.01:  # Very fast, increase batch size
                            adaptive_batch_size = min(adaptive_batch_size + 1, max_batch_size)
                            current_batch_timeout = min(current_batch_timeout * 1.1, max_batch_timeout)
                        elif avg_time_per_chunk > 0.05:  # Slower, decrease batch size
                            adaptive_batch_size = max(adaptive_batch_size - 1, 2)
                            current_batch_timeout = max(current_batch_timeout * 0.9, min_batch_timeout)

                        logger.debug(f"[StreamChannel] 📥 Batch completed: {chunks_received} chunks in {download_time:.3f}s, adaptive_size={adaptive_batch_size}")

                    except Exception as e:
                        logger.error(f"[StreamChannel] 📥 Download stream error for peer {self.peer_id}: {e}")
                        # Reset batch processing parameters on error
                        adaptive_batch_size = 2
                        current_batch_timeout = min_batch_timeout

                    batch_requests.clear()

                # 🚀 Regular performance reporting and optimization
                current_time = time.time()
                if current_time - last_performance_check > 30.0:  # Check every 30 seconds
                    if processed_batches > 0:
                        avg_batch_time = total_download_time / processed_batches
                        avg_chunks_per_batch = total_chunks_downloaded / processed_batches
                        queue_size = self.download_response_queue.qsize()
                        logger.debug(f"[StreamChannel] 📊 Download pipeline performance for peer {self.peer_id}:")
                        logger.debug(f"[StreamChannel] 📊   {processed_batches} batches, {total_chunks_downloaded} chunks")
                        logger.debug(f"[StreamChannel] 📊   Avg: {avg_chunks_per_batch:.1f} chunks/batch, {avg_batch_time:.3f}s/batch")
                        logger.debug(f"[StreamChannel] 📊   Current adaptive_size: {adaptive_batch_size}, response_queue: {queue_size}/{self.max_download_batch_size}")
                    last_performance_check = current_time

            except Exception as e:
                logger.error(f"[StreamChannel] 📥 Download batch processor error for peer {self.peer_id}: {e}")
                batch_requests.clear()
                time.sleep(0.1)  # Avoid error loops

        logger.debug(f"[StreamChannel] 📥 Download pipeline processor ended for peer {self.peer_id}")
        logger.debug(f"[StreamChannel] 📊 Final stats: {processed_batches} batches, {total_chunks_downloaded} total chunks downloaded")
            
    def _handle_control_response(self, response):
        """🔧 Fix: Handle control message responses - Support all response types"""
        logger.debug(f"[StreamChannel] Received control response from peer {self.peer_id}: type={response.response_type}")
        
        if not self.client_instance:
            logger.warning(f"[StreamChannel] No client instance for peer {self.peer_id}, cannot process control response")
            return

        try:
            # Note: Message is imported at module level to avoid import lock contention

            # 🔧 Fix: Correctly handle all ChunkResponseType enum values
            if response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_ACK:
                # ACK response: Confirm received control message
                logger.debug(f"[StreamChannel] Received ACK from peer {self.peer_id} for chunk {response.chunk_id}")
                # ACK messages usually don't need special processing, just log
                return
                
            elif response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_NACK:
                # NACK response: Reject/error response
                logger.warning(f"[StreamChannel] Received NACK from peer {self.peer_id} for chunk {response.chunk_id}")
                # TODO: Can implement retransmission logic here
                return
                
            elif response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_HAVE_RESP:
                # HAVE response: Peer notifies having a chunk
                content = {
                    'round_num': response.round_num,
                    'source_client_id': getattr(response, 'source_client_id', response.sender_id),
                    'chunk_id': response.chunk_id
                }
                msg_type = 'have'
                
            elif response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_INTERESTED:
                # INTERESTED response: Peer expresses interest
                content = {
                    'round_num': response.round_num,
                    'peer_id': response.sender_id
                }
                msg_type = 'interested'
                
            else:
                # 🔧 Use numeric values for compatibility checking
                response_type_value = int(response.response_type)
                logger.warning(f"[StreamChannel] Unknown control response type: {response_type_value} from peer {self.peer_id}")
                
                # 🔧 Try to infer response type
                if response_type_value == 0:  # CHUNK_ACK
                    logger.debug(f"[StreamChannel] Treating response type 0 as ACK from peer {self.peer_id}")
                    return
                elif response_type_value == 1:  # CHUNK_NACK  
                    logger.warning(f"[StreamChannel] Treating response type 1 as NACK from peer {self.peer_id}")
                    return
                elif response_type_value == 2:  # CHUNK_HAVE_RESP
                    content = {
                        'round_num': response.round_num,
                        'source_client_id': getattr(response, 'source_client_id', response.sender_id),
                        'chunk_id': response.chunk_id
                    }
                    msg_type = 'have'
                elif response_type_value == 3:  # CHUNK_INTERESTED
                    content = {
                        'round_num': response.round_num,
                        'peer_id': response.sender_id
                    }
                    msg_type = 'interested'
                else:
                    logger.error(f"[StreamChannel] Unsupported response type: {response_type_value} from peer {self.peer_id}")
                    return
                
            # Create Message object and call callback
            if 'msg_type' in locals() and 'content' in locals():
                message = Message(
                    msg_type=msg_type,
                    sender=self.peer_id,
                    receiver=[self.client_id],
                    content=content
                )
                
                # 🔧 Call correct callback function
                if msg_type == 'have':
                    logger.debug(f"[StreamChannel] ✅ Calling callback_funcs_for_have for peer {self.peer_id}")
                    self.client_instance.callback_funcs_for_have(message)
                elif msg_type == 'interested':
                    logger.debug(f"[StreamChannel] ✅ Calling callback_funcs_for_interested for peer {self.peer_id}")
                    if hasattr(self.client_instance, 'callback_funcs_for_interested'):
                        self.client_instance.callback_funcs_for_interested(message)
                    else:
                        logger.debug(f"[StreamChannel] No callback for interested messages")
                elif msg_type == 'bitfield':
                    logger.debug(f"[StreamChannel] ✅ Calling callback_funcs_for_bitfield for peer {self.peer_id}")
                    if hasattr(self.client_instance, 'callback_funcs_for_bitfield'):
                        self.client_instance.callback_funcs_for_bitfield(message)
                    else:
                        logger.debug(f"[StreamChannel] No callback for bitfield messages")
                
        except Exception as e:
            logger.error(f"[StreamChannel] Error handling control response from peer {self.peer_id}: {e}")
            import traceback
            logger.error(f"[StreamChannel] Traceback: {traceback.format_exc()}")
        
    def _handle_download_response(self, response):
        """🔧 Optimization: Handle chunk download response - Deduplicate statistics, prevent duplicate processing"""        
        if not self.client_instance:
            logger.warning(f"[StreamChannel] No client instance for peer {self.peer_id}, cannot process chunk download")
            return
            
        try:
            # Validate response data
            if not response.response_data:
                logger.error(f"[StreamChannel] 🚫 Empty chunk data from peer {self.peer_id}, chunk_id={response.chunk_id}")
                return
            
            # 🔧 Unified statistics management (count only once)
            chunk_size = len(response.response_data)
            self.chunks_received += 1
            self.bytes_received += chunk_size
            
            # 🔧 Update deduplication state
            source_client_id = getattr(response, 'source_client_id', response.sender_id)
            self.mark_chunk_completed(response.round_num, source_client_id, response.chunk_id)

            # Create ChunkData object
            # Note: Message and ChunkData are imported at module level to avoid import lock contention
            import hashlib

            # Calculate checksum
            checksum = hashlib.sha256(response.response_data).hexdigest()
            
            # Create ChunkData object to wrap raw data
            chunk_data = ChunkData(response.response_data, checksum)
            
            # Construct message content (compatible with traditional BitTorrent PIECE message format)
            content = {
                'round_num': response.round_num,
                'source_client_id': source_client_id,  # Use correct field
                'chunk_id': response.chunk_id,
                'data': chunk_data,
                'checksum': checksum
            }
            
            # Create Message object
            message = Message(
                msg_type='piece',
                sender=self.peer_id,
                receiver=[self.client_id],
                content=content
            )
            
            # Call callback function to handle chunk piece
            logger.debug(f"[StreamChannel] Calling callback_funcs_for_piece for peer {self.peer_id}, chunk {response.sender_id}:{response.chunk_id}")
            self.client_instance.callback_funcs_for_piece(message)
            
        except Exception as e:
            logger.error(f"[StreamChannel] Error handling chunk download from peer {self.peer_id}: {e}")
        
    def _handle_chunk_response(self, response):
        """Compatibility method: Handle received chunk response"""
        self.chunks_received += 1
        self.bytes_received += len(response.response_data) if response.response_data else 0
        logger.debug(f"[StreamChannel] Received chunk response from peer {self.peer_id}")
        
    def _calculate_request_priority(self, importance_score: float, rarity_score: int) -> tuple:
        """🎯 Calculate request priority: Importance first, sort by rarity when importance is similar"""
        import random
        importance_jitter = random.uniform(-0.01, 0.01)
        rarity_jitter = random.uniform(-0.1, 0.1)
        # Primary priority: importance (negative value because PriorityQueue is min-heap, we want high importance first)
        primary_priority = -importance_score + importance_jitter * rarity_score
        # primary_priority = -random.random()
        # Secondary priority: rarity (smaller value means rarer, higher priority)
        secondary_priority = rarity_score + rarity_jitter
        
        # Third priority: timestamp (ensure FIFO under same priority)
        tertiary_priority = random.random()
        
        return (primary_priority, secondary_priority, tertiary_priority)
        
    def send_chunk_request(self, round_num: int, source_client_id: int, chunk_id: int, 
                          importance_score: float = 0.0, rarity_score: int = 0):
        """🔧 Enhanced deduplication: Send chunk request - Strictly prevent duplicate requests"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
        
        # 🔧 Key deduplication mechanism: Check if this chunk has already been requested
        chunk_key = (round_num, source_client_id, chunk_id)
        if self.send_retries == 4:
            self.send_retries = 0
        
        # Initialize deduplication sets
        if not hasattr(self, 'requested_chunks'):
            self.requested_chunks = set()
        if not hasattr(self, 'completed_chunks'):
            self.completed_chunks = set()
        
        # 🔧 Strict deduplication check
        if chunk_key in self.requested_chunks and self.send_retries < 3:
            logger.debug(f"[StreamChannel] 🚫 DUPLICATE REQUEST blocked: chunk {source_client_id}:{chunk_id} already requested from peer {self.peer_id}")
            self.send_retries+=1
            return False
        
        if chunk_key in self.completed_chunks and self.send_retries < 3:
            logger.debug(f"[StreamChannel] 🚫 REDUNDANT REQUEST blocked: chunk {source_client_id}:{chunk_id} already completed from peer {self.peer_id}")
            self.send_retries+=1
            return False
        
        # 🔧 Queue size limit (avoid memory leak)
        if self.download_request_queue.qsize() >= self.download_request_queue.maxsize * 0.8:
            logger.warning(f"[StreamChannel] 🚫 REQUEST QUEUE near full ({self.download_request_queue.qsize()}/{self.download_request_queue.maxsize}), rejecting new request")
            return False
            
        request = gRPC_comm_manager_pb2.ChunkStreamRequest(
            sender_id=self.client_id,
            receiver_id=self.peer_id,
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
            importance_score=importance_score,
            timestamp=int(time.time() * 1000)
        )
        
        try:
            # 🎯 Calculate priority: CHUNK_REQUEST sorted by importance+rarity
            priority = self._calculate_request_priority(importance_score, rarity_score)
            
            # 🚀 Strict request queue management - Use priority queue
            self.download_request_queue.put((priority, request), timeout=0.5)
            
            # 🔧 Record request to prevent duplication
            self.requested_chunks.add(chunk_key)
            
            logger.debug(f"[StreamChannel] ✅ Priority request queued: chunk {source_client_id}:{chunk_id} (importance:{importance_score:.3f}, rarity:{rarity_score}) to peer {self.peer_id}")
            logger.debug(f"[StreamChannel] 📊 Request stats: {len(self.requested_chunks)} requested, {len(self.completed_chunks)} completed")
            
            return True
        except queue.Full:
            logger.error(f"[StreamChannel] 🚫 Download request queue full for peer {self.peer_id}")
            return False
        except Exception as e:
            logger.error(f"[StreamChannel] 🚫 Failed to queue request: {e}")
            return False
            
    def mark_chunk_completed(self, round_num: int, source_client_id: int, chunk_id: int):
        """🔧 Mark chunk as completed, clear deduplication state"""
        chunk_key = (round_num, source_client_id, chunk_id)
        
        if hasattr(self, 'requested_chunks'):
            self.requested_chunks.discard(chunk_key)  # Remove from request set
            
        logger.debug(f"[StreamChannel] ✅ Marked chunk {source_client_id}:{chunk_id} as completed for peer {self.peer_id}")
            
    def send_chunk_data(self, round_num: int, source_client_id: int, chunk_id: int,
                       chunk_data: bytes, checksum: str, importance_score: float = 0.0, rarity_score: int = 0):
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
            # 🎯 计算上传优先级：CHUNK_PIECE按importance+rare排序
            priority = self._calculate_request_priority(importance_score, rarity_score)
            
            # 🚀 使用专用上传队列，按优先级排序
            logger.debug(f"[🚀 StreamChannel] Client {self.client_id}: Queueing priority chunk data {source_client_id}:{chunk_id} (importance:{importance_score:.3f}, rarity:{rarity_score}) to peer {self.peer_id}")
            logger.debug(f"[🚀 StreamChannel] Client {self.client_id}: Chunk data size: {len(chunk_data)}, queue size before: {self.upload_request_queue.qsize()}")
            
            self.upload_request_queue.put((priority, request), timeout=1.0)
            self.chunks_sent += 1
            self.bytes_sent += len(chunk_data)
            
            logger.debug(f"[🚀 StreamChannel] Client {self.client_id}: Successfully queued chunk data {source_client_id}:{chunk_id} to peer {self.peer_id}")
            logger.debug(f"[🚀 StreamChannel] Client {self.client_id}: Queue size after: {self.upload_request_queue.qsize()}")
            return True
        except queue.Full:
            logger.error(f"[StreamChannel] Upload queue full for peer {self.peer_id}")
            return False
    
    def send_control_message(self, msg_type: str, round_num: int, **kwargs) -> bool:
        """🚀 优化3：发送BitTorrent控制消息 - 使用专用控制流队列"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
        
        # 🚀 智能消息类型映射 - 使用专用的BitTorrent控制消息类型
        chunk_type_map = {
            'bitfield': gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
            'have': gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            'cancel': gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL,
            'request': gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
            'piece': gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE,
            # 🔧 修复：使用专用的BitTorrent协议消息类型，避免与chunk消息混淆
            'interested': gRPC_comm_manager_pb2.ChunkType.CHUNK_INTERESTED_REQ,
            'unchoke': gRPC_comm_manager_pb2.ChunkType.CHUNK_UNCHOKE_REQ,
            'choke': gRPC_comm_manager_pb2.ChunkType.CHUNK_CHOKE_REQ
        }
        
        if msg_type not in chunk_type_map:
            logger.error(f"[StreamChannel] 🚫 Unsupported control message type: {msg_type}")
            return False
        
        # 🚀 高效消息构建
        request = gRPC_comm_manager_pb2.ChunkStreamRequest(
            sender_id=self.client_id,
            receiver_id=self.peer_id,
            round_num=round_num,
            chunk_type=chunk_type_map[msg_type],
            timestamp=int(time.time() * 1000)
        )
        
        # 🚀 根据消息类型设置特定字段
        if msg_type == 'bitfield':
            # bitfield消息：使用chunk_data传输bitfield数据
            bitfield_data = kwargs.get('bitfield', [])
            request.chunk_data = json.dumps(bitfield_data).encode('utf-8')
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending BITFIELD to peer {self.peer_id}, chunks: {len(bitfield_data)}")
            
        elif msg_type == 'have':
            # have消息：标识拥有的chunk
            request.source_client_id = kwargs.get('source_client_id', 0)
            request.chunk_id = kwargs.get('chunk_id', 0)
            request.importance_score = kwargs.get('importance_score', 0.0)
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending HAVE to peer {self.peer_id}, chunk: {request.source_client_id}:{request.chunk_id}")
            
        elif msg_type == 'cancel':
            # cancel消息：取消chunk请求
            request.source_client_id = kwargs.get('source_client_id', 0)
            request.chunk_id = kwargs.get('chunk_id', 0)
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending CANCEL to peer {self.peer_id}, chunk: {request.source_client_id}:{request.chunk_id}")
            
        elif msg_type == 'interested':
            # interested消息：表示对peer感兴趣 - 使用专用消息类型
            request.source_client_id = self.client_id
            request.chunk_id = 0
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending INTERESTED to peer {self.peer_id}")
            
        elif msg_type == 'unchoke':
            # unchoke消息：允许peer发送请求 - 使用专用消息类型
            request.source_client_id = self.client_id
            request.chunk_id = 0
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending UNCHOKE to peer {self.peer_id}")
            
        elif msg_type == 'choke':
            # choke消息：禁止peer发送请求 - 使用专用消息类型
            request.source_client_id = self.client_id  
            request.chunk_id = 0
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending CHOKE to peer {self.peer_id}")
        
        try:
            # 🚀 使用专用控制消息队列，提高分发效率
            self.control_request_queue.put(request, timeout=0.5)  # 更短超时时间
            self.control_msg_count += 1
            logger.debug(f"[StreamChannel] ✅ Client {self.client_id}: Queued {msg_type.upper()} message to control pipeline for peer {self.peer_id}")
            return True
            
        except queue.Full:
            logger.warning(f"[StreamChannel] 🚫 Control pipeline queue full for peer {self.peer_id}, dropping {msg_type} message")
            return False
        except Exception as e:
            logger.error(f"[StreamChannel] 🚫 Failed to queue {msg_type} message for peer {self.peer_id}: {e}")
            return False
            
    def close(self):
        """Close streaming channel"""
        if not self.is_active:
            return

        self.is_active = False

        # Send shutdown signals to all queues
        try:
            self.request_queue.put(None)
            # 🎯 Priority queues need special handling for shutdown signals: use highest priority
            shutdown_priority = (-999999, 0, 0)  # Highest priority shutdown signal
            self.upload_request_queue.put((shutdown_priority, None))
            self.download_request_queue.put((shutdown_priority, None))
        except:
            pass

        # 🚀 DEADLOCK FIX: Send shutdown signal to download response worker
        try:
            if hasattr(self, "download_response_queue"):
                self.download_response_queue.put(None)
        except:
            pass

        # Wait for threads to finish (including download response worker)
        threads = [
            self.control_receiver_thread,
            self.download_sender_thread,
            self.upload_sender_thread,
            getattr(self, "download_response_worker_thread", None),  # 🚀 DEADLOCK FIX
        ]

        for thread in threads:
            if thread and thread.is_alive():
                thread.join(timeout=1.0)

        # Close gRPC channel
        try:
            self.channel.close()
            logger.debug(f"[StreamChannel] Closed streaming to peer {self.peer_id}")
        except:
            pass
            
    def get_stats(self):
        """Get channel statistics"""
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
    """🚀 gRPC Streaming Channel Manager - Create dedicated channels during topology construction"""
    
    def __init__(self, client_id, client_instance=None):
        self.client_id = client_id
        self.client_instance = client_instance
        self.channels: Dict[int, StreamingChannel] = {}  # peer_id -> StreamingChannel
        self.server_stubs: Dict[str, gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub] = {}
        
        # Performance statistics
        self.total_bytes_sent = 0
        self.total_bytes_received = 0
        self.total_chunks_sent = 0
        self.total_chunks_received = 0
        
        logger.debug(f"[StreamingManager] Initialized for client {client_id}")
        
    def create_channels_for_topology(self, neighbor_addresses: Dict[int, Tuple[str, int]]):
        """
        🚀 Create dedicated streaming channels for all neighbors during topology construction - Fix message size limits
        Args:
            neighbor_addresses: {peer_id: (host, port)}
        """
        logger.debug(f"[StreamingManager] Client {self.client_id}: Creating enhanced streaming channels for topology")
        logger.debug(f"[StreamingManager] Neighbor addresses: {neighbor_addresses}")
        
        # 🚀 Fix: gRPC channel configuration - Set large message windows
        max_message_size = 512 * 1024 * 1024  # 512MB message limit
        max_receive_size = 512 * 1024 * 1024  # 512MB receive limit
        
        # 🚀 HIGH-PERFORMANCE gRPC OPTIONS - Optimized for high concurrency and large data transfer
        # 🔧 CRITICAL FIX: Prevent TCP buffer deadlock in P2P chunk exchange
        grpc_options = [
            # === MESSAGE SIZE LIMITS ===
            ('grpc.max_send_message_length', max_message_size),
            ('grpc.max_receive_message_length', max_receive_size),

            # === 🔧 CRITICAL: TCP Socket Buffer - Prevent deadlock ===
            ('grpc.so_reuseport', 1),                                # Allow port reuse
            ('grpc.tcp_socket_recv_buffer_size', 8 * 1024 * 1024),   # 8MB receive buffer
            ('grpc.tcp_socket_send_buffer_size', 8 * 1024 * 1024),   # 8MB send buffer

            # === 🔧 FIX: KEEPALIVE SETTINGS - Solve "too many pings" error ===
            ('grpc.keepalive_time_ms', 120000),         # 120s keepalive (reduced frequency)
            ('grpc.keepalive_timeout_ms', 20000),       # 20s keepalive timeout
            ('grpc.keepalive_permit_without_calls', False), # ❌ Disable ping without active calls

            # === 🔧 CRITICAL: HTTP2 PING CONTROL - Prevent server rejection ===
            ('grpc.http2.max_pings_without_data', 2),   # ✅ Limit to 2 pings without data
            ('grpc.http2.min_time_between_pings_ms', 30000), # 30s minimum ping interval
            ('grpc.http2.min_ping_interval_without_data_ms', 600000), # 10min interval without data

            # === 🚀 HIGH-CONCURRENCY OPTIMIZATIONS ===
            ('grpc.so_reuseaddr', 1),                   # Socket reuse
            ('grpc.max_connection_idle_ms', 300000),    # 5min connection idle timeout
            ('grpc.max_connection_age_ms', 1800000),    # 30min max connection age
            ('grpc.max_connection_age_grace_ms', 60000), # 1min grace period before force close

            # === 🔧 CRITICAL: HTTP/2 Flow Control - Prevent blocking ===
            ('grpc.http2.initial_window_size', 64 * 1024 * 1024),           # 64MB (was missing)
            ('grpc.http2.initial_connection_window_size', 128 * 1024 * 1024),# 128MB (was missing)
            ('grpc.http2.max_frame_size', 16777215),    # 16MB max frame size
            ('grpc.http2.bdp_probe', True),             # Enable bandwidth-delay probing
            ('grpc.http2.write_buffer_size', 16 * 1024 * 1024),  # 16MB write buffer (was 64KB!)

            # === 🚀 FLOW CONTROL OPTIMIZATIONS ===
            ('grpc.http2.hpack_table_size.decoder', 65536), # 64KB HPACK decoder table
            ('grpc.http2.hpack_table_size.encoder', 65536), # 64KB HPACK encoder table
            ('grpc.http2.max_concurrent_streams', 1000), # Support 1000 concurrent streams

            # === 🚀 MEMORY AND PERFORMANCE ===
            ('grpc.max_metadata_size', 16384),          # 16KB metadata limit
            ('grpc.use_local_subchannel_pool', 1),      # Use local subchannel pool
        ]
        
        logger.debug(f"[StreamingManager] 🚀 Enhanced gRPC configuration:")
        logger.debug(f"[StreamingManager] 📦 Max message size: {max_message_size / (1024*1024):.0f}MB")
        logger.debug(f"[StreamingManager] 📥 Max receive size: {max_receive_size / (1024*1024):.0f}MB")
        
        for peer_id, addr_info in neighbor_addresses.items():
            if peer_id == self.client_id:
                continue  # Skip self

            # Handle both TCP and UDS address formats
            if isinstance(addr_info, tuple):
                host, port = addr_info
                # Check if it's a UDS address (host starts with unix://)
                if str(host).startswith('unix://'):
                    address = host  # UDS: use directly
                else:
                    address = f"{host}:{port}"  # TCP: format as host:port
            elif isinstance(addr_info, str):
                address = addr_info  # Already formatted (could be UDS or TCP)
            else:
                logger.error(f"[StreamingManager] Unknown address format for peer {peer_id}: {addr_info}")
                continue

            try:
                # 🚀 Create enhanced gRPC channel - Support large message transmission
                # Works with both TCP (host:port) and UDS (unix:///path) addresses
                channel = grpc.insecure_channel(address, options=grpc_options)
                
                # Create stub
                stub = gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub(channel)
                
                # Create streaming channel wrapper
                stream_channel = StreamingChannel(peer_id, channel, stub, client_id=self.client_id, client_instance=self.client_instance)
                
                # Start streaming (auto fallback if failed)
                stream_channel.start_streaming()
                
                # Always save channel, can be used for traditional messages even if streaming fails
                self.channels[peer_id] = stream_channel
                self.server_stubs[address] = stub
                
                is_uds = address.startswith('unix://')
                mode_str = "UDS" if is_uds else "TCP"

                if stream_channel.is_active:
                    logger.debug(f"[StreamingManager] ✅ Enhanced streaming channel ({mode_str}): Client {self.client_id} -> Peer {peer_id} ({address})")
                    logger.debug(f"[StreamingManager] 🚀 Support for chunks up to {max_message_size / (1024*1024):.0f}MB")
                else:
                    logger.debug(f"[StreamingManager] 🔧 Traditional fallback channel ({mode_str}): Client {self.client_id} -> Peer {peer_id} ({address})")

            except Exception as e:
                logger.error(f"[StreamingManager] Failed to create enhanced channel to peer {peer_id} at {address}: {e}")
                
        logger.debug(f"[StreamingManager] Client {self.client_id}: Created {len(self.channels)} enhanced streaming channels")
        logger.debug(f"[StreamingManager] 🚀 STREAMING OPTIMIZATION: BitTorrent messages now support high-performance streaming:")
        logger.debug(f"[StreamingManager]   📦 CHUNK_PIECE: Large chunk data via dedicated upload streams") 
        logger.debug(f"[StreamingManager]   📤 CHUNK_REQUEST: Chunk requests via control streams")
        logger.debug(f"[StreamingManager]   🎯 CHUNK_HAVE: Have notifications via control streams") 
        logger.debug(f"[StreamingManager]   📋 CHUNK_BITFIELD: Bitfield updates via control streams")
        logger.debug(f"[StreamingManager]   ❌ CHUNK_CANCEL: Cancel requests via control streams")
        logger.debug(f"[StreamingManager]   🔄 Auto fallback to traditional messaging if streaming fails")
        
    def send_chunk_request(self, peer_id: int, round_num: int, source_client_id: int, 
                          chunk_id: int, importance_score: float = 0.0, rarity_score: int = 0) -> bool:
        """Send chunk request through streaming channel"""
        if peer_id not in self.channels:
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id} for chunk request")
            return False
            
        return self.channels[peer_id].send_chunk_request(
            round_num, source_client_id, chunk_id, importance_score, rarity_score)
            
    def send_bittorrent_message(self, peer_id: int, msg_type: str, **kwargs) -> bool:
        """🚀 Unified BitTorrent message sending interface - Support all message types"""
        if peer_id not in self.channels:
            logger.debug(f"[StreamingManager] 🔍 Attempting to send {msg_type.upper()} message to peer {peer_id}")
            logger.debug(f"[StreamingManager] 🔍 Client {self.client_id} has channels to peers: {list(self.channels.keys())}")
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id}")
            return False
        
        # Build different requests based on message type
        if msg_type == 'piece':
            return self.channels[peer_id].send_chunk_data(**kwargs)
        elif msg_type == 'request':
            return self.channels[peer_id].send_chunk_request(**kwargs)
        elif msg_type in ['bitfield', 'have', 'cancel', 'interested', 'unchoke', 'choke']:
            return self.channels[peer_id].send_control_message(msg_type, **kwargs)
        else:
            logger.error(f"[StreamingManager] Unknown BitTorrent message type: {msg_type}")
            return False
    
    def send_chunk_data(self, peer_id: int, round_num: int, source_client_id: int,
                       chunk_id: int, chunk_data: bytes, checksum: str, 
                       importance_score: float = 0.0, rarity_score: int = 0) -> bool:
        """Send chunk data through streaming channel"""
        if peer_id not in self.channels:
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id} for chunk data")
            return False
            
        success = self.channels[peer_id].send_chunk_data(
            round_num, source_client_id, chunk_id, chunk_data, checksum, importance_score, rarity_score)
            
        if success:
            self.total_chunks_sent += 1
            self.total_bytes_sent += len(chunk_data)
            
        return success
        
    def get_active_peers(self) -> List[int]:
        """Get list of all active peers"""
        return [peer_id for peer_id, channel in self.channels.items() if channel.is_active]
        
    def close_all_channels(self):
        """Close all streaming channels"""
        logger.debug(f"[StreamingManager] Client {self.client_id}: Closing all streaming channels")
        
        for peer_id, channel in self.channels.items():
            channel.close()
            
        self.channels.clear()
        self.server_stubs.clear()
        
        logger.debug(f"[StreamingManager] Client {self.client_id}: All streaming channels closed")
        
    def get_channel_stats(self):
        """Get statistics for all channels"""
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