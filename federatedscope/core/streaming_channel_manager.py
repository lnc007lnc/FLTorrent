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
import json

from federatedscope.core.proto import gRPC_comm_manager_pb2_grpc
from federatedscope.core.proto import gRPC_comm_manager_pb2

logger = logging.getLogger(__name__)


class StreamingChannel:
    """单个streaming通道封装"""
    
    def __init__(self, peer_id, channel, stub, stream_type='bidirectional', client_id=-1, client_instance=None):
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
        """内部方法：启动多流并发streaming - 🚀 多重优化设计"""
        # 🚀 多流并发优化：创建专用流管道
        
        # 🔧 CRITICAL FIX: 先设置is_active=True，再创建generators
        self.is_active = True
        logger.info(f"[StreamChannel] 🚀 Activating multi-pipeline streaming to peer {self.peer_id}")
        
        # 🚀 优化1：流管道复用 - 创建专用队列和缓冲区
        self.control_request_queue = queue.Queue(maxsize=500)  # 控制消息队列
        self.upload_request_queue = queue.PriorityQueue(maxsize=500)   # 🎯 上传优先队列 (importance+rare排序)
        self.download_request_queue = queue.PriorityQueue(maxsize=500) # 🎯 下载优先队列 (importance+rare排序)
        
        # 🚀 优化2：性能监控和统计
        self.control_msg_count = 0
        self.upload_chunk_count = 0
        self.download_request_count = 0
        self.last_activity_time = time.time()
        
        # 🚀 优化3：智能流分配
        # 1. 控制流：专门处理轻量级控制消息 (HAVE, BITFIELD, INTERESTED等)
        self.control_stream = self.stub.streamChunks(self._enhanced_control_generator())
        logger.info(f"[StreamChannel] ✅ Control pipeline established for peer {self.peer_id}")
        
        # 2. 上传流：专门传输大块chunk数据 (在专用线程中管理)
        # 3. 下载流：批量处理chunk请求 (在专用线程中管理)
        
        # 🚀 优化4：专用线程池，提高并发性能
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
        
        # 启动优化的流管道处理器
        self.control_receiver_thread.start()
        self.download_sender_thread.start()
        self.upload_sender_thread.start()
        
        logger.info(f"[StreamChannel] 🚀 Multi-pipeline streaming ACTIVE for peer {self.peer_id}")
        logger.info(f"[StreamChannel] 📊 Performance monitoring enabled - Control/Upload/Download pipelines ready")
            
    def _enhanced_control_generator(self):
        """🚀 优化1：增强的控制消息流生成器 - 专门处理轻量级控制消息"""
        logger.info(f"[StreamChannel] 🎛️ Control pipeline generator started for peer {self.peer_id}")
        
        # 🚀 性能优化参数
        heartbeat_interval = 30.0  # 心跳间隔
        last_heartbeat = time.time()
        processed_count = 0
        
        while self.is_active:
            try:
                # 🚀 优化：使用专用控制队列，避免消息混乱
                try:
                    request = self.control_request_queue.get(timeout=0.1)  # 更快的响应时间
                except queue.Empty:
                    # 🚀 优化：发送心跳保持连接活跃
                    current_time = time.time()
                    if current_time - last_heartbeat > heartbeat_interval:
                        # 发送轻量级心跳消息维持连接
                        heartbeat_request = self._create_heartbeat_message()
                        if heartbeat_request:
                            yield heartbeat_request
                            last_heartbeat = current_time
                            logger.debug(f"[StreamChannel] 💓 Heartbeat sent to peer {self.peer_id}")
                    continue
                
                if request is None:  # Sentinel for shutdown
                    logger.info(f"[StreamChannel] 🎛️ Control pipeline shutdown for peer {self.peer_id}")
                    break
                
                # 🚀 优化：智能消息分流 - 只处理控制消息
                if self._is_control_message(request):
                    yield request
                    processed_count += 1
                    self.control_msg_count += 1
                    self.last_activity_time = time.time()
                    
                    # 🚀 性能监控
                    if processed_count % 50 == 0:
                        logger.debug(f"[StreamChannel] 🎛️ Control pipeline processed {processed_count} messages for peer {self.peer_id}")
                else:
                    # 非控制消息重新路由到正确的队列
                    self._route_message_to_correct_pipeline(request)
                    
                self.control_request_queue.task_done()
                
            except Exception as e:
                logger.error(f"[StreamChannel] 🎛️ Control generator error for peer {self.peer_id}: {e}")
                time.sleep(0.1)  # 避免错误循环
                
        logger.info(f"[StreamChannel] 🎛️ Control pipeline generator ended for peer {self.peer_id}, processed {processed_count} messages")
        
    def _is_control_message(self, request) -> bool:
        """判断是否为控制消息"""
        control_types = [
            gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
            gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL,
            # 添加更多控制消息类型
        ]
        return request.chunk_type in control_types
        
    def _create_heartbeat_message(self):
        """创建心跳消息维持连接"""
        try:
            return gRPC_comm_manager_pb2.ChunkStreamRequest(
                sender_id=self.client_id,
                receiver_id=self.peer_id,
                round_num=0,  # 心跳消息不需要具体轮次
                chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
                timestamp=int(time.time() * 1000)
            )
        except Exception:
            return None
            
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
        logger.info(f"[🚀 UploadGenerator] Client {self.client_id}: Upload stream generator started for peer {self.peer_id}")
        
        # 🔧 FIX: 添加等待机制参数
        empty_queue_wait = 0.01  # 10ms等待时间
        max_idle_time = 1.0     # 最大空闲时间1秒
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
    
    def _enhanced_upload_stream_processor(self):
        """🚀 优化2：增强的上传流处理器 - 地下管道模式与批量传输"""
        logger.info(f"[StreamChannel] 📤 Upload pipeline processor started for peer {self.peer_id}")
        
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
                        logger.info(f"[StreamChannel] 📤 Retrying upload pipeline in {retry_delay}s...")
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
        logger.info(f"[StreamChannel] 📤 Upload pipeline processor ended for peer {self.peer_id} (is_active={self.is_active})")
        
        # 🚀 最终性能报告
        if total_chunks_uploaded > 0:
            avg_upload_time = total_upload_time / max(total_chunks_uploaded, 1)
            logger.info(f"[StreamChannel] 📊 Upload pipeline final stats for peer {self.peer_id}:")
            logger.info(f"[StreamChannel] 📊   {total_chunks_uploaded} chunks uploaded in {total_upload_time:.3f}s")
            logger.info(f"[StreamChannel] 📊   Average: {avg_upload_time:.3f}s per chunk")
        
        logger.info(f"[StreamChannel] 📤 Upload pipeline processor ended for peer {self.peer_id}")
        
    def _enhanced_upload_generator(self):
        """🚀 优化2：增强的上传流生成器 - 永不停止的地下管道"""
        logger.debug(f"[StreamChannel] 📤 Upload pipeline generator started for peer {self.peer_id}")
        
        # 🚀 地下管道优化参数
        empty_queue_wait = 0.01        # 10ms等待时间
        heartbeat_interval = 60.0      # 60秒心跳间隔
        last_heartbeat = time.time()
        processed_chunks = 0
        consecutive_empty_checks = 0
        max_empty_checks = 100         # 最大空检查次数 (1秒)
        
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
                        current_time = time.time()
                        if current_time - last_heartbeat > heartbeat_interval:
                            heartbeat_msg = self._create_upload_heartbeat()
                            if heartbeat_msg:
                                yield heartbeat_msg
                                last_heartbeat = current_time
                                logger.debug(f"[StreamChannel] 📤💓 Upload heartbeat sent to peer {self.peer_id}")
                        consecutive_empty_checks = 0  # 重置计数器
                    continue
                
                if request is None:  # Sentinel for shutdown
                    logger.info(f"[StreamChannel] 📤 Upload generator shutdown for peer {self.peer_id}")
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
                
        logger.info(f"[StreamChannel] 📤 Upload generator ended for peer {self.peer_id}, processed {processed_chunks} chunks")
        
    def _create_upload_heartbeat(self):
        """创建上传心跳消息"""
        try:
            return gRPC_comm_manager_pb2.ChunkStreamRequest(
                sender_id=self.client_id,
                receiver_id=self.peer_id,
                round_num=0,
                chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,  # 轻量级心跳
                timestamp=int(time.time() * 1000),
                chunk_data=b'heartbeat'  # 最小化数据
            )
        except Exception:
            return None
                
    def _enhanced_control_response_handler(self):
        """🚀 优化1：增强的控制消息响应处理器 - 高性能响应处理"""
        logger.info(f"[StreamChannel] 🎛️ Control pipeline response handler started for peer {self.peer_id}")
        
        processed_responses = 0
        error_count = 0
        last_stats_report = time.time()
        
        try:
            for response in self.control_stream:
                if not self.is_active:
                    logger.info(f"[StreamChannel] 🎛️ Control response handler stopping for peer {self.peer_id}")
                    break
                
                # 🚀 优化：快速响应处理
                start_time = time.time()
                try:
                    self._handle_control_response(response)
                    processed_responses += 1
                    self.last_activity_time = time.time()
                    
                    # 🚀 性能监控
                    processing_time = time.time() - start_time
                    if processing_time > 0.1:  # 超过100ms的慢响应
                        logger.warning(f"[StreamChannel] 🐌 Slow control response processing: {processing_time:.3f}s for peer {self.peer_id}")
                    
                except Exception as e:
                    error_count += 1
                    logger.error(f"[StreamChannel] 🎛️ Control response processing error for peer {self.peer_id}: {e}")
                    if error_count > 10:  # 错误过多时断开连接
                        logger.error(f"[StreamChannel] 🎛️ Too many errors ({error_count}), stopping control pipeline for peer {self.peer_id}")
                        break
                
                # 🚀 定期性能报告
                current_time = time.time()
                if current_time - last_stats_report > 60.0:  # 每分钟报告一次
                    logger.info(f"[StreamChannel] 📊 Control pipeline stats for peer {self.peer_id}: {processed_responses} responses, {error_count} errors")
                    last_stats_report = current_time
                    
        except Exception as e:
            logger.error(f"[StreamChannel] 🎛️ Control response handler fatal error for peer {self.peer_id}: {e}")
        finally:
            logger.info(f"[StreamChannel] 🎛️ Control response handler ended for peer {self.peer_id}: {processed_responses} responses processed, {error_count} errors")
            
    def _enhanced_download_batch_processor(self):
        """🚀 优化3：增强的下载批处理器 - 智能批量处理chunk请求"""
        logger.info(f"[StreamChannel] 📥 Download pipeline processor started for peer {self.peer_id}")
        
        # 🚀 智能批处理参数
        batch_requests = []
        adaptive_batch_size = 5   # 自适应批处理大小
        max_batch_size = 20       # 最大批处理大小
        min_batch_timeout = 0.05  # 最小批处理超时 (50ms)
        max_batch_timeout = 0.2   # 最大批处理超时 (200ms)
        current_batch_timeout = min_batch_timeout
        
        # 🚀 性能监控
        processed_batches = 0
        total_chunks_downloaded = 0
        total_download_time = 0.0
        last_performance_check = time.time()
        
        while self.is_active:
            batch_start_time = time.time()
            
            try:
                # 🚀 优化：自适应批量收集
                end_time = time.time() + current_batch_timeout
                while time.time() < end_time and len(batch_requests) < adaptive_batch_size:
                    try:
                        priority_item = self.download_request_queue.get(timeout=0.01)
                        if priority_item is None:  # 兼容旧格式的关闭信号
                            logger.info(f"[StreamChannel] 📥 Download pipeline shutdown signal for peer {self.peer_id}")
                            break
                        # 🎯 从优先队列中解包：(priority, request)
                        priority, request = priority_item
                        if request is None:  # 新格式的关闭信号
                            logger.info(f"[StreamChannel] 📥 Download pipeline shutdown signal for peer {self.peer_id}")
                            break
                        batch_requests.append(request)
                    except queue.Empty:
                        break
                
                # 🚀 优化：智能批量发送
                if batch_requests:
                    batch_size = len(batch_requests)
                    logger.debug(f"[StreamChannel] 📥 Sending batch of {batch_size} download requests to peer {self.peer_id}")
                    
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
                    
                    # 🚀 性能优化：并行处理下载响应
                    download_start = time.time()
                    try:
                        download_stream = self.stub.downloadChunks(batch_request)
                        chunks_received = 0
                        
                        for chunk_response in download_stream:
                            self._handle_download_response(chunk_response)
                            chunks_received += 1
                            self.download_request_count += 1
                        
                        download_time = time.time() - download_start
                        total_download_time += download_time
                        total_chunks_downloaded += chunks_received
                        processed_batches += 1
                        
                        # 🚀 自适应优化：根据性能调整批处理参数
                        avg_time_per_chunk = download_time / max(chunks_received, 1)
                        if avg_time_per_chunk < 0.01:  # 很快，增加批大小
                            adaptive_batch_size = min(adaptive_batch_size + 2, max_batch_size)
                            current_batch_timeout = min(current_batch_timeout * 1.1, max_batch_timeout)
                        elif avg_time_per_chunk > 0.05:  # 较慢，减小批大小
                            adaptive_batch_size = max(adaptive_batch_size - 1, 3)
                            current_batch_timeout = max(current_batch_timeout * 0.9, min_batch_timeout)
                        
                        logger.debug(f"[StreamChannel] 📥 Batch completed: {chunks_received} chunks in {download_time:.3f}s, adaptive_size={adaptive_batch_size}")
                        
                    except Exception as e:
                        logger.error(f"[StreamChannel] 📥 Download stream error for peer {self.peer_id}: {e}")
                        # 错误时重置批处理参数
                        adaptive_batch_size = 5
                        current_batch_timeout = min_batch_timeout
                    
                    batch_requests.clear()
                    
                # 🚀 定期性能报告和优化
                current_time = time.time()
                if current_time - last_performance_check > 30.0:  # 每30秒检查一次
                    if processed_batches > 0:
                        avg_batch_time = total_download_time / processed_batches
                        avg_chunks_per_batch = total_chunks_downloaded / processed_batches
                        logger.info(f"[StreamChannel] 📊 Download pipeline performance for peer {self.peer_id}:")
                        logger.info(f"[StreamChannel] 📊   {processed_batches} batches, {total_chunks_downloaded} chunks")
                        logger.info(f"[StreamChannel] 📊   Avg: {avg_chunks_per_batch:.1f} chunks/batch, {avg_batch_time:.3f}s/batch")
                        logger.info(f"[StreamChannel] 📊   Current adaptive_size: {adaptive_batch_size}, timeout: {current_batch_timeout:.3f}s")
                    last_performance_check = current_time
                    
            except Exception as e:
                logger.error(f"[StreamChannel] 📥 Download batch processor error for peer {self.peer_id}: {e}")
                batch_requests.clear()
                time.sleep(0.1)  # 避免错误循环
            
        logger.info(f"[StreamChannel] 📥 Download pipeline processor ended for peer {self.peer_id}")
        logger.info(f"[StreamChannel] 📊 Final stats: {processed_batches} batches, {total_chunks_downloaded} total chunks downloaded")
            
    def _handle_control_response(self, response):
        """🔧 修复：处理控制消息响应 - 支持所有响应类型"""
        logger.info(f"[StreamChannel] Received control response from peer {self.peer_id}: type={response.response_type}")
        
        if not self.client_instance:
            logger.warning(f"[StreamChannel] No client instance for peer {self.peer_id}, cannot process control response")
            return
            
        try:
            from federatedscope.core.message import Message
            
            # 🔧 修复：正确处理所有ChunkResponseType枚举值
            if response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_ACK:
                # ACK响应：确认收到控制消息
                logger.debug(f"[StreamChannel] Received ACK from peer {self.peer_id} for chunk {response.chunk_id}")
                # ACK消息通常不需要特殊处理，只记录日志
                return
                
            elif response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_NACK:
                # NACK响应：拒绝/错误响应
                logger.warning(f"[StreamChannel] Received NACK from peer {self.peer_id} for chunk {response.chunk_id}")
                # TODO: 可以在这里实现重传逻辑
                return
                
            elif response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_HAVE_RESP:
                # HAVE响应：peer通知拥有某个chunk
                content = {
                    'round_num': response.round_num,
                    'source_client_id': getattr(response, 'source_client_id', response.sender_id),
                    'chunk_id': response.chunk_id
                }
                msg_type = 'have'
                
            elif response.response_type == gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_INTERESTED:
                # INTERESTED响应：peer表示感兴趣
                content = {
                    'round_num': response.round_num,
                    'peer_id': response.sender_id
                }
                msg_type = 'interested'
                
            else:
                # 🔧 使用数值进行兼容性检查
                response_type_value = int(response.response_type)
                logger.warning(f"[StreamChannel] Unknown control response type: {response_type_value} from peer {self.peer_id}")
                
                # 🔧 尝试推断响应类型
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
                
            # 创建Message对象并调用callback
            if 'msg_type' in locals() and 'content' in locals():
                message = Message(
                    msg_type=msg_type,
                    sender=self.peer_id,
                    receiver=[self.client_id],
                    content=content
                )
                
                # 🔧 调用正确的callback函数
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
        """🔧 优化：处理chunk下载响应 - 去重统计，防重复处理"""        
        if not self.client_instance:
            logger.warning(f"[StreamChannel] No client instance for peer {self.peer_id}, cannot process chunk download")
            return
            
        try:
            # 验证响应数据
            if not response.response_data:
                logger.error(f"[StreamChannel] 🚫 Empty chunk data from peer {self.peer_id}, chunk_id={response.chunk_id}")
                return
            
            # 🔧 统一统计管理 (只统计一次)
            chunk_size = len(response.response_data)
            self.chunks_received += 1
            self.bytes_received += chunk_size
            
            # 🔧 更新去重状态
            source_client_id = getattr(response, 'source_client_id', response.sender_id)
            self.mark_chunk_completed(response.round_num, source_client_id, response.chunk_id)
            
            # 创建ChunkData对象
            from federatedscope.core.message import Message, ChunkData
            import hashlib
                
            # 计算校验和
            checksum = hashlib.sha256(response.response_data).hexdigest()
            
            # 创建ChunkData对象包装原始数据
            chunk_data = ChunkData(response.response_data, checksum)
            
            # 构造消息内容 (与传统BitTorrent PIECE消息格式兼容)
            content = {
                'round_num': response.round_num,
                'source_client_id': source_client_id,  # 使用正确的字段
                'chunk_id': response.chunk_id,
                'data': chunk_data,
                'checksum': checksum
            }
            
            # 创建Message对象
            message = Message(
                msg_type='piece',
                sender=self.peer_id,
                receiver=[self.client_id],
                content=content
            )
            
            # 调用callback函数处理chunk piece
            logger.debug(f"[StreamChannel] Calling callback_funcs_for_piece for peer {self.peer_id}, chunk {response.sender_id}:{response.chunk_id}")
            self.client_instance.callback_funcs_for_piece(message)
            
        except Exception as e:
            logger.error(f"[StreamChannel] Error handling chunk download from peer {self.peer_id}: {e}")
        
    def _handle_chunk_response(self, response):
        """兼容性方法：处理收到的chunk响应"""
        self.chunks_received += 1
        self.bytes_received += len(response.response_data) if response.response_data else 0
        logger.info(f"[StreamChannel] Received chunk response from peer {self.peer_id}")
        
    def _calculate_request_priority(self, importance_score: float, rarity_score: int) -> tuple:
        """🎯 计算请求优先级：importance优先，相近importance时按rare排序"""
        import random
        importance_jitter = random.uniform(-0.01, 0.01)
        rarity_jitter = random.uniform(-0.1, 0.1)
        # 主优先级：importance (取负值因为PriorityQueue是最小堆，我们要高importance优先)
        primary_priority = -importance_score + importance_jitter * rarity_score
        # primary_priority = -random.random()
        # 次优先级：rarity (数值越小越稀有，优先级越高)
        secondary_priority = rarity_score + rarity_jitter
        
        # 第三优先级：时间戳 (确保同优先级下的FIFO)
        tertiary_priority = random.random()
        
        return (primary_priority, secondary_priority, tertiary_priority)
        
    def send_chunk_request(self, round_num: int, source_client_id: int, chunk_id: int, 
                          importance_score: float = 0.0, rarity_score: int = 0):
        """🔧 增强去重：发送chunk请求 - 严格防止重复请求"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
        
        # 🔧 关键去重机制：检查是否已经请求过这个chunk
        chunk_key = (round_num, source_client_id, chunk_id)
        
        # 初始化去重集合
        if not hasattr(self, 'requested_chunks'):
            self.requested_chunks = set()
        if not hasattr(self, 'completed_chunks'):
            self.completed_chunks = set()
        
        # 🔧 严格去重检查
        if chunk_key in self.requested_chunks:
            logger.debug(f"[StreamChannel] 🚫 DUPLICATE REQUEST blocked: chunk {source_client_id}:{chunk_id} already requested from peer {self.peer_id}")
            return False
        
        if chunk_key in self.completed_chunks:
            logger.debug(f"[StreamChannel] 🚫 REDUNDANT REQUEST blocked: chunk {source_client_id}:{chunk_id} already completed from peer {self.peer_id}")
            return False
        
        # 🔧 队列大小限制 (避免内存泄漏)
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
            # 🎯 计算优先级：CHUNK_REQUEST按importance+rare排序
            priority = self._calculate_request_priority(importance_score, rarity_score)
            
            # 🚀 严格的请求队列管理 - 使用优先级队列
            self.download_request_queue.put((priority, request), timeout=0.5)
            
            # 🔧 记录请求，防止重复
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
        """🔧 标记chunk已完成，清理去重状态"""
        chunk_key = (round_num, source_client_id, chunk_id)
        
        if hasattr(self, 'requested_chunks'):
            self.requested_chunks.discard(chunk_key)  # 从请求集合中移除
        if hasattr(self, 'completed_chunks'):
            self.completed_chunks.add(chunk_key)      # 加入完成集合
            
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
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Queueing priority chunk data {source_client_id}:{chunk_id} (importance:{importance_score:.3f}, rarity:{rarity_score}) to peer {self.peer_id}")
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Chunk data size: {len(chunk_data)}, queue size before: {self.upload_request_queue.qsize()}")
            
            self.upload_request_queue.put((priority, request), timeout=1.0)
            self.chunks_sent += 1
            self.bytes_sent += len(chunk_data)
            
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Successfully queued chunk data {source_client_id}:{chunk_id} to peer {self.peer_id}")
            logger.info(f"[🚀 StreamChannel] Client {self.client_id}: Queue size after: {self.upload_request_queue.qsize()}")
            return True
        except queue.Full:
            logger.error(f"[StreamChannel] Upload queue full for peer {self.peer_id}")
            return False
    
    def send_control_message(self, msg_type: str, round_num: int, **kwargs) -> bool:
        """🚀 优化3：发送BitTorrent控制消息 - 使用专用控制流队列"""
        if not self.is_active:
            logger.warning(f"[StreamChannel] Cannot send to inactive channel for peer {self.peer_id}")
            return False
        
        # 🚀 智能消息类型映射
        chunk_type_map = {
            'bitfield': gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD,
            'have': gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            'cancel': gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL,
            'request': gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
            'piece': gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE,
            # BitTorrent控制消息扩展
            'interested': gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            'unchoke': gRPC_comm_manager_pb2.ChunkType.CHUNK_HAVE,
            'choke': gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL
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
            # interested消息：表示对peer感兴趣
            request.source_client_id = self.client_id
            request.chunk_id = 0
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending INTERESTED to peer {self.peer_id}")
            
        elif msg_type == 'unchoke':
            # unchoke消息：允许peer发送请求
            request.source_client_id = self.client_id
            request.chunk_id = 0
            logger.debug(f"[StreamChannel] 🎛️ Client {self.client_id}: Sending UNCHOKE to peer {self.peer_id}")
            
        elif msg_type == 'choke':
            # choke消息：禁止peer发送请求
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
        """关闭streaming通道"""
        if not self.is_active:
            return
            
        self.is_active = False
        
        # 发送关闭信号到各个队列
        try:
            self.request_queue.put(None)
            # 🎯 优先队列需要特殊处理关闭信号：使用最高优先级
            shutdown_priority = (-999999, 0, 0)  # 最高优先级的关闭信号
            self.upload_request_queue.put((shutdown_priority, None))
            self.download_request_queue.put((shutdown_priority, None))
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
    
    def __init__(self, client_id, client_instance=None):
        self.client_id = client_id
        self.client_instance = client_instance
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
        🚀 在拓扑构建时为所有邻居创建专用streaming通道 - 修复消息大小限制
        Args:
            neighbor_addresses: {peer_id: (host, port)}
        """
        logger.info(f"[StreamingManager] Client {self.client_id}: Creating enhanced streaming channels for topology")
        logger.info(f"[StreamingManager] Neighbor addresses: {neighbor_addresses}")
        
        # 🚀 修复: gRPC通道配置 - 设置大消息窗口
        max_message_size = 512 * 1024 * 1024  # 512MB消息限制
        max_receive_size = 512 * 1024 * 1024  # 512MB接收限制
        
        grpc_options = [
            ('grpc.max_send_message_length', max_message_size),
            ('grpc.max_receive_message_length', max_receive_size),
            ('grpc.keepalive_time_ms', 30000),          # 30秒keepalive
            ('grpc.keepalive_timeout_ms', 10000),       # 10秒keepalive超时
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10000),
            ('grpc.http2.min_ping_interval_without_data_ms', 300000),
            ('grpc.so_reuseaddr', 1),
        ]
        
        logger.info(f"[StreamingManager] 🚀 Enhanced gRPC configuration:")
        logger.info(f"[StreamingManager] 📦 Max message size: {max_message_size / (1024*1024):.0f}MB")
        logger.info(f"[StreamingManager] 📥 Max receive size: {max_receive_size / (1024*1024):.0f}MB")
        
        for peer_id, (host, port) in neighbor_addresses.items():
            if peer_id == self.client_id:
                continue  # 跳过自己
                
            try:
                # 🚀 创建增强的gRPC通道 - 支持大消息传输
                address = f"{host}:{port}"
                channel = grpc.insecure_channel(address, options=grpc_options)
                
                # 创建stub
                stub = gRPC_comm_manager_pb2_grpc.gRPCComServeFuncStub(channel)
                
                # 创建streaming通道封装
                stream_channel = StreamingChannel(peer_id, channel, stub, client_id=self.client_id, client_instance=self.client_instance)
                
                # 启动streaming (如果失败会自动回退)
                stream_channel.start_streaming()
                
                # 总是保存通道，即使streaming失败也可以用于传统消息
                self.channels[peer_id] = stream_channel
                self.server_stubs[address] = stub
                
                if stream_channel.is_active:
                    logger.info(f"[StreamingManager] ✅ Enhanced streaming channel: Client {self.client_id} -> Peer {peer_id} ({address})")
                    logger.info(f"[StreamingManager] 🚀 Support for chunks up to {max_message_size / (1024*1024):.0f}MB")
                else:
                    logger.info(f"[StreamingManager] 🔧 Traditional fallback channel: Client {self.client_id} -> Peer {peer_id} ({address})")
                
            except Exception as e:
                logger.error(f"[StreamingManager] Failed to create enhanced channel to peer {peer_id} at {host}:{port}: {e}")
                
        logger.info(f"[StreamingManager] Client {self.client_id}: Created {len(self.channels)} enhanced streaming channels")
        logger.info(f"[StreamingManager] 🚀 STREAMING OPTIMIZATION: BitTorrent messages now support high-performance streaming:")
        logger.info(f"[StreamingManager]   📦 CHUNK_PIECE: Large chunk data via dedicated upload streams") 
        logger.info(f"[StreamingManager]   📤 CHUNK_REQUEST: Chunk requests via control streams")
        logger.info(f"[StreamingManager]   🎯 CHUNK_HAVE: Have notifications via control streams") 
        logger.info(f"[StreamingManager]   📋 CHUNK_BITFIELD: Bitfield updates via control streams")
        logger.info(f"[StreamingManager]   ❌ CHUNK_CANCEL: Cancel requests via control streams")
        logger.info(f"[StreamingManager]   🔄 Auto fallback to traditional messaging if streaming fails")
        
    def send_chunk_request(self, peer_id: int, round_num: int, source_client_id: int, 
                          chunk_id: int, importance_score: float = 0.0, rarity_score: int = 0) -> bool:
        """通过streaming通道发送chunk请求"""
        if peer_id not in self.channels:
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id} for chunk request")
            return False
            
        return self.channels[peer_id].send_chunk_request(
            round_num, source_client_id, chunk_id, importance_score, rarity_score)
            
    def send_bittorrent_message(self, peer_id: int, msg_type: str, **kwargs) -> bool:
        """🚀 统一的BitTorrent消息发送接口 - 支持所有消息类型"""
        if peer_id not in self.channels:
            logger.info(f"[StreamingManager] 🔍 Attempting to send {msg_type.upper()} message to peer {peer_id}")
            logger.info(f"[StreamingManager] 🔍 Client {self.client_id} has channels to peers: {list(self.channels.keys())}")
            logger.warning(f"[StreamingManager] No streaming channel to peer {peer_id}")
            return False
        
        # 根据消息类型构建不同的请求
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
        """通过streaming通道发送chunk数据"""
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