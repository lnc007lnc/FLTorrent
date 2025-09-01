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
logger.setLevel(logging.INFO)

class gRPCComServeFunc(gRPC_comm_manager_pb2_grpc.gRPCComServeFuncServicer):
    def __init__(self, chunk_manager=None):
        self.msg_queue = deque()
        # 🚀 Streaming queues for chunk transfer
        self.chunk_stream_queues = {}  # {client_id: deque()}
        # 🚀 CRITICAL FIX: Add chunk_manager reference for data access (will be set by client)
        self.chunk_manager = chunk_manager
        
        # 🔧 FIX: Singleton thread management - standby mode normally, start when needed
        self._background_processor = None
        self._processor_lock = threading.Lock()
        self._active_uploads = 0  # active upload connection counter
    
    def _ensure_background_processor(self):
        """🔧 Lazy loading: only start background processing thread when needed"""
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
        """🔧 Shared background processing thread - standby mode, processes chunk data for all clients"""
        logger.info("[🎯 gRPCServer] 🚇 Background processor thread started, entering standby mode")
        
        while True:
            try:
                # Standby mode: sleep when no active connections
                if self._active_uploads == 0:
                    time.sleep(0.1)  # 100ms standby check interval
                    continue
                
                # Processing mode: Process message queue when there are active connections
                # Actual chunk processing logic can be added here
                time.sleep(0.01)  # 10ms processing interval
                
            except Exception as e:
                logger.error(f"[🎯 gRPCServer] 🚇 Background processor error: {e}")
                time.sleep(1.0)  # Error recovery interval
        
    def sendMessage(self, request, context):
        self.msg_queue.append(request)
        return gRPC_comm_manager_pb2.MessageResponse(msg='ACK')
        
    def streamChunks(self, request_iterator, context):
        """🚀 Bidirectional streaming RPC for chunk control messages"""
        logger.debug("[gRPCServer] streamChunks method called")
        logger.debug(f"[🔍 gRPCServer] streamChunks called from peer: {context.peer()}")
        
        try:
            for request in request_iterator:
                logger.debug(f"[🔍 gRPCServer] Received request: sender_id={request.sender_id}, receiver_id={request.receiver_id}, chunk_type={request.chunk_type}, from peer: {context.peer()}")
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
                        logger.debug(f"[🎯 gRPCServer] Routing REQUEST from {request.sender_id} to {request.receiver_id}")
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
        """🚀 Optimization 2: Enhanced underground pipeline mode - Never-stopping high-performance chunk upload processing"""
        logger.debug("[🎯 gRPCServer] 📤 Enhanced upload pipeline started - UNDERGROUND MODE")
        
        # 🚀 Underground pipeline performance parameters
        successful_chunks = 0
        failed_chunks = 0
        error_messages = []
        processing_start_time = time.time()
        client_id = 0  # Will be obtained from the first request
        
        # 🚀 Performance monitoring
        chunk_sizes = []
        processing_times = []
        last_performance_report = time.time()
        
        def enhanced_underground_processor():
            """🚀 Enhanced underground pipeline processor - High performance, never-stopping, intelligent error handling"""
            nonlocal successful_chunks, failed_chunks, error_messages, chunk_sizes, processing_times, client_id, last_performance_report
            
            logger.debug("[🎯 gRPCServer] 🚇 Enhanced underground pipeline started - PERFORMANCE MODE")
            
            # 🚀 Underground pipeline batch processing optimization
            chunk_batch = []
            batch_size = 10  # Batch processing size
            batch_timeout = 0.1  # 100ms batch processing timeout
            last_batch_time = time.time()
            
            try:
                for request in request_iterator:
                    request_start_time = time.time()
                    
                    # 🚀 Get client_id (from first request)
                    if client_id == 0 and request.sender_id:
                        client_id = request.sender_id
                        logger.info(f"[🎯 gRPCServer] 📤 Client {client_id} connected to upload pipeline")
                    
                    # 🚀 Fast request classification and processing
                    if request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE:
                        chunk_size = len(request.chunk_data) if request.chunk_data else 0
                        logger.debug(f"[🎯 gRPCServer] 🚇 Processing CHUNK_PIECE - size={chunk_size}B, chunk={request.source_client_id}:{request.chunk_id}")
                        
                        try:
                            # 🚀 Batch conversion optimization - Collect to batch processing queue
                            chunk_batch.append(request)
                            
                            # 🚀 Intelligent batch processing trigger conditions
                            current_time = time.time()
                            should_process_batch = (
                                len(chunk_batch) >= batch_size or  # Batch size reached
                                (current_time - last_batch_time) > batch_timeout or  # Timeout
                                chunk_size > 1024 * 1024  # Large chunk processed immediately
                            )
                            
                            if should_process_batch:
                                # 🚀 Batch process chunks
                                batch_start_time = time.time()
                                for chunk_req in chunk_batch:
                                    converted_msg = self._convert_chunk_to_message(chunk_req)
                                    self.msg_queue.append(converted_msg)
                                    successful_chunks += 1
                                    
                                    # Performance statistics
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
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL,
                        # 🔧 Add new BitTorrent control message types
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_INTERESTED_REQ,
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_UNCHOKE_REQ,
                        gRPC_comm_manager_pb2.ChunkType.CHUNK_CHOKE_REQ
                    ]:
                        # 🚀 Control message fast processing channel
                        logger.debug(f"[🎯 gRPCServer] 🚇 Processing control message - type={request.chunk_type}")
                        converted_msg = self._convert_chunk_to_message(request)
                        self.msg_queue.append(converted_msg)
                        successful_chunks += 1
                        
                    else:
                        # 🚀 Heartbeat message processing
                        if request.chunk_data == b'heartbeat':
                            logger.debug(f"[🎯 gRPCServer] 🚇💓 Heartbeat received from client {request.sender_id}")
                        else:
                            failed_chunks += 1
                            error_msg = f"Unknown chunk type: {request.chunk_type}"
                            error_messages.append(error_msg)
                            logger.warning(f"[🎯 gRPCServer] 🚇 {error_msg}")
                    
                    # 🚀 Processing time monitoring
                    processing_time = time.time() - request_start_time
                    if processing_time > 0.05:  # Slow processing over 50ms
                        logger.debug(f"[🎯 gRPCServer] 🐌 Slow chunk processing: {processing_time:.3f}s")
                    
                    # 🚀 Regular performance reporting
                    current_time = time.time()
                    if current_time - last_performance_report > 120.0:  # Report every 2 minutes
                        if chunk_sizes:
                            avg_chunk_size = sum(chunk_sizes) / len(chunk_sizes)
                            avg_processing_time = sum(processing_times) / len(processing_times)
                            logger.info(f"[🎯 gRPCServer] 📊 Underground pipeline performance:")
                            logger.info(f"[🎯 gRPCServer] 📊   {successful_chunks} chunks processed, {failed_chunks} failed")
                            logger.debug(f"[🎯 gRPCServer] 📊   Avg chunk size: {avg_chunk_size:.0f}B, processing time: {avg_processing_time:.3f}s")
                        last_performance_report = current_time
                
                # 🚀 Process remaining batch data
                if chunk_batch:
                    logger.debug(f"[🎯 gRPCServer] 🚇 Processing final batch: {len(chunk_batch)} chunks")
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
                # 🚀 Final performance statistics
                total_time = time.time() - processing_start_time
                logger.debug(f"[🎯 gRPCServer] 🚇 Underground pipeline finished:")
                logger.debug(f"[🎯 gRPCServer] 📊   Success: {successful_chunks}, Failed: {failed_chunks}")
                logger.debug(f"[🎯 gRPCServer] 📊   Total time: {total_time:.3f}s")
                if successful_chunks > 0:
                    logger.debug(f"[🎯 gRPCServer] 📊   Throughput: {successful_chunks/total_time:.1f} chunks/sec")
        
        # 🔧 FIX: Use shared background thread, avoid creating new thread for each request
        with self._processor_lock:
            self._active_uploads += 1
        
        # 🔧 Ensure background processing thread exists (lazy loading)
        self._ensure_background_processor()
        
        # 🚀 Process directly in current thread, or pass data to background thread
        logger.debug(f"[🎯 gRPCServer] Processing upload request for client {client_id}")
        enhanced_underground_processor()
        
        # 🔧 Decrease active connection count after processing
        with self._processor_lock:
            self._active_uploads = max(0, self._active_uploads - 1)
        
        # 🚀 Return processing result
        logger.debug("[🎯 gRPCServer] Upload processing completed")
        return gRPC_comm_manager_pb2.ChunkBatchResponse(
            client_id=client_id,
            successful_chunks=successful_chunks,
            failed_chunks=failed_chunks,
            error_messages=error_messages
        )
        
    def downloadChunks(self, request, context):
        """🚀 Server streaming RPC for batch chunk download"""
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
                
                # 🚀 Enhanced error handling: Check chunk_manager and get chunk data
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
                
                # 🚀 Unified error response handling
                if error_message:
                    logger.warning(f"[gRPCServer] 📤 Sending NACK for chunk {chunk_req.source_client_id}:{chunk_req.chunk_id}: {error_message} from {request.sender_id}")
                    yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                        sender_id=request.client_id,
                        receiver_id=request.sender_id,
                        success=False,
                        response_type=gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_NACK,
                        round_num=request.round_num,
                        chunk_id=chunk_req.chunk_id,
                        error_message=error_message
                    )
                    continue
                
                # 🚀 Success response: Return actual chunk data
                logger.debug(f"[gRPCServer] 📤 Sending chunk data for {chunk_req.source_client_id}:{chunk_req.chunk_id} to client {request.client_id}")
                
                # chunk_data is already bytes from optimized cache/write_queue, no need to re-serialize
                if isinstance(chunk_data, bytes):
                    serialized_data = chunk_data  # Use bytes directly
                else:
                    serialized_data = pickle.dumps(chunk_data)  # Fallback for legacy data
                checksum = hashlib.sha256(serialized_data).hexdigest()
                data_size = len(serialized_data)
                
                logger.debug(f"[gRPCServer] 📤 Chunk data prepared: size={data_size}B, checksum={checksum[:8]}...")
                
                yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                    sender_id=request.client_id,
                    receiver_id=request.sender_id,
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
            # 🚀 CRITICAL FIX: Correctly create piece message with all necessary chunk data
            from federatedscope.core.message import ChunkData
            
            
            # Create ChunkData wrapper
            chunk_wrapper = ChunkData(chunk_request.chunk_data, chunk_request.checksum)
            
            # Create complete protobuf message with all piece data
            message_request = gRPC_comm_manager_pb2.MessageRequest()
            
            # Set message content - simulate traditional piece message structure
            # Note: We need to manually construct message content, like traditional messages in _send_piece
            from federatedscope.core.message import Message
            
            # Create traditional format Message object
            traditional_msg = Message(
                msg_type='piece',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'source_client_id': chunk_request.source_client_id,
                    'chunk_id': chunk_request.chunk_id,
                    'data': chunk_wrapper,  # ChunkData object containing raw bytes
                    'checksum': chunk_request.checksum
                }
            )
            
            logger.debug(f"[🔧 gRPCServer] Created traditional message: type={traditional_msg.msg_type}, sender={traditional_msg.sender}")
            logger.debug(f"[🔧 gRPCServer] Message content keys: {list(traditional_msg.content.keys())}")
            
            # Convert Message object to protobuf format
            message_request = traditional_msg.transform(to_list=True)
            
            logger.debug(f"[🔧 gRPCServer] Successfully converted CHUNK_PIECE to MessageRequest")
            return message_request
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST:
            # Process request message
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
            
            logger.debug(f"[🔧 gRPCServer] Converting CHUNK_HAVE to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_BITFIELD:
            # 🚀 Process bitfield message
            from federatedscope.core.message import Message
            
            # Decode bitfield data
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
            
            logger.debug(f"[🔧 gRPCServer] Converting CHUNK_BITFIELD to traditional message format, chunks: {len(bitfield_data)}")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_CANCEL:
            # 🚀 Process cancel message
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
            
            logger.debug(f"[🔧 gRPCServer] Converting CHUNK_CANCEL to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_INTERESTED_REQ:
            # 🔧 Process interested message
            from federatedscope.core.message import Message
            
            traditional_msg = Message(
                msg_type='interested',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'peer_id': chunk_request.sender_id
                }
            )
            
            logger.debug(f"[🔧 gRPCServer] Converting CHUNK_INTERESTED_REQ to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_UNCHOKE_REQ:
            # 🔧 Process unchoke message
            from federatedscope.core.message import Message
            
            traditional_msg = Message(
                msg_type='unchoke',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'peer_id': chunk_request.sender_id
                }
            )
            
            logger.debug(f"[🔧 gRPCServer] Converting CHUNK_UNCHOKE_REQ to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        elif chunk_request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_CHOKE_REQ:
            # 🔧 Process choke message
            from federatedscope.core.message import Message
            
            traditional_msg = Message(
                msg_type='choke',
                sender=chunk_request.sender_id,
                receiver=[chunk_request.receiver_id],
                state=chunk_request.round_num,
                content={
                    'round_num': chunk_request.round_num,
                    'peer_id': chunk_request.sender_id
                }
            )
            
            logger.debug(f"[🔧 gRPCServer] Converting CHUNK_CHOKE_REQ to traditional message format")
            return traditional_msg.transform(to_list=True)
            
        else:
            logger.warning(f"[🔧 gRPCServer] Unknown chunk_type: {chunk_request.chunk_type}")
            # Return empty MessageRequest as fallback
            return gRPC_comm_manager_pb2.MessageRequest()

    def receive(self):
        while len(self.msg_queue) == 0:
            continue
        msg = self.msg_queue.popleft()
        return msg
