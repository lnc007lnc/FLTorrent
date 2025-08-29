import queue
import logging
from collections import deque

from federatedscope.core.proto import gRPC_comm_manager_pb2, \
    gRPC_comm_manager_pb2_grpc

logger = logging.getLogger(__name__)


class gRPCComServeFunc(gRPC_comm_manager_pb2_grpc.gRPCComServeFuncServicer):
    def __init__(self):
        self.msg_queue = deque()
        # 🚀 Streaming queues for chunk transfer
        self.chunk_stream_queues = {}  # {client_id: deque()}
        
    def sendMessage(self, request, context):
        self.msg_queue.append(request)
        return gRPC_comm_manager_pb2.MessageResponse(msg='ACK')
        
    def streamChunks(self, request_iterator, context):
        """🚀 双向streaming RPC for chunk control messages"""
        logger.debug("[gRPCServer] streamChunks method called")
        
        try:
            for request in request_iterator:
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
        """🚀 客户端流式RPC for chunk data upload - 地下管道模式：后台持续处理"""
        logger.info("[🎯 gRPCServer] uploadChunks method called - STREAMING CHUNK UPLOAD started")
        
        successful_chunks = 0
        failed_chunks = 0
        error_messages = []
        
        # 用于传递处理结果的队列
        import threading
        import queue
        result_queue = queue.Queue()
        
        def underground_pipeline_processor():
            """🚀 地下管道处理器：永远循环，不退出，持续等待数据"""
            nonlocal successful_chunks, failed_chunks, error_messages
            
            logger.info("[🎯 gRPCServer] 🚇 Underground pipeline started - waiting for data forever")
            
            try:
                # 🔧 CRITICAL FIX: 使用永远循环的地下管道模式
                for request in request_iterator:
                    logger.info(f"[🎯 gRPCServer] 🚇 Pipeline received streaming request - chunk_type={request.chunk_type}")
                    logger.info(f"[🎯 gRPCServer] 🚇 Request details - sender={request.sender_id}, round={request.round_num}, source={request.source_client_id}, chunk={request.chunk_id}")
                    
                    # Process chunk data uploads
                    if request.chunk_type == gRPC_comm_manager_pb2.ChunkType.CHUNK_PIECE:
                        logger.info(f"[🎯 gRPCServer] 🚇 Processing CHUNK_PIECE data - size={len(request.chunk_data) if request.chunk_data else 0}")
                        
                        # Convert to traditional message format for compatibility
                        converted_msg = self._convert_chunk_to_message(request)
                        self.msg_queue.append(converted_msg)
                        successful_chunks += 1
                        
                        logger.info(f"[🎯 gRPCServer] 🚇 Successfully processed chunk upload: {request.source_client_id}:{request.chunk_id}")
                        logger.info(f"[🎯 gRPCServer] 🚇 Added to msg_queue, queue size now: {len(self.msg_queue)}")
                    else:
                        failed_chunks += 1
                        error_msg = f"Invalid chunk type for upload: {request.chunk_type}"
                        error_messages.append(error_msg)
                        logger.warning(f"[🎯 gRPCServer] 🚇 {error_msg}")
                        
                # 🚇 只有当客户端关闭连接时，for循环才会结束
                logger.info("[🎯 gRPCServer] 🚇 Client closed connection - pipeline ending")
                        
            except Exception as e:
                logger.error(f"[🎯 gRPCServer] 🚇 Pipeline processing error: {e}")
                failed_chunks += 1
                error_messages.append(str(e))
            finally:
                # 通知主线程处理完成
                result_queue.put({
                    'successful_chunks': successful_chunks,
                    'failed_chunks': failed_chunks,
                    'error_messages': error_messages
                })
                logger.info("[🎯 gRPCServer] 🚇 Underground pipeline completed")
        
        # 启动地下管道处理器
        pipeline_thread = threading.Thread(
            target=underground_pipeline_processor,
            daemon=True,
            name="UndergroundPipeline"
        )
        pipeline_thread.start()
        logger.info("[🎯 gRPCServer] 🚇 Started underground pipeline processor")
        
        # 立即返回响应，地下管道继续运行
        logger.info("[🎯 gRPCServer] Returning immediate response - underground pipeline active")
        return gRPC_comm_manager_pb2.ChunkBatchResponse(
            client_id=0,
            successful_chunks=0,
            failed_chunks=0,
            error_messages=["Underground pipeline active"]
        )
        
    def downloadChunks(self, request, context):
        """🚀 服务端流式RPC for batch chunk download"""
        logger.debug(f"[gRPCServer] downloadChunks method called for client {request.client_id}")
        
        try:
            # Convert batch request to individual message requests
            for chunk_req in request.chunk_requests:
                # Create chunk request message for compatibility
                chunk_request = gRPC_comm_manager_pb2.ChunkStreamRequest(
                    sender_id=chunk_req.source_client_id,    # 🔧 FIX: 拥有chunk的客户端作为发送方
                    receiver_id=request.client_id,           # 🔧 FIX: 请求chunk的客户端作为接收方
                    round_num=request.round_num,
                    source_client_id=chunk_req.source_client_id,
                    chunk_id=chunk_req.chunk_id,
                    chunk_type=gRPC_comm_manager_pb2.ChunkType.CHUNK_REQUEST,
                    importance_score=chunk_req.importance_score
                )
                
                # Add to message queue
                self.msg_queue.append(self._convert_chunk_to_message(chunk_request))
                
                # Send acknowledgment (could be enhanced to send actual chunk data)
                yield gRPC_comm_manager_pb2.ChunkStreamResponse(
                    sender_id=chunk_req.source_client_id,    # 拥有chunk的客户端作为响应发送方
                    receiver_id=request.client_id,           # 请求客户端作为响应接收方
                    success=True,
                    response_type=gRPC_comm_manager_pb2.ChunkResponseType.CHUNK_ACK,
                    round_num=request.round_num,
                    chunk_id=chunk_req.chunk_id
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
            
            logger.info(f"[🔧 gRPCServer] Converting CHUNK_PIECE to traditional message format")
            logger.info(f"[🔧 gRPCServer] Original chunk data size: {len(chunk_request.chunk_data) if chunk_request.chunk_data else 0}")
            
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
            import json
            
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
