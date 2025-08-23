"""
BitTorrent协议管理器
实现经典BitTorrent协议用于FederatedScope中的chunk交换
"""

import time
import hashlib
import random
import logging
from typing import Dict, Set, List, Tuple, Optional, Any

logger = logging.getLogger(__name__)


class BitTorrentManager:
    """管理BitTorrent协议的核心逻辑（包含关键Bug修复）"""
    
    def __init__(self, client_id: int, round_num: int, chunk_manager, comm_manager, neighbors: List[int]):
        self.client_id = client_id
        self.round_num = round_num  # 🔴 关键：当前轮次
        self.chunk_manager = chunk_manager
        self.comm_manager = comm_manager
        self.neighbors = neighbors  # 🔧 修复：直接传入邻居列表
        
        # BitTorrent状态
        self.peer_bitfields: Dict[int, Dict] = {}  # {peer_id: bitfield}
        self.interested_in: Set[int] = set()  # 感兴趣的peers
        self.interested_by: Set[int] = set()  # 对我感兴趣的peers
        self.choked_peers: Set[int] = set()  # 被choke的peers
        self.unchoked_peers: Set[int] = set()  # unchoked的peers（可以下载）
        
        # 性能管理
        self.download_rate: Dict[int, float] = {}  # {peer_id: bytes/sec}
        self.upload_rate: Dict[int, float] = {}  # {peer_id: bytes/sec}
        self.last_unchoke_time = 0
        self.optimistic_unchoke_peer = None
        
        # 🔧 修复：简化状态管理，避免复杂的锁机制
        # FederatedScope是单线程消息驱动，不需要锁
        
        # 🔧 Bug修复2: 防死锁机制
        self.ever_unchoked: Set[int] = set()  # 记录曾经unchoke过的peers
        self.last_activity: Dict[int, float] = {}  # {peer_id: timestamp} 最后活动时间
        self.stalled_threshold = 30.0  # 30秒无活动视为stalled
        
        # 🔧 Bug修复3: 消息重传机制
        self.pending_requests: Dict[Tuple, Tuple[int, float]] = {}  # {(source_id, chunk_id): (peer_id, timestamp)}
        self.request_timeout = 5.0  # 5秒请求超时
        self.max_retries = 3  # 最大重试次数
        self.retry_count: Dict[Tuple, int] = {}  # {(source_id, chunk_id): count}
        
        # 🆕 双池请求管理系统 - 解决优先级反转和重复选择问题
        self.MAX_ACTIVE_REQUESTS = 2  # 活跃请求池大小：实际发送的并发请求数量
        self.MAX_PENDING_QUEUE = 2    # 待发送队列池大小：预选择的chunk队列大小
        self.pending_queue: List[Tuple] = []  # 待发送队列：按重要性排序的chunk列表
        
        # 🔧 Bug修复4: 确保最小unchoke数量
        self.MIN_UNCHOKE_SLOTS = 1  # 至少保持1个unchoke，防止完全死锁
        self.MAX_UPLOAD_SLOTS = 4
        
        # 🔧 修复：不使用后台线程，通过消息回调检查超时
        self.last_timeout_check = time.time()
        
        # 统计信息
        self.total_downloaded = 0
        self.total_uploaded = 0
        self.chunks_per_client = 10  # 默认值，可配置
        
        # 🔧 CRITICAL FIX: Exchange state management
        self.is_stopped = False  # Stop flag for exchange termination
        
        logger.info(f"[BT] BitTorrentManager initialized for client {client_id}, round {round_num}")
        
    def start_exchange(self):
        """启动BitTorrent chunk交换流程（无需Tracker）"""
        logger.info(f"[BT] Client {self.client_id}: Starting BitTorrent exchange")
        logger.info(f"[BT] Client {self.client_id}: Neighbors: {self.neighbors}")
        
        # 1. 直接向所有拓扑邻居发送bitfield
        for neighbor_id in self.neighbors:
            logger.info(f"[BT] Client {self.client_id}: Sending bitfield to neighbor {neighbor_id}")
            self._send_bitfield(neighbor_id)
        
        # 2. 启动定期unchoke算法（每10秒）
        self._schedule_regular_unchoke()
        
        # 3. 启动optimistic unchoke（每30秒）
        self._schedule_optimistic_unchoke()
        
    def handle_bitfield(self, sender_id: int, bitfield_content: Dict):
        """处理接收到的bitfield消息（包含重要性分数）"""
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring bitfield from peer {sender_id} - exchange stopped")
            return
        
        # 🆕 处理新格式的bitfield（包含重要性分数）
        if isinstance(bitfield_content, dict) and 'bitfield' in bitfield_content:
            # 新格式：{round_num: x, bitfield: [{round, source, chunk, importance_score}, ...]}
            bitfield_list = bitfield_content.get('bitfield', [])
            
            # 转换为内部格式并存储重要性分数
            bitfield = {}
            if not hasattr(self, 'peer_importance_scores'):
                self.peer_importance_scores = {}  # {peer_id: {chunk_key: importance_score}}
            
            if sender_id not in self.peer_importance_scores:
                self.peer_importance_scores[sender_id] = {}
            
            for chunk_entry in bitfield_list:
                chunk_key = (chunk_entry['round'], chunk_entry['source'], chunk_entry['chunk'])
                bitfield[chunk_key] = True
                
                # 🆕 存储重要性分数
                importance_score = chunk_entry.get('importance_score', 0.0)
                self.peer_importance_scores[sender_id][chunk_key] = importance_score
                
                logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has chunk {chunk_key} with importance {importance_score:.4f}")
        else:
            # 兼容旧格式
            bitfield = bitfield_content
        
        self.peer_bitfields[sender_id] = bitfield
        logger.debug(f"[BT] Client {self.client_id}: Received bitfield from peer {sender_id} with {len(bitfield)} chunks")
        
        # 🔧 调试：输出详细的bitfield分析
        logger.debug(f"[BT] Client {self.client_id}: BitTorrent Manager received bitfield from peer {sender_id}:")
        if bitfield:
            for chunk_key, has_chunk in bitfield.items():
                round_num, source_id, chunk_id = chunk_key
                importance_score = self.peer_importance_scores.get(sender_id, {}).get(chunk_key, 0.0)
                logger.debug(f"[BT] Client {self.client_id}: - Round {round_num}, Source {source_id}, Chunk {chunk_id}: {has_chunk} (importance: {importance_score:.4f})")
        else:
            logger.warning(f"[BT] Client {self.client_id}: ⚠️ BitTorrent Manager got EMPTY bitfield from peer {sender_id}!")
        
        # 检查是否有我需要的chunks
        if self._has_interesting_chunks(sender_id):
            logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has interesting chunks, sending interested")
            self._send_interested(sender_id)
        else:
            logger.info(f"[BT] Client {self.client_id}: Peer {sender_id} has no interesting chunks")
            
    def handle_interested(self, sender_id: int):
        """处理interested消息"""
        self.interested_by.add(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} is interested")
        # 根据当前upload slots决定是否unchoke
        self._evaluate_unchoke(sender_id)
        
    def handle_request(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int):
        """处理chunk请求"""
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring request from peer {sender_id} - exchange stopped")
            return
        logger.debug(f"[BT-HANDLE] Client {self.client_id}: Handling request from {sender_id} for chunk {source_client_id}:{chunk_id}")
        
        # 🔴 验证轮次匹配
        if round_num != self.round_num:
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Round mismatch - Request round {round_num} vs BitTorrent round {self.round_num}")
            logger.warning(f"[BT-HANDLE] Client {self.client_id}: Skipping request due to round mismatch")
            return
            
        if sender_id not in self.choked_peers:
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is not choked, processing request")
            # 发送chunk数据
            # 🔴 添加round_num参数到get_chunk_data
            logger.debug(f"[BT-HANDLE] Client {self.client_id}: Querying chunk_data with params (round={round_num}, source_client={source_client_id}, chunk_id={chunk_id})")
            chunk_data = self.chunk_manager.get_chunk_data(round_num, source_client_id, chunk_id)
            if chunk_data is not None:
                # 发送chunk数据，即使是空的chunk也要发送
                chunk_size = len(chunk_data) if hasattr(chunk_data, '__len__') else 0
                if chunk_size > 0:
                    logger.debug(f"[BT-HANDLE] Client {self.client_id}: Found non-empty chunk data (size={chunk_size}), sending piece to {sender_id}")
                else:
                    logger.debug(f"[BT-HANDLE] Client {self.client_id}: Found empty chunk data (size={chunk_size}), sending empty piece to {sender_id}")
                
                self._send_piece(sender_id, round_num, source_client_id, chunk_id, chunk_data)
                logger.debug(f"[BT-HANDLE] Client {self.client_id}: Successfully sent chunk {source_client_id}:{chunk_id} to peer {sender_id}")
            else:
                logger.warning(f"[BT-HANDLE] Client {self.client_id}: Chunk {source_client_id}:{chunk_id} not found in database (round={round_num})")
                logger.warning(f"[BT-HANDLE] Client {self.client_id}: Database query returned: {chunk_data}")
        else:
            logger.info(f"[BT-HANDLE] Client {self.client_id}: Peer {sender_id} is choked, ignoring request")
            
    def handle_piece(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data: bytes, checksum: str):
        """
        处理接收到的chunk数据（包含完整性校验）
        🔴 关键修改：验证round_num匹配
        """
        # 🔧 CRITICAL FIX: Check if exchange is stopped
        if self.is_stopped:
            logger.debug(f"[BT] Client {self.client_id}: Ignoring piece from peer {sender_id} - exchange stopped")
            return
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Received piece from {sender_id} for chunk {source_client_id}:{chunk_id} (piece_round={round_num}, bt_round={self.round_num}, timestamp={time.time():.3f})")
        
        # 🔴 验证轮次是否匹配
        if round_num != self.round_num:
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Round mismatch - Piece round {round_num} vs BitTorrent round {self.round_num}")
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Rejecting piece due to round mismatch")
            return False
            
        # 🔧 修复：chunk_data现在是base64编码的字符串，需要解码后校验
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Received encoded chunk data, type={type(chunk_data)}, size={len(chunk_data)}")
        
        # 解码base64数据
        try:
            import base64
            import pickle
            decoded_data = base64.b64decode(chunk_data.encode('utf-8'))
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Decoded base64 data, size={len(decoded_data)}")
        except Exception as e:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Failed to decode base64 data: {e}")
            return False
        
        # 对解码后的序列化数据计算哈希
        calculated_checksum = hashlib.sha256(decoded_data).hexdigest()
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Checksum verification - calculated={calculated_checksum[:8]}..., received={checksum[:8]}..., size={len(decoded_data)}")
        
        if calculated_checksum != checksum:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Chunk integrity check failed for {source_client_id}:{chunk_id}")
            logger.error(f"[BT-PIECE] Client {self.client_id}: Expected={checksum}, Got={calculated_checksum}")
            # 重新请求这个chunk
            chunk_key = (round_num, source_client_id, chunk_id)
            self.retry_count[chunk_key] = self.retry_count.get(chunk_key, 0) + 1
            logger.warning(f"[BT-PIECE] Client {self.client_id}: Retry count for chunk {chunk_key}: {self.retry_count[chunk_key]}")
            return False
        
        # 🔧 反序列化得到原始chunk数据
        try:
            deserialized_data = pickle.loads(decoded_data)
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Successfully deserialized chunk data, type={type(deserialized_data)}")
        except Exception as e:
            logger.error(f"[BT-PIECE] Client {self.client_id}: Failed to deserialize chunk data: {e}")
            return False
        
        # 保存到本地数据库
        # 🔴 传递round_num到save方法，使用反序列化后的数据
        self.chunk_manager.save_remote_chunk(round_num, source_client_id, chunk_id, deserialized_data)
        
        # 清除pending请求
        chunk_key = (round_num, source_client_id, chunk_id)
        if chunk_key in self.pending_requests:
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Clearing pending request for chunk {chunk_key}")
            del self.pending_requests[chunk_key]
            logger.debug(f"[BT-PIECE] Client {self.client_id}: Active pool: {len(self.pending_requests)}/{self.MAX_ACTIVE_REQUESTS}, Queue: {len(self.pending_queue)}/{self.MAX_PENDING_QUEUE}")
            
            # 🆕 双池系统：从队列池转移请求到活跃池
            self._transfer_from_queue_to_active()
        else:
            logger.debug(f"[BT-PIECE] Client {self.client_id}: No pending request found for chunk {chunk_key}")
        
        # 向所有邻居发送have消息
        # 🔴 传递round_num信息
        logger.debug(f"[BT-PIECE] Client {self.client_id}: Broadcasting have message for chunk {source_client_id}:{chunk_id}")
        self._broadcast_have(round_num, source_client_id, chunk_id)
        
        # 更新下载速率和活动时间
        self._update_download_rate(sender_id, len(decoded_data))
        self.last_activity[sender_id] = time.time()
        self.total_downloaded += len(decoded_data)
        
        logger.debug(f"[BT] Client {self.client_id}: Received chunk {source_client_id}:{chunk_id} from peer {sender_id}")
        return True
        
    def handle_have(self, sender_id: int, round_num: int, source_client_id: int, chunk_id: int, importance_score: float = 0.0):
        """处理have消息（包含重要性分数）"""
        if round_num != self.round_num:
            return
            
        chunk_key = (round_num, source_client_id, chunk_id)
        if sender_id not in self.peer_bitfields:
            self.peer_bitfields[sender_id] = {}
        self.peer_bitfields[sender_id][chunk_key] = True
        
        # 🆕 存储重要性分数
        if not hasattr(self, 'peer_importance_scores'):
            self.peer_importance_scores = {}
        if sender_id not in self.peer_importance_scores:
            self.peer_importance_scores[sender_id] = {}
        
        self.peer_importance_scores[sender_id][chunk_key] = importance_score
        
        logger.debug(f"[BT] Client {self.client_id}: Peer {sender_id} has chunk {source_client_id}:{chunk_id} with importance {importance_score:.4f}")
        
    def handle_choke(self, sender_id: int):
        """处理choke消息"""
        self.choked_peers.add(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Choked by peer {sender_id}")
        
    def handle_unchoke(self, sender_id: int):
        """处理unchoke消息"""
        self.choked_peers.discard(sender_id)
        logger.debug(f"[BT] Client {self.client_id}: Unchoked by peer {sender_id}")
        
    def _importance_guided_selection(self) -> Optional[Tuple]:
        """重要性指导的chunk选择算法（importance-first + rarest-first混合策略）"""
        # 🆕 重要性分数差异阈值（当两个chunk重要性差异小于该值时认为相似）
        IMPORTANCE_SIMILARITY_THRESHOLD = 0.01  # 0.01 means chunks with importance difference < 1% are considered similar
        
        # 统计每个chunk的稀有度
        chunk_availability = {}
        for peer_id, bitfield in self.peer_bitfields.items():
            for chunk_key, has_chunk in bitfield.items():
                # 🔴 只考虑当前轮次的chunks
                if has_chunk and chunk_key[0] == self.round_num:
                    chunk_availability[chunk_key] = chunk_availability.get(chunk_key, 0) + 1
        
        # 选择可获得的chunks
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        
        # 🔧 排除已拥有和正在请求的chunks
        needed_chunks = []
        for chunk_key, availability_count in chunk_availability.items():
            if chunk_key not in my_bitfield and chunk_key not in self.pending_requests:
                # 🆕 获取chunk重要性分数
                importance_score = self._get_chunk_importance_score(chunk_key)
                needed_chunks.append({
                    'chunk_key': chunk_key,
                    'availability': availability_count,
                    'importance': importance_score
                })
        
        # 🔧 调试信息
        pending_chunks = [k for k, v in chunk_availability.items() if k in self.pending_requests]
        already_have = [k for k, v in chunk_availability.items() if k in my_bitfield]
        
        if not chunk_availability:
            if not hasattr(self, '_logged_no_chunks'):
                logger.info(f"[BT] Client {self.client_id}: No chunks available from peers. Peer count: {len(self.peer_bitfields)}")
                for peer_id, bitfield in self.peer_bitfields.items():
                    logger.info(f"[BT] Client {self.client_id}: Peer {peer_id} bitfield size: {len(bitfield)}")
                self._logged_no_chunks = True
        elif not needed_chunks:
            total_available = len(chunk_availability)
            pending_count = len(pending_chunks)
            have_count = len(already_have)
            logger.debug(f"[BT] Client {self.client_id}: No needed chunks - Total: {total_available}, Already have: {have_count}, Pending: {pending_count}")
            
            if not hasattr(self, '_logged_all_chunks'):
                logger.info(f"[BT] Client {self.client_id}: All chunks handled - My chunks: {len(my_bitfield)}, Pending requests: {len(self.pending_requests)}")
                self._logged_all_chunks = True
        
        if needed_chunks:
            # 🆕 重要性指导的选择策略
            logger.debug(f"[BT] Client {self.client_id}: Evaluating {len(needed_chunks)} candidate chunks for selection")
            
            # 1. 按重要性分数降序排序
            needed_chunks.sort(key=lambda x: x['importance'], reverse=True)
            
            if len(needed_chunks) == 1:
                selected = needed_chunks[0]
                logger.debug(f"[BT] Client {self.client_id}: Selected only candidate chunk {selected['chunk_key']} (importance: {selected['importance']:.4f}, rarity: {selected['availability']})")
                return selected['chunk_key']
            
            # 2. 找到重要性最高的chunk
            highest_importance = needed_chunks[0]['importance']
            
            # 3. 找到所有与最高重要性相近的chunks
            similar_importance_chunks = []
            for chunk in needed_chunks:
                importance_diff = abs(chunk['importance'] - highest_importance)
                if importance_diff <= IMPORTANCE_SIMILARITY_THRESHOLD:
                    similar_importance_chunks.append(chunk)
                else:
                    break  # 由于已经排序，后续chunks重要性更低
            
            logger.debug(f"[BT] Client {self.client_id}: Found {len(similar_importance_chunks)} chunks with similar high importance (threshold: {IMPORTANCE_SIMILARITY_THRESHOLD})")
            
            # 4. 在相似重要性的chunks中按稀有度选择
            if len(similar_importance_chunks) == 1:
                selected = similar_importance_chunks[0]
                logger.info(f"[BT] Client {self.client_id}: Selected chunk {selected['chunk_key']} by importance priority (importance: {selected['importance']:.4f}, rarity: {selected['availability']})")
                return selected['chunk_key']
            else:
                # 按稀有度排序（越少peer拥有越稀有）
                similar_importance_chunks.sort(key=lambda x: (x['availability'], random.random()))
                selected = similar_importance_chunks[0]
                logger.info(f"[BT] Client {self.client_id}: Selected chunk {selected['chunk_key']} by rarity among high-importance chunks (importance: {selected['importance']:.4f}, rarity: {selected['availability']})")
                return selected['chunk_key']
        
        return None
    
    def _transfer_from_queue_to_active(self):
        """从待发送队列转移请求到活跃池"""
        while (len(self.pending_requests) < self.MAX_ACTIVE_REQUESTS and 
               len(self.pending_queue) > 0):
            
            # 从队列头部取出chunk（已按重要性排序）
            chunk_key = self.pending_queue.pop(0)
            
            # 检查chunk是否仍然需要
            my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
            if chunk_key in my_bitfield or chunk_key in self.pending_requests:
                continue  # 跳过已拥有或正在请求的chunk
            
            # 找到拥有该chunk的peer
            peer_id = self._find_peer_with_chunk(chunk_key)
            if peer_id and peer_id not in self.choked_peers:
                round_num, source_id, chunk_id = chunk_key
                success = self._send_request(peer_id, source_id, chunk_id)
                if success:
                    logger.debug(f"[BT-POOL] Client {self.client_id}: Transferred chunk {chunk_key} from queue to active pool")
                    break  # 成功转移一个请求
                else:
                    logger.debug(f"[BT-POOL] Client {self.client_id}: Failed to transfer chunk {chunk_key} to active pool")
            else:
                logger.debug(f"[BT-POOL] Client {self.client_id}: No available peer for chunk {chunk_key}")
    
    def _fill_pending_queue(self):
        """填充待发送队列（只在队列为空时调用）"""
        if len(self.pending_queue) > 0:
            return  # 队列不为空，不需要填充
        
        logger.debug(f"[BT-POOL] Client {self.client_id}: Filling pending queue...")
        
        # 获取所有需要的chunks并按重要性排序
        needed_chunks = []
        
        # 统计每个chunk的稀有度
        chunk_availability = {}
        for peer_id, bitfield in self.peer_bitfields.items():
            for chunk_key, has_chunk in bitfield.items():
                if has_chunk and chunk_key[0] == self.round_num:
                    chunk_availability[chunk_key] = chunk_availability.get(chunk_key, 0) + 1
        
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        
        # 选择需要的chunks
        for chunk_key, availability_count in chunk_availability.items():
            if (chunk_key not in my_bitfield and 
                chunk_key not in self.pending_requests and
                chunk_key not in self.pending_queue):
                
                importance_score = self._get_chunk_importance_score(chunk_key)
                needed_chunks.append({
                    'chunk_key': chunk_key,
                    'availability': availability_count,
                    'importance': importance_score
                })
        
        if needed_chunks:
            # 按重要性排序，重要性高的在前
            needed_chunks.sort(key=lambda x: x['importance'], reverse=True)
            
            # 填充队列，最多填充到MAX_PENDING_QUEUE大小
            for i, chunk in enumerate(needed_chunks[:self.MAX_PENDING_QUEUE]):
                self.pending_queue.append(chunk['chunk_key'])
            
            logger.info(f"[BT-POOL] Client {self.client_id}: Filled pending queue with {len(self.pending_queue)} chunks (from {len(needed_chunks)} candidates)")
            
            # 输出前几个高重要性chunks的详细信息
            for i, chunk in enumerate(needed_chunks[:3]):
                logger.debug(f"[BT-POOL] Client {self.client_id}: Queue #{i+1}: {chunk['chunk_key']} (importance: {chunk['importance']:.4f}, rarity: {chunk['availability']})")
        else:
            logger.debug(f"[BT-POOL] Client {self.client_id}: No chunks available to fill queue")
    
    def _get_chunk_importance_score(self, chunk_key: Tuple[int, int, int]) -> float:
        """获取chunk的重要性分数"""
        round_num, source_client_id, chunk_id = chunk_key
        
        # 🆕 从所有已知重要性分数中获取
        # 1. 首先检查是否是自己的chunk
        if source_client_id == self.client_id:
            chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(round_num)
            if chunk_id in chunk_importance_scores:
                chunk_data = chunk_importance_scores[chunk_id]
                return chunk_data.get('importance_score', 0.0)
        
        # 2. 从peer的bitfield中获取重要性分数
        if hasattr(self, 'peer_importance_scores'):
            for peer_id, peer_scores in self.peer_importance_scores.items():
                if chunk_key in peer_scores:
                    return peer_scores[chunk_key]
        
        # 3. 默认返回0.0
        return 0.0
    
    def _rarest_first_selection(self) -> Optional[Tuple]:
        """Rarest First chunk选择算法（兼容性别名）"""
        return self._importance_guided_selection()
        
    def _regular_unchoke_algorithm(self):
        """经典的Reciprocal Unchoke算法（包含防死锁改进）"""
        # 🔧 Bug修复6: 动态调整upload slots
        # Star拓扑中心节点需要更多slots
        if self._is_central_node():
            self.MAX_UPLOAD_SLOTS = 8
        
        # 根据下载速率排序interested peers
        interested_peers = list(self.interested_by)
        interested_peers.sort(key=lambda p: self.download_rate.get(p, 0), reverse=True)
        
        # 选择前N个peers进行regular unchoke（预留1个给optimistic）
        regular_slots = self.MAX_UPLOAD_SLOTS - 1
        new_unchoked = set(interested_peers[:regular_slots])
        
        # 🔧 Bug修复7: 公平性保证 - 确保每个peer至少被unchoke过一次
        for peer_id in self.interested_by:
            if peer_id not in self.ever_unchoked and len(new_unchoked) < self.MAX_UPLOAD_SLOTS:
                new_unchoked.add(peer_id)
                self.ever_unchoked.add(peer_id)
                logger.debug(f"[BT] Fairness unchoke for peer {peer_id}")
        
        # 🔧 Bug修复8: 确保至少有MIN_UNCHOKE_SLOTS个unchoke
        if len(new_unchoked) == 0 and len(self.interested_by) > 0:
            # 随机选择一个peer进行unchoke，防止完全死锁
            emergency_peer = random.choice(list(self.interested_by))
            new_unchoked.add(emergency_peer)
            logger.warning(f"[BT] Emergency unchoke for peer {emergency_peer}")
        
        # 更新choke/unchoke状态
        for peer_id in self.unchoked_peers - new_unchoked:
            self._send_choke(peer_id)
        for peer_id in new_unchoked - self.unchoked_peers:
            self._send_unchoke(peer_id)
            
        self.unchoked_peers = new_unchoked
        
    def _optimistic_unchoke(self):
        """Optimistic unchoke机制（防死锁的关键）"""
        # 从被choke的interested peers中随机选择一个
        choked_interested = self.interested_by - self.unchoked_peers
        if choked_interested:
            # 🔧 Bug修复9: 优先选择从未unchoke过的peer
            never_unchoked = choked_interested - self.ever_unchoked
            if never_unchoked:
                self.optimistic_unchoke_peer = random.choice(list(never_unchoked))
            else:
                self.optimistic_unchoke_peer = random.choice(list(choked_interested))
            
            self._send_unchoke(self.optimistic_unchoke_peer)
            self.unchoked_peers.add(self.optimistic_unchoke_peer)
            self.ever_unchoked.add(self.optimistic_unchoke_peer)
            logger.info(f"[BT] Optimistic unchoke for peer {self.optimistic_unchoke_peer}")
            
    def _is_central_node(self) -> bool:
        """🐛 Bug修复27: 判断是否为star拓扑的中心节点"""
        # 简单判断：如果连接的邻居数量超过总节点数的一半，可能是中心节点
        if len(self.neighbors) > 2:  # 假设3个以上连接为中心节点
            return True
        return False
        
    def _find_alternative_peers(self, chunk_key: Tuple, exclude: int = None) -> List[int]:
        """🐛 Bug修复28: 查找拥有指定chunk的替代peers"""
        alternatives = []
        for peer_id, bitfield in self.peer_bitfields.items():
            if peer_id != exclude and chunk_key in bitfield and bitfield[chunk_key]:
                alternatives.append(peer_id)
        return alternatives
        
    def _find_peer_with_chunk(self, chunk_key: Tuple) -> Optional[int]:
        """查找拥有指定chunk的peer"""
        for peer_id, bitfield in self.peer_bitfields.items():
            if chunk_key in bitfield and bitfield[chunk_key]:
                return peer_id
        return None
        
    def _send_bitfield(self, peer_id: int):
        """向指定peer发送bitfield（包含重要性分数）"""
        from federatedscope.core.message import Message
        
        # 🔧 修复：将bitfield转换为可序列化的格式
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        logger.info(f"[BT] Client {self.client_id}: My bitfield for round {self.round_num}: {len(my_bitfield)} chunks")
        
        # 🆕 获取本轮次的chunk重要性分数
        chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(self.round_num)
        logger.debug(f"[BT] Client {self.client_id}: Got {len(chunk_importance_scores)} importance scores for round {self.round_num}")
        
        # 🔧 调试：详细输出我拥有的chunks
        if my_bitfield:
            logger.info(f"[BT] Client {self.client_id}: My chunks for round {self.round_num}:")
            for chunk_key, has_chunk in my_bitfield.items():
                if has_chunk:
                    round_num, source_id, chunk_id = chunk_key
                    # 静默记录拥有的chunks
                    pass
        else:
            logger.warning(f"[BT] Client {self.client_id}: ⚠️ I have NO chunks for round {self.round_num}!")
        
        # 转换为列表格式，包含重要性分数
        bitfield_list = []
        for (round_num, source_id, chunk_id), has_chunk in my_bitfield.items():
            if has_chunk:
                # 🆕 获取chunk重要性分数
                importance_score = 0.0
                if source_id == self.client_id and chunk_id in chunk_importance_scores:
                    # 自己的chunk，使用本地保存的重要性分数
                    chunk_data = chunk_importance_scores[chunk_id]
                    importance_score = chunk_data.get('importance_score', 0.0)
                    logger.debug(f"[BT] Client {self.client_id}: Using local importance {importance_score:.4f} for own chunk {chunk_id}")
                
                bitfield_list.append({
                    'round': round_num,
                    'source': source_id,
                    'chunk': chunk_id,
                    'importance_score': importance_score  # 🆕 添加重要性分数
                })
        
        logger.info(f"[BT] Client {self.client_id}: Sending {len(bitfield_list)} chunks in bitfield to peer {peer_id}")
        
        self.comm_manager.send(
            Message(msg_type='bitfield',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={
                       'round_num': self.round_num,
                       'bitfield': bitfield_list
                   })
        )
        
    def _send_interested(self, peer_id: int):
        """发送interested消息"""
        self.interested_in.add(peer_id)
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='interested',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={})
        )
        
    def _send_unchoke(self, peer_id: int):
        """发送unchoke消息"""
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='unchoke',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={})
        )
        
    def _send_choke(self, peer_id: int):
        """发送choke消息"""
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='choke',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={})
        )
    
    def _broadcast_have(self, round_num: int, source_client_id: int, chunk_id: int):
        """向所有邻居发送have消息（包含重要性分数）"""
        # 🔴 have消息包含轮次信息
        from federatedscope.core.message import Message
        
        # 🆕 获取chunk重要性分数
        importance_score = 0.0
        if source_client_id == self.client_id:
            # 自己的chunk，从数据库获取重要性分数
            chunk_importance_scores = self.chunk_manager.get_chunk_importance_scores(round_num)
            if chunk_id in chunk_importance_scores:
                chunk_data = chunk_importance_scores[chunk_id]
                importance_score = chunk_data.get('importance_score', 0.0)
                logger.debug(f"[BT] Client {self.client_id}: Broadcasting have with importance {importance_score:.4f} for own chunk {chunk_id}")
        
        for neighbor_id in self.neighbors:
            self.comm_manager.send(
                Message(msg_type='have',
                       sender=self.client_id,
                       receiver=[neighbor_id],
                       state=round_num,
                       content={
                           'round_num': round_num,
                           'source_client_id': source_client_id,
                           'chunk_id': chunk_id,
                           'importance_score': importance_score  # 🆕 添加重要性分数
                       })
            )
                
    def check_timeouts(self):
        """🔧 修复：非阻塞超时检查，在消息处理中调用"""
        current_time = time.time()
        
        # 每秒检查一次
        if current_time - self.last_timeout_check < 1.0:
            return
        
        self.last_timeout_check = current_time
        timeout_requests = []
        
        # 查找超时的请求
        for chunk_key, (peer_id, timestamp) in self.pending_requests.items():
            if current_time - timestamp > self.request_timeout:
                timeout_requests.append((chunk_key, peer_id))
        
        # 处理超时请求
        for chunk_key, peer_id in timeout_requests:
            # 🔴 chunk_key现在包含轮次信息
            round_num, source_id, chunk_id = chunk_key
            retry_count = self.retry_count.get(chunk_key, 0)
            
            if retry_count < self.max_retries:
                # 重新请求
                logger.warning(f"[BT] Request timeout for chunk {chunk_key}, retrying ({retry_count+1}/{self.max_retries})")
                
                # 从其他peer请求
                alternative_peers = self._find_alternative_peers(chunk_key, exclude=peer_id)
                if alternative_peers:
                    new_peer = alternative_peers[0]
                    # 🔴 传递正确的参数给_send_request
                    self._send_request(new_peer, source_id, chunk_id)
                    self.pending_requests[chunk_key] = (new_peer, current_time)
                    self.retry_count[chunk_key] = retry_count + 1
                else:
                    logger.error(f"[BT] No alternative peers for chunk {chunk_key}")
                    del self.pending_requests[chunk_key]
            else:
                # 达到最大重试次数
                logger.error(f"[BT] Max retries reached for chunk {chunk_key}")
                del self.pending_requests[chunk_key]
                if chunk_key in self.retry_count:
                    del self.retry_count[chunk_key]
                        
    def _send_request(self, peer_id: int, source_id: int, chunk_id: int):
        """发送chunk请求（双池管理系统）"""
        # 🔴 chunk_key包含轮次信息
        chunk_key = (self.round_num, source_id, chunk_id)
        
        # 🔧 CRITICAL FIX: Check for duplicate requests to prevent network flooding
        if chunk_key in self.pending_requests:
            existing_peer, existing_time = self.pending_requests[chunk_key]
            logger.debug(f"[BT-REQ] Client {self.client_id}: DUPLICATE REQUEST PREVENTED for chunk {source_id}:{chunk_id} - already pending from peer {existing_peer} for {time.time() - existing_time:.1f}s")
            return False
        
        # 🆕 检查活跃池是否已满
        if len(self.pending_requests) >= self.MAX_ACTIVE_REQUESTS:
            logger.debug(f"[BT-REQ] Client {self.client_id}: ACTIVE POOL FULL ({len(self.pending_requests)}/{self.MAX_ACTIVE_REQUESTS}), skipping request for chunk {source_id}:{chunk_id}")
            return False
        
        self.pending_requests[chunk_key] = (peer_id, time.time())
        
        logger.debug(f"[BT-REQ] Client {self.client_id}: Sending request to peer {peer_id} for chunk {source_id}:{chunk_id}")
        logger.debug(f"[BT-REQ] Client {self.client_id}: Active pool: {len(self.pending_requests)}/{self.MAX_ACTIVE_REQUESTS}, Queue: {len(self.pending_queue)}/{self.MAX_PENDING_QUEUE}")
        
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='request',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=self.round_num,
                   content={
                       'round_num': self.round_num,  # 🔴 请求的轮次
                       'source_client_id': source_id,
                       'chunk_id': chunk_id
                   })
        )
        return True
    
    def _send_piece(self, peer_id: int, round_num: int, source_client_id: int, chunk_id: int, chunk_data):
        """发送chunk数据"""
        # 🔧 修复：预序列化chunk_data并base64编码避免网络传输中的数据类型变化
        import pickle
        import base64
        serialized_data = pickle.dumps(chunk_data)
        encoded_data = base64.b64encode(serialized_data).decode('utf-8')
        checksum = hashlib.sha256(serialized_data).hexdigest()
        
        logger.debug(f"[BT-SEND] Client {self.client_id}: Serializing chunk {source_client_id}:{chunk_id}, original_type={type(chunk_data)}, serialized_size={len(serialized_data)}, encoded_size={len(encoded_data)}")
        
        # 🔴 消息包含轮次信息
        from federatedscope.core.message import Message
        self.comm_manager.send(
            Message(msg_type='piece',
                   sender=self.client_id,
                   receiver=[peer_id],
                   state=round_num,
                   content={
                       'round_num': round_num,  # 🔴 chunk所属轮次
                       'source_client_id': source_client_id,
                       'chunk_id': chunk_id,
                       'data': encoded_data,  # 🔧 发送base64编码的字符串
                       'checksum': checksum
                   })
        )
        
        # 更新上传统计
        self.total_uploaded += len(serialized_data)
        
    def _has_interesting_chunks(self, peer_id: int) -> bool:
        """检查peer是否有我需要的chunks"""
        if peer_id not in self.peer_bitfields:
            return False
            
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        peer_bitfield = self.peer_bitfields[peer_id]
        
        # 检查peer是否有我没有的chunks
        for chunk_key, has_chunk in peer_bitfield.items():
            if has_chunk and chunk_key not in my_bitfield and chunk_key[0] == self.round_num:
                return True
        return False
        
    def _evaluate_unchoke(self, peer_id: int):
        """评估是否unchoke指定peer"""
        if len(self.unchoked_peers) < self.MAX_UPLOAD_SLOTS:
            self._send_unchoke(peer_id)
            self.unchoked_peers.add(peer_id)
            self.ever_unchoked.add(peer_id)
            
    def _schedule_regular_unchoke(self):
        """安排定期unchoke"""
        # 在实际实现中，这应该通过定时器或消息循环调用
        self.last_unchoke_time = time.time()
        
    def _schedule_optimistic_unchoke(self):
        """安排optimistic unchoke"""
        # 在实际实现中，这应该通过定时器或消息循环调用
        pass
        
    def _update_download_rate(self, peer_id: int, bytes_received: int):
        """更新下载速率统计"""
        current_time = time.time()
        if peer_id not in self.last_activity:
            self.last_activity[peer_id] = current_time
            self.download_rate[peer_id] = 0
            
        time_diff = current_time - self.last_activity[peer_id]
        if time_diff > 0:
            # 简单的速率计算
            rate = bytes_received / time_diff
            # 指数移动平均
            if peer_id in self.download_rate:
                self.download_rate[peer_id] = 0.8 * self.download_rate[peer_id] + 0.2 * rate
            else:
                self.download_rate[peer_id] = rate
                
    def get_progress(self) -> Dict[str, Any]:
        """获取交换进度信息"""
        my_bitfield = self.chunk_manager.get_global_bitfield(self.round_num)
        total_expected = len(self.neighbors) * self.chunks_per_client + self.chunks_per_client  # 包括自己的chunks
        
        return {
            'chunks_collected': len(my_bitfield),
            'total_expected': total_expected,
            'progress_ratio': len(my_bitfield) / total_expected if total_expected > 0 else 0,
            'active_peers': len(self.peer_bitfields),
            'pending_requests': len(self.pending_requests),
            'bytes_downloaded': self.total_downloaded,
            'bytes_uploaded': self.total_uploaded
        }
    
    def stop_exchange(self):
        """
        🔧 CRITICAL FIX: Stop BitTorrent exchange immediately
        This method is called when server timeout occurs or new round begins
        to prevent interference with next round BitTorrent operations
        """
        logger.info(f"[BT] Client {self.client_id}: Stopping BitTorrent exchange for round {self.round_num}")
        
        # Set stop flag
        self.is_stopped = True
        
        # Clear all pending operations
        self.pending_requests.clear()
        self.retry_count.clear()
        
        # Clear peer state
        self.peer_bitfields.clear()
        self.interested_in.clear()
        self.interested_by.clear()
        self.choked_peers.clear()
        self.unchoked_peers.clear()
        self.ever_unchoked.clear()
        self.last_activity.clear()
        
        # Clear rate tracking
        self.download_rate.clear()
        self.upload_rate.clear()
        
        logger.info(f"[BT] Client {self.client_id}: BitTorrent exchange stopped successfully")
        logger.info(f"[BT] Client {self.client_id}: Final stats - Downloaded: {self.total_downloaded} bytes, Uploaded: {self.total_uploaded} bytes")