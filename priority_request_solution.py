#!/usr/bin/env python3
"""
FederatedScope BitTorrent优先级请求管理解决方案
解决优先级反转和请求阻塞问题
"""

import heapq
import time
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class ChunkRequest:
    """Chunk请求信息"""
    peer_id: int
    round_num: int
    source_client_id: int
    chunk_id: int
    importance_score: float
    timestamp: float
    requester_id: int  # 请求方ID


class PriorityRequestManager:
    """优先级请求管理器 - 解决BitTorrent优先级反转问题"""
    
    def __init__(self, client_id: int):
        self.client_id = client_id
        
        # 📊 Configuration - 经典BitTorrent参数
        self.MAX_CONCURRENT_REQUESTS_PER_PEER = 5  # 每个peer最多5个并发请求
        self.MAX_TOTAL_CONCURRENT_REQUESTS = 20    # 总共最多20个并发请求
        self.REQUEST_TIMEOUT = 10.0                # 请求超时时间
        self.PRIORITY_REEVALUATION_INTERVAL = 2.0  # 优先级重评估间隔
        
        # 📋 Request tracking
        self.pending_requests: Dict[Tuple, ChunkRequest] = {}  # {(round,source,chunk): request}
        self.requests_per_peer: Dict[int, int] = {}           # {peer_id: count}
        
        # 🎯 Priority upload queue (for handling incoming requests)
        self.upload_queue: List[Tuple] = []  # Priority queue: (-importance, timestamp, request)
        
        # ⏰ Timing
        self.last_reevaluation = time.time()
        
    # =================== REQUEST SENDING (下载端) ===================
    
    def can_make_request(self, peer_id: int) -> bool:
        """检查是否可以向指定peer发送更多请求"""
        # 检查per-peer限制
        peer_requests = self.requests_per_peer.get(peer_id, 0)
        if peer_requests >= self.MAX_CONCURRENT_REQUESTS_PER_PEER:
            return False
            
        # 检查全局限制
        if len(self.pending_requests) >= self.MAX_TOTAL_CONCURRENT_REQUESTS:
            return False
            
        return True
    
    def make_request(self, peer_id: int, round_num: int, source_client_id: int, 
                    chunk_id: int, importance_score: float) -> bool:
        """发送chunk请求（带优先级控制）"""
        
        if not self.can_make_request(peer_id):
            # 尝试取消低优先度请求为高优先度让路
            if self._try_cancel_low_priority_request(importance_score):
                pass  # 成功取消，可以继续
            else:
                return False  # 无法发送请求
        
        # 创建请求
        chunk_key = (round_num, source_client_id, chunk_id)
        request = ChunkRequest(
            peer_id=peer_id,
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            importance_score=importance_score,
            timestamp=time.time(),
            requester_id=self.client_id
        )
        
        # 记录请求
        self.pending_requests[chunk_key] = request
        self.requests_per_peer[peer_id] = self.requests_per_peer.get(peer_id, 0) + 1
        
        print(f"🚀 Client {self.client_id}: Made request to peer {peer_id} for chunk "
              f"{source_client_id}:{chunk_id} (importance: {importance_score:.4f}, "
              f"pending: {len(self.pending_requests)})")
        
        return True
    
    def _try_cancel_low_priority_request(self, new_importance: float) -> bool:
        """尝试取消低优先度请求为高优先度请求让路"""
        
        # 找到最低优先度的请求
        lowest_priority = float('inf')
        lowest_request_key = None
        
        for chunk_key, request in self.pending_requests.items():
            if request.importance_score < lowest_priority:
                lowest_priority = request.importance_score
                lowest_request_key = chunk_key
        
        # 如果新请求优先度显著更高，取消旧请求
        if lowest_request_key and new_importance > lowest_priority * 1.5:  # 50%提升阈值
            canceled_request = self.pending_requests[lowest_request_key]
            self.cancel_request(lowest_request_key)
            
            print(f"🚫 Client {self.client_id}: Canceled low-priority request "
                  f"{canceled_request.source_client_id}:{canceled_request.chunk_id} "
                  f"(importance: {canceled_request.importance_score:.4f}) "
                  f"for higher priority request (importance: {new_importance:.4f})")
            
            return True
        
        return False
    
    def cancel_request(self, chunk_key: Tuple):
        """取消指定的chunk请求"""
        if chunk_key in self.pending_requests:
            request = self.pending_requests[chunk_key]
            
            # 发送取消消息到peer (需要在实际BitTorrent中实现)
            # self._send_cancel_message(request.peer_id, request.round_num, 
            #                          request.source_client_id, request.chunk_id)
            
            # 清理记录
            del self.pending_requests[chunk_key]
            self.requests_per_peer[request.peer_id] -= 1
            if self.requests_per_peer[request.peer_id] == 0:
                del self.requests_per_peer[request.peer_id]
    
    def handle_piece_received(self, chunk_key: Tuple):
        """处理接收到chunk数据"""
        if chunk_key in self.pending_requests:
            request = self.pending_requests[chunk_key]
            print(f"✅ Client {self.client_id}: Received chunk {request.source_client_id}:{request.chunk_id} "
                  f"from peer {request.peer_id} (importance: {request.importance_score:.4f})")
            
            # 清理请求记录
            del self.pending_requests[chunk_key]
            self.requests_per_peer[request.peer_id] -= 1
            if self.requests_per_peer[request.peer_id] == 0:
                del self.requests_per_peer[request.peer_id]
    
    # =================== REQUEST HANDLING (上传端) ===================
    
    def add_upload_request(self, requester_id: int, round_num: int, 
                          source_client_id: int, chunk_id: int, importance_score: float):
        """添加上传请求到优先级队列"""
        request = ChunkRequest(
            peer_id=requester_id,  # 这里peer_id是请求方
            round_num=round_num,
            source_client_id=source_client_id,
            chunk_id=chunk_id,
            importance_score=importance_score,
            timestamp=time.time(),
            requester_id=requester_id
        )
        
        # 使用负重要度分数实现最大堆（重要度高的优先）
        priority_tuple = (-importance_score, request.timestamp, request)
        heapq.heappush(self.upload_queue, priority_tuple)
        
        print(f"📥 Client {self.client_id}: Queued upload request from peer {requester_id} "
              f"for chunk {source_client_id}:{chunk_id} (importance: {importance_score:.4f}, "
              f"queue_size: {len(self.upload_queue)})")
    
    def get_next_upload_request(self) -> Optional[ChunkRequest]:
        """获取下一个要处理的上传请求（优先级最高）"""
        if not self.upload_queue:
            return None
            
        _, _, request = heapq.heappop(self.upload_queue)
        
        print(f"🔼 Client {self.client_id}: Processing upload request from peer {request.requester_id} "
              f"for chunk {request.source_client_id}:{request.chunk_id} "
              f"(importance: {request.importance_score:.4f})")
        
        return request
    
    # =================== PERIODIC MAINTENANCE ===================
    
    def reevaluate_priorities(self, current_importance_scores: Dict[Tuple, float]):
        """定期重新评估请求优先级"""
        current_time = time.time()
        if current_time - self.last_reevaluation < self.PRIORITY_REEVALUATION_INTERVAL:
            return
        
        self.last_reevaluation = current_time
        
        # 检查是否有请求的重要度发生了显著变化
        to_cancel = []
        for chunk_key, request in self.pending_requests.items():
            current_importance = current_importance_scores.get(chunk_key, request.importance_score)
            
            # 如果重要度显著下降，考虑取消
            if current_importance < request.importance_score * 0.5:  # 下降50%以上
                to_cancel.append(chunk_key)
        
        # 取消低优先度请求
        for chunk_key in to_cancel:
            self.cancel_request(chunk_key)
            print(f"🚫 Client {self.client_id}: Canceled request due to priority drop: {chunk_key}")
    
    def check_timeouts(self):
        """检查超时请求"""
        current_time = time.time()
        timed_out = []
        
        for chunk_key, request in self.pending_requests.items():
            if current_time - request.timestamp > self.REQUEST_TIMEOUT:
                timed_out.append(chunk_key)
        
        for chunk_key in timed_out:
            print(f"⏰ Client {self.client_id}: Request timed out: {chunk_key}")
            self.cancel_request(chunk_key)
    
    def get_status(self) -> Dict:
        """获取管理器状态"""
        return {
            'pending_requests': len(self.pending_requests),
            'upload_queue_size': len(self.upload_queue),
            'requests_per_peer': dict(self.requests_per_peer),
            'avg_pending_importance': sum(r.importance_score for r in self.pending_requests.values()) / 
                                    len(self.pending_requests) if self.pending_requests else 0
        }


# =================== USAGE EXAMPLE ===================

def demonstrate_priority_management():
    """演示优先级请求管理的使用"""
    
    print("🎯 FederatedScope BitTorrent优先级请求管理演示\n")
    
    # 创建Client 1的请求管理器
    client1_manager = PriorityRequestManager(client_id=1)
    
    print("场景1: Client1向Client2发送多个低重要度请求")
    for i in range(8):  # 超过per-peer限制
        success = client1_manager.make_request(
            peer_id=2, 
            round_num=1, 
            source_client_id=2, 
            chunk_id=i, 
            importance_score=0.07
        )
        print(f"   Request {i}: {'Success' if success else 'Failed (limit reached)'}")
    
    print(f"\n状态: {client1_manager.get_status()}")
    
    print("\n场景2: Client3加入，Client1发现高重要度chunk")
    success = client1_manager.make_request(
        peer_id=3, 
        round_num=1, 
        source_client_id=3, 
        chunk_id=0, 
        importance_score=0.35  # 高重要度
    )
    print(f"   High-priority request: {'Success' if success else 'Failed'}")
    
    print(f"\n最终状态: {client1_manager.get_status()}")
    
    print("\n" + "="*80)
    print("✅ 优先级管理演示完成")


if __name__ == "__main__":
    demonstrate_priority_management()