#!/usr/bin/env python3
"""
BitTorrent优先级管理集成指南
将PriorityRequestManager集成到现有BitTorrentManager中
"""

from typing import Optional

# ================== 集成方案1: 最小侵入式集成 ==================

class BitTorrentManagerPatch:
    """
    对现有BitTorrentManager的最小修改方案
    保持向后兼容性的同时添加优先级管理
    """
    
    def __init__(self, original_manager):
        self.original_manager = original_manager
        
        # 添加优先级管理器
        from priority_request_solution import PriorityRequestManager
        self.priority_manager = PriorityRequestManager(original_manager.client_id)
        
        # 保存原始方法的引用
        self.original_send_request = original_manager._send_request
        self.original_handle_request = original_manager.handle_request
        
        # 替换方法
        original_manager._send_request = self.enhanced_send_request
        original_manager.handle_request = self.enhanced_handle_request
    
    def enhanced_send_request(self, peer_id, source_id, chunk_id):
        """增强的请求发送方法 - 添加并发控制"""
        # 获取chunk重要度
        chunk_key = (self.original_manager.round_num, source_id, chunk_id)
        importance_score = self.original_manager._get_chunk_importance_score(chunk_key)
        
        # 检查是否可以发送请求
        if self.priority_manager.can_make_request(peer_id):
            # 记录请求
            if self.priority_manager.make_request(
                peer_id, self.original_manager.round_num, source_id, chunk_id, importance_score
            ):
                # 发送实际请求
                self.original_send_request(peer_id, source_id, chunk_id)
                return True
        
        print(f"🚫 Client {self.original_manager.client_id}: Request blocked due to concurrency limit")
        return False
    
    def enhanced_handle_request(self, sender_id, round_num, source_client_id, chunk_id):
        """增强的请求处理方法 - 添加优先级队列"""
        # 获取chunk重要度
        chunk_key = (round_num, source_client_id, chunk_id)
        importance_score = self.original_manager._get_chunk_importance_score(chunk_key)
        
        # 添加到优先级队列而不是立即处理
        self.priority_manager.add_upload_request(
            sender_id, round_num, source_client_id, chunk_id, importance_score
        )
        
        # 处理优先级队列中的请求
        self._process_upload_queue()
    
    def _process_upload_queue(self):
        """处理优先级上传队列"""
        while True:
            request = self.priority_manager.get_next_upload_request()
            if not request:
                break
                
            # 使用原始处理逻辑，但跳过队列逻辑
            self._handle_single_request(request)
    
    def _handle_single_request(self, request):
        """处理单个请求"""
        # 调用原始的handle_request逻辑（去除队列部分）
        if request.requester_id not in self.original_manager.choked_peers:
            chunk_data = self.original_manager.chunk_manager.get_chunk_data(
                request.round_num, request.source_client_id, request.chunk_id
            )
            if chunk_data is not None:
                self.original_manager._send_piece(
                    request.requester_id, request.round_num, 
                    request.source_client_id, request.chunk_id, chunk_data
                )


# ================== 集成方案2: 配置项集成 ==================

BITTORRENT_PRIORITY_CONFIG = """
# 在cfg_bittorrent.py中添加以下配置

def extend_bittorrent_cfg(cfg):
    # ... 现有配置 ...
    
    # 🆕 优先级管理配置
    cfg.bittorrent.enable_priority_management = True  # 启用优先级管理
    cfg.bittorrent.max_concurrent_requests_per_peer = 5  # 每peer最大并发请求
    cfg.bittorrent.max_total_concurrent_requests = 20    # 全局最大并发请求
    cfg.bittorrent.priority_cancel_threshold = 1.5       # 优先级取消阈值
    cfg.bittorrent.request_timeout = 10.0               # 请求超时时间
    cfg.bittorrent.priority_reevaluation_interval = 2.0 # 优先级重评估间隔
"""

# ================== 集成方案3: 完整BitTorrentManager重构 ==================

class EnhancedBitTorrentManager:
    """
    完全集成优先级管理的BitTorrentManager
    推荐用于新部署或主要版本升级
    """
    
    def __init__(self, client_id, round_num, chunk_manager, comm_manager, neighbors, cfg):
        # 现有初始化代码...
        self.client_id = client_id
        self.round_num = round_num
        self.chunk_manager = chunk_manager
        self.comm_manager = comm_manager
        self.neighbors = neighbors
        
        # 🆕 优先级管理器
        from priority_request_solution import PriorityRequestManager
        self.priority_manager = PriorityRequestManager(client_id)
        
        # 🆕 配置优先级管理参数
        if hasattr(cfg.bittorrent, 'enable_priority_management') and cfg.bittorrent.enable_priority_management:
            self.priority_manager.MAX_CONCURRENT_REQUESTS_PER_PEER = cfg.bittorrent.max_concurrent_requests_per_peer
            self.priority_manager.MAX_TOTAL_CONCURRENT_REQUESTS = cfg.bittorrent.max_total_concurrent_requests
            self.priority_manager.REQUEST_TIMEOUT = cfg.bittorrent.request_timeout
        
        # 现有BitTorrent状态...
        self.peer_bitfields = {}
        self.interested_in = set()
        self.choked_peers = set()
        # ...
    
    def _importance_guided_selection_with_concurrency(self) -> Optional[tuple]:
        """
        增强的chunk选择算法 - 考虑并发限制
        """
        # 使用现有的重要度选择逻辑
        candidate_chunk = self._importance_guided_selection()
        
        if candidate_chunk:
            round_num, source_id, chunk_id = candidate_chunk
            # 找到拥有该chunk的peer
            peer_id = self._find_peer_with_chunk(candidate_chunk)
            
            if peer_id and self.priority_manager.can_make_request(peer_id):
                return candidate_chunk
            else:
                # 如果无法请求首选chunk，寻找替代方案
                return self._find_alternative_chunk()
        
        return None
    
    def _find_alternative_chunk(self) -> Optional[tuple]:
        """寻找可请求的替代chunk"""
        # 获取所有可用chunks
        available_chunks = []
        for peer_id, bitfield in self.peer_bitfields.items():
            if self.priority_manager.can_make_request(peer_id):
                for chunk_key in bitfield:
                    if chunk_key not in self.chunk_manager.get_global_bitfield(self.round_num):
                        importance = self._get_chunk_importance_score(chunk_key)
                        available_chunks.append((chunk_key, importance, peer_id))
        
        # 按重要度排序并返回最高优先度的可请求chunk
        if available_chunks:
            available_chunks.sort(key=lambda x: x[1], reverse=True)
            return available_chunks[0][0]  # 返回chunk_key
        
        return None
    
    def enhanced_send_request(self, peer_id, source_id, chunk_id):
        """增强的请求发送 - 集成优先级管理"""
        chunk_key = (self.round_num, source_id, chunk_id)
        importance_score = self._get_chunk_importance_score(chunk_key)
        
        # 通过优先级管理器发送请求
        if self.priority_manager.make_request(
            peer_id, self.round_num, source_id, chunk_id, importance_score
        ):
            # 发送实际网络请求
            from federatedscope.core.message import Message
            self.comm_manager.send(
                Message(msg_type='request',
                       sender=self.client_id,
                       receiver=[peer_id],
                       state=self.round_num,
                       content={
                           'round_num': self.round_num,
                           'source_client_id': source_id,
                           'chunk_id': chunk_id
                       })
            )
            return True
        return False
    
    def enhanced_handle_request(self, sender_id, round_num, source_client_id, chunk_id):
        """增强的请求处理 - 优先级队列"""
        chunk_key = (round_num, source_client_id, chunk_id)
        importance_score = self._get_chunk_importance_score(chunk_key)
        
        # 添加到优先级队列
        self.priority_manager.add_upload_request(
            sender_id, round_num, source_client_id, chunk_id, importance_score
        )
        
        # 异步处理队列（可以在主循环中调用）
        self._process_priority_upload_queue()
    
    def _process_priority_upload_queue(self):
        """处理优先级上传队列 - 可在主循环中定期调用"""
        # 每次处理一定数量的请求避免阻塞
        max_process_per_call = 3
        processed = 0
        
        while processed < max_process_per_call:
            request = self.priority_manager.get_next_upload_request()
            if not request:
                break
            
            # 处理单个请求
            self._handle_priority_request(request)
            processed += 1
    
    def _handle_priority_request(self, request):
        """处理优先级请求"""
        if request.requester_id not in self.choked_peers:
            chunk_data = self.chunk_manager.get_chunk_data(
                request.round_num, request.source_client_id, request.chunk_id
            )
            if chunk_data is not None:
                self._send_piece(
                    request.requester_id, request.round_num,
                    request.source_client_id, request.chunk_id, chunk_data
                )
    
    def handle_piece(self, sender_id, round_num, source_client_id, chunk_id, chunk_data, checksum):
        """增强的piece处理 - 更新优先级管理器"""
        # 调用原有piece处理逻辑
        result = super().handle_piece(sender_id, round_num, source_client_id, chunk_id, chunk_data, checksum)
        
        # 通知优先级管理器请求已完成
        chunk_key = (round_num, source_client_id, chunk_id)
        self.priority_manager.handle_piece_received(chunk_key)
        
        return result
    
    def periodic_maintenance(self):
        """定期维护 - 在主循环中调用"""
        # 检查超时
        self.priority_manager.check_timeouts()
        
        # 处理优先级队列
        self._process_priority_upload_queue()
        
        # 重新评估优先级（如果需要）
        current_importance_scores = {}
        for chunk_key in self.priority_manager.pending_requests.keys():
            current_importance_scores[chunk_key] = self._get_chunk_importance_score(chunk_key)
        
        self.priority_manager.reevaluate_priorities(current_importance_scores)


# ================== 使用示例 ==================

def integration_example():
    """集成示例"""
    
    print("🔧 BitTorrent优先级管理集成示例\n")
    
    print("方案1: 最小侵入式集成 (推荐用于现有系统)")
    print("- 保持现有代码不变")
    print("- 通过装饰器/代理模式添加优先级管理") 
    print("- 向后兼容，风险最小")
    print("- 集成代码: BitTorrentManagerPatch")
    
    print("\n方案2: 配置驱动集成")
    print("- 添加配置选项控制优先级管理")
    print("- 可以动态启用/禁用功能")
    print("- 需要修改配置文件")
    
    print("\n方案3: 完整重构集成")
    print("- 将优先级管理深度集成到BitTorrentManager")
    print("- 最优性能，最佳用户体验")
    print("- 需要较大改动，适合新版本")
    
    print("\n🎯 推荐实施步骤:")
    print("1. 先使用方案1进行测试验证")
    print("2. 确认效果后考虑方案3的完整集成")
    print("3. 添加详细的性能监控和日志")
    print("4. 在真实多客户端环境中验证")


if __name__ == "__main__":
    integration_example()