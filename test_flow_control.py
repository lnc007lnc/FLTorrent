#!/usr/bin/env python3
"""
测试BitTorrent流控机制
"""
import sys
import os
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

from federatedscope.core.bittorrent_manager import BitTorrentManager
from federatedscope.core.chunk_manager import ChunkManager

class MockCommManager:
    def __init__(self):
        self.sent_messages = []
        
    def send(self, message):
        self.sent_messages.append(message)
        print(f"📤 发送消息: {message.msg_type} -> {message.receiver}")

def test_flow_control():
    """测试BitTorrent流控机制"""
    print("🚦 测试BitTorrent流控机制...")
    
    # 创建mock对象
    chunk_manager = ChunkManager(client_id=1)
    comm_manager = MockCommManager()
    
    # 创建BitTorrent Manager
    bt_manager = BitTorrentManager(
        client_id=1,
        round_num=0,
        neighbors=[2, 3],
        chunk_manager=chunk_manager,
        comm_manager=comm_manager
    )
    
    print(f"📊 流控参数:")
    print(f"  最大并发请求: {bt_manager.max_concurrent_requests}")
    print(f"  请求间隔: {bt_manager.request_interval}s")
    print(f"  Pipeline限制: {bt_manager.pipelining_limit}")
    
    # 测试1: 快速发送多个请求，应该被流控
    print(f"\n🧪 测试1: 快速发送10个请求到peer 2...")
    start_time = time.time()
    
    for chunk_id in range(10):
        bt_manager._send_request(peer_id=2, source_id=2, chunk_id=chunk_id)
    
    elapsed = time.time() - start_time
    print(f"⏱️  发送10个请求耗时: {elapsed:.3f}s")
    print(f"📨 实际发送的消息数: {len(comm_manager.sent_messages)}")
    print(f"📋 队列中的请求数: {len(bt_manager.request_queue)}")
    print(f"⏳ Pending请求数: {len(bt_manager.pending_requests)}")
    
    # 测试2: 模拟接收chunk，释放队列
    print(f"\n🧪 测试2: 模拟接收chunk，处理队列...")
    
    # 模拟接收一个chunk
    import hashlib
    test_data = b"test chunk data"
    checksum = hashlib.sha256(test_data).hexdigest()
    
    initial_queue_size = len(bt_manager.request_queue)
    initial_pending = len(bt_manager.pending_requests)
    
    bt_manager.handle_piece(
        sender_id=2,
        round_num=0,
        source_client_id=2,
        chunk_id=0,
        chunk_data=test_data,
        checksum=checksum
    )
    
    final_queue_size = len(bt_manager.request_queue)
    final_pending = len(bt_manager.pending_requests)
    
    print(f"📋 队列变化: {initial_queue_size} -> {final_queue_size}")
    print(f"⏳ Pending变化: {initial_pending} -> {final_pending}")
    print(f"📨 新发送的消息数: {len(comm_manager.sent_messages)}")
    
    # 测试3: 超时检查和队列处理
    print(f"\n🧪 测试3: 超时检查和队列处理...")
    
    # 强制触发队列处理
    bt_manager.last_queue_process = 0  # 重置时间
    bt_manager.check_timeouts()
    
    print(f"📋 最终队列大小: {len(bt_manager.request_queue)}")
    print(f"⏳ 最终Pending数: {len(bt_manager.pending_requests)}")
    print(f"📨 总发送消息数: {len(comm_manager.sent_messages)}")
    
    # 验证流控效果
    if len(bt_manager.pending_requests) <= bt_manager.max_concurrent_requests:
        print("✅ 并发请求数控制正常")
    else:
        print("❌ 并发请求数超限")
        
    if initial_queue_size > final_queue_size:
        print("✅ 队列处理机制正常")
    else:
        print("⚠️ 队列没有被处理")

if __name__ == "__main__":
    test_flow_control()