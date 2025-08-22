#!/usr/bin/env python3
"""
BitTorrent Manager Unit Tests
测试BitTorrent核心功能组件
"""

import unittest
import tempfile
import os
import time
import sqlite3
from unittest.mock import Mock, patch, MagicMock

# 添加项目路径
import sys
sys.path.insert(0, '/mnt/g/FLtorrent_combine/FederatedScope-master')

from federatedscope.core.bittorrent_manager import BitTorrentManager
from federatedscope.core.chunk_manager import ChunkManager
from federatedscope.core.message import Message


class TestBitTorrentManager(unittest.TestCase):
    """测试BitTorrentManager类的核心功能"""
    
    def setUp(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()
        self.client_id = 1
        self.round_num = 1
        self.neighbors = [2, 3, 4]
        
        # 创建模拟的chunk_manager
        self.chunk_manager = Mock()
        self.chunk_manager.client_id = self.client_id
        self.chunk_manager.current_round = self.round_num
        
        # 创建模拟的comm_manager
        self.comm_manager = Mock()
        
        # 初始化BitTorrentManager
        self.bt_manager = BitTorrentManager(
            client_id=self.client_id,
            round_num=self.round_num,
            chunk_manager=self.chunk_manager,
            comm_manager=self.comm_manager,
            neighbors=self.neighbors
        )
        
    def tearDown(self):
        """清理测试环境"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_initialization(self):
        """测试BitTorrentManager初始化"""
        self.assertEqual(self.bt_manager.client_id, self.client_id)
        self.assertEqual(self.bt_manager.round_num, self.round_num)
        self.assertEqual(self.bt_manager.neighbors, self.neighbors)
        self.assertIsInstance(self.bt_manager.peer_bitfields, dict)
        self.assertIsInstance(self.bt_manager.interested_in, set)
        self.assertIsInstance(self.bt_manager.interested_by, set)
        
    def test_start_exchange(self):
        """测试启动BitTorrent交换"""
        with patch.object(self.bt_manager, '_send_bitfield') as mock_send_bitfield:
            self.bt_manager.start_exchange()
            
            # 验证向所有邻居发送了bitfield
            self.assertEqual(mock_send_bitfield.call_count, len(self.neighbors))
            for neighbor in self.neighbors:
                mock_send_bitfield.assert_any_call(neighbor)
                
    def test_handle_bitfield(self):
        """测试处理bitfield消息"""
        sender_id = 2
        bitfield = {(1, 2, 1): True, (1, 2, 2): True}
        
        with patch.object(self.bt_manager, '_has_interesting_chunks', return_value=True) as mock_interesting:
            with patch.object(self.bt_manager, '_send_interested') as mock_send_interested:
                self.bt_manager.handle_bitfield(sender_id, bitfield)
                
                # 验证bitfield被正确存储
                self.assertEqual(self.bt_manager.peer_bitfields[sender_id], bitfield)
                # 验证发送了interested消息
                mock_send_interested.assert_called_once_with(sender_id)
                
    def test_handle_interested(self):
        """测试处理interested消息"""
        sender_id = 2
        
        with patch.object(self.bt_manager, '_evaluate_unchoke') as mock_evaluate:
            self.bt_manager.handle_interested(sender_id)
            
            # 验证sender被加入interested_by集合
            self.assertIn(sender_id, self.bt_manager.interested_by)
            # 验证调用了评估unchoke
            mock_evaluate.assert_called_once_with(sender_id)
            
    def test_handle_request(self):
        """测试处理chunk请求"""
        sender_id = 2
        source_client_id = 1
        chunk_id = 1
        test_data = b"test chunk data"
        
        # 模拟chunk_manager返回数据
        self.chunk_manager.get_chunk_data.return_value = test_data
        
        with patch.object(self.bt_manager, '_send_piece') as mock_send_piece:
            self.bt_manager.handle_request(sender_id, self.round_num, source_client_id, chunk_id)
            
            # 验证调用了get_chunk_data
            self.chunk_manager.get_chunk_data.assert_called_once_with(self.round_num, source_client_id, chunk_id)
            # 验证发送了piece
            mock_send_piece.assert_called_once_with(sender_id, self.round_num, source_client_id, chunk_id, test_data)
            
    def test_handle_piece_success(self):
        """测试成功处理piece消息"""
        sender_id = 2
        source_client_id = 3
        chunk_id = 1
        chunk_data = b"test chunk data"
        import hashlib
        checksum = hashlib.sha256(chunk_data).hexdigest()
        
        with patch.object(self.bt_manager, '_broadcast_have') as mock_broadcast:
            with patch.object(self.bt_manager, '_update_download_rate') as mock_update_rate:
                result = self.bt_manager.handle_piece(sender_id, self.round_num, source_client_id, chunk_id, chunk_data, checksum)
                
                # 验证返回True表示成功
                self.assertTrue(result)
                # 验证调用了save_remote_chunk
                self.chunk_manager.save_remote_chunk.assert_called_once_with(self.round_num, source_client_id, chunk_id, chunk_data)
                # 验证广播了have消息
                mock_broadcast.assert_called_once_with(self.round_num, source_client_id, chunk_id)
                
    def test_handle_piece_wrong_round(self):
        """测试处理错误轮次的piece消息"""
        sender_id = 2
        wrong_round = 2  # 不同的轮次
        source_client_id = 3
        chunk_id = 1
        chunk_data = b"test chunk data"
        checksum = "dummy_checksum"
        
        result = self.bt_manager.handle_piece(sender_id, wrong_round, source_client_id, chunk_id, chunk_data, checksum)
        
        # 验证返回False表示失败
        self.assertFalse(result)
        # 验证没有调用save_remote_chunk
        self.chunk_manager.save_remote_chunk.assert_not_called()
        
    def test_handle_piece_wrong_checksum(self):
        """测试处理错误checksum的piece消息"""
        sender_id = 2
        source_client_id = 3
        chunk_id = 1
        chunk_data = b"test chunk data"
        wrong_checksum = "wrong_checksum"
        
        result = self.bt_manager.handle_piece(sender_id, self.round_num, source_client_id, chunk_id, chunk_data, wrong_checksum)
        
        # 验证返回False表示失败
        self.assertFalse(result)
        # 验证没有调用save_remote_chunk
        self.chunk_manager.save_remote_chunk.assert_not_called()
        
    def test_rarest_first_selection(self):
        """测试Rarest First chunk选择算法"""
        # 设置peer bitfields
        self.bt_manager.peer_bitfields = {
            2: {(1, 2, 1): True, (1, 2, 2): True},
            3: {(1, 3, 1): True, (1, 2, 1): True},  # chunk (1,2,1)有2个peers
            4: {(1, 4, 1): True}  # chunk (1,4,1)只有1个peer
        }
        
        # 模拟当前拥有的chunks
        my_bitfield = {(1, 1, 1): True}  # 只有自己的chunks
        self.chunk_manager.get_global_bitfield.return_value = my_bitfield
        
        selected_chunk = self.bt_manager._rarest_first_selection()
        
        # 验证选择了稀有的chunk - 算法选择可用性最少的chunks之一
        rarest_chunks = [(1, 4, 1), (1, 3, 1), (1, 2, 2)]  # 这些都只有1个peer
        self.assertIn(selected_chunk, rarest_chunks)
        
    def test_find_peer_with_chunk(self):
        """测试查找拥有特定chunk的peer"""
        chunk_key = (1, 2, 1)
        self.bt_manager.peer_bitfields = {
            2: {chunk_key: True, (1, 2, 2): True},
            3: {(1, 3, 1): True},
            4: {chunk_key: True}
        }
        
        peer = self.bt_manager._find_peer_with_chunk(chunk_key)
        
        # 验证找到了拥有该chunk的peer
        self.assertIn(peer, [2, 4])
        
    def test_check_timeouts(self):
        """测试超时检查机制"""
        # 添加一个超时的请求
        chunk_key = (1, 2, 1)
        old_time = time.time() - 10  # 10秒前
        self.bt_manager.pending_requests[chunk_key] = (2, old_time)
        self.bt_manager.request_timeout = 5.0  # 5秒超时
        self.bt_manager.last_timeout_check = 0  # 强制检查
        
        # 模拟有替代peers
        self.bt_manager.peer_bitfields = {
            3: {chunk_key: True}
        }
        
        with patch.object(self.bt_manager, '_send_request') as mock_send_request:
            with patch.object(self.bt_manager, '_find_alternative_peers', return_value=[3]) as mock_find_alt:
                self.bt_manager.check_timeouts()
                
                # 验证重新发送了请求
                mock_send_request.assert_called()
            
    def test_get_progress(self):
        """测试获取交换进度"""
        # 模拟当前拥有的chunks
        my_bitfield = {(1, 1, 1): True, (1, 2, 1): True}
        self.chunk_manager.get_global_bitfield.return_value = my_bitfield
        
        progress = self.bt_manager.get_progress()
        
        # 验证进度信息
        self.assertEqual(progress['chunks_collected'], 2)
        self.assertIn('progress_ratio', progress)
        self.assertIn('active_peers', progress)
        self.assertIn('pending_requests', progress)
        

class TestChunkManagerBitTorrent(unittest.TestCase):
    """测试ChunkManager的BitTorrent扩展功能"""
    
    def setUp(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()
        self.client_id = 1
        self.current_round = 1
        
        # 创建ChunkManager实例（使用正确的构造函数）
        self.chunk_manager = ChunkManager(client_id=self.client_id)
        self.db_path = self.chunk_manager.db_path
        
    def tearDown(self):
        """清理测试环境"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        
    def test_database_initialization(self):
        """测试数据库初始化"""
        # 验证数据库文件存在
        self.assertTrue(os.path.exists(self.db_path))
        
        # 验证BitTorrent表已创建
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 检查bt_chunks表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bt_chunks'")
            self.assertIsNotNone(cursor.fetchone())
            
            # 检查bt_exchange_status表  
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bt_exchange_status'")
            self.assertIsNotNone(cursor.fetchone())
            
            # 检查bt_sessions表
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bt_sessions'")
            self.assertIsNotNone(cursor.fetchone())
            
    def test_save_remote_chunk(self):
        """测试保存远程chunk"""
        round_num = 1
        source_client_id = 2
        chunk_id = 1
        chunk_data = b"test remote chunk data"
        
        self.chunk_manager.save_remote_chunk(round_num, source_client_id, chunk_id, chunk_data)
        
        # 验证chunk被保存到bt_chunks表
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT round_num, source_client_id, chunk_id, holder_client_id 
                FROM bt_chunks 
                WHERE round_num=? AND source_client_id=? AND chunk_id=?
            """, (round_num, source_client_id, chunk_id))
            
            result = cursor.fetchone()
            self.assertIsNotNone(result)
            self.assertEqual(result[0], round_num)
            self.assertEqual(result[1], source_client_id)
            self.assertEqual(result[2], chunk_id)
            self.assertEqual(result[3], self.client_id)  # holder_client_id
            
    def test_get_chunk_data(self):
        """测试获取chunk数据"""
        round_num = 1
        source_client_id = 2
        chunk_id = 1
        chunk_data = b"test chunk data for retrieval"
        
        # 先保存chunk
        self.chunk_manager.save_remote_chunk(round_num, source_client_id, chunk_id, chunk_data)
        
        # 获取chunk数据
        retrieved_data = self.chunk_manager.get_chunk_data(round_num, source_client_id, chunk_id)
        
        # 验证数据正确
        self.assertEqual(retrieved_data, chunk_data)
        
    def test_get_global_bitfield(self):
        """测试获取全局bitfield"""
        # 保存一些远程chunks
        self.chunk_manager.save_remote_chunk(1, 2, 1, b"chunk1")
        self.chunk_manager.save_remote_chunk(1, 2, 2, b"chunk2")
        self.chunk_manager.save_remote_chunk(1, 3, 1, b"chunk3")
        
        # 获取bitfield
        bitfield = self.chunk_manager.get_global_bitfield(1)
        
        # 验证bitfield包含正确的chunks
        expected_keys = {(1, 2, 1), (1, 2, 2), (1, 3, 1)}
        for key in expected_keys:
            self.assertIn(key, bitfield)
            self.assertTrue(bitfield[key])


if __name__ == '__main__':
    print("开始BitTorrent单元测试...")
    
    # 设置测试套件
    test_suite = unittest.TestSuite()
    
    # 添加BitTorrentManager测试
    bt_manager_tests = unittest.TestLoader().loadTestsFromTestCase(TestBitTorrentManager)
    test_suite.addTests(bt_manager_tests)
    
    # 添加ChunkManager BitTorrent扩展测试
    chunk_manager_tests = unittest.TestLoader().loadTestsFromTestCase(TestChunkManagerBitTorrent)
    test_suite.addTests(chunk_manager_tests)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 输出结果
    if result.wasSuccessful():
        print(f"\n✅ 所有测试通过！共运行了 {result.testsRun} 个测试")
    else:
        print(f"\n❌ 测试失败！{len(result.failures)} 个失败，{len(result.errors)} 个错误")
        for test, error in result.failures + result.errors:
            print(f"  - {test}: {error}")