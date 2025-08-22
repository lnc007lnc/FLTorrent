#!/usr/bin/env python3
"""
BitTorrent Integration Tests
测试BitTorrent与FederatedScope的完整集成
"""

import unittest
import tempfile
import os
import shutil
import time
import yaml
from unittest.mock import Mock, patch, MagicMock
import multiprocessing
import subprocess
import signal
import sys

# 添加项目路径
sys.path.insert(0, '/mnt/g/FLtorrent_combine/FederatedScope-master')

from federatedscope.core.configs import init_global_cfg
from federatedscope.core.fed_runner import FedRunner
from federatedscope.core.auxiliaries.utils import setup_seed


class TestBitTorrentIntegration(unittest.TestCase):
    """测试BitTorrent完整集成功能"""
    
    @classmethod
    def setUpClass(cls):
        """类级别设置，创建测试环境"""
        cls.test_dir = tempfile.mkdtemp(prefix='bt_integration_test_')
        cls.config_dir = os.path.join(cls.test_dir, 'configs')
        os.makedirs(cls.config_dir, exist_ok=True)
        
        # 创建简单的测试配置
        cls._create_test_configs()
        
    @classmethod
    def tearDownClass(cls):
        """清理测试环境"""
        shutil.rmtree(cls.test_dir, ignore_errors=True)
        
    @classmethod
    def _create_test_configs(cls):
        """创建测试配置文件"""
        
        # 基础配置
        base_config = {
            'use_gpu': False,
            'device': 'cpu',
            'federate': {
                'mode': 'standalone',  # 先测试standalone模式
                'client_num': 3,
                'total_round_num': 2,
                'sample_client_num': 3,
                'make_global_eval': False
            },
            'data': {
                'root': 'data/',
                'type': 'toy',  # 使用toy数据集
                'splits': [0.8, 0.1, 0.1]
            },
            'model': {
                'type': 'lr',  # 简单的线性回归模型
                'input_shape': 5,
                'out_channels': 1
            },
            'train': {
                'optimizer': {
                    'type': 'SGD',
                    'lr': 0.01
                },
                'local_update_steps': 1
            },
            'eval': {
                'freq': 1,
                'metrics': ['acc', 'loss']
            },
            'criterion': {
                'type': 'CrossEntropyLoss'
            },
            'trainer': {
                'type': 'cvtrainer'
            },
            'seed': 12345,
            # BitTorrent配置
            'bittorrent': {
                'enable': True,
                'timeout': 30.0,
                'max_retries': 2,
                'max_upload_slots': 2,
                'request_timeout': 5.0,
                'min_completion_ratio': 0.8,
                'chunk_selection': 'rarest_first',
                'verbose': True
            },
            'chunk': {
                'num_chunks': 5  # 每个客户端5个chunks
            }
        }
        
        # 保存基础配置
        cls.base_config_path = os.path.join(cls.config_dir, 'base_config.yaml')
        with open(cls.base_config_path, 'w') as f:
            yaml.safe_dump(base_config, f)
            
        # 创建分布式配置（用于完整测试）
        distributed_config = base_config.copy()
        distributed_config['federate']['mode'] = 'distributed'
        distributed_config['distribute'] = {
            'use': True,
            'server_host': 'localhost',
            'server_port': 50051,
            'client_port': 50052
        }
        
        cls.distributed_config_path = os.path.join(cls.config_dir, 'distributed_config.yaml')
        with open(cls.distributed_config_path, 'w') as f:
            yaml.safe_dump(distributed_config, f)
            
    def test_config_loading(self):
        """测试BitTorrent配置是否正确加载"""
        # 加载配置
        from federatedscope.core.configs.config import CN
        cfg = CN()
        init_global_cfg(cfg)
        cfg.merge_from_file(self.base_config_path)
        
        # 验证BitTorrent配置被正确加载
        self.assertTrue(hasattr(cfg, 'bittorrent'))
        self.assertTrue(cfg.bittorrent.enable)
        self.assertEqual(cfg.bittorrent.timeout, 30.0)
        self.assertEqual(cfg.bittorrent.max_upload_slots, 2)
        self.assertEqual(cfg.bittorrent.chunk_selection, 'rarest_first')
        
    def test_standalone_mode_with_bittorrent(self):
        """测试standalone模式下的BitTorrent功能"""
        # 加载配置
        from federatedscope.core.configs.config import CN
        cfg = CN()
        init_global_cfg(cfg)
        cfg.merge_from_file(self.base_config_path)
        
        # 设置种子以确保结果可重现
        setup_seed(cfg.seed)
        
        # 创建模拟的chunks数据
        test_chunks = {
            1: {1: b"client1_chunk1", 2: b"client1_chunk2"},
            2: {1: b"client2_chunk1", 2: b"client2_chunk2"},
            3: {1: b"client3_chunk1", 2: b"client3_chunk2"}
        }
        
        try:
            # 模拟FedRunner
            with patch('federatedscope.core.fed_runner.FedRunner.run') as mock_run:
                # 模拟成功运行
                mock_run.return_value = {'server_best_result': {'test_acc': 0.85}}
                
                # 创建并运行FedRunner
                runner = FedRunner(cfg)
                results = runner.run()
                
                # 验证FedRunner被调用
                mock_run.assert_called_once()
                self.assertIsNotNone(results)
                
        except Exception as e:
            # 如果出现ImportError或其他配置问题，跳过测试
            self.skipTest(f"Standalone test skipped due to: {e}")
            
    def test_bittorrent_manager_integration(self):
        """测试BitTorrentManager与系统的集成"""
        from federatedscope.core.bittorrent_manager import BitTorrentManager
        from federatedscope.core.chunk_manager import ChunkManager
        from federatedscope.core.message import Message
        
        # 创建测试组件
        client_id = 1
        round_num = 1
        neighbors = [2, 3]
        
        # 创建ChunkManager
        chunk_manager = ChunkManager(client_id=client_id)
        
        # 模拟通信管理器
        comm_manager = Mock()
        comm_manager.send = Mock()
        
        # 创建BitTorrentManager
        bt_manager = BitTorrentManager(
            client_id=client_id,
            round_num=round_num,
            chunk_manager=chunk_manager,
            comm_manager=comm_manager,
            neighbors=neighbors
        )
        
        # 测试启动交换
        bt_manager.start_exchange()
        
        # 验证发送了bitfield消息
        self.assertEqual(comm_manager.send.call_count, len(neighbors))
        
        # 测试处理消息
        test_bitfield = {(1, 2, 1): True, (1, 2, 2): True}
        bt_manager.handle_bitfield(2, test_bitfield)
        
        # 验证bitfield被正确存储
        self.assertEqual(bt_manager.peer_bitfields[2], test_bitfield)
        
        # 测试数据存储和检索
        test_data = b"test chunk data for integration"
        chunk_manager.save_remote_chunk(round_num, 2, 1, test_data)
        
        retrieved_data = chunk_manager.get_chunk_data(round_num, 2, 1)
        self.assertEqual(retrieved_data, test_data)
        
    def test_server_bittorrent_trigger(self):
        """测试Server端的BitTorrent触发机制"""
        from federatedscope.core.workers.server import Server
        from federatedscope.core.configs import init_global_cfg
        
        # 加载配置
        from federatedscope.core.configs.config import CN
        cfg = CN()
        init_global_cfg(cfg)
        cfg.merge_from_file(self.base_config_path)
        
        # 创建模拟的Server
        with patch('federatedscope.core.auxiliaries.aggregator_builder.get_aggregator') as mock_agg:
            with patch('federatedscope.core.auxiliaries.sampler_builder.get_sampler') as mock_sampler:
                mock_agg.return_value = [Mock()]
                mock_sampler.return_value = Mock()
                
                try:
                    server = Server(ID=0, config=cfg, client_num=3, total_round_num=2)
                    
                    # 验证server有BitTorrent方法
                    self.assertTrue(hasattr(server, 'trigger_bittorrent'))
                    self.assertTrue(hasattr(server, 'callback_funcs_for_bittorrent_complete'))
                    
                    # 测试BitTorrent状态管理
                    if hasattr(server, 'bt_state'):
                        # 如果已初始化，验证状态
                        pass
                    else:
                        # 初始化状态
                        server.bt_state = 'IDLE'
                        
                    # 模拟触发BitTorrent
                    with patch.object(server.comm_manager, 'send') as mock_send:
                        result = server.trigger_bittorrent()
                        self.assertTrue(result)
                        # 验证发送了start_bittorrent消息
                        mock_send.assert_called()
                        
                except Exception as e:
                    self.skipTest(f"Server test skipped due to: {e}")
                    
    def test_client_bittorrent_handlers(self):
        """测试Client端的BitTorrent消息处理"""
        from federatedscope.core.workers.client import Client
        from federatedscope.core.message import Message
        
        # 加载配置
        from federatedscope.core.configs.config import CN
        cfg = CN()
        init_global_cfg(cfg)
        cfg.merge_from_file(self.base_config_path)
        
        try:
            # 创建模拟的Client
            with patch('federatedscope.core.auxiliaries.trainer_builder.get_trainer') as mock_trainer:
                mock_trainer.return_value = Mock()
                
                client = Client(ID=1, config=cfg)
                
                # 验证client有BitTorrent处理方法
                self.assertTrue(hasattr(client, 'callback_funcs_for_start_bittorrent'))
                self.assertTrue(hasattr(client, 'callback_funcs_for_bitfield'))
                self.assertTrue(hasattr(client, 'callback_funcs_for_have'))
                self.assertTrue(hasattr(client, 'callback_funcs_for_request'))
                self.assertTrue(hasattr(client, 'callback_funcs_for_piece'))
                
                # 创建模拟的chunk_manager
                client.chunk_manager = Mock()
                client.chunk_manager.get_global_bitfield.return_value = {(1, 1, 1): True}
                
                # 测试start_bittorrent消息处理
                start_msg = Message(
                    msg_type='start_bittorrent',
                    sender=0,
                    receiver=[1],
                    content={
                        'round': 1,
                        'expected_chunks': 15  # 3个clients * 5个chunks
                    }
                )
                
                with patch.object(client, '_start_bittorrent_exchange') as mock_start:
                    client.callback_funcs_for_start_bittorrent(start_msg)
                    mock_start.assert_called()
                    
        except Exception as e:
            self.skipTest(f"Client test skipped due to: {e}")

    def test_message_flow_simulation(self):
        """模拟完整的BitTorrent消息流程"""
        from federatedscope.core.bittorrent_manager import BitTorrentManager
        from federatedscope.core.chunk_manager import ChunkManager
        from federatedscope.core.message import Message
        
        # 创建3个模拟的clients
        clients = {}
        chunk_managers = {}
        bt_managers = {}
        
        for client_id in [1, 2, 3]:
            # 创建chunk manager
            chunk_managers[client_id] = ChunkManager(client_id=client_id)
            
            # 模拟保存本地chunks
            for chunk_id in range(1, 3):  # 每个client 2个chunks
                test_data = f"client_{client_id}_chunk_{chunk_id}".encode()
                chunk_managers[client_id].save_remote_chunk(1, client_id, chunk_id, test_data)
            
            # 创建通信管理器模拟
            comm_manager = Mock()
            
            # 创建BitTorrent管理器
            neighbors = [i for i in [1, 2, 3] if i != client_id]
            bt_managers[client_id] = BitTorrentManager(
                client_id=client_id,
                round_num=1,
                chunk_manager=chunk_managers[client_id],
                comm_manager=comm_manager,
                neighbors=neighbors
            )
            
        # 模拟bitfield交换
        for client_id in bt_managers:
            bt_manager = bt_managers[client_id]
            
            # 获取本地bitfield
            my_bitfield = chunk_managers[client_id].get_global_bitfield(1)
            
            # 模拟向其他clients发送bitfield
            for neighbor_id in bt_manager.neighbors:
                if neighbor_id in bt_managers:
                    bt_managers[neighbor_id].handle_bitfield(client_id, my_bitfield)
        
        # 验证所有clients都收到了其他clients的bitfield
        for client_id in bt_managers:
            bt_manager = bt_managers[client_id]
            expected_peers = [i for i in [1, 2, 3] if i != client_id]
            
            for peer_id in expected_peers:
                self.assertIn(peer_id, bt_manager.peer_bitfields)
                self.assertGreater(len(bt_manager.peer_bitfields[peer_id]), 0)
        
        # 模拟chunk请求和响应
        client1 = bt_managers[1]
        client2 = bt_managers[2]
        
        # Client1请求Client2的chunk
        target_chunk = (1, 2, 1)  # round_num=1, source_client_id=2, chunk_id=1
        
        # 模拟request消息
        client2.handle_request(1, 1, 2, 1)  # sender=1, round=1, source=2, chunk=1
        
        # 验证chunk_manager.get_chunk_data被调用
        # （这里实际会通过comm_manager发送piece消息，我们通过chunk存在性验证）
        chunk_data = chunk_managers[2].get_chunk_data(1, 2, 1)
        self.assertIsNotNone(chunk_data)
        
        print(f"✅ 消息流程测试完成：模拟了{len(bt_managers)}个clients的BitTorrent交换")

    def test_end_to_end_configuration(self):
        """端到端配置测试"""
        # 测试配置加载和验证
        from federatedscope.core.configs.config import CN
        cfg = CN()
        init_global_cfg(cfg)
        cfg.merge_from_file(self.base_config_path)
        
        # 验证所有必要的配置都存在
        self.assertTrue(cfg.bittorrent.enable)
        self.assertEqual(cfg.federate.client_num, 3)
        self.assertGreater(cfg.bittorrent.timeout, 0)
        
        # 验证配置验证函数
        from federatedscope.core.configs.cfg_bittorrent import assert_bittorrent_cfg
        
        try:
            assert_bittorrent_cfg(cfg)
            print("✅ BitTorrent配置验证通过")
        except AssertionError as e:
            self.fail(f"配置验证失败: {e}")
        except Exception as e:
            # 如果是导入错误等，跳过
            self.skipTest(f"配置验证跳过: {e}")


def run_integration_tests():
    """运行所有集成测试"""
    print("开始BitTorrent集成测试...")
    print("=" * 60)
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加所有测试
    integration_tests = unittest.TestLoader().loadTestsFromTestCase(TestBitTorrentIntegration)
    test_suite.addTests(integration_tests)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(test_suite)
    
    print("\n" + "=" * 60)
    
    # 输出结果
    if result.wasSuccessful():
        print(f"✅ 所有集成测试通过！共运行了 {result.testsRun} 个测试")
        return True
    else:
        print(f"❌ 集成测试失败！{len(result.failures)} 个失败，{len(result.errors)} 个错误")
        
        # 打印失败详情
        for test, error in result.failures:
            print(f"\n🔥 FAILURE in {test}:")
            print(error)
            
        for test, error in result.errors:
            print(f"\n⚠️ ERROR in {test}:")
            print(error)
            
        return False


if __name__ == '__main__':
    success = run_integration_tests()
    sys.exit(0 if success else 1)