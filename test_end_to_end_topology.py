#!/usr/bin/env python3
"""
端到端拓扑构建测试 - 模拟真实的服务端和客户端交互
"""

import sys
import os
sys.path.insert(0, os.getcwd())

# Set protobuf implementation
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import time
import threading
from unittest.mock import Mock, MagicMock
import logging

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MockDistributedFL:
    """模拟分布式FL环境进行拓扑测试"""
    
    def __init__(self):
        self.server = None
        self.clients = {}
        self.message_queue = []
        
    def create_mock_server(self):
        """创建模拟服务端"""
        print("🖥️ Creating mock server with topology support...")
        
        from federatedscope.core.configs.config import global_cfg
        from federatedscope.core.workers.connection_handler_mixin import ConnectionHandlerMixin
        from federatedscope.core.topology_manager import TopologyManager
        
        # 加载服务端配置
        cfg = global_cfg.clone()
        cfg.merge_from_file('topology_test_server.yaml')
        cfg.freeze(inform=False, save=False)
        
        # 创建模拟服务端
        class MockServer(ConnectionHandlerMixin):
            def __init__(self, cfg):
                ConnectionHandlerMixin.__init__(self)
                self.ID = 0
                self._cfg = cfg
                self.state = 0
                self.cur_timestamp = time.time()
                self.client_num = cfg.federate.client_num
                self.join_in_client_num = 0
                self.join_in_info = {}
                self.comm_manager = MockCommManager()
                
                # 初始化拓扑管理器
                if hasattr(cfg, 'topology') and cfg.topology.use:
                    self.topology_manager = TopologyManager(
                        topology_type=cfg.topology.type,
                        client_list=[]
                    )
                    logger.info(f"Server: Topology construction enabled ({cfg.topology.type})")
                else:
                    self.topology_manager = None
                    
            def simulate_client_join(self, client_id):
                """模拟客户端加入"""
                self.join_in_client_num += 1
                logger.info(f"Server: Client {client_id} joined ({self.join_in_client_num}/{self.client_num})")
                
                if self.join_in_client_num == self.client_num:
                    logger.info("Server: All clients joined, starting topology construction...")
                    if self.topology_manager:
                        self._construct_network_topology_mock()
                        
            def _construct_network_topology_mock(self):
                """模拟拓扑构建过程"""
                logger.info("🌐 Server: Starting network topology construction...")
                
                # 更新客户端列表
                client_list = list(range(1, self.client_num + 1))
                self.topology_manager.client_list = client_list
                
                # 计算拓扑
                topology_graph = self.topology_manager.compute_topology()
                logger.info(f"📋 Server: Computed topology: {topology_graph}")
                
                # 发送拓扑指令
                for client_id, neighbors in topology_graph.items():
                    if neighbors:
                        logger.info(f"📨 Server: Sending topology instruction to Client {client_id}: connect to {neighbors}")
                        # 模拟发送拓扑指令
                        
                return topology_graph
        
        self.server = MockServer(cfg)
        return self.server
        
    def create_mock_client(self, client_id):
        """创建模拟客户端"""
        print(f"💻 Creating mock client {client_id}...")
        
        from federatedscope.core.configs.config import global_cfg
        from federatedscope.core.connection_monitor import ConnectionMonitor
        
        # 加载客户端配置
        cfg = global_cfg.clone()
        cfg.merge_from_file('topology_test_client.yaml')
        cfg.distribute.data_idx = client_id
        cfg.distribute.client_port = 50052 + client_id - 1
        cfg.freeze(inform=False, save=False)
        
        class MockClient:
            def __init__(self, client_id, cfg):
                self.ID = client_id
                self._cfg = cfg
                self.comm_manager = MockCommManager()
                self.connection_monitor = ConnectionMonitor(
                    client_id=client_id,
                    comm_manager=self.comm_manager,
                    server_id=0
                )
                
            def simulate_topology_instruction(self, neighbors_to_connect):
                """模拟接收拓扑指令并建立连接"""
                logger.info(f"🌐 Client {self.ID}: Received topology instruction to connect to {neighbors_to_connect}")
                
                for neighbor_id in neighbors_to_connect:
                    # 模拟连接建立
                    success = self._simulate_connection_to_peer(neighbor_id)
                    if success:
                        logger.info(f"✅ Client {self.ID}: Successfully connected to Client {neighbor_id}")
                        # 报告连接成功
                        self.connection_monitor.report_connection_established(
                            peer_id=neighbor_id,
                            details={'topology_connection': True, 'simulation': True}
                        )
                    else:
                        logger.warning(f"❌ Client {self.ID}: Failed to connect to Client {neighbor_id}")
                        self.connection_monitor.report_connection_lost(
                            peer_id=neighbor_id,
                            details={'topology_connection': True, 'error': 'simulation_failure'}
                        )
                        
            def _simulate_connection_to_peer(self, peer_id):
                """模拟连接建立，90%成功率"""
                import random
                return random.random() < 0.9
                
        client = MockClient(client_id, cfg)
        self.clients[client_id] = client
        return client
        
    def run_topology_test(self):
        """运行完整的拓扑构建测试"""
        print("\n🧪 Running end-to-end topology construction test...")
        print("=" * 60)
        
        try:
            # 1. 创建服务端
            server = self.create_mock_server()
            
            # 2. 创建客户端
            clients = {}
            for i in range(1, 4):  # 3个客户端
                clients[i] = self.create_mock_client(i)
                
            # 3. 模拟客户端加入过程
            print(f"\n📥 Phase 1: Client Join-in")
            for client_id in clients:
                server.simulate_client_join(client_id)
                time.sleep(0.1)  # 模拟网络延迟
                
            # 4. 获取拓扑结构并模拟连接建立
            if server.topology_manager:
                topology_graph = server.topology_manager.topology_graph
                print(f"\n🔗 Phase 2: Topology Connection Establishment")
                
                # 让每个客户端按拓扑指令建立连接
                for client_id, neighbors in topology_graph.items():
                    if neighbors and client_id in clients:
                        clients[client_id].simulate_topology_instruction(neighbors)
                        time.sleep(0.1)
                        
                # 5. 收集连接消息并更新服务端拓扑状态
                print(f"\n📊 Phase 3: Server Connection Tracking")
                
                # 模拟服务端接收连接消息
                for client_id, client in clients.items():
                    # 获取客户端发送的连接消息（模拟）
                    sent_messages = client.comm_manager.sent_messages
                    for message in sent_messages:
                        if message.msg_type == 'connect_msg':
                            # 服务端处理连接消息
                            server.callback_funcs_for_connection(message)
                            
                # 6. 检查拓扑构建状态
                print(f"\n🎯 Phase 4: Topology Completion Check")
                is_complete = server.topology_manager.is_topology_complete()
                
                if is_complete:
                    print("🎉 SUCCESS: Topology construction completed!")
                    server.topology_manager.print_topology_status()
                    return True
                else:
                    print("⚠️  PARTIAL: Topology construction incomplete")
                    server.topology_manager.print_topology_status()
                    return False
                    
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False


class MockCommManager:
    """模拟通信管理器"""
    def __init__(self):
        self.sent_messages = []
        
    def send(self, message):
        self.sent_messages.append(message)
        # logger.debug(f"MockComm: Sent {message.msg_type} from {message.sender} to {message.receiver}")


def main():
    print("🌐 END-TO-END TOPOLOGY CONSTRUCTION TEST")
    print("=" * 60)
    print("This test simulates:")
    print("  1. Server starts with topology configuration")
    print("  2. Multiple clients join the FL system")
    print("  3. Server computes and sends topology instructions")
    print("  4. Clients establish connections per instructions")
    print("  5. Server tracks topology construction progress")
    print("  6. Topology construction completes")
    
    # 运行测试
    mock_fl = MockDistributedFL()
    success = mock_fl.run_topology_test()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 END-TO-END TOPOLOGY TEST PASSED!")
        print("✅ Network topology construction works in simulated FL environment")
        print("✅ Ready for actual distributed FL deployment")
    else:
        print("❌ END-TO-END TOPOLOGY TEST FAILED!")
        print("   Some connections may have failed, but this could be normal in simulation")
        print("   Check the logs above for details")
        
    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)