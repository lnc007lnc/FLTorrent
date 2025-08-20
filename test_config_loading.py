#!/usr/bin/env python3
"""
测试拓扑配置是否能正确加载
"""

import sys
import os
sys.path.insert(0, os.getcwd())

# Set protobuf implementation
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

def test_topology_config_loading():
    """测试拓扑配置加载"""
    print("🔧 Testing topology configuration loading...")
    
    try:
        # 导入FederatedScope配置系统
        from federatedscope.core.configs.config import global_cfg, CN
        
        print("✅ Successfully imported configuration system")
        
        # 创建配置副本
        cfg = global_cfg.clone()
        
        # 检查是否有topology配置
        if hasattr(cfg, 'topology'):
            print("✅ Topology configuration is available in global config")
            print(f"   Default topology.use: {cfg.topology.use}")
            print(f"   Default topology.type: {cfg.topology.type}")
            print(f"   Default topology.timeout: {cfg.topology.timeout}")
        else:
            print("❌ Topology configuration not found in global config")
            print("   Available config sections:", list(cfg.keys()))
            return False
            
        # 尝试从YAML文件加载配置
        print("\n🧪 Testing YAML config loading...")
        
        # 测试服务端配置
        cfg_server = global_cfg.clone()
        cfg_server.merge_from_file('topology_test_server.yaml')
        
        print("✅ Server config loaded successfully")
        print(f"   topology.use: {cfg_server.topology.use}")
        print(f"   topology.type: {cfg_server.topology.type}")
        print(f"   topology.verbose: {cfg_server.topology.verbose}")
        print(f"   federate.client_num: {cfg_server.federate.client_num}")
        print(f"   distribute.role: {cfg_server.distribute.role}")
        
        # 测试客户端配置
        cfg_client = global_cfg.clone()
        cfg_client.merge_from_file('topology_test_client.yaml')
        
        print("✅ Client config loaded successfully")
        print(f"   topology.use: {cfg_client.topology.use}")
        print(f"   distribute.role: {cfg_client.distribute.role}")
        
        # 验证配置有效性
        print("\n🔍 Validating configuration...")
        
        # 冻结配置以触发验证
        cfg_server.freeze(inform=False, save=False)
        print("✅ Server configuration validation passed")
        
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_topology_manager_import():
    """测试拓扑管理器导入"""
    print("\n🔧 Testing topology manager import...")
    
    try:
        from federatedscope.core.topology_manager import TopologyManager, TopologyType
        print("✅ TopologyManager imported successfully")
        
        # 测试创建拓扑管理器
        manager = TopologyManager(topology_type='star', client_list=[1, 2, 3])
        topology = manager.compute_topology()
        
        print(f"✅ Topology computation works: {topology}")
        
        return True
        
    except Exception as e:
        print(f"❌ Topology manager test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_connection_monitor_import():
    """测试连接监控导入"""
    print("\n🔧 Testing connection monitor import...")
    
    try:
        from federatedscope.core.connection_monitor import ConnectionMonitor
        from federatedscope.core.workers.connection_handler_mixin import ConnectionHandlerMixin
        
        print("✅ Connection monitoring components imported successfully")
        return True
        
    except Exception as e:
        print(f"❌ Connection monitor test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🧪 TOPOLOGY CONFIGURATION LOADING TESTS")
    print("=" * 50)
    
    success = True
    
    # 运行所有测试
    success &= test_topology_config_loading()
    success &= test_topology_manager_import()
    success &= test_connection_monitor_import()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 ALL CONFIGURATION TESTS PASSED!")
        print("✅ Ready to run actual distributed FL with topology construction")
    else:
        print("❌ SOME TESTS FAILED!")
        print("   Please fix the issues before running distributed FL")
        sys.exit(1)