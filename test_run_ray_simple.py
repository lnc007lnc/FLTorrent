#!/usr/bin/env python3
"""
简化版run_ray测试脚本 - 验证基本功能
"""

import os
import sys
import time
import logging
from pathlib import Path

# 添加到系统路径
sys.path.insert(0, str(Path(__file__).parent))

from run_ray import FLConfig, CONFIG, RayV2FederatedLearning

def test_basic_functionality():
    """测试基本功能"""
    
    # 设置简化配置
    CONFIG.CLIENT_NUM = 2
    CONFIG.TOTAL_ROUNDS = 2
    CONFIG.MONITOR_DURATION = 30
    CONFIG.USE_DOCKER = False  # 强制使用非Docker模式
    CONFIG.ENABLE_NETWORK_SIMULATION = False
    
    print(f"🧪 测试配置: {CONFIG.CLIENT_NUM}个客户端, {CONFIG.TOTAL_ROUNDS}轮训练, 监控{CONFIG.MONITOR_DURATION}秒")
    print(f"🐳 Docker模式: {'启用' if CONFIG.USE_DOCKER else '禁用'}")
    print(f"🌐 网络仿真: {'启用' if CONFIG.ENABLE_NETWORK_SIMULATION else '禁用'}")
    
    # 创建Ray FL实例
    ray_fl = RayV2FederatedLearning()
    
    try:
        # 1. 测试环境清理
        print("\n🧹 测试环境清理...")
        ray_fl.cleanup_environment()
        print("✅ 环境清理成功")
        
        # 2. 测试Ray集群初始化  
        print("\n🚀 测试Ray集群初始化...")
        ray_fl.initialize_ray_cluster()
        print("✅ Ray集群初始化成功")
        
        # 3. 测试GPU资源分配
        print("\n🎯 测试GPU资源分配...")
        gpu_allocation = ray_fl.allocate_gpu_resources()
        print(f"✅ GPU分配结果: {gpu_allocation}")
        
        # 4. 测试配置生成
        print("\n📋 测试配置生成...")
        base_config = ray_fl.generate_base_config()
        print("✅ 配置生成成功")
        
        # 5. 测试设备分配
        print("\n📱 测试设备分配...")
        device_assignments = ray_fl._create_diverse_device_fleet(CONFIG.CLIENT_NUM)
        for i, device in enumerate(device_assignments):
            print(f"   设备{i+1}: {device.device_type} ({device.cpu_limit} CPU, {device.memory_limit} 内存)")
        print("✅ 设备分配成功")
        
        print("\n🎉 基本功能测试通过!")
        return True
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # 清理Ray
        try:
            import ray
            ray.shutdown()
            print("🧹 Ray集群已关闭")
        except:
            pass

def test_docker_availability():
    """测试Docker可用性"""
    print("\n🐳 测试Docker可用性...")
    
    from run_ray import DockerManager
    
    docker_manager = DockerManager()
    
    if docker_manager.docker_available:
        print("✅ Docker可用")
        
        # 测试环境设置
        if docker_manager.setup_docker_environment():
            print("✅ Docker环境设置成功")
        else:
            print("❌ Docker环境设置失败")
    else:
        print("⚠️  Docker不可用，将使用CPU后备模式")

def main():
    """主测试函数"""
    print("🚀 开始run_ray简化测试\n")
    
    # 设置日志级别
    logging.basicConfig(level=logging.INFO)
    
    try:
        # 测试Docker可用性
        test_docker_availability()
        
        # 测试基本功能
        success = test_basic_functionality()
        
        if success:
            print("\n🎉 所有测试通过! run_ray.py基本功能正常")
        else:
            print("\n❌ 测试失败，需要进一步调试")
            
    except KeyboardInterrupt:
        print("\n👋 测试被中断")
    except Exception as e:
        print(f"\n💥 意外错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()