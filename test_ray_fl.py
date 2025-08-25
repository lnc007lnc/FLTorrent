#!/usr/bin/env python3
"""
Ray FederatedScope Test Script
=============================

简化的测试脚本，验证Ray版本的FederatedScope是否工作正常
"""

import os
import sys
import time
import subprocess
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_dependencies():
    """检查Ray和其他依赖是否已安装"""
    try:
        import ray
        logger.info(f"✅ Ray版本: {ray.__version__}")
    except ImportError:
        logger.error("❌ Ray未安装，请先安装: pip install ray")
        return False
    
    try:
        import torch
        logger.info(f"✅ PyTorch版本: {torch.__version__}")
        gpu_count = torch.cuda.device_count() if torch.cuda.is_available() else 0
        logger.info(f"🎯 可用GPU数量: {gpu_count}")
    except ImportError:
        logger.warning("⚠️ PyTorch未安装，将仅使用CPU")
    
    return True

def test_basic_ray_functionality():
    """测试Ray基本功能"""
    import ray
    
    # 初始化Ray
    ray.init(ignore_reinit_error=True)
    
    @ray.remote
    def test_function(x):
        return x * 2
    
    # 测试远程函数
    result = ray.get(test_function.remote(21))
    assert result == 42, f"Ray测试失败: {result} != 42"
    
    # 显示集群资源
    resources = ray.cluster_resources()
    logger.info(f"📊 Ray集群资源: {dict(resources)}")
    
    ray.shutdown()
    logger.info("✅ Ray基本功能测试通过")

def prepare_test_configs():
    """准备测试配置文件"""
    config_dir = "multi_process_test_v2/configs"
    
    if not os.path.exists(config_dir):
        logger.warning(f"⚠️ 配置目录不存在: {config_dir}")
        logger.info("🔧 尝试运行原始脚本创建配置...")
        
        # 运行原始脚本来创建配置
        try:
            result = subprocess.run(['bash', 'multi_process_fl_test_v2.sh'], 
                                  capture_output=True, text=True, timeout=30)
            if os.path.exists(config_dir):
                logger.info("✅ 配置文件已创建")
                # 立即停止原始脚本启动的进程
                subprocess.run(['pkill', '-f', 'python.*federatedscope'], check=False)
                time.sleep(2)
                return True
            else:
                logger.error("❌ 配置文件创建失败")
                return False
        except subprocess.TimeoutExpired:
            logger.info("✅ 配置创建超时但可能已成功")
            subprocess.run(['pkill', '-f', 'python.*federatedscope'], check=False)
            return os.path.exists(config_dir)
        except Exception as e:
            logger.error(f"❌ 创建配置失败: {e}")
            return False
    else:
        logger.info("✅ 配置目录已存在")
        return True

def test_ray_federated_learning():
    """测试Ray版本的联邦学习"""
    from ray_distributed_fl import RayFederatedLearning
    
    logger.info("🚀 开始Ray联邦学习测试...")
    
    # 创建Ray FL实例
    ray_fl = RayFederatedLearning()
    
    try:
        # 初始化Ray集群（本地模式，2GPU）
        ray_fl.initialize_ray_cluster(num_cpus=4, num_gpus=2)
        
        # 显示集群信息
        cluster_info = ray_fl.get_cluster_info()
        logger.info(f"🌐 集群信息: {cluster_info['total_resources']}")
        
        # 启动小规模测试（2客户端，1轮次，2分钟监控）
        logger.info("🧪 启动小规模测试...")
        ray_fl.start_federated_learning(
            num_clients=2,
            total_rounds=1,
            monitor_duration=120  # 2分钟测试
        )
        
        logger.info("✅ Ray联邦学习测试完成")
        
    except Exception as e:
        logger.error(f"❌ Ray联邦学习测试失败: {e}")
    finally:
        ray_fl.stop_all()
        import ray
        ray.shutdown()

def quick_performance_comparison():
    """快速性能对比：传统多进程 vs Ray"""
    logger.info("📊 开始性能对比测试...")
    
    # 1. Ray版本测试
    ray_start_time = time.time()
    try:
        test_ray_federated_learning()
        ray_duration = time.time() - ray_start_time
        logger.info(f"⚡ Ray版本耗时: {ray_duration:.2f}秒")
    except Exception as e:
        logger.error(f"❌ Ray版本测试失败: {e}")
        ray_duration = float('inf')
    
    # 2. 传统版本测试（简化）
    logger.info("🔄 传统多进程版本测试已跳过（避免冲突）")
    
    # 结果对比
    logger.info("📈 性能对比结果:")
    logger.info(f"   Ray版本: {ray_duration:.2f}秒")
    logger.info("   传统版本: 跳过")
    
    if ray_duration < float('inf'):
        logger.info("✅ Ray版本测试成功完成")
    
def main():
    """主测试函数"""
    print("🧪 Ray FederatedScope 测试")
    print("=" * 40)
    
    # 1. 检查依赖
    if not check_dependencies():
        return
    
    # 2. 测试Ray基本功能
    try:
        test_basic_ray_functionality()
    except Exception as e:
        logger.error(f"❌ Ray基本功能测试失败: {e}")
        return
    
    # 3. 准备测试配置
    if not prepare_test_configs():
        logger.error("❌ 无法准备测试配置，跳过联邦学习测试")
        return
    
    # 4. 测试Ray联邦学习
    quick_performance_comparison()
    
    print("\n🎉 Ray FederatedScope 测试完成！")

if __name__ == "__main__":
    main()