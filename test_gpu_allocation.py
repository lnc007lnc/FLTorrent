#!/usr/bin/env python3
"""
GPU资源分配测试脚本
验证Ray是否正确检测硬件资源并按需分配
"""

import ray
import psutil
import time
import logging
from typing import Optional

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def detect_system_resources():
    """检测系统硬件资源"""
    # CPU资源
    num_cpus = psutil.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)
    
    # GPU资源检测
    num_gpus = 0
    gpu_info = "无GPU"
    try:
        import torch
        if torch.cuda.is_available():
            num_gpus = torch.cuda.device_count()
            gpu_names = [torch.cuda.get_device_name(i) for i in range(num_gpus)]
            gpu_info = f"{num_gpus}个GPU: {', '.join(gpu_names)}"
    except ImportError:
        pass
    
    return {
        "cpus": num_cpus,
        "memory_gb": memory_gb,
        "gpus": num_gpus,
        "gpu_info": gpu_info
    }

@ray.remote
class GPUTestActor:
    """GPU测试Actor"""
    
    def __init__(self, actor_id: int, role: str):
        self.actor_id = actor_id
        self.role = role
        
    def test_gpu_access(self):
        """测试GPU访问"""
        import os
        result = {
            "actor_id": self.actor_id,
            "role": self.role,
            "node_ip": ray.util.get_node_ip_address(),
            "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES", "未设置"),
            "gpu_available": False,
            "gpu_count": 0,
            "gpu_devices": []
        }
        
        try:
            import torch
            if torch.cuda.is_available():
                result["gpu_available"] = True
                result["gpu_count"] = torch.cuda.device_count()
                result["gpu_devices"] = [
                    torch.cuda.get_device_name(i) for i in range(result["gpu_count"])
                ]
        except ImportError:
            result["error"] = "PyTorch未安装"
        except Exception as e:
            result["error"] = str(e)
            
        return result

def test_ray_gpu_allocation():
    """测试Ray GPU资源分配"""
    
    # 1. 检测系统资源
    system_resources = detect_system_resources()
    logger.info(f"🖥️ 系统资源: CPU={system_resources['cpus']}, 内存={system_resources['memory_gb']:.1f}GB, {system_resources['gpu_info']}")
    
    # 2. 初始化Ray
    ray_config = {
        "num_cpus": system_resources["cpus"],
        "num_gpus": system_resources["gpus"],
        "ignore_reinit_error": True
    }
    
    ray.init(**ray_config)
    
    # 3. 查看Ray集群资源
    cluster_resources = ray.cluster_resources()
    available_resources = ray.available_resources()
    
    logger.info(f"☁️ Ray集群资源: {dict(cluster_resources)}")
    logger.info(f"💾 可用资源: {dict(available_resources)}")
    
    # 4. 模拟FederatedScope的GPU分配策略
    total_gpus = int(cluster_resources.get('GPU', 0))
    client_num = 5
    
    # 服务器：强制使用CPU
    server_resources = {"num_cpus": 2}
    logger.info(f"🖥️ 服务器资源配置: {server_resources}")
    
    # 客户端：根据设备类型分配GPU
    device_types = ["smartphone_high", "smartphone_low", "raspberry_pi", "iot_device", "edge_server"]
    client_configs = []
    
    gpu_assigned = 0
    for i in range(client_num):
        client_id = i + 1
        device_type = device_types[i % len(device_types)]
        
        # 基础资源配置
        if device_type == "smartphone_high" or device_type == "edge_server":
            client_resources = {"num_cpus": 1.0}
            # 分配GPU（如果可用）
            if gpu_assigned < total_gpus:
                client_resources["num_gpus"] = 1
                gpu_assigned += 1
                gpu_status = f"GPU-{gpu_assigned-1}"
            else:
                gpu_status = "CPU"
        else:
            client_resources = {"num_cpus": 0.5}
            gpu_status = "CPU"
            
        client_configs.append({
            "client_id": client_id,
            "device_type": device_type,
            "resources": client_resources,
            "gpu_status": gpu_status
        })
        
        logger.info(f"📱 客户端{client_id} ({device_type}): {client_resources}, 使用{gpu_status}")
    
    # 5. 创建测试Actors
    test_actors = []
    
    # 创建服务器Actor
    try:
        server_actor = GPUTestActor.options(**server_resources).remote(0, "server")
        test_actors.append(("server", server_actor))
        logger.info("✅ 服务器Actor创建成功")
    except Exception as e:
        logger.error(f"❌ 服务器Actor创建失败: {e}")
    
    # 创建客户端Actors
    for config in client_configs:
        try:
            client_actor = GPUTestActor.options(**config["resources"]).remote(
                config["client_id"], f"client_{config['device_type']}"
            )
            test_actors.append((f"client_{config['client_id']}", client_actor))
            logger.info(f"✅ 客户端{config['client_id']} Actor创建成功")
        except Exception as e:
            logger.error(f"❌ 客户端{config['client_id']} Actor创建失败: {e}")
    
    # 6. 测试GPU访问
    logger.info("\n🧪 开始GPU访问测试...")
    time.sleep(2)  # 等待Actors初始化
    
    gpu_test_futures = [actor.test_gpu_access.remote() for name, actor in test_actors]
    gpu_test_results = ray.get(gpu_test_futures)
    
    # 7. 分析结果
    logger.info("\n📊 GPU访问测试结果:")
    server_gpu_count = 0
    client_gpu_count = 0
    
    for result in gpu_test_results:
        role = result["role"]
        gpu_info = f"GPU数量: {result['gpu_count']}" if result['gpu_available'] else "无GPU访问"
        
        if "server" in role:
            server_gpu_count += result['gpu_count']
            logger.info(f"🖥️ {role}: {gpu_info}")
        else:
            client_gpu_count += result['gpu_count']
            logger.info(f"📱 {role}: {gpu_info}")
    
    # 8. 验证分配策略
    logger.info(f"\n🎯 GPU资源分配验证:")
    logger.info(f"   服务器GPU使用: {server_gpu_count} (预期: 0)")
    logger.info(f"   客户端GPU使用: {client_gpu_count} (预期: {min(total_gpus, gpu_assigned)})")
    
    allocation_correct = (server_gpu_count == 0) and (client_gpu_count <= total_gpus)
    status = "✅ 正确" if allocation_correct else "❌ 错误"
    
    logger.info(f"   分配策略: {status}")
    
    return {
        "system_resources": system_resources,
        "ray_resources": dict(cluster_resources),
        "server_gpu_count": server_gpu_count,
        "client_gpu_count": client_gpu_count,
        "allocation_correct": allocation_correct,
        "detailed_results": gpu_test_results
    }

def main():
    """主测试函数"""
    logger.info("🚀 开始GPU资源分配测试")
    
    try:
        results = test_ray_gpu_allocation()
        
        logger.info("\n📋 测试总结:")
        logger.info(f"   系统GPU数量: {results['system_resources']['gpus']}")
        logger.info(f"   Ray检测GPU数量: {results['ray_resources'].get('GPU', 0)}")
        logger.info(f"   服务器GPU使用: {results['server_gpu_count']}")
        logger.info(f"   客户端GPU使用: {results['client_gpu_count']}")
        logger.info(f"   分配策略正确性: {'✅' if results['allocation_correct'] else '❌'}")
        
        if results['allocation_correct']:
            logger.info("\n🎉 GPU资源分配测试通过！")
        else:
            logger.info("\n⚠️ GPU资源分配需要调优")
            
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        ray.shutdown()
        logger.info("🧹 Ray集群已关闭")

if __name__ == "__main__":
    main()