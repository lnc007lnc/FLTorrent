#!/usr/bin/env python3
"""
简化版Ray联邦学习测试脚本
专注测试Ray和GPU资源分配，不使用Docker
"""

import ray
import os
import sys
import time
import yaml
import logging
import psutil
from typing import Dict, List, Optional, Any

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 简化配置
class SimpleConfig:
    CLIENT_NUM = 3
    TOTAL_ROUNDS = 2
    MONITOR_DURATION = 60
    OUTPUT_DIR = "ray_simple_output"
    
CONFIG = SimpleConfig()

@ray.remote
class SimpleServer:
    """简化的服务器Actor（CPU模式）"""
    
    def __init__(self, server_id: int):
        self.server_id = server_id
        self.node_ip = ray.util.get_node_ip_address()
        
    def start(self):
        """模拟服务器启动"""
        logger.info(f"🖥️ 服务器{self.server_id}启动 - CPU模式")
        time.sleep(2)  # 模拟初始化时间
        return {"status": "started", "node_ip": self.node_ip}
        
    def get_status(self):
        """获取服务器状态"""
        return {
            "status": "running",
            "server_id": self.server_id,
            "node_ip": self.node_ip,
            "mode": "CPU"
        }
    
    def stop(self):
        """停止服务器"""
        logger.info(f"🖥️ 服务器{self.server_id}停止")

@ray.remote
class SimpleClient:
    """简化的客户端Actor"""
    
    def __init__(self, client_id: int, gpu_assigned: bool):
        self.client_id = client_id
        self.gpu_assigned = gpu_assigned
        self.node_ip = ray.util.get_node_ip_address()
        
    def start(self):
        """模拟客户端启动"""
        mode = "GPU" if self.gpu_assigned else "CPU"
        logger.info(f"📱 客户端{self.client_id}启动 - {mode}模式")
        
        # 检查GPU访问
        gpu_info = self._check_gpu_access()
        time.sleep(1)  # 模拟初始化时间
        
        return {
            "status": "started", 
            "client_id": self.client_id,
            "mode": mode,
            "gpu_info": gpu_info
        }
    
    def _check_gpu_access(self):
        """检查GPU访问情况"""
        try:
            import torch
            if torch.cuda.is_available():
                gpu_count = torch.cuda.device_count()
                return {
                    "available": True,
                    "count": gpu_count,
                    "current_device": torch.cuda.current_device() if gpu_count > 0 else None
                }
        except:
            pass
        return {"available": False}
        
    def get_status(self):
        """获取客户端状态"""
        return {
            "status": "running",
            "client_id": self.client_id,
            "node_ip": self.node_ip,
            "gpu_assigned": self.gpu_assigned
        }
    
    def stop(self):
        """停止客户端"""
        logger.info(f"📱 客户端{self.client_id}停止")

class SimpleRayFL:
    """简化的Ray联邦学习控制器"""
    
    def __init__(self):
        self.server_actor = None
        self.client_actors = []
        
    def initialize_ray(self):
        """初始化Ray集群"""
        # 检测硬件资源
        num_cpus = psutil.cpu_count()
        num_gpus = 0
        
        try:
            import torch
            if torch.cuda.is_available():
                num_gpus = torch.cuda.device_count()
        except ImportError:
            pass
        
        # 初始化Ray
        ray.init(
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            ignore_reinit_error=True,
            include_dashboard=True,
            dashboard_host="0.0.0.0",
            dashboard_port=8265
        )
        
        resources = ray.cluster_resources()
        logger.info(f"🚀 Ray集群资源: {dict(resources)}")
        
        return num_gpus
    
    def allocate_gpu_resources(self, total_gpus: int):
        """分配GPU资源"""
        # 服务器：强制CPU
        server_gpu = None
        
        # 客户端：按需分配GPU
        client_gpus = []
        gpu_assigned = 0
        
        for i in range(CONFIG.CLIENT_NUM):
            # 前面的客户端优先分配GPU
            if gpu_assigned < total_gpus:
                client_gpus.append(gpu_assigned)
                gpu_assigned += 1
            else:
                client_gpus.append(None)
        
        logger.info(f"🎯 GPU分配 - 服务器:CPU, 客户端:{client_gpus}")
        return server_gpu, client_gpus
    
    def start_federated_learning(self):
        """启动联邦学习"""
        logger.info(f"🚀 启动简化版Ray联邦学习")
        logger.info(f"📊 配置: {CONFIG.CLIENT_NUM}个客户端, {CONFIG.TOTAL_ROUNDS}轮训练")
        
        # 初始化Ray
        total_gpus = self.initialize_ray()
        
        # GPU资源分配
        server_gpu, client_gpus = self.allocate_gpu_resources(total_gpus)
        
        # 启动服务器（CPU模式）
        server_resources = {"num_cpus": 1}
        self.server_actor = SimpleServer.options(**server_resources).remote(0)
        
        server_result = ray.get(self.server_actor.start.remote())
        logger.info(f"✅ 服务器启动结果: {server_result}")
        
        # 启动客户端
        successful_clients = 0
        for i in range(CONFIG.CLIENT_NUM):
            client_id = i + 1
            gpu_assigned = client_gpus[i] is not None
            
            # 客户端资源配置
            client_resources = {"num_cpus": 0.5}
            if gpu_assigned:
                client_resources["num_gpus"] = 1
            
            try:
                client_actor = SimpleClient.options(**client_resources).remote(
                    client_id, gpu_assigned
                )
                self.client_actors.append(client_actor)
                
                client_result = ray.get(client_actor.start.remote())
                logger.info(f"✅ 客户端{client_id}启动结果: {client_result}")
                successful_clients += 1
                
            except Exception as e:
                logger.error(f"❌ 客户端{client_id}启动失败: {e}")
        
        logger.info(f"✅ 成功启动 {successful_clients}/{CONFIG.CLIENT_NUM} 个客户端")
        
        # 监控训练
        self.monitor_training()
        
    def monitor_training(self):
        """监控训练进度"""
        logger.info(f"📊 开始监控训练（{CONFIG.MONITOR_DURATION}秒）...")
        
        start_time = time.time()
        
        while time.time() - start_time < CONFIG.MONITOR_DURATION:
            elapsed = int(time.time() - start_time)
            
            # 检查状态
            server_status = ray.get(self.server_actor.get_status.remote())
            client_statuses = ray.get([
                actor.get_status.remote() for actor in self.client_actors
            ])
            
            running_clients = len([s for s in client_statuses if s["status"] == "running"])
            gpu_clients = len([s for s in client_statuses if s["gpu_assigned"]])
            cpu_clients = running_clients - gpu_clients
            
            # 资源使用情况
            cluster_resources = ray.cluster_resources()
            available_resources = ray.available_resources()
            
            logger.info(
                f"⏰ {elapsed}s | 服务器:运行中 | "
                f"客户端: GPU={gpu_clients}, CPU={cpu_clients} | "
                f"总资源: CPU={cluster_resources.get('CPU', 0):.1f}, "
                f"GPU={cluster_resources.get('GPU', 0)}"
            )
            
            time.sleep(10)
    
    def stop_all(self):
        """停止所有Actor"""
        logger.info("🛑 停止所有Actor...")
        
        try:
            if self.server_actor:
                ray.get(self.server_actor.stop.remote())
            
            if self.client_actors:
                stop_futures = [actor.stop.remote() for actor in self.client_actors]
                ray.get(stop_futures)
        except Exception as e:
            logger.warning(f"⚠️ Actor停止警告: {e}")
        
        logger.info("✅ 所有Actor已停止")

def main():
    """主函数"""
    print(f"""
==============================================================
🚀 简化版Ray联邦学习测试
==============================================================
📊 配置:
   • 客户端数量: {CONFIG.CLIENT_NUM}
   • 训练轮数: {CONFIG.TOTAL_ROUNDS}
   • 监控时长: {CONFIG.MONITOR_DURATION}s

💡 目标: 验证Ray和GPU资源分配功能
📝 模式: 纯CPU/GPU模拟，无Docker，无FederatedScope依赖
==============================================================
""")
    
    ray_fl = SimpleRayFL()
    
    try:
        ray_fl.start_federated_learning()
        print("\n🎉 简化版Ray联邦学习测试完成！")
        print("🌐 Ray Dashboard: http://127.0.0.1:8265")
        
    except KeyboardInterrupt:
        print("\n👋 收到中断信号，正在清理...")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        ray_fl.stop_all()
        ray.shutdown()
        print("🧹 资源清理完成")

if __name__ == "__main__":
    main()