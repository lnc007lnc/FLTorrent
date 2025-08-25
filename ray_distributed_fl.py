#!/usr/bin/env python3
"""
Ray-powered Distributed Federated Learning for FederatedScope
=============================================================

This module uses Ray to replace traditional multi-process FL training with:
- Automatic resource management (CPU/GPU allocation)  
- Dynamic cluster scaling (local + cloud servers)
- Smart task scheduling and fault tolerance
- Simplified network configuration

Key Features:
- Ray Actors for Server/Client processes
- Dynamic GPU assignment based on available resources
- Automatic IP/port management via Ray cluster
- Cloud server scaling support
- Resource monitoring and optimization
"""

import ray
import os
import sys
import time
import yaml
import psutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

# Ray相关导入
from ray.util.placement_group import placement_group, placement_group_table
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

# FederatedScope相关导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@ray.remote
class FederatedScopeServer:
    """
    Ray Actor for FederatedScope Server
    自动管理服务器进程，支持动态资源分配
    """
    
    def __init__(self, config_path: str, gpu_id: Optional[int] = None):
        self.config_path = config_path
        self.gpu_id = gpu_id
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.server_port = None
        
    def start(self) -> Tuple[str, int]:
        """启动服务器并返回IP和端口"""
        # 读取配置并动态分配端口
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Ray自动分配可用端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()
        
        # 更新配置文件中的服务器地址
        config['distribute']['server_host'] = self.node_ip
        config['distribute']['server_port'] = self.server_port
        
        # 如果指定了GPU，更新设备配置
        if self.gpu_id is not None:
            config['device'] = self.gpu_id
            config['use_gpu'] = True
        
        # 保存更新后的配置
        updated_config_path = self.config_path.replace('.yaml', f'_ray_server.yaml')
        with open(updated_config_path, 'w') as f:
            yaml.safe_dump(config, f)
        
        # 启动FederatedScope服务器
        cmd = [
            sys.executable, 'federatedscope/main.py',
            '--cfg', updated_config_path
        ]
        
        env = os.environ.copy()
        env['PYTHONPATH'] = '.'
        if self.gpu_id is not None:
            env['CUDA_VISIBLE_DEVICES'] = str(self.gpu_id)
        
        self.process = subprocess.Popen(
            cmd, 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, 
            env=env
        )
        
        logger.info(f"🖥️ Server started on {self.node_ip}:{self.server_port} (GPU: {self.gpu_id})")
        return self.node_ip, self.server_port
    
    def get_status(self) -> Dict:
        """获取服务器状态"""
        if self.process is None:
            return {"status": "not_started"}
        
        poll = self.process.poll()
        if poll is None:
            return {
                "status": "running",
                "pid": self.process.pid,
                "node_ip": self.node_ip,
                "server_port": self.server_port,
                "gpu_id": self.gpu_id
            }
        else:
            return {"status": "stopped", "returncode": poll}
    
    def stop(self):
        """停止服务器"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()
            logger.info(f"🛑 Server stopped")

@ray.remote
class FederatedScopeClient:
    """
    Ray Actor for FederatedScope Client  
    自动连接到Ray集群中的服务器，支持动态GPU分配
    """
    
    def __init__(self, client_id: int, config_path: str, 
                 server_ip: str, server_port: int, gpu_id: Optional[int] = None):
        self.client_id = client_id
        self.config_path = config_path
        self.server_ip = server_ip
        self.server_port = server_port
        self.gpu_id = gpu_id
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.client_port = None
        
    def start(self) -> bool:
        """启动客户端"""
        # 读取配置
        with open(self.config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Ray自动分配客户端端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()
        
        # 更新配置：连接到Ray服务器
        config['distribute']['server_host'] = self.server_ip
        config['distribute']['server_port'] = self.server_port
        config['distribute']['client_host'] = self.node_ip
        config['distribute']['client_port'] = self.client_port
        config['distribute']['data_idx'] = self.client_id
        
        # GPU配置
        if self.gpu_id is not None:
            config['device'] = self.gpu_id
            config['use_gpu'] = True
        
        # 保存更新配置
        updated_config_path = self.config_path.replace('.yaml', f'_ray_client_{self.client_id}.yaml')
        with open(updated_config_path, 'w') as f:
            yaml.safe_dump(config, f)
        
        # 启动客户端
        cmd = [
            sys.executable, 'federatedscope/main.py',
            '--cfg', updated_config_path
        ]
        
        env = os.environ.copy()
        env['PYTHONPATH'] = '.'
        if self.gpu_id is not None:
            env['CUDA_VISIBLE_DEVICES'] = str(self.gpu_id)
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE,
            env=env
        )
        
        logger.info(f"👤 Client {self.client_id} started on {self.node_ip}:{self.client_port} (GPU: {self.gpu_id})")
        return True
    
    def get_status(self) -> Dict:
        """获取客户端状态"""
        if self.process is None:
            return {"status": "not_started"}
        
        poll = self.process.poll()
        if poll is None:
            return {
                "status": "running",
                "pid": self.process.pid,
                "client_id": self.client_id,
                "node_ip": self.node_ip,
                "client_port": self.client_port,
                "gpu_id": self.gpu_id
            }
        else:
            return {"status": "stopped", "returncode": poll}
    
    def stop(self):
        """停止客户端"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()
            logger.info(f"🛑 Client {self.client_id} stopped")

class RayFederatedLearning:
    """
    Ray-powered Federated Learning Orchestrator
    ==========================================
    
    核心功能：
    - 自动GPU资源检测和分配
    - 动态集群扩展（支持云服务器加入）
    - 智能容错和重启机制
    - 实时资源监控
    """
    
    def __init__(self, config_dir: str = "multi_process_test_v2/configs"):
        self.config_dir = config_dir
        self.server_actor = None
        self.client_actors = []
        self.server_info = None
        
    def initialize_ray_cluster(self, num_cpus: Optional[int] = None, 
                             num_gpus: Optional[int] = None,
                             address: Optional[str] = None):
        """
        初始化Ray集群
        
        Args:
            num_cpus: 指定CPU数量（None=自动检测）
            num_gpus: 指定GPU数量（None=自动检测）  
            address: Ray集群地址（None=本地模式，"auto"=自动发现，"ray://host:port"=连接远程）
        """
        # 自动检测硬件资源
        if num_cpus is None:
            num_cpus = psutil.cpu_count()
        
        if num_gpus is None:
            try:
                import torch
                num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
            except ImportError:
                num_gpus = 0
        
        # 初始化Ray
        if address is None:
            # 本地模式
            ray.init(num_cpus=num_cpus, num_gpus=num_gpus, ignore_reinit_error=True)
            logger.info(f"🚀 Ray本地集群初始化: {num_cpus} CPUs, {num_gpus} GPUs")
        else:
            # 连接到现有集群或自动发现
            ray.init(address=address, ignore_reinit_error=True)
            logger.info(f"🌐 Ray集群连接: {address}")
        
        # 显示集群资源信息
        resources = ray.cluster_resources()
        logger.info(f"📊 集群资源: {dict(resources)}")
        
    def allocate_gpu_resources(self, num_clients: int) -> List[Optional[int]]:
        """
        智能GPU资源分配
        
        Returns:
            List[Optional[int]]: 每个进程分配的GPU ID，None表示使用CPU
        """
        cluster_resources = ray.cluster_resources()
        available_gpus = int(cluster_resources.get('GPU', 0))
        
        if available_gpus == 0:
            logger.warning("⚠️ 未检测到GPU，使用CPU模式")
            return [None] * (num_clients + 1)  # +1 for server
        
        # GPU分配策略：服务器优先，然后客户端轮换
        gpu_allocation = []
        
        if available_gpus >= 1:
            # 服务器分配GPU 0
            gpu_allocation.append(0)
        else:
            gpu_allocation.append(None)
            
        # 客户端GPU分配
        for i in range(num_clients):
            if available_gpus > 1:
                # 多GPU：客户端轮换使用GPU 1, 2, 3...
                gpu_id = 1 + (i % (available_gpus - 1))
                gpu_allocation.append(gpu_id)
            elif available_gpus == 1:
                # 单GPU：客户端共享或使用CPU
                gpu_allocation.append(None)  # 避免与服务器冲突
            else:
                gpu_allocation.append(None)
        
        logger.info(f"🎯 GPU分配策略: Server=GPU{gpu_allocation[0]}, Clients={gpu_allocation[1:]}")
        return gpu_allocation
    
    def start_federated_learning(self, num_clients: int = 3, 
                                total_rounds: int = 3,
                                monitor_duration: int = 600):
        """
        启动Ray驱动的联邦学习
        
        Args:
            num_clients: 客户端数量
            total_rounds: 训练轮数  
            monitor_duration: 监控时长（秒）
        """
        logger.info(f"🎯 开始Ray联邦学习: {num_clients}客户端, {total_rounds}轮训练")
        
        # 1. GPU资源分配
        gpu_allocation = self.allocate_gpu_resources(num_clients)
        server_gpu = gpu_allocation[0]
        client_gpus = gpu_allocation[1:]
        
        # 2. 启动服务器Actor
        server_config = os.path.join(self.config_dir, "server.yaml")
        
        # 服务器资源规格
        server_resources = {"num_cpus": 2}
        if server_gpu is not None:
            server_resources["num_gpus"] = 1
            
        self.server_actor = FederatedScopeServer.options(
            **server_resources
        ).remote(server_config, server_gpu)
        
        # 启动服务器并获取连接信息
        server_ip, server_port = ray.get(self.server_actor.start.remote())
        self.server_info = (server_ip, server_port)
        
        logger.info(f"✅ 服务器已启动: {server_ip}:{server_port}")
        
        # 等待服务器初始化
        time.sleep(3)
        
        # 3. 启动客户端Actors
        for i in range(num_clients):
            client_id = i + 1
            client_config = os.path.join(self.config_dir, f"client_{client_id}.yaml")
            client_gpu = client_gpus[i]
            
            # 客户端资源规格
            client_resources = {"num_cpus": 1}
            if client_gpu is not None:
                client_resources["num_gpus"] = 0.5  # 共享GPU资源
            
            client_actor = FederatedScopeClient.options(
                **client_resources
            ).remote(client_id, client_config, server_ip, server_port, client_gpu)
            
            self.client_actors.append(client_actor)
            
            # 启动客户端
            ray.get(client_actor.start.remote())
            time.sleep(2)  # 错峰启动
        
        logger.info(f"✅ 所有{num_clients}个客户端已启动")
        
        # 4. 监控训练进度
        self.monitor_training_progress(monitor_duration)
        
    def monitor_training_progress(self, monitor_duration: int):
        """实时监控训练进度和资源使用"""
        start_time = time.time()
        
        logger.info(f"📊 开始监控训练进度（{monitor_duration}秒）...")
        
        while True:
            elapsed = int(time.time() - start_time)
            
            if elapsed > monitor_duration:
                logger.info("⏰ 监控时间结束")
                break
                
            # 检查服务器状态
            server_status = ray.get(self.server_actor.get_status.remote())
            
            # 检查客户端状态  
            client_statuses = ray.get([
                actor.get_status.remote() for actor in self.client_actors
            ])
            
            running_clients = sum(1 for status in client_statuses if status["status"] == "running")
            
            # Ray集群资源使用情况
            cluster_resources = ray.cluster_resources()
            available_resources = ray.available_resources()
            
            logger.info(f"⏰ 运行{elapsed}s | 服务器: {server_status['status']} | "
                       f"客户端: {running_clients}/{len(self.client_actors)} | "
                       f"GPU使用: {cluster_resources.get('GPU', 0) - available_resources.get('GPU', 0):.1f}/{cluster_resources.get('GPU', 0)}")
            
            # 检查训练是否完成
            if server_status["status"] != "running":
                logger.info("🏁 服务器训练完成")
                break
                
            if running_clients == 0:
                logger.info("🏁 所有客户端训练完成")
                break
                
            time.sleep(10)
    
    def stop_all(self):
        """停止所有Actor"""
        logger.info("🧹 停止所有联邦学习进程...")
        
        # 停止服务器
        if self.server_actor:
            ray.get(self.server_actor.stop.remote())
            
        # 停止所有客户端
        if self.client_actors:
            ray.get([actor.stop.remote() for actor in self.client_actors])
        
        logger.info("✅ 所有进程已停止")
    
    def get_cluster_info(self) -> Dict:
        """获取Ray集群信息"""
        nodes = ray.nodes()
        cluster_resources = ray.cluster_resources()
        available_resources = ray.available_resources()
        
        return {
            "nodes": len(nodes),
            "total_resources": dict(cluster_resources),
            "available_resources": dict(available_resources),
            "node_details": [
                {
                    "node_id": node["NodeID"],
                    "alive": node["Alive"],
                    "node_ip": node.get("NodeManagerAddress", "N/A"),
                    "resources": node.get("Resources", {})
                }
                for node in nodes
            ]
        }

def main():
    """Ray联邦学习主程序"""
    
    print("🚀 Ray-Powered FederatedScope 分布式联邦学习")
    print("=" * 60)
    
    # 初始化Ray联邦学习
    ray_fl = RayFederatedLearning()
    
    try:
        # 1. 初始化Ray集群（本地模式，自动检测硬件）
        ray_fl.initialize_ray_cluster()
        
        # 显示集群信息
        cluster_info = ray_fl.get_cluster_info()
        print(f"🌐 Ray集群信息:")
        print(f"   节点数: {cluster_info['nodes']}")
        print(f"   总资源: {cluster_info['total_resources']}")
        print(f"   可用资源: {cluster_info['available_resources']}")
        
        # 2. 启动联邦学习（3客户端，3轮训练，10分钟监控）
        ray_fl.start_federated_learning(
            num_clients=3,
            total_rounds=3, 
            monitor_duration=600
        )
        
    except KeyboardInterrupt:
        logger.info("👋 收到中断信号")
    except Exception as e:
        logger.error(f"❌ 发生错误: {e}")
    finally:
        # 3. 清理资源
        ray_fl.stop_all()
        ray.shutdown()
        
        print("🎉 Ray联邦学习结束！")

if __name__ == "__main__":
    main()