#!/usr/bin/env python3
"""
Ray-Powered FederatedScope V2 Script
===================================

一键启动分布式联邦学习，完全替代传统shell脚本
- 自动GPU资源管理和分配
- 动态IP端口分配
- 实时监控和日志
- 支持云服务器扩展

直接运行即可启动完整的FL系统
"""

import ray
import os
import sys
import time
import yaml
import logging
import psutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

# ============================================================================
# 🔧 配置区域 - 所有设置集中在此处，方便修改
# ============================================================================

@dataclass
class FLConfig:
    """联邦学习配置参数"""
    
    # === 基础设置 ===
    CLIENT_NUM: int = 3                    # 客户端数量
    TOTAL_ROUNDS: int = 3                  # 训练轮数
    CHUNK_NUM: int = 10                    # 每个客户端模型chunk数
    IMPORTANCE_METHOD: str = "snip"         # chunk重要度方法: magnitude, l2_norm, snip, fisher
    
    # === 数据集设置 ===
    DATASET: str = "CIFAR10@torchvision"   # 数据集
    BATCH_SIZE: int = 32                   # 批处理大小
    DATA_SPLIT_ALPHA: float = 0.1          # LDA数据划分参数
    
    # === 模型设置 ===
    MODEL_TYPE: str = "convnet2"          # 模型类型
    MODEL_HIDDEN: int = 512               # 隐藏层大小
    MODEL_OUT_CHANNELS: int = 10          # 输出通道数
    MODEL_DROPOUT: float = 0.0            # Dropout率
    
    # === 训练设置 ===
    LOCAL_UPDATE_STEPS: int = 5           # 本地训练步数
    LEARNING_RATE: float = 0.01           # 学习率
    OPTIMIZER: str = "SGD"                # 优化器
    WEIGHT_DECAY: float = 0.0001          # 权重衰减
    GRAD_CLIP: float = 5.0                # 梯度裁剪
    
    # === BitTorrent设置 ===
    BITTORRENT_TIMEOUT: float = 600.0     # BitTorrent超时
    BT_CHUNK_SELECTION: str = "rarest_first"  # chunk选择策略
    BT_MIN_COMPLETION_RATIO: float = 0.8   # 最小完成比率
    
    # === 拓扑设置 ===
    TOPOLOGY_TYPE: str = "star"           # 拓扑类型: star, ring, mesh
    TOPOLOGY_TIMEOUT: float = 600.0       # 拓扑构建超时
    
    # === Ray资源设置 ===
    RAY_AUTO_GPU_DETECTION: bool = True   # 自动GPU检测
    RAY_MAX_CPUS: Optional[int] = None     # 最大CPU数（None=自动）
    RAY_MAX_GPUS: Optional[int] = None     # 最大GPU数（None=自动）
    
    # === 监控设置 ===
    MONITOR_DURATION: int = 600           # 监控时长（秒）
    LOG_LEVEL: str = "INFO"               # 日志级别
    ENABLE_RAY_DASHBOARD: bool = True     # 启用Ray Dashboard
    
    # === 输出设置 ===
    OUTPUT_DIR: str = "ray_v2_output"     # 输出目录
    LOG_DIR: str = "logs"                 # 日志目录

# 创建全局配置实例
CONFIG = FLConfig()

# ============================================================================
# 📊 日志设置
# ============================================================================

def setup_logging():
    """设置日志系统"""
    # 确保日志目录存在
    os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=getattr(logging, CONFIG.LOG_LEVEL),
        format=log_format,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{CONFIG.LOG_DIR}/run_ray.log")
        ]
    )
    return logging.getLogger(__name__)

# ============================================================================
# 🎭 Ray Actor定义
# ============================================================================

@ray.remote
class RayFederatedScopeServer:
    """Ray驱动的FederatedScope服务器Actor"""
    
    def __init__(self, config: Dict[str, Any], gpu_id: Optional[int] = None):
        self.config = config
        self.gpu_id = gpu_id
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.server_port = None
        self.log_file = None
        
    def start(self) -> Tuple[str, int]:
        """启动服务器"""
        # 动态分配端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()
        
        # 更新配置
        self.config['distribute']['server_host'] = self.node_ip
        self.config['distribute']['server_port'] = self.server_port
        
        if self.gpu_id is not None:
            self.config['device'] = self.gpu_id
            self.config['use_gpu'] = True
        
        # 创建配置文件
        config_path = f"{CONFIG.OUTPUT_DIR}/configs/server.yaml"
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # 设置日志文件 - 使用节点名
        self.log_file = f"{CONFIG.LOG_DIR}/server.log"
        os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
        
        # 启动进程
        cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]
        env = os.environ.copy()
        env['PYTHONPATH'] = '.'
        if self.gpu_id is not None:
            env['CUDA_VISIBLE_DEVICES'] = str(self.gpu_id)
        
        with open(self.log_file, 'w') as log_f:
            self.process = subprocess.Popen(
                cmd, stdout=log_f, stderr=log_f, env=env
            )
        
        return self.node_ip, self.server_port
    
    def get_status(self) -> Dict:
        """获取状态"""
        if self.process is None:
            return {"status": "not_started"}
        
        poll = self.process.poll()
        if poll is None:
            return {
                "status": "running",
                "pid": self.process.pid,
                "node_ip": self.node_ip,
                "server_port": self.server_port,
                "gpu_id": self.gpu_id,
                "log_file": self.log_file
            }
        else:
            return {"status": "stopped", "returncode": poll}
    
    def stop(self):
        """停止服务器"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()

@ray.remote
class RayFederatedScopeClient:
    """Ray驱动的FederatedScope客户端Actor"""
    
    def __init__(self, client_id: int, config: Dict[str, Any], 
                 server_ip: str, server_port: int, gpu_id: Optional[int] = None):
        self.client_id = client_id
        self.config = config.copy()
        self.server_ip = server_ip
        self.server_port = server_port
        self.gpu_id = gpu_id
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.client_port = None
        self.log_file = None
        
    def start(self) -> bool:
        """启动客户端"""
        # 动态分配端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()
        
        # 更新客户端特定配置
        self.config['distribute']['server_host'] = self.server_ip
        self.config['distribute']['server_port'] = self.server_port
        self.config['distribute']['client_host'] = self.node_ip
        self.config['distribute']['client_port'] = self.client_port
        self.config['distribute']['data_idx'] = self.client_id
        
        # 客户端专用种子
        self.config['seed'] = 12345 + self.client_id
        
        if self.gpu_id is not None:
            # 当使用CUDA_VISIBLE_DEVICES时，FederatedScope应该使用设备0
            # 因为环境变量限制了可见的GPU，PyTorch会重新编号为0
            self.config['device'] = 0
            self.config['use_gpu'] = True
        
        # 客户端输出目录
        self.config['outdir'] = f"{CONFIG.OUTPUT_DIR}/client_{self.client_id}_output"
        
        # 创建配置文件
        config_path = f"{CONFIG.OUTPUT_DIR}/configs/client_{self.client_id}.yaml"
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # 设置日志文件 - 使用节点名
        self.log_file = f"{CONFIG.LOG_DIR}/client_{self.client_id}.log"
        
        # 启动进程
        cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]
        env = os.environ.copy()
        env['PYTHONPATH'] = '.'
        if self.gpu_id is not None:
            env['CUDA_VISIBLE_DEVICES'] = str(self.gpu_id)
        
        with open(self.log_file, 'w') as log_f:
            self.process = subprocess.Popen(
                cmd, stdout=log_f, stderr=log_f, env=env
            )
        
        return True
    
    def get_status(self) -> Dict:
        """获取状态"""
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
                "gpu_id": self.gpu_id,
                "log_file": self.log_file
            }
        else:
            return {"status": "stopped", "returncode": poll}
    
    def stop(self):
        """停止客户端"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()

# ============================================================================
# 🚀 主要执行类
# ============================================================================

class RayV2FederatedLearning:
    """Ray V2 联邦学习主控制器"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.server_actor = None
        self.client_actors = []
        self.server_info = None
        self.cleanup_performed = False
        
    def initialize_ray_cluster(self):
        """初始化Ray集群"""
        # 检测硬件资源
        num_cpus = CONFIG.RAY_MAX_CPUS or psutil.cpu_count()
        num_gpus = CONFIG.RAY_MAX_GPUS
        
        if CONFIG.RAY_AUTO_GPU_DETECTION and num_gpus is None:
            try:
                import torch
                num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
            except ImportError:
                num_gpus = 0
        
        # 初始化Ray
        ray_config = {
            "num_cpus": num_cpus,
            "num_gpus": num_gpus or 0,
            "ignore_reinit_error": True
        }
        
        if CONFIG.ENABLE_RAY_DASHBOARD:
            ray_config.update({
                "include_dashboard": True,
                "dashboard_host": "0.0.0.0",
                "dashboard_port": 8265
            })
        
        ray.init(**ray_config)
        
        resources = ray.cluster_resources()
        self.logger.info(f"🚀 Ray集群初始化完成:")
        self.logger.info(f"   📊 资源: {dict(resources)}")
        
        if CONFIG.ENABLE_RAY_DASHBOARD:
            dashboard_url = f"http://127.0.0.1:8265"
            self.logger.info(f"   🌐 Dashboard: {dashboard_url}")
        
    def generate_base_config(self) -> Dict[str, Any]:
        """生成基础配置"""
        return {
            'use_gpu': True,
            'device': 0,  # 将被动态覆盖
            'seed': 12345,  # 将被动态覆盖
            
            'federate': {
                'client_num': CONFIG.CLIENT_NUM,
                'mode': 'distributed',
                'total_round_num': CONFIG.TOTAL_ROUNDS,
                'sample_client_num': CONFIG.CLIENT_NUM
            },
            
            'distribute': {
                'use': True,
                'server_host': '127.0.0.1',  # 将被动态覆盖
                'server_port': 50051,        # 将被动态覆盖
                'client_host': '127.0.0.1',  # 将被动态覆盖
                'client_port': 50052,        # 将被动态覆盖
                'role': 'server',            # 将被动态覆盖
                'data_idx': 0                # 将被动态覆盖
            },
            
            'data': {
                'root': 'data/',
                'type': CONFIG.DATASET,
                'splits': [0.8, 0.1, 0.1],
                'num_workers': 0,
                'transform': [['ToTensor'], ['Normalize', {
                    'mean': [0.4914, 0.4822, 0.4465], 
                    'std': [0.2470, 0.2435, 0.2616]
                }]],
                'test_transform': [['ToTensor'], ['Normalize', {
                    'mean': [0.4914, 0.4822, 0.4465], 
                    'std': [0.2470, 0.2435, 0.2616]
                }]],
                'args': [{'download': True}],
                'splitter': 'lda',
                'splitter_args': [{'alpha': CONFIG.DATA_SPLIT_ALPHA}]
            },
            
            'dataloader': {
                'batch_size': CONFIG.BATCH_SIZE
            },
            
            'model': {
                'type': CONFIG.MODEL_TYPE,
                'hidden': CONFIG.MODEL_HIDDEN,
                'out_channels': CONFIG.MODEL_OUT_CHANNELS,
                'dropout': CONFIG.MODEL_DROPOUT
            },
            
            'train': {
                'local_update_steps': CONFIG.LOCAL_UPDATE_STEPS,
                'batch_or_epoch': 'epoch',
                'optimizer': {
                    'lr': CONFIG.LEARNING_RATE,
                    'type': CONFIG.OPTIMIZER,
                    'weight_decay': CONFIG.WEIGHT_DECAY
                }
            },
            
            'grad': {
                'grad_clip': CONFIG.GRAD_CLIP
            },
            
            'criterion': {
                'type': 'CrossEntropyLoss'
            },
            
            'trainer': {
                'type': 'cvtrainer'
            },
            
            'eval': {
                'freq': 1,
                'metrics': ['acc', 'correct'],
                'best_res_update_round_wise_key': 'test_acc'
            },
            
            'topology': {
                'use': True,
                'type': CONFIG.TOPOLOGY_TYPE,
                'timeout': CONFIG.TOPOLOGY_TIMEOUT,
                'verbose': True
            },
            
            'bittorrent': {
                'enable': True,
                'timeout': CONFIG.BITTORRENT_TIMEOUT,
                'verbose': True,
                'chunk_selection': CONFIG.BT_CHUNK_SELECTION,
                'min_completion_ratio': CONFIG.BT_MIN_COMPLETION_RATIO
            },
            
            'chunk': {
                'num_chunks': CONFIG.CHUNK_NUM,
                'importance_method': CONFIG.IMPORTANCE_METHOD
            },
            
            'chunk_num': CONFIG.CHUNK_NUM,
            'chunk_importance_method': CONFIG.IMPORTANCE_METHOD,
            
            'outdir': f'{CONFIG.OUTPUT_DIR}/server_output'
        }
        
    def allocate_gpu_resources(self) -> List[Optional[int]]:
        """智能GPU资源分配"""
        cluster_resources = ray.cluster_resources()
        available_gpus = int(cluster_resources.get('GPU', 0))
        
        if available_gpus == 0:
            self.logger.warning("⚠️ 未检测到GPU，使用CPU模式")
            return [None] * (CONFIG.CLIENT_NUM + 1)
        
        gpu_allocation = []
        
        # 服务器使用CPU，让GPU全部给客户端
        gpu_allocation.append(None)
        
        # 客户端GPU分配：平均分配到所有可用GPU
        for i in range(CONFIG.CLIENT_NUM):
            if available_gpus > 0:
                # 客户端轮换分配到所有可用GPU
                gpu_id = i % available_gpus
                gpu_allocation.append(gpu_id)
            else:
                gpu_allocation.append(None)
        
        self.logger.info(f"🎯 GPU分配: Server=CPU, Clients={gpu_allocation[1:]}")
        return gpu_allocation
    
    def cleanup_environment(self):
        """清理环境"""
        if self.cleanup_performed:
            return
            
        self.logger.info("🧹 清理环境...")
        
        # 停止旧进程
        subprocess.run(['pkill', '-9', '-f', 'python.*federatedscope'], 
                      capture_output=True, check=False)
        
        # 清理数据库文件
        subprocess.run(['rm', '-rf', 'tmp/client_*/client_*_chunks.db'], 
                      shell=True, check=False)
        
        # 清理日志目录
        for log_dir in ['connection_logs', 'topology_logs', 'bittorrent_logs']:
            subprocess.run(['rm', '-rf', log_dir], check=False)
        
        # 创建输出目录
        os.makedirs(CONFIG.OUTPUT_DIR, exist_ok=True)
        os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
        
        time.sleep(1)
        self.cleanup_performed = True
        self.logger.info("✅ 环境清理完成")
    
    def start_federated_learning(self):
        """启动联邦学习"""
        self.logger.info(f"🚀 启动Ray V2联邦学习")
        self.logger.info(f"📊 配置: {CONFIG.CLIENT_NUM}个客户端, {CONFIG.TOTAL_ROUNDS}轮训练, 总节点数: {CONFIG.CLIENT_NUM + 1}")
        
        # 清理环境
        self.cleanup_environment()
        
        # 初始化Ray
        self.initialize_ray_cluster()
        
        # GPU资源分配
        gpu_allocation = self.allocate_gpu_resources()
        server_gpu = gpu_allocation[0]
        client_gpus = gpu_allocation[1:]
        
        # 生成配置
        base_config = self.generate_base_config()
        
        # 启动服务器
        server_config = base_config.copy()
        server_config['distribute']['role'] = 'server'
        
        server_resources = {"num_cpus": 2}
        if server_gpu is not None:
            server_resources["num_gpus"] = 1
        
        self.server_actor = RayFederatedScopeServer.options(**server_resources).remote(
            server_config, server_gpu
        )
        
        server_ip, server_port = ray.get(self.server_actor.start.remote())
        self.server_info = (server_ip, server_port)
        
        self.logger.info(f"✅ 服务器已启动: {server_ip}:{server_port}")
        time.sleep(3)
        
        # 启动客户端
        for i in range(CONFIG.CLIENT_NUM):
            client_id = i + 1
            client_gpu = client_gpus[i]
            
            client_config = base_config.copy()
            client_config['distribute']['role'] = 'client'
            
            client_resources = {"num_cpus": 1}
            if client_gpu is not None:
                # 计算每个GPU上的客户端数量，合理分配GPU资源
                available_gpus = int(ray.cluster_resources().get('GPU', 0))
                clients_per_gpu = (CONFIG.CLIENT_NUM + available_gpus - 1) // available_gpus  # 向上取整
                gpu_fraction = 1.0 / clients_per_gpu
                client_resources["num_gpus"] = min(gpu_fraction, 0.8)  # 最多使用80%的GPU资源
            
            client_actor = RayFederatedScopeClient.options(**client_resources).remote(
                client_id, client_config, server_ip, server_port, client_gpu
            )
            
            self.client_actors.append(client_actor)
            ray.get(client_actor.start.remote())
            time.sleep(2)
        
        self.logger.info(f"✅ 所有{CONFIG.CLIENT_NUM}个客户端已启动完成")
        
        # 监控训练
        self.monitor_training()
        
    def monitor_training(self):
        """监控训练进度"""
        self.logger.info(f"📊 开始监控训练（{CONFIG.MONITOR_DURATION}秒）...")
        
        start_time = time.time()
        
        while True:
            elapsed = int(time.time() - start_time)
            
            if elapsed > CONFIG.MONITOR_DURATION:
                self.logger.info("⏰ 监控时间结束")
                break
            
            # 检查状态
            server_status = ray.get(self.server_actor.get_status.remote())
            client_statuses = ray.get([
                actor.get_status.remote() for actor in self.client_actors
            ])
            
            running_clients = sum(1 for s in client_statuses if s["status"] == "running")
            
            # 资源使用情况
            cluster_resources = ray.cluster_resources()
            available_resources = ray.available_resources()
            gpu_used = cluster_resources.get('GPU', 0) - available_resources.get('GPU', 0)
            
            self.logger.info(
                f"⏰ {elapsed}s | 服务器: {server_status['status']} | "
                f"客户端: {running_clients}/{CONFIG.CLIENT_NUM} | "
                f"GPU: {gpu_used:.1f}/{cluster_resources.get('GPU', 0)}"
            )
            
            # 检查训练完成
            if server_status["status"] != "running":
                self.logger.info("🏁 服务器训练完成")
                break
            
            if running_clients == 0:
                self.logger.info("🏁 所有客户端训练完成")
                break
            
            time.sleep(10)
    
    def stop_all(self):
        """停止所有进程"""
        self.logger.info("🛑 停止所有联邦学习进程...")
        
        if self.server_actor:
            ray.get(self.server_actor.stop.remote())
        
        if self.client_actors:
            ray.get([actor.stop.remote() for actor in self.client_actors])
        
        self.logger.info("✅ 所有进程已停止")
        
    def generate_results_summary(self):
        """生成结果摘要"""
        self.logger.info("📈 生成结果摘要...")
        
        # 检查日志文件
        log_files = {
            'server': f"{CONFIG.LOG_DIR}/server.log",
            'clients': [f"{CONFIG.LOG_DIR}/client_{i}.log" for i in range(1, CONFIG.CLIENT_NUM + 1)]
        }
        
        summary = {
            'configuration': {
                'client_num': CONFIG.CLIENT_NUM,
                'total_rounds': CONFIG.TOTAL_ROUNDS,
                'dataset': CONFIG.DATASET,
                'model': CONFIG.MODEL_TYPE,
                'chunk_num': CONFIG.CHUNK_NUM,
                'importance_method': CONFIG.IMPORTANCE_METHOD
            },
            'log_files': log_files,
            'output_directories': {
                'server': f"{CONFIG.OUTPUT_DIR}/server_output",
                'clients': [f"{CONFIG.OUTPUT_DIR}/client_{i}_output" for i in range(1, CONFIG.CLIENT_NUM + 1)]
            }
        }
        
        # 保存摘要
        summary_file = f"{CONFIG.OUTPUT_DIR}/results_summary.yaml"
        with open(summary_file, 'w') as f:
            yaml.safe_dump(summary, f)
        
        self.logger.info(f"📄 结果摘要已保存: {summary_file}")
        
        return summary

# ============================================================================
# 🎬 主程序入口
# ============================================================================

def display_banner():
    """显示启动横幅"""
    banner = f"""
{'='*80}
🚀 Ray-Powered FederatedScope V2 Script
{'='*80}
📊 配置信息:
   • 客户端数量: {CONFIG.CLIENT_NUM}
   • 训练轮数: {CONFIG.TOTAL_ROUNDS}
   • 数据集: {CONFIG.DATASET}
   • 模型: {CONFIG.MODEL_TYPE}
   • Chunk数: {CONFIG.CHUNK_NUM}
   • 重要度方法: {CONFIG.IMPORTANCE_METHOD}
   • 监控时长: {CONFIG.MONITOR_DURATION}s

💡 输出目录: {CONFIG.OUTPUT_DIR}
📝 日志目录: {CONFIG.LOG_DIR}
{'='*80}
"""
    print(banner)

def main():
    """主函数"""
    display_banner()
    
    ray_fl = RayV2FederatedLearning()
    
    try:
        # 启动联邦学习
        ray_fl.start_federated_learning()
        
        # 生成结果摘要
        summary = ray_fl.generate_results_summary()
        
        print("\n🎉 Ray V2 联邦学习完成！")
        print(f"📄 结果摘要: {CONFIG.OUTPUT_DIR}/results_summary.yaml")
        print(f"📝 日志文件: {CONFIG.LOG_DIR}/")
        
        if CONFIG.ENABLE_RAY_DASHBOARD:
            print(f"🌐 Ray Dashboard: http://127.0.0.1:8265")
            
    except KeyboardInterrupt:
        print("\n👋 收到中断信号，正在清理...")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 清理资源
        ray_fl.stop_all()
        ray.shutdown()
        print("🧹 资源清理完成")

if __name__ == "__main__":
    main()