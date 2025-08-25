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
import docker
import random
import threading
import signal
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

# ============================================================================
# 🔧 配置区域 - 所有设置集中在此处，方便修改
# ============================================================================

@dataclass
class EdgeDeviceProfile:
    """边缘设备硬件和网络配置档案"""
    device_id: str
    device_type: str  # "smartphone_high", "smartphone_low", "raspberry_pi", "iot_device", "edge_server"
    docker_image: str
    
    # 硬件资源限制
    cpu_limit: str = "0.5"                 # CPU限制 (Docker格式)
    memory_limit: str = "1g"               # 内存限制 (Docker格式) 
    storage_limit: str = "8g"              # 存储限制
    
    # 网络特性
    bandwidth_up_kbps: int = 10000         # 上行带宽 (kbps)
    bandwidth_down_kbps: int = 50000       # 下行带宽 (kbps)
    latency_ms: int = 50                   # 网络延迟 (毫秒)
    packet_loss_rate: float = 0.00        # 丢包率 (0-1)
    jitter_ms: int = 10                    # 网络抖动 (毫秒)
    
    # 设备特性
    training_speed_multiplier: float = 1.0  # 训练速度倍数
    availability_ratio: float = 0.9        # 可用性比例 (0-1)
    battery_constraint: bool = False        # 是否有电池限制
    mobility_pattern: str = "static"        # 移动模式: static, mobile, intermittent

@dataclass
class FLConfig:
    """联邦学习配置参数"""
    
    # === 基础设置 ===
    CLIENT_NUM: int = 4                     # 客户端数量（测试用）
    TOTAL_ROUNDS: int = 10                   # 训练轮数（测试用）
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
    
    # === Docker设置 ===
    USE_DOCKER: bool = True                # 启用Docker容器模式
    BASE_DOCKER_IMAGE: str = "federatedscope:base"  # 基础镜像
    DOCKER_NETWORK_NAME: str = "fl_network"         # Docker网络名称
    ENABLE_NETWORK_SIMULATION: bool = True          # 启用网络仿真
    
    # === 网络仿真设置 ===
    NETWORK_PROFILES: Dict[str, Dict] = field(default_factory=lambda: {
        "smartphone_high": {
            "bandwidth_up_kbps": 50000,
            "bandwidth_down_kbps": 100000,
            "latency_ms": 20,
            "packet_loss_rate": 0.005,
            "jitter_ms": 5
        },
        "smartphone_low": {
            "bandwidth_up_kbps": 5000,
            "bandwidth_down_kbps": 20000,
            "latency_ms": 100,
            "packet_loss_rate": 0.02,
            "jitter_ms": 20
        },
        "raspberry_pi": {
            "bandwidth_up_kbps": 10000,
            "bandwidth_down_kbps": 50000,
            "latency_ms": 30,
            "packet_loss_rate": 0.01,
            "jitter_ms": 10
        },
        "iot_device": {
            "bandwidth_up_kbps": 128,
            "bandwidth_down_kbps": 512,
            "latency_ms": 300,
            "packet_loss_rate": 0.05,
            "jitter_ms": 50
        },
        "edge_server": {
            "bandwidth_up_kbps": 100000,
            "bandwidth_down_kbps": 1000000,
            "latency_ms": 5,
            "packet_loss_rate": 0.001,
            "jitter_ms": 1
        }
    })
    
    # === 设备分布配置 ===
    DEVICE_DISTRIBUTION: Dict[str, float] = field(default_factory=lambda: {
        "smartphone_high": 1,    # 20% 高端手机
        "smartphone_low": 0,     # 40% 低端手机
        "raspberry_pi": 0,       # 15% 树莓派
        "iot_device": 0,         # 20% IoT设备
        "edge_server": 0         # 5% 边缘服务器
    })
    
    # === 训练设置 ===
    LOCAL_UPDATE_STEPS: int = 1           # 本地训练步数
    LEARNING_RATE: float = 0.01           # 学习率
    OPTIMIZER: str = "SGD"                # 优化器
    WEIGHT_DECAY: float = 0.0001          # 权重衰减
    GRAD_CLIP: float = 5.0                # 梯度裁剪
    
    # === BitTorrent设置 ===
    BITTORRENT_TIMEOUT: float = 600.0     # BitTorrent超时
    BT_CHUNK_SELECTION: str = "rarest_first"  # chunk选择策略
    BT_MIN_COMPLETION_RATIO: float = 0.8   # 最小完成比率
    
    # === 拓扑设置 ===
    TOPOLOGY_TYPE: str = "mesh"         # 拓扑类型: star, ring, fully_connected, mesh, random
    TOPOLOGY_TIMEOUT: float = 600.0       # 拓扑构建超时
    TOPOLOGY_CONNECTIONS: int = 2         # 每个节点连接数（mesh:恰好连接数, random:最少连接数）
    
    # === Docker和网络仿真设置 ===
    USE_DOCKER: bool = True               # 启用Docker容器化
    ENABLE_NETWORK_SIMULATION: bool = True # 启用网络仿真
    DOCKER_BASE_IMAGE: str = "federatedscope:base"  # Docker基础镜像
    
    # === Ray资源设置 ===
    RAY_AUTO_GPU_DETECTION: bool = True   # 自动GPU检测
    RAY_MAX_CPUS: Optional[int] = None     # 最大CPU数（None=自动）
    RAY_MAX_GPUS: Optional[int] = None     # 最大GPU数（None=自动）
    
    # === 监控设置 ===
    MONITOR_DURATION: int = 999999999           # 监控时长（秒）
    LOG_LEVEL: str = "INFO"               # 日志级别
    ENABLE_RAY_DASHBOARD: bool = True     # 启用Ray Dashboard
    
    # === 输出设置 ===
    OUTPUT_DIR: str = "ray_v2_output"     # 输出目录
    LOG_DIR: str = "logs"                 # 日志目录

# 边缘设备配置档案库
EDGE_DEVICE_PROFILES = {
    "smartphone_high": EdgeDeviceProfile(
        device_id="smartphone_high",
        device_type="smartphone", 
        docker_image="federatedscope:base",  # 临时使用base镜像
        cpu_limit="1.0", memory_limit="2g", storage_limit="32g",
        bandwidth_up_kbps=50000, bandwidth_down_kbps=100000,
        latency_ms=20, packet_loss_rate=0.005, jitter_ms=5,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        mobility_pattern="static"
    ),
    
    "smartphone_low": EdgeDeviceProfile(
        device_id="smartphone_low", 
        device_type="smartphone",
        docker_image="federatedscope:base",  # 临时使用base镜像
        cpu_limit="0.3", memory_limit="2g", storage_limit="8g",
        bandwidth_up_kbps=5000, bandwidth_down_kbps=20000,
        latency_ms=100, packet_loss_rate=0.02, jitter_ms=20,
        training_speed_multiplier=0.6, availability_ratio=1.0,
        battery_constraint=False, mobility_pattern="static"
    ),
    
    "raspberry_pi": EdgeDeviceProfile(
        device_id="raspberry_pi",
        device_type="edge_device",
        docker_image="federatedscope:base",  # 临时使用base镜像
        cpu_limit="0.6", memory_limit="2g", storage_limit="64g",
        bandwidth_up_kbps=10000, bandwidth_down_kbps=50000,
        latency_ms=30, packet_loss_rate=0.01, jitter_ms=10,
        training_speed_multiplier=0.7, availability_ratio=1.0,
        mobility_pattern="static"
    ),
    
    "iot_device": EdgeDeviceProfile(
        device_id="iot_device",
        device_type="iot",
        docker_image="federatedscope:base",  # 临时使用base镜像
        cpu_limit="0.1", memory_limit="2g", storage_limit="2g", 
        bandwidth_up_kbps=128, bandwidth_down_kbps=512,
        latency_ms=300, packet_loss_rate=0.05, jitter_ms=50,
        training_speed_multiplier=0.3, availability_ratio=1.0,
        battery_constraint=False, mobility_pattern="static"
    ),
    
    "edge_server": EdgeDeviceProfile(
        device_id="edge_server",
        device_type="edge_server", 
        docker_image="federatedscope:base",  # 临时使用base镜像
        cpu_limit="2.0", memory_limit="2g", storage_limit="100g",
        bandwidth_up_kbps=100000, bandwidth_down_kbps=1000000,
        latency_ms=10, packet_loss_rate=0.001, jitter_ms=2,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        mobility_pattern="static"
    )
}

# 创建全局配置实例
CONFIG = FLConfig()

# ============================================================================
# 🌐 网络仿真类
# ============================================================================

class NetworkSimulator:
    """Docker容器网络仿真控制器"""
    
    def __init__(self):
        self.active_limitations = {}
        
    def apply_network_constraints(self, container, profile: EdgeDeviceProfile):
        """为Docker容器应用网络约束"""
        if not CONFIG.ENABLE_NETWORK_SIMULATION:
            return True
            
        try:
            # 在容器内安装并配置tc (traffic control)
            setup_commands = [
                # 安装iproute2 (包含tc命令)
                "apt-get update -qq && apt-get install -y iproute2 > /dev/null 2>&1 || apk add iproute2 > /dev/null 2>&1 || true",
                
                # 删除现有的队列规则
                "tc qdisc del dev eth0 root 2>/dev/null || true",
                
                # 创建根HTB队列
                "tc qdisc add dev eth0 root handle 1: htb default 30",
                
                # 设置总带宽限制 (上行)
                f"tc class add dev eth0 parent 1: classid 1:1 htb rate {profile.bandwidth_up_kbps}kbit ceil {profile.bandwidth_up_kbps}kbit",
                
                # 添加网络延迟、抖动和丢包
                f"tc qdisc add dev eth0 parent 1:1 handle 10: netem delay {profile.latency_ms}ms {profile.jitter_ms}ms loss {profile.packet_loss_rate * 100}%"
            ]
            
            for cmd in setup_commands:
                result = container.exec_run(
                    f"sh -c '{cmd}'", 
                    privileged=True,
                    user="root"
                )
                # 记录网络配置结果但不终止 (某些命令可能失败但不影响整体)
                if result.exit_code != 0 and "tc qdisc add" in cmd:
                    print(f"⚠️  网络配置警告 - 客户端{profile.device_id}: {result.output.decode()[:100]}")
            
            # 记录成功应用的限制
            container_name = container.name
            self.active_limitations[container_name] = {
                "bandwidth_up_kbps": profile.bandwidth_up_kbps,
                "latency_ms": profile.latency_ms,
                "packet_loss_rate": profile.packet_loss_rate,
                "jitter_ms": profile.jitter_ms
            }
            
            return True
            
        except Exception as e:
            print(f"❌ 网络约束应用失败 - {profile.device_id}: {e}")
            return False
    
    def simulate_network_fluctuation(self, container_name: str, duration: int = 60):
        """模拟网络波动"""
        if container_name not in self.active_limitations:
            return
            
        def fluctuation_thread():
            try:
                docker_client = docker.from_env()
                container = docker_client.containers.get(container_name)
                base_config = self.active_limitations[container_name]
                
                for _ in range(duration):
                    # 随机改变网络条件 (±30%)
                    multiplier = random.uniform(0.7, 1.3)
                    
                    new_bandwidth = int(base_config["bandwidth_up_kbps"] * multiplier)
                    new_latency = max(10, int(base_config["latency_ms"] * multiplier))
                    
                    # 更新网络限制
                    fluctuation_cmd = f"tc class change dev eth0 classid 1:1 htb rate {new_bandwidth}kbit ceil {new_bandwidth}kbit"
                    container.exec_run(f"sh -c '{fluctuation_cmd}'", privileged=True)
                    
                    time.sleep(1)
                    
            except Exception as e:
                print(f"网络波动模拟错误: {e}")
        
        # 启动后台线程执行网络波动
        threading.Thread(target=fluctuation_thread, daemon=True).start()
    
    def get_network_stats(self, container_name: str) -> Dict:
        """获取容器网络统计信息"""
        try:
            docker_client = docker.from_env()
            container = docker_client.containers.get(container_name)
            
            # 执行网络统计命令
            result = container.exec_run("cat /proc/net/dev")
            if result.exit_code == 0:
                lines = result.output.decode().split('\n')
                for line in lines:
                    if 'eth0:' in line:
                        parts = line.split()
                        return {
                            "rx_bytes": int(parts[1]),
                            "rx_packets": int(parts[2]),
                            "tx_bytes": int(parts[9]), 
                            "tx_packets": int(parts[10]),
                            "active_limits": self.active_limitations.get(container_name, {})
                        }
            return {"error": "无法获取网络统计"}
            
        except Exception as e:
            return {"error": str(e)}

class DockerManager:
    """Docker环境管理器"""
    
    def __init__(self):
        self.client = None
        self.network_simulator = NetworkSimulator()
        self.fl_network = None
        self.docker_available = self._check_docker_availability()
        
        if self.docker_available:
            try:
                self.client = docker.from_env()
            except Exception as e:
                print(f"⚠️  Docker连接失败: {e}")
                self.docker_available = False
                
    def _check_docker_availability(self) -> bool:
        """检查Docker是否可用"""
        try:
            # 快速检查Docker命令是否存在
            result = subprocess.run(['which', 'docker'], 
                                  capture_output=True, text=True, timeout=2)
            if result.returncode != 0:
                print(f"⚠️  Docker命令未安装")
                return False
            
            # 检查Docker版本
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, 
                                  check=True, timeout=3)
            print(f"🐳 Docker版本: {result.stdout.strip()}")
            
            # 快速检查Docker服务状态
            result = subprocess.run(['docker', 'ps', '--format', 'table {{.ID}}'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                print(f"✅ Docker服务正常运行")
                return True
            else:
                print(f"⚠️  Docker服务未运行: {result.stderr.strip()[:100]}")
                return False
            
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"⚠️  Docker不可用: {e}")
            return False
        
    def setup_docker_environment(self):
        """设置Docker环境"""
        try:
            # 创建专用网络
            try:
                self.fl_network = self.client.networks.get(CONFIG.DOCKER_NETWORK_NAME)
                print(f"📶 使用现有Docker网络: {CONFIG.DOCKER_NETWORK_NAME}")
            except docker.errors.NotFound:
                self.fl_network = self.client.networks.create(
                    CONFIG.DOCKER_NETWORK_NAME,
                    driver="bridge",
                    options={
                        "com.docker.network.bridge.enable_icc": "true",
                        "com.docker.network.bridge.enable_ip_masquerade": "true"
                    }
                )
                print(f"📶 创建Docker网络: {CONFIG.DOCKER_NETWORK_NAME}")
            
            return True
            
        except Exception as e:
            print(f"❌ Docker环境设置失败: {e}")
            return False
    
    def cleanup_docker_environment(self):
        """清理Docker环境"""
        try:
            # 停止并删除所有FL相关容器
            containers = self.client.containers.list(all=True)
            for container in containers:
                if container.name.startswith('fl_'):
                    try:
                        container.stop(timeout=10)
                        container.remove()
                        print(f"🗑️  清理容器: {container.name}")
                    except:
                        pass
            
            # 删除网络 (如果没有其他容器使用)
            if self.fl_network:
                try:
                    self.fl_network.remove()
                    print(f"🗑️  清理网络: {CONFIG.DOCKER_NETWORK_NAME}")
                except:
                    pass
                    
        except Exception as e:
            print(f"⚠️  Docker清理警告: {e}")
    
    def check_required_images(self) -> bool:
        """检查所需的Docker镜像是否存在"""
        if not self.docker_available:
            return False
        
        try:
            # 简化版：只检查base镜像，所有设备使用同一个镜像
            required_images = [
                CONFIG.DOCKER_BASE_IMAGE,  # "federatedscope:base"
            ]
            
            missing_images = []
            for image_name in required_images:
                try:
                    self.client.images.get(image_name)
                    print(f"✅ Docker镜像已存在: {image_name}")
                except docker.errors.ImageNotFound:
                    missing_images.append(image_name)
                    print(f"❌ Docker镜像缺失: {image_name}")
            
            if missing_images:
                print(f"\n🚨 缺失 {len(missing_images)} 个Docker镜像: {missing_images}")
                return False
            else:
                print("✅ 所有Docker镜像都已就绪")
                return True
                
        except Exception as e:
            print(f"❌ 检查Docker镜像时出错: {e}")
            return False
    
    def build_required_images(self) -> bool:
        """自动构建所需的Docker镜像"""
        if not self.docker_available:
            return False
        
        print("🐳 开始自动构建FederatedScope Docker镜像...")
        
        # 简化版：只构建base镜像，所有设备共用
        build_configs = [
            {
                "dockerfile": "docker/Dockerfile.base",
                "tag": "federatedscope:base",
                "name": "基础镜像"
            }
        ]
        
        build_success = True
        
        for config in build_configs:
            dockerfile_path = config["dockerfile"]
            tag = config["tag"]
            name = config["name"]
            
            # 检查Dockerfile是否存在
            if not os.path.exists(dockerfile_path):
                print(f"⚠️  Dockerfile不存在: {dockerfile_path}，跳过构建 {tag}")
                continue
            
            print(f"📦 正在构建 {name} ({tag})...")
            
            try:
                # 使用Docker Python API构建镜像
                build_logs = self.client.api.build(
                    path='.',  # 构建上下文为当前目录
                    dockerfile=dockerfile_path,
                    tag=tag,
                    rm=True,  # 构建后删除中间容器
                    decode=True,  # 解码构建日志
                    pull=False  # 不自动拉取基础镜像
                )
                
                # 显示构建进度
                for log_line in build_logs:
                    if 'stream' in log_line:
                        log_msg = log_line['stream'].strip()
                        if log_msg and not log_msg.startswith(' ---> '):
                            print(f"   {log_msg}")
                    elif 'error' in log_line:
                        print(f"❌ 构建错误: {log_line['error']}")
                        build_success = False
                        break
                
                if build_success:
                    print(f"✅ {name} 构建成功")
                else:
                    print(f"❌ {name} 构建失败")
                    break
                    
            except Exception as e:
                print(f"❌ 构建 {name} 时出错: {e}")
                build_success = False
                break
        
        if build_success:
            print("🎉 所有Docker镜像构建成功!")
            return True
        else:
            print("❌ Docker镜像构建失败")
            return False
    
    def ensure_images_ready(self) -> bool:
        """确保所需的Docker镜像已就绪（检查+自动构建）"""
        if not self.docker_available:
            print("⚠️  Docker不可用，跳过镜像检查")
            return False
        
        print("🔍 检查Docker镜像状态...")
        
        # 首先检查镜像是否存在
        if self.check_required_images():
            return True
        
        # 镜像不完整，询问用户是否自动构建
        print("\n🤔 是否自动构建缺失的Docker镜像？")
        print("   这可能需要5-10分钟时间...")
        
        # 在自动化环境中直接构建，不需要用户确认
        if os.getenv('CI') or os.getenv('AUTOMATED_BUILD'):
            user_choice = 'y'
        else:
            user_choice = input("   输入 [y/N]: ").lower().strip()
        
        if user_choice in ['y', 'yes', '是']:
            return self.build_required_images()
        else:
            print("⚠️  用户取消自动构建，将使用非Docker模式")
            return False

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
class FallbackFederatedScopeServer:
    """后备的FederatedScope服务器Actor（非Docker模式）"""
    
    def __init__(self, config: Dict[str, Any], gpu_id: Optional[int] = None):
        self.config = config
        self.gpu_id = gpu_id
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.server_port = None
        
    def start(self) -> Tuple[str, int]:
        """启动本地进程服务器"""
        # 动态分配端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()
        
        # 更新配置
        self.config['distribute']['server_host'] = self.node_ip
        self.config['distribute']['server_port'] = self.server_port
        self.config['use_gpu'] = False  # 强制CPU模式
        
        # 准备配置文件
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        os.makedirs(config_dir, exist_ok=True)
        config_path = f"{config_dir}/server.yaml"
        
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # 启动进程
        try:
            # 设置日志文件
            self.log_file = f"{CONFIG.LOG_DIR}/server.log"
            os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
            
            # 设置环境变量（按照原始版本的简单方式）
            env = os.environ.copy()
            env['PYTHONPATH'] = '.'
            if self.gpu_id is not None:
                env['CUDA_VISIBLE_DEVICES'] = str(self.gpu_id)
            
            # 启动进程（使用原始版本的方式，包括日志重定向）
            cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]
            
            with open(self.log_file, 'w') as log_f:
                self.process = subprocess.Popen(
                    cmd, stdout=log_f, stderr=log_f, env=env
                )
            
            return self.node_ip, self.server_port
        except Exception as e:
            print(f"启动服务器进程失败: {e}")
            return None, None
    
    def get_status(self) -> Dict:
        """获取进程状态"""
        if self.process is None:
            return {"status": "not_started"}
        
        if self.process.poll() is None:
            return {
                "status": "running",
                "node_ip": self.node_ip,
                "server_port": self.server_port,
                "pid": self.process.pid
            }
        else:
            return {"status": "finished", "return_code": self.process.returncode}
    
    def stop(self):
        """停止进程"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()

@ray.remote
class FallbackFederatedScopeClient:
    """后备的FederatedScope客户端Actor（非Docker模式）"""
    
    def __init__(self, client_id: int, config: Dict[str, Any], 
                 server_ip: str, server_port: int, device_profile: EdgeDeviceProfile):
        self.client_id = client_id
        self.config = config.copy()
        self.server_ip = server_ip
        self.server_port = server_port
        self.device_profile = device_profile
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.client_port = None
        
    def start(self) -> bool:
        """启动本地进程客户端"""
        # 动态分配端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()
        
        # 更新客户端网络配置
        self.config['distribute']['server_host'] = self.server_ip
        self.config['distribute']['server_port'] = self.server_port
        self.config['distribute']['client_host'] = self.node_ip
        self.config['distribute']['client_port'] = self.client_port
        self.config['distribute']['data_idx'] = self.client_id
        
        # 客户端专用种子
        self.config['seed'] = 12345 + self.client_id
        
        # GPU模式配置 - 所有客户端都使用GPU（分数分配）
        self.config['use_gpu'] = True
        
        # 准备配置和输出目录
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        output_dir = f"{CONFIG.OUTPUT_DIR}/client_{self.client_id}_output"
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        
        config_path = f"{config_dir}/client_{self.client_id}.yaml"
        self.config['outdir'] = output_dir
        
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # 启动进程
        try:
            # 设置日志文件
            self.log_file = f"{CONFIG.LOG_DIR}/client_{self.client_id}.log"
            os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
            
            # 设置环境变量（按照原始版本的简单方式）
            env = os.environ.copy()
            env['PYTHONPATH'] = '.'
            # 注意：这是FallbackFederatedScopeClient，不使用GPU
            # GPU配置已在self.config中设置
            
            # 启动进程（使用原始版本的方式，包括日志重定向）
            cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]
            
            with open(self.log_file, 'w') as log_f:
                self.process = subprocess.Popen(
                    cmd, stdout=log_f, stderr=log_f, env=env
                )
            
            return True
        except Exception as e:
            print(f"启动客户端{self.client_id}进程失败: {e}")
            return False
    
    def get_status(self) -> Dict:
        """获取进程状态"""
        if self.process is None:
            return {"status": "not_started"}
        
        if self.process.poll() is None:
            return {
                "status": "running",
                "client_id": self.client_id,
                "device_type": self.device_profile.device_type,
                "node_ip": self.node_ip,
                "client_port": self.client_port,
                "pid": self.process.pid
            }
        else:
            return {"status": "finished", "return_code": self.process.returncode}
    
    def stop(self):
        """停止进程"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()

@ray.remote
class DockerFederatedScopeServer:
    """Docker化的FederatedScope服务器Actor"""
    
    def __init__(self, config: Dict[str, Any], gpu_id: Optional[int] = None):
        self.config = config
        self.gpu_id = gpu_id
        self.container = None
        self.docker_client = docker.from_env()
        self.node_ip = ray.util.get_node_ip_address()
        self.server_port = None
        self.container_name = "fl_server"
        
    def _get_absolute_path(self, path: str) -> str:
        """安全地获取绝对路径，处理Ray Actor中的工作目录问题"""
        try:
            # 如果路径已经是绝对路径，直接返回
            if os.path.isabs(path):
                return path
            # 尝试获取绝对路径
            return os.path.abspath(path)
        except (OSError, FileNotFoundError):
            # 如果当前工作目录不存在，使用固定的基础目录
            base_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master"
            return os.path.join(base_dir, path)
    
    def start(self) -> Tuple[str, int]:
        """启动Docker服务器容器"""
        # 动态分配端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()
        
        # 更新配置 - Docker模式三段地址格式：绑定IP|报告IP|报告端口
        container_bind_ip = '0.0.0.0'  # 容器内绑定地址
        external_access_ip = self.node_ip  # 外部访问IP
        external_access_port = self.server_port  # 宿主机映射端口
        self.config['distribute']['server_host'] = f"{container_bind_ip}|{external_access_ip}|{external_access_port}"
        self.config['distribute']['server_port'] = 50051  # 容器内端口（保持整数类型）
        
        if self.gpu_id is not None:
            self.config['device'] = 0  # 容器内GPU ID
            self.config['use_gpu'] = True
        
        # 准备配置文件
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        os.makedirs(config_dir, exist_ok=True)
        config_path = f"{config_dir}/server.yaml"
        
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # 准备日志目录 - 使用CONFIG.LOG_DIR确保路径一致
        log_dir = CONFIG.LOG_DIR  # 使用统一的日志目录
        os.makedirs(log_dir, exist_ok=True)
        
        # Docker容器配置
        container_config = {
            "image": CONFIG.DOCKER_BASE_IMAGE,
            "name": self.container_name,
            "hostname": "fl-server",
            "detach": True,
            "remove": True,  # 容器停止后自动删除
            
            # 端口映射：容器内50051 -> 主机随机端口
            "ports": {50051: self.server_port},
            
            # 环境变量 - 按照原始版本的简单方式
            "environment": {
                "PYTHONPATH": "/app"
            },
            
            # 卷挂载 - 修复Ray Actor中的路径问题
            "volumes": {
                self._get_absolute_path(config_path): {"bind": "/app/config.yaml", "mode": "ro"},
                self._get_absolute_path(log_dir): {"bind": "/app/logs", "mode": "rw"},
                self._get_absolute_path("data"): {"bind": "/app/data", "mode": "rw"}
            },
            
            # 启动命令 - 使用shell包装以设置工作目录和环境，并重定向日志到挂载目录
            "command": ["sh", "-c", "cd /app && PYTHONPATH=/app python federatedscope/main.py --cfg /app/config.yaml > /app/logs/server.log 2>&1"]
        }
        
        # GPU支持
        if self.gpu_id is not None:
            container_config["device_requests"] = [
                docker.types.DeviceRequest(device_ids=[str(self.gpu_id)], capabilities=[['gpu']])
            ]
        
        try:
            # 启动容器
            self.container = self.docker_client.containers.run(**container_config)
            return self.node_ip, self.server_port
            
        except Exception as e:
            print(f"启动服务器容器失败: {e}")
            return None, None
    
    def get_status(self) -> Dict:
        """获取Docker容器状态"""
        if self.container is None:
            return {"status": "not_started"}
        
        try:
            self.container.reload()
            return {
                "status": self.container.status,
                "container_id": self.container.id[:12], 
                "node_ip": self.node_ip,
                "server_port": self.server_port,
                "gpu_id": self.gpu_id,
                "container_name": self.container_name
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def stop(self):
        """停止Docker容器"""
        if self.container:
            try:
                self.container.stop(timeout=10)
            except Exception as e:
                print(f"停止服务器容器失败: {e}")

@ray.remote
class DockerFederatedScopeClient:
    """Docker化的FederatedScope客户端Actor"""
    
    def __init__(self, client_id: int, config: Dict[str, Any], 
                 server_ip: str, server_port: int, device_profile: EdgeDeviceProfile):
        self.client_id = client_id
        self.config = config.copy()
        self.server_ip = server_ip
        self.server_port = server_port
        self.device_profile = device_profile
        self.container = None
        self.docker_client = docker.from_env()
        self.node_ip = ray.util.get_node_ip_address()
        self.client_port = None
        self.container_name = f"fl_client_{client_id}"
        
    def _get_absolute_path(self, path: str) -> str:
        """安全地获取绝对路径，处理Ray Actor中的工作目录问题"""
        try:
            # 如果路径已经是绝对路径，直接返回
            if os.path.isabs(path):
                return path
            # 尝试获取绝对路径
            return os.path.abspath(path)
        except (OSError, FileNotFoundError):
            # 如果当前工作目录不存在，使用固定的基础目录
            base_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master"
            return os.path.join(base_dir, path)
    
    def start(self) -> bool:
        """启动Docker客户端容器"""
        # 动态分配端口
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()
        
        # 应用设备特定配置
        self._apply_device_constraints()
        
        # 更新客户端网络配置  
        self.config['distribute']['server_host'] = self.server_ip
        self.config['distribute']['server_port'] = self.server_port
        # Docker模式三段地址格式：绑定IP|报告IP|报告端口
        container_bind_ip = '0.0.0.0'  # 容器内绑定地址
        external_access_ip = self.node_ip  # 外部访问IP  
        external_access_port = self.client_port  # 宿主机映射端口
        self.config['distribute']['client_host'] = f"{container_bind_ip}|{external_access_ip}|{external_access_port}"
        self.config['distribute']['client_port'] = 50052  # 容器内端口（保持整数类型）
        self.config['distribute']['data_idx'] = self.client_id
        
        # 客户端专用种子
        self.config['seed'] = 12345 + self.client_id
        
        # 🎮 GPU配置：基于Ray资源分配而非容器内检测
        # Docker容器启动前无法检测CUDA，应该基于Ray的GPU分配来决定
        # 注意：use_gpu将在容器启动后由FederatedScope重新检测
        self.config['device'] = 0  # 容器内默认使用GPU 0（如果可用）
        self.config['use_gpu'] = True  # 临时设置，容器内会重新检测
        
        # 客户端输出目录
        self.config['outdir'] = f"/app/output"
        
        # 准备配置和输出目录
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        output_dir = f"{CONFIG.OUTPUT_DIR}/client_{self.client_id}_output"
        log_dir = CONFIG.LOG_DIR  # 使用统一的日志目录，与宿主机保持一致
        data_dir = "data"
        
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True) 
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)
        
        config_path = f"{config_dir}/client_{self.client_id}.yaml"
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # 启动Docker容器
        return self._start_docker_container(config_path, output_dir, log_dir, data_dir)
    
    def _apply_device_constraints(self):
        """根据设备特性应用配置约束"""
        profile = self.device_profile
        
        # 根据设备能力调整batch size
        if profile.device_type == "iot_device":
            self.config['dataloader']['batch_size'] = 8  # IoT设备使用小批量
        elif profile.device_type == "smartphone_low":
            self.config['dataloader']['batch_size'] = 16  # 低端手机适中
        else:
            self.config['dataloader']['batch_size'] = 32  # 高端设备使用原始配置
        
        # 根据训练速度调整本地更新步数
        base_steps = self.config['train']['local_update_steps']
        adjusted_steps = max(1, int(base_steps * profile.training_speed_multiplier))
        self.config['train']['local_update_steps'] = adjusted_steps
        
    def _start_docker_container(self, config_path: str, output_dir: str, log_dir: str, data_dir: str) -> bool:
        """启动Docker容器"""
        profile = self.device_profile
        
        # 计算CPU限额 (以微秒为单位)
        cpu_period = 100000  # 100ms
        cpu_quota = int(float(profile.cpu_limit) * cpu_period)
        
        # Docker容器配置
        container_config = {
            "image": profile.docker_image,
            "name": self.container_name,
            "hostname": f"client-{self.client_id}",
            "detach": True,
            "remove": True,
            
            # 资源限制
            "cpu_period": cpu_period,
            "cpu_quota": cpu_quota,
            "mem_limit": profile.memory_limit,
            "memswap_limit": profile.memory_limit,  # 禁用swap
            
            # 端口映射
            "ports": {50052: self.client_port},
            
            # 环境变量 - 按照原始版本的简单方式
            "environment": {
                "PYTHONPATH": "/app"
            },
            
            # 卷挂载 - 修复Ray Actor中的路径问题
            "volumes": {
                self._get_absolute_path(config_path): {"bind": "/app/config.yaml", "mode": "ro"},
                self._get_absolute_path(output_dir): {"bind": "/app/output", "mode": "rw"},
                self._get_absolute_path(log_dir): {"bind": "/app/logs", "mode": "rw"},
                self._get_absolute_path(data_dir): {"bind": "/app/data", "mode": "rw"}
            },
            
            # 特权模式(用于网络控制)
            "privileged": True,
            
            # 启动命令 - 使用shell包装以设置工作目录和环境，并重定向日志到挂载目录
            "command": ["sh", "-c", f"cd /app && PYTHONPATH=/app python federatedscope/main.py --cfg /app/config.yaml > /app/logs/client_{self.client_id}.log 2>&1"]
        }
        
        # GPU支持 - 基于Ray资源分配决定容器GPU访问权限
        # 从Actor类的资源选项中获取GPU分配信息
        # 这个信息在Actor创建时就已经确定了
        try:
            # 获取当前Actor的资源分配（从类变量或实例保存的信息）
            actor_options = ray.get_runtime_context().current_actor
            # 由于无法直接获取Actor的资源配置，我们使用一个更简单的方法：
            # 检查集群中是否有GPU资源，如果有就给容器GPU访问权限
            cluster_resources = ray.cluster_resources()
            has_gpu_in_cluster = cluster_resources.get('GPU', 0) > 0
            
            if has_gpu_in_cluster:
                # 如果集群有GPU资源，给Docker容器GPU访问权限
                # FederatedScope会在容器内自动检测并决定是否使用GPU
                container_config["device_requests"] = [
                    docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])
                ]
                print(f"🎮 客户端{self.client_id}: 集群有GPU资源，启用容器GPU访问")
            else:
                print(f"💻 客户端{self.client_id}: 集群无GPU资源，使用CPU模式")
                
        except Exception as e:
            # 如果获取资源信息失败，默认启用GPU访问（让FederatedScope自己决定）
            container_config["device_requests"] = [
                docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])
            ]
            print(f"🎮 客户端{self.client_id}: 无法获取资源信息({e})，默认启用GPU访问")
        
        try:
            # 启动容器
            self.container = self.docker_client.containers.run(**container_config)
            
            # 应用网络限制
            if CONFIG.ENABLE_NETWORK_SIMULATION:
                self._apply_network_constraints()
            
            # 启动设备行为仿真
            self._start_device_behavior_simulation()
            
            return True
            
        except Exception as e:
            print(f"启动客户端{self.client_id}容器失败: {e}")
            return False
    
    def _apply_network_constraints(self):
        """应用网络约束使用tc (traffic control)"""
        if not self.container:
            return
            
        profile = self.device_profile
        
        # 转换带宽单位
        up_bandwidth = f"{profile.bandwidth_up_kbps}kbit"
        down_bandwidth = f"{profile.bandwidth_down_kbps}kbit"
        delay = f"{profile.latency_ms}ms"
        jitter = f"{profile.jitter_ms}ms" 
        loss = f"{profile.packet_loss_rate * 100}%"
        
        # tc命令序列
        tc_commands = [
            # 清除现有规则
            "tc qdisc del dev eth0 root 2>/dev/null || true",
            
            # 设置上行带宽限制
            "tc qdisc add dev eth0 root handle 1: htb default 12",
            f"tc class add dev eth0 parent 1: classid 1:1 htb rate {up_bandwidth} ceil {up_bandwidth}",
            
            # 添加网络延迟和丢包
            f"tc qdisc add dev eth0 parent 1:1 handle 10: netem delay {delay} {jitter} loss {loss}",
        ]
        
        # 执行tc命令
        for cmd in tc_commands:
            try:
                result = self.container.exec_run(f"sh -c '{cmd}'", privileged=True)
                if result.exit_code != 0:
                    print(f"网络配置命令失败: {cmd}, 错误: {result.output.decode()}")
            except Exception as e:
                print(f"执行网络命令失败: {cmd}, 错误: {e}")
                
    def _start_device_behavior_simulation(self):
        """启动设备行为仿真（电池、移动性等）"""
        profile = self.device_profile
        
        if profile.mobility_pattern == "intermittent":
            # 间歇性设备仿真
            threading.Thread(target=self._simulate_intermittent_connectivity, daemon=True).start()
            
        if profile.battery_constraint:
            # 电池约束仿真
            threading.Thread(target=self._simulate_battery_drain, daemon=True).start()
    
    def _simulate_intermittent_connectivity(self):
        """仿真间歇性连接"""
        if not self.container:
            return
            
        while True:
            try:
                # 根据可用性比例决定连接状态
                if random.random() > self.device_profile.availability_ratio:
                    # 设备离线
                    offline_duration = random.randint(10, 60)  # 10-60秒离线
                    self.container.pause()
                    print(f"📱 设备{self.client_id}离线 {offline_duration}秒")
                    time.sleep(offline_duration)
                    
                    # 重新上线
                    self.container.unpause() 
                    print(f"📱 设备{self.client_id}重新上线")
                    
                # 在线时间
                online_duration = random.randint(60, 300)  # 1-5分钟在线
                time.sleep(online_duration)
                
            except Exception as e:
                print(f"间歇性连接仿真错误: {e}")
                break
                
    def _simulate_battery_drain(self):
        """仿真电池消耗"""
        if not self.container:
            return
            
        # 仿真电池级别 (30分钟后低电量模式)
        battery_life = 30 * 60  # 30分钟
        low_battery_threshold = 0.2  # 20%电量
        
        time.sleep(battery_life * (1 - low_battery_threshold))
        
        # 进入低电量模式（降低性能） 
        print(f"🔋 设备{self.client_id}进入低电量模式")
        
        # 模拟性能下降（减少CPU限额）
        try:
            # 将CPU使用率降低到70%
            cpu_quota = int(float(self.device_profile.cpu_limit) * 0.7 * 100000)
            self.container.update(cpu_quota=cpu_quota)
        except Exception as e:
            print(f"更新CPU限额失败: {e}")
            
        # 等待剩余电量耗尽
        time.sleep(battery_life * low_battery_threshold)
        
        # 电量耗尽，设备关机
        print(f"🥐 设备{self.client_id}电量耗尽，自动关机")
        try:
            self.container.stop()
        except Exception as e:
            print(f"关机失败: {e}")
    
    def get_status(self) -> Dict:
        """获取Docker容器状态"""
        if self.container is None:
            return {"status": "not_started"}
        
        try:
            self.container.reload()
            return {
                "status": self.container.status,
                "container_id": self.container.id[:12],
                "client_id": self.client_id,
                "device_type": self.device_profile.device_type,
                "node_ip": self.node_ip,
                "client_port": self.client_port,
                "container_name": self.container_name,
                "cpu_limit": self.device_profile.cpu_limit,
                "memory_limit": self.device_profile.memory_limit,
                "network_profile": {
                    "bandwidth_up": f"{self.device_profile.bandwidth_up_kbps}kbps",
                    "bandwidth_down": f"{self.device_profile.bandwidth_down_kbps}kbps", 
                    "latency": f"{self.device_profile.latency_ms}ms",
                    "packet_loss": f"{self.device_profile.packet_loss_rate*100}%"
                }
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}
    
    def stop(self):
        """停止Docker容器"""
        if self.container:
            try:
                self.container.stop(timeout=10)
            except Exception as e:
                print(f"停止客户端{self.client_id}容器失败: {e}")
                
    def simulate_device_failure(self, duration: int = 60):
        """仿真设备故障"""
        if self.container:
            self.container.pause()
            print(f"🔴 设备{self.client_id}发生故障，离线{duration}秒")
            
            # 定时恢复
            def recover():
                time.sleep(duration)
                try:
                    if self.container.status == "paused":
                        self.container.unpause()
                        print(f"�️ 设备{self.client_id}故障恢复")
                except Exception as e:
                    print(f"设备恢复失败: {e}")
            
            threading.Thread(target=recover, daemon=True).start()

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
        # 检查CUDA可用性
        use_gpu = False
        try:
            import torch
            use_gpu = torch.cuda.is_available()
            if not use_gpu:
                self.logger.warning("⚠️ CUDA不可用，所有节点将使用CPU模式")
        except ImportError:
            self.logger.warning("⚠️ PyTorch未安装，使用CPU模式")
            
        return {
            'use_gpu': use_gpu,
            'device': 0 if use_gpu else -1,  # GPU设备ID或CPU模式
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
                'connections': CONFIG.TOPOLOGY_CONNECTIONS,
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
        
    def allocate_gpu_resources(self) -> List[Optional[float]]:
        """动态分数GPU资源分配：服务器使用CPU，所有节点使用分数GPU"""
        cluster_resources = ray.cluster_resources()
        available_gpus = float(cluster_resources.get('GPU', 0))
        
        if available_gpus == 0:
            self.logger.warning("⚠️ 未检测到GPU，所有节点使用CPU模式")
            return [None] + [None] * CONFIG.CLIENT_NUM
        
        # 🖥️ 服务器固定使用CPU（不分配GPU）
        gpu_allocation = [None]  # 服务器CPU模式
        
        # 🎮 定义设备性能对应的GPU资源分配比例
        device_gpu_ratios = {
            "edge_server": 0.5,        # 边缘服务器 - 高性能 50%
            "smartphone_high": 0.35,   # 高端手机 - 中等性能 35% 
            "smartphone_low": 0.25,    # 低端手机 - 低性能 25%
            "raspberry_pi": 0.25,      # 树莓派 - 低性能 25%
            "iot_device": 0.15,        # IoT设备 - 最低性能 15%
        }
        
        # 计算所有客户端需要的GPU总量
        total_required_gpu = 0.0
        client_requirements = []
        
        # 只使用ratio>0的设备类型进行GPU分配
        active_device_types = [dt for dt, ratio in CONFIG.DEVICE_DISTRIBUTION.items() if ratio > 0]
        if not active_device_types:
            active_device_types = ["smartphone_high"]  # 默认设备类型
            
        for i in range(CONFIG.CLIENT_NUM):
            device_type = active_device_types[i % len(active_device_types)]
            required_gpu = device_gpu_ratios.get(device_type, 0.3)  # 默认0.3
            client_requirements.append((device_type, required_gpu))
            total_required_gpu += required_gpu
        
        # 🎯 智能缩放：始终保持90-100%的GPU利用率
        target_utilization = 0.80  # 目标80%利用率（降低资源竞争）
        target_gpu_usage = available_gpus * target_utilization
        scaling_factor = target_gpu_usage / total_required_gpu
        
        if scaling_factor < 1.0:
            self.logger.warning(f"⚠️ GPU需求({total_required_gpu:.2f}) 超过目标使用量({target_gpu_usage:.2f})，向下缩放({scaling_factor:.2f})")
        elif scaling_factor > 1.0:
            self.logger.info(f"🚀 GPU充足({total_required_gpu:.2f} < {target_gpu_usage:.2f})，向上扩展({scaling_factor:.2f})提升性能")
        else:
            self.logger.info(f"⚡ GPU分配已优化，达到目标利用率({target_utilization*100:.0f}%)")
        
        # 分配GPU资源给每个客户端
        actual_total_gpu = 0.0
        for device_type, required_gpu in client_requirements:
            allocated_gpu = required_gpu * scaling_factor
            gpu_allocation.append(allocated_gpu)
            actual_total_gpu += allocated_gpu
        
        # 生成分配摘要
        device_summary = {}
        for (device_type, _), allocated_gpu in zip(client_requirements, gpu_allocation[1:]):
            if device_type not in device_summary:
                device_summary[device_type] = {'count': 0, 'total_gpu': 0.0}
            device_summary[device_type]['count'] += 1
            device_summary[device_type]['total_gpu'] += allocated_gpu
        
        gpu_summary = {
            "total_available_gpus": available_gpus,
            "total_allocated_gpus": actual_total_gpu,
            "utilization_rate": f"{(actual_total_gpu/available_gpus)*100:.1f}%",
            "server": "CPU only",
            "scaling_factor": scaling_factor,
            "device_allocation": device_summary
        }
        
        self.logger.info(f"🎯 分数GPU资源分配: {gpu_summary}")
        self.logger.info(f"📋 客户端详细分配: {[f'{alloc:.2f}' if alloc else 'CPU' for alloc in gpu_allocation[1:]]}")
        return gpu_allocation
    
    def _create_diverse_device_fleet(self, num_devices: int) -> List[EdgeDeviceProfile]:
        """创建多样化的边缘设备队列"""
        device_assignments = []
        device_types = list(CONFIG.DEVICE_DISTRIBUTION.keys())
        
        # 按照配置的分布比例分配设备类型
        for device_type, ratio in CONFIG.DEVICE_DISTRIBUTION.items():
            if ratio <= 0:  # 跳过比例为0的设备类型
                continue
            count = max(1, int(num_devices * ratio))
            
            for i in range(count):
                if len(device_assignments) >= num_devices:
                    break
                    
                # 获取基础设备档案并创建变体
                base_profile = EDGE_DEVICE_PROFILES[device_type]
                device_variant = self._create_device_variant(base_profile, len(device_assignments) + 1)
                device_assignments.append(device_variant)
        
        # 如果设备不够，随机添加（只从比例>0的设备类型中选择）
        while len(device_assignments) < num_devices:
            available_types = [dt for dt, ratio in CONFIG.DEVICE_DISTRIBUTION.items() if ratio > 0]
            if not available_types:
                break  # 没有可用的设备类型
            device_type = random.choice(available_types)
            base_profile = EDGE_DEVICE_PROFILES[device_type]
            device_variant = self._create_device_variant(base_profile, len(device_assignments) + 1)
            device_assignments.append(device_variant)
        
        # 记录设备分布
        distribution_summary = {}
        for assignment in device_assignments:
            device_type = assignment.device_type
            distribution_summary[device_type] = distribution_summary.get(device_type, 0) + 1
        
        self.logger.info(f"📱 边缘设备分布: {distribution_summary}")
        return device_assignments[:num_devices]
    
    def _create_device_variant(self, base_profile: EdgeDeviceProfile, device_id: int) -> EdgeDeviceProfile:
        """创建设备变体（增加真实性）"""
        import copy
        variant = copy.deepcopy(base_profile)
        
        # 设备ID唯一化
        variant.device_id = f"{base_profile.device_id}_{device_id}"
        
        # 添加随机变化（±20%）
        variation_factor = random.uniform(0.8, 1.2)
        
        # CPU变化
        base_cpu = float(variant.cpu_limit)
        variant.cpu_limit = f"{base_cpu * variation_factor:.1f}"
        
        # 网络变化
        variant.bandwidth_up_kbps = int(variant.bandwidth_up_kbps * variation_factor)
        variant.bandwidth_down_kbps = int(variant.bandwidth_down_kbps * variation_factor)
        variant.latency_ms = max(10, int(variant.latency_ms * variation_factor))
        
        # 可用性随机化
        variant.availability_ratio *= random.uniform(0.9, 1.0)
        
        return variant
    
    def _get_ray_resources_for_device(self, device_profile: EdgeDeviceProfile) -> Dict[str, Any]:
        """根据设备类型获取Ray资源分配"""
        base_cpu = max(0.1, float(device_profile.cpu_limit))
        
        # 从设备配置中解析内存限制
        memory_str = device_profile.memory_limit.lower()
        if memory_str.endswith('g'):
            memory_bytes = int(float(memory_str[:-1]) * 1024 * 1024 * 1024)
        elif memory_str.endswith('m'):
            memory_bytes = int(float(memory_str[:-1]) * 1024 * 1024)
        else:
            memory_bytes = 1024 * 1024 * 1024  # 默认1GB
        
        # 根据设备类型调整CPU资源
        if device_profile.device_type == "iot":
            return {"num_cpus": 0.2, "memory": memory_bytes}
        elif device_profile.device_type == "smartphone":
            return {"num_cpus": base_cpu, "memory": memory_bytes}
        elif device_profile.device_type == "edge_server":
            return {"num_cpus": base_cpu, "memory": memory_bytes}
        else:  # raspberry_pi, edge_device
            return {"num_cpus": base_cpu, "memory": memory_bytes}
    
    def cleanup_environment(self):
        """清理环境"""
        if self.cleanup_performed:
            return
            
        self.logger.info("🧹 清理环境...")
        
        # 停止旧进程
        subprocess.run(['pkill', '-9', '-f', 'python.*federatedscope'], 
                      capture_output=True, check=False)
        
        # 清理数据库文件
        try:
            import glob
            db_files = glob.glob('tmp/client_*/client_*_chunks.db')
            for db_file in db_files:
                if os.path.exists(db_file):
                    os.remove(db_file)
        except Exception as e:
            self.logger.debug(f"清理数据库文件失败: {e}")
        
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
        
        # 初始化Docker环境 (如果启用)
        if CONFIG.USE_DOCKER:
            self.docker_manager = DockerManager()
            if not self.docker_manager.docker_available:
                self.logger.warning("⚠️  Docker不可用，切换到非容器模式")
                CONFIG.USE_DOCKER = False
                # 继续使用非Docker模式
            else:
                # 检查并确保Docker镜像就绪
                self.logger.info("🔍 检查Docker镜像...")
                if not self.docker_manager.ensure_images_ready():
                    self.logger.warning("⚠️  Docker镜像未就绪，切换到非容器模式")
                    CONFIG.USE_DOCKER = False
                else:
                    # 设置Docker网络环境
                    if not self.docker_manager.setup_docker_environment():
                        self.logger.error("❌ Docker环境设置失败，切换到非容器模式")
                        CONFIG.USE_DOCKER = False
                    else:
                        self.logger.info("✅ Docker环境和镜像初始化成功")
        
        # 初始化Ray
        self.initialize_ray_cluster()
        
        # GPU资源分配
        gpu_allocation = self.allocate_gpu_resources()
        server_gpu = gpu_allocation[0]
        client_gpus = gpu_allocation[1:]
        
        # 生成配置
        base_config = self.generate_base_config()
        
        # 🖥️ 启动服务器（固定使用CPU资源）
        server_config = base_config.copy()
        server_config['distribute']['role'] = 'server'
        server_config['use_gpu'] = False  # 服务器强制使用CPU
        
        # 服务器资源配置：只使用CPU，不占用GPU
        server_resources = {"num_cpus": 2}
        # 注意：server_gpu始终为None，服务器不使用GPU
        
        # 根据Docker可用性选择Actor类型
        if CONFIG.USE_DOCKER:
            self.server_actor = DockerFederatedScopeServer.options(**server_resources).remote(
                server_config, server_gpu
            )
        else:
            self.server_actor = FallbackFederatedScopeServer.options(**server_resources).remote(
                server_config, server_gpu
            )
        
        server_ip, server_port = ray.get(self.server_actor.start.remote())
        self.server_info = (server_ip, server_port)
        
        self.logger.info(f"✅ 服务器已启动: {server_ip}:{server_port}")
        time.sleep(3)
        
        # 创建多样化的边缘设备
        device_assignments = self._create_diverse_device_fleet(CONFIG.CLIENT_NUM)
        
        # 启动Docker客户端
        successful_clients = 0
        for i, device_profile in enumerate(device_assignments):
            client_id = i + 1
            
            client_config = base_config.copy()
            client_config['distribute']['role'] = 'client'
            
            # Ray资源分配（基于设备类型和GPU分配）
            client_resources = self._get_ray_resources_for_device(device_profile)
            
            # 🎮 添加分数GPU资源分配（所有客户端都使用GPU）
            client_gpu = client_gpus[i] if i < len(client_gpus) else None
            if client_gpu is not None:
                client_resources["num_gpus"] = client_gpu  # 分配分数GPU资源
            
            try:
                # 根据Docker可用性选择Actor类型
                if CONFIG.USE_DOCKER:
                    client_actor = DockerFederatedScopeClient.options(**client_resources).remote(
                        client_id, client_config, server_ip, server_port, device_profile
                    )
                else:
                    client_actor = FallbackFederatedScopeClient.options(**client_resources).remote(
                        client_id, client_config, server_ip, server_port, device_profile
                    )
                
                self.client_actors.append(client_actor)
                
                # 启动客户端容器
                start_result = ray.get(client_actor.start.remote())
                if start_result:
                    successful_clients += 1
                    self.logger.info(f"✅ 客户端{client_id} ({device_profile.device_type}) 启动成功")
                else:
                    self.logger.error(f"❌ 客户端{client_id} ({device_profile.device_type}) 启动失败")
                    
                time.sleep(3)  # 给Docker容器更多启动时间
                
            except Exception as e:
                self.logger.error(f"❌ 客户端{client_id} 创建失败: {e}")
        
        if successful_clients < CONFIG.CLIENT_NUM * 0.7:  # 至少70%成功
            self.logger.error(f"❌ 客户端启动成功率过低: {successful_clients}/{CONFIG.CLIENT_NUM}")
            return
            
        self.logger.info(f"✅ {successful_clients}/{CONFIG.CLIENT_NUM} 个Docker客户端启动成功")
        
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
            
            # 计算实际GPU使用情况
            total_gpus = float(cluster_resources.get('GPU', 0))
            available_gpus = float(available_resources.get('GPU', 0))
            gpu_used = total_gpus - available_gpus
            
            # 统计运行中的客户端（所有客户端都使用分数GPU）
            total_clients = sum(1 for s in client_statuses if s["status"] == "running")
            
            self.logger.info(
                f"⏰ {elapsed}s | 服务器: {server_status['status']} (CPU) | "
                f"客户端: {total_clients}个节点(分数GPU) | "
                f"Ray GPU使用: {gpu_used:.1f}/{total_gpus:.1f} ({(gpu_used/total_gpus)*100:.1f}%)"
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
        """停止所有进程和Docker容器"""
        self.logger.info("🛑 停止所有联邦学习进程...")
        
        # 停止Ray Actors
        try:
            if self.server_actor:
                ray.get(self.server_actor.stop.remote())
            
            if self.client_actors:
                stop_futures = [actor.stop.remote() for actor in self.client_actors]
                ray.get(stop_futures)
        except Exception as e:
            self.logger.warning(f"⚠️  Ray Actors停止警告: {e}")
        
        # 清理Docker环境
        if CONFIG.USE_DOCKER and hasattr(self, 'docker_manager'):
            try:
                self.docker_manager.cleanup_docker_environment()
                self.logger.info("✅ Docker环境已清理")
            except Exception as e:
                self.logger.warning(f"⚠️  Docker清理警告: {e}")
        
        self.logger.info("✅ 所有资源已停止")
        
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
    docker_status = "✅ 启用" if CONFIG.USE_DOCKER else "❌ 禁用"
    network_sim_status = "✅ 启用" if CONFIG.ENABLE_NETWORK_SIMULATION else "❌ 禁用"
    
    banner = f"""
{'='*80}
🚀 Ray-Powered FederatedScope V2 Script (Docker Edition)
{'='*80}
📊 配置信息:
   • 客户端数量: {CONFIG.CLIENT_NUM}
   • 训练轮数: {CONFIG.TOTAL_ROUNDS}
   • 数据集: {CONFIG.DATASET}
   • 模型: {CONFIG.MODEL_TYPE}
   • Chunk数: {CONFIG.CHUNK_NUM}
   • 重要度方法: {CONFIG.IMPORTANCE_METHOD}
   • 监控时长: {CONFIG.MONITOR_DURATION}s

🐳 Docker模式: {docker_status}
🌐 网络仿真: {network_sim_status}
📱 设备分布: {dict(CONFIG.DEVICE_DISTRIBUTION)}

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