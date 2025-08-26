#!/usr/bin/env python3
"""
Ray-Powered FederatedScope V2 Script
===================================

One-click distributed federated learning, completely replacing traditional shell scripts
- Automatic GPU resource management and allocation
- Dynamic IP port allocation
- Real-time monitoring and logging
- Cloud server extension support

Run directly to start complete FL system
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
# 🔧 Configuration Area - All settings centralized here for easy modification
# ============================================================================

@dataclass
class EdgeDeviceProfile:
    """Edge device hardware and network configuration profile"""
    device_id: str
    device_type: str  # "smartphone_high", "smartphone_low", "raspberry_pi", "iot_device", "edge_server"
    docker_image: str
    
    # Hardware resource limits
    cpu_limit: str = "0.5"                 # CPU limit (Docker format)
    memory_limit: str = "1g"               # Memory limit (Docker format) 
    storage_limit: str = "8g"              # Storage limit
    
    # Network characteristics
    bandwidth_up_kbps: int = 10000         # Uplink bandwidth (kbps)
    bandwidth_down_kbps: int = 50000       # Downlink bandwidth (kbps)
    latency_ms: int = 50                   # Network latency (milliseconds)
    packet_loss_rate: float = 0.00        # Packet loss rate (0-1)
    jitter_ms: int = 10                    # Network jitter (milliseconds)
    
    # Device characteristics
    training_speed_multiplier: float = 1.0  # Training speed multiplier
    availability_ratio: float = 0.9        # Availability ratio (0-1)
    battery_constraint: bool = False        # Whether has battery constraint
    mobility_pattern: str = "static"        # Mobility pattern: static, mobile, intermittent

@dataclass
class FLConfig:
    """Federated learning configuration parameters"""
    
    # === Basic Settings ===
    CLIENT_NUM: int = 2                     # Number of clients (for testing)
    TOTAL_ROUNDS: int = 20                   # Increased rounds for better convergence
    CHUNK_NUM: int = 10                    # Number of model chunks per client
    IMPORTANCE_METHOD: str = "snip"         # Chunk importance method: magnitude, l2_norm, snip, fisher
    
    # === Dataset Settings ===
    # CNN Settings (uncomment for computer vision tasks)
    # DATASET: str = "CIFAR10@torchvision"   # CNN Dataset
    # BATCH_SIZE: int = 32                   # CNN Batch size
    # DATA_SPLIT_ALPHA: float = 0.1          # LDA data split parameter
    
    # Transformer/NLP Settings (current active)
    DATASET: str = "shakespeare"            # Shakespeare text dataset
    BATCH_SIZE: int = 8                     # Small batch size for testing
    DATA_SPLIT_ALPHA: float = 0.5          # LDA data split parameter (less extreme split)
    
    # === Model Settings ===
    # CNN Model Settings (uncomment for computer vision tasks)
    # MODEL_TYPE: str = "convnet2"          # CNN Model type
    # MODEL_HIDDEN: int = 512               # Hidden layer size
    # MODEL_OUT_CHANNELS: int = 10          # Number of output channels for CNN
    # MODEL_DROPOUT: float = 0.0            # Dropout rate
    
    # Transformer/NLP Model Settings (current active)
    MODEL_TYPE: str = "lstm"                # LSTM model for text
    MODEL_HIDDEN: int = 256                 # Increased hidden size for better capacity
    MODEL_OUT_CHANNELS: int = 80            # Vocab size for shakespeare
    MODEL_DROPOUT: float = 0.1             # Add dropout for regularization
    
    # === Docker Settings ===
    USE_DOCKER: bool = True               # Disable Docker for testing (enable after rebuilding image)
    BASE_DOCKER_IMAGE: str = "federatedscope:base"  # Base image
    DOCKER_NETWORK_NAME: str = "fl_network"         # Docker network name
    ENABLE_NETWORK_SIMULATION: bool = True          # Enable network simulation
    
    # === Network Simulation Settings ===
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
    
    # === Device Distribution Configuration ===
    DEVICE_DISTRIBUTION: Dict[str, float] = field(default_factory=lambda: {
        "smartphone_high": 1,    # 20% High-end smartphones
        "smartphone_low": 0,     # 40% Low-end smartphones
        "raspberry_pi": 0,       # 15% Raspberry Pi
        "iot_device": 0,         # 20% IoT devices
        "edge_server": 0         # 5% Edge servers
    })
    
    # === Training Settings ===
    LOCAL_UPDATE_STEPS: int = 5           # Increased local training steps
    LEARNING_RATE: float = 0.001          # Reduced learning rate for LSTM stability
    OPTIMIZER: str = "Adam"               # Adam optimizer for better convergence
    WEIGHT_DECAY: float = 0.0001          # Weight decay
    GRAD_CLIP: float = 1.0                # Reduced gradient clipping for LSTM
    
    # === BitTorrent Settings ===
    BITTORRENT_TIMEOUT: float = 600.0     # BitTorrent timeout
    BT_CHUNK_SELECTION: str = "rarest_first"  # Chunk selection strategy
    BT_MIN_COMPLETION_RATIO: float = 0.8   # Minimum completion ratio
    
    # === Topology Settings ===
    TOPOLOGY_TYPE: str = "random"         # Topology type: star, ring, fully_connected, mesh, random
    TOPOLOGY_TIMEOUT: float = 600.0       # Topology construction timeout
    TOPOLOGY_CONNECTIONS: int = 2         # Connections per node (mesh: exact connections, random: minimum connections)
    
    # === Network Simulation Settings ===
    ENABLE_NETWORK_SIMULATION: bool = True # Enable network simulation
    DOCKER_BASE_IMAGE: str = "federatedscope:base"  # Docker base image
    
    # === Ray Resource Settings ===
    RAY_AUTO_GPU_DETECTION: bool = True   # Automatic GPU detection
    RAY_MAX_CPUS: Optional[int] = None     # Maximum CPU count (None=auto)
    RAY_MAX_GPUS: Optional[int] = None     # Maximum GPU count (None=auto)
    
    # === Monitoring Settings ===
    MONITOR_DURATION: int = 999999999           # Monitoring duration (seconds)
    LOG_LEVEL: str = "INFO"               # Log level
    ENABLE_RAY_DASHBOARD: bool = True     # Enable Ray Dashboard
    
    # === Output Settings ===
    OUTPUT_DIR: str = "ray_v2_output"     # Output directory
    LOG_DIR: str = "logs"                 # Log directory

# Edge device configuration profile library
EDGE_DEVICE_PROFILES = {
    "smartphone_high": EdgeDeviceProfile(
        device_id="smartphone_high",
        device_type="smartphone", 
        docker_image="federatedscope:base",  # Temporarily use base image
        cpu_limit="1.0", memory_limit="2g", storage_limit="32g",
        bandwidth_up_kbps=50000, bandwidth_down_kbps=100000,
        latency_ms=20, packet_loss_rate=0.005, jitter_ms=5,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        mobility_pattern="static"
    ),
    
    "smartphone_low": EdgeDeviceProfile(
        device_id="smartphone_low", 
        device_type="smartphone",
        docker_image="federatedscope:base",  # Temporarily use base image
        cpu_limit="0.3", memory_limit="2g", storage_limit="8g",
        bandwidth_up_kbps=5000, bandwidth_down_kbps=20000,
        latency_ms=100, packet_loss_rate=0.02, jitter_ms=20,
        training_speed_multiplier=0.6, availability_ratio=1.0,
        battery_constraint=False, mobility_pattern="static"
    ),
    
    "raspberry_pi": EdgeDeviceProfile(
        device_id="raspberry_pi",
        device_type="edge_device",
        docker_image="federatedscope:base",  # Temporarily use base image
        cpu_limit="0.6", memory_limit="2g", storage_limit="64g",
        bandwidth_up_kbps=10000, bandwidth_down_kbps=50000,
        latency_ms=30, packet_loss_rate=0.01, jitter_ms=10,
        training_speed_multiplier=0.7, availability_ratio=1.0,
        mobility_pattern="static"
    ),
    
    "iot_device": EdgeDeviceProfile(
        device_id="iot_device",
        device_type="iot",
        docker_image="federatedscope:base",  # Temporarily use base image
        cpu_limit="0.1", memory_limit="2g", storage_limit="2g", 
        bandwidth_up_kbps=128, bandwidth_down_kbps=512,
        latency_ms=300, packet_loss_rate=0.05, jitter_ms=50,
        training_speed_multiplier=0.3, availability_ratio=1.0,
        battery_constraint=False, mobility_pattern="static"
    ),
    
    "edge_server": EdgeDeviceProfile(
        device_id="edge_server",
        device_type="edge_server", 
        docker_image="federatedscope:base",  # Temporarily use base image
        cpu_limit="2.0", memory_limit="2g", storage_limit="100g",
        bandwidth_up_kbps=100000, bandwidth_down_kbps=1000000,
        latency_ms=10, packet_loss_rate=0.001, jitter_ms=2,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        mobility_pattern="static"
    )
}

# Create global configuration instance
CONFIG = FLConfig()

# ============================================================================
# 🌐 Network Simulation Classes
# ============================================================================

class NetworkSimulator:
    """Docker container network simulation controller"""
    
    def __init__(self):
        self.active_limitations = {}
        
    def apply_network_constraints(self, container, profile: EdgeDeviceProfile):
        """Apply network constraints to Docker container"""
        if not CONFIG.ENABLE_NETWORK_SIMULATION:
            return True
            
        try:
            # Install and configure tc (traffic control) inside container
            setup_commands = [
                # Install iproute2 (includes tc command)
                "apt-get update -qq && apt-get install -y iproute2 > /dev/null 2>&1 || apk add iproute2 > /dev/null 2>&1 || true",
                
                # Remove existing queue rules
                "tc qdisc del dev eth0 root 2>/dev/null || true",
                
                # Create root HTB queue
                "tc qdisc add dev eth0 root handle 1: htb default 30",
                
                # Set total bandwidth limit (upstream)
                f"tc class add dev eth0 parent 1: classid 1:1 htb rate {profile.bandwidth_up_kbps}kbit ceil {profile.bandwidth_up_kbps}kbit",
                
                # Add network delay, jitter, and packet loss
                f"tc qdisc add dev eth0 parent 1:1 handle 10: netem delay {profile.latency_ms}ms {profile.jitter_ms}ms loss {profile.packet_loss_rate * 100}%"
            ]
            
            for cmd in setup_commands:
                result = container.exec_run(
                    f"sh -c '{cmd}'", 
                    privileged=True,
                    user="root"
                )
                # Record network configuration results but do not terminate (some commands may fail without affecting overall operation)
                if result.exit_code != 0 and "tc qdisc add" in cmd:
                    print(f"⚠️  Network configuration warning - Client {profile.device_id}: {result.output.decode()[:100]}")
            
            # Record successfully applied limitations
            container_name = container.name
            self.active_limitations[container_name] = {
                "bandwidth_up_kbps": profile.bandwidth_up_kbps,
                "latency_ms": profile.latency_ms,
                "packet_loss_rate": profile.packet_loss_rate,
                "jitter_ms": profile.jitter_ms
            }
            
            return True
            
        except Exception as e:
            print(f"❌ Network constraint application failed - {profile.device_id}: {e}")
            return False
    
    def simulate_network_fluctuation(self, container_name: str, duration: int = 60):
        """Simulate network fluctuation"""
        if container_name not in self.active_limitations:
            return
            
        def fluctuation_thread():
            try:
                docker_client = docker.from_env()
                container = docker_client.containers.get(container_name)
                base_config = self.active_limitations[container_name]
                
                for _ in range(duration):
                    # Randomly change network conditions (±30%)
                    multiplier = random.uniform(0.7, 1.3)
                    
                    new_bandwidth = int(base_config["bandwidth_up_kbps"] * multiplier)
                    new_latency = max(10, int(base_config["latency_ms"] * multiplier))
                    
                    # Update network limits
                    fluctuation_cmd = f"tc class change dev eth0 classid 1:1 htb rate {new_bandwidth}kbit ceil {new_bandwidth}kbit"
                    container.exec_run(f"sh -c '{fluctuation_cmd}'", privileged=True)
                    
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Network fluctuation simulation error: {e}")
        
        # Start background thread to execute network fluctuation
        threading.Thread(target=fluctuation_thread, daemon=True).start()
    
    def get_network_stats(self, container_name: str) -> Dict:
        """Get container network statistics information"""
        try:
            docker_client = docker.from_env()
            container = docker_client.containers.get(container_name)
            
            # Execute network statistics command
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
            return {"error": "Unable to get network statistics"}
            
        except Exception as e:
            return {"error": str(e)}

class DockerManager:
    """Docker environment manager with local storage configuration"""
    
    def __init__(self):
        self.client = None
        self.network_simulator = NetworkSimulator()
        self.fl_network = None
        self.local_docker_root = f"{os.getcwd()}/docker_data"
        self.docker_available = self._check_docker_availability()
        
        if self.docker_available:
            try:
                # Configure Docker client to use local storage
                self.client = docker.from_env()
                self._setup_local_docker_storage()
            except Exception as e:
                print(f"⚠️  Docker connection failed: {e}")
                self.docker_available = False
                
    def _setup_local_docker_storage(self):
        """Setup local Docker storage directory and check system disk space"""
        try:
            # Check system disk space first
            statvfs = os.statvfs('/')
            free_space_gb = (statvfs.f_frsize * statvfs.f_bavail) / (1024**3)
            total_space_gb = (statvfs.f_frsize * statvfs.f_blocks) / (1024**3)
            used_percent = ((total_space_gb - free_space_gb) / total_space_gb) * 100
            
            if used_percent > 85:
                current_dir = os.getcwd()
                parent_dir = os.path.dirname(current_dir)
                suggested_docker_root = os.path.join(parent_dir, "docker")
                
                print(f"⚠️  WARNING: System disk is {used_percent:.1f}% full ({free_space_gb:.1f}GB free)")
                print(f"   Docker images are stored in system disk (/var/lib/docker)")
                print(f"   💡 To move Docker data to local disk, create /etc/docker/daemon.json:")
                print(f'   {{')
                print(f'     "data-root": "{suggested_docker_root}"')
                print(f'   }}')
                print(f"   Then run: sudo systemctl restart docker")
                print(f"   This will move Docker data from system to data disk.")
            
            # Create local Docker storage directories (for temp files only)
            storage_dirs = [
                f"{self.local_docker_root}/images",
                f"{self.local_docker_root}/containers", 
                f"{self.local_docker_root}/volumes",
                f"{self.local_docker_root}/buildkit",
                f"{self.local_docker_root}/tmp"
            ]
            
            for storage_dir in storage_dirs:
                os.makedirs(storage_dir, exist_ok=True)
            
            print(f"🐳 Docker temp storage configured: {self.local_docker_root}")
            
            # Set environment variables for Docker temp files
            docker_env_vars = {
                'DOCKER_TMPDIR': f"{self.local_docker_root}/tmp",
                'DOCKER_HOST': os.getenv('DOCKER_HOST', 'unix:///var/run/docker.sock')
            }
            
            for key, value in docker_env_vars.items():
                os.environ[key] = value
                
        except Exception as e:
            print(f"⚠️  Failed to setup local Docker storage: {e}")
                
    def _check_docker_availability(self) -> bool:
        """Check if Docker is available"""
        try:
            # Quick check if Docker command exists
            result = subprocess.run(['which', 'docker'], 
                                  capture_output=True, text=True, timeout=2)
            if result.returncode != 0:
                print(f"⚠️  Docker command not installed")
                return False
            
            # Check Docker version
            result = subprocess.run(['docker', '--version'], 
                                  capture_output=True, text=True, 
                                  check=True, timeout=3)
            print(f"🐳 Docker version: {result.stdout.strip()}")
            
            # Quick check Docker service status
            result = subprocess.run(['docker', 'ps', '--format', 'table {{.ID}}'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                print(f"✅ Docker service running normally")
                return True
            else:
                print(f"⚠️  Docker service not running: {result.stderr.strip()[:100]}")
                return False
            
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
            print(f"⚠️  Docker not available: {e}")
            return False
        
    def setup_docker_environment(self):
        """Set up Docker environment"""
        try:
            # Create dedicated network
            try:
                self.fl_network = self.client.networks.get(CONFIG.DOCKER_NETWORK_NAME)
                print(f"📶 Using existing Docker network: {CONFIG.DOCKER_NETWORK_NAME}")
            except docker.errors.NotFound:
                self.fl_network = self.client.networks.create(
                    CONFIG.DOCKER_NETWORK_NAME,
                    driver="bridge",
                    options={
                        "com.docker.network.bridge.enable_icc": "true",
                        "com.docker.network.bridge.enable_ip_masquerade": "true"
                    }
                )
                print(f"📶 Creating Docker network: {CONFIG.DOCKER_NETWORK_NAME}")
            
            return True
            
        except Exception as e:
            print(f"❌ Docker environment setup failed: {e}")
            return False
    
    def cleanup_docker_environment(self):
        """Clean up Docker environment"""
        try:
            # Stop and remove all FL-related containers
            containers = self.client.containers.list(all=True)
            for container in containers:
                if container.name.startswith('fl_'):
                    try:
                        container.stop(timeout=10)
                        container.remove()
                        print(f"🗑️  Cleaning up container: {container.name}")
                    except:
                        pass
            
            # Remove network (if no other containers are using it)
            if self.fl_network:
                try:
                    self.fl_network.remove()
                    print(f"🗑️  Cleaning up network: {CONFIG.DOCKER_NETWORK_NAME}")
                except:
                    pass
            
            # Clean up local Docker storage temp files (keep directories for reuse)
            try:
                temp_dir = f"{self.local_docker_root}/tmp"
                if os.path.exists(temp_dir):
                    subprocess.run(['rm', '-rf', f"{temp_dir}/*"], shell=True, check=False)
                    print(f"🗑️  Cleaning up Docker temp storage: {temp_dir}")
            except:
                pass
                    
        except Exception as e:
            print(f"⚠️  Docker cleanup warning: {e}")
    
    def check_required_images(self) -> bool:
        """Check if required Docker images exist"""
        if not self.docker_available:
            return False
        
        try:
            # Simplified version: only check base image, all devices use the same image
            required_images = [
                CONFIG.DOCKER_BASE_IMAGE,  # "federatedscope:base"
            ]
            
            missing_images = []
            for image_name in required_images:
                try:
                    self.client.images.get(image_name)
                    print(f"✅ Docker image exists: {image_name}")
                except docker.errors.ImageNotFound:
                    missing_images.append(image_name)
                    print(f"❌ Docker image missing: {image_name}")
            
            if missing_images:
                print(f"\n🚨 Missing {len(missing_images)} Docker images: {missing_images}")
                return False
            else:
                print("✅ All Docker images are ready")
                return True
                
        except Exception as e:
            print(f"❌ Error checking Docker images: {e}")
            return False
    
    def build_required_images(self) -> bool:
        """Automatically build required Docker images"""
        if not self.docker_available:
            return False
        
        print("🐳 Starting automatic build of FederatedScope Docker images...")
        
        # Simplified version: only build base image, shared by all devices
        build_configs = [
            {
                "dockerfile": "docker/Dockerfile.base",
                "tag": "federatedscope:base",
                "name": "Base image"
            }
        ]
        
        build_success = True
        
        for config in build_configs:
            dockerfile_path = config["dockerfile"]
            tag = config["tag"]
            name = config["name"]
            
            # Check if Dockerfile exists
            if not os.path.exists(dockerfile_path):
                print(f"⚠️  Dockerfile does not exist: {dockerfile_path}, skipping build {tag}")
                continue
            
            print(f"📦 Building {name} ({tag})...")
            
            try:
                # Use Docker Python API to build image with local storage
                build_logs = self.client.api.build(
                    path='.',  # Build context is current directory
                    dockerfile=dockerfile_path,
                    tag=tag,
                    rm=True,  # Remove intermediate containers after build
                    decode=True,  # Decode build logs
                    pull=False,  # Don't automatically pull base image
                    buildargs={
                        'BUILDKIT_INLINE_CACHE': '1'
                    },
                    # Use local build cache directory
                    cache_from=[tag]
                )
                
                # Display build progress
                for log_line in build_logs:
                    if 'stream' in log_line:
                        log_msg = log_line['stream'].strip()
                        if log_msg and not log_msg.startswith(' ---> '):
                            print(f"   {log_msg}")
                    elif 'error' in log_line:
                        print(f"❌ Build error: {log_line['error']}")
                        build_success = False
                        break
                
                if build_success:
                    print(f"✅ {name} build successful")
                else:
                    print(f"❌ {name} build failed")
                    break
                    
            except Exception as e:
                print(f"❌ Error building {name}: {e}")
                build_success = False
                break
        
        if build_success:
            print("🎉 All Docker images built successfully!")
            # Clean up build cache to save system disk space
            try:
                print("🧹 Cleaning Docker build cache...")
                self.client.api.prune_builds()
                print("✅ Docker build cache cleaned")
            except Exception as e:
                print(f"⚠️  Warning: Failed to clean build cache: {e}")
            return True
        else:
            print("❌ Docker image build failed")
            return False
    
    def ensure_images_ready(self) -> bool:
        """Ensure required Docker images are ready (check + auto-build)"""
        if not self.docker_available:
            print("⚠️  Docker not available, skipping image check")
            return False
        
        print("🔍 Checking Docker image status...")
        
        # First check if images exist
        if self.check_required_images():
            return True
        
        # Images are incomplete, ask user if auto-build should proceed
        print("\n🤔 Auto-build missing Docker images?")
        print("   This may take 5-10 minutes...")
        
        # In automated environment, build directly without user confirmation
        if os.getenv('CI') or os.getenv('AUTOMATED_BUILD'):
            user_choice = 'y'
        else:
            user_choice = input("   Enter [y/N]: ").lower().strip()
        
        if user_choice in ['y', 'yes']:
            return self.build_required_images()
        else:
            print("⚠️  User canceled auto-build, will use non-Docker mode")
            return False

# ============================================================================
# 📊 Logging Setup
# ============================================================================

def setup_logging():
    """Set up logging system"""
    # Ensure log directory exists
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
# 🎭 Ray Actor Definitions
# ============================================================================

@ray.remote
class FallbackFederatedScopeServer:
    """Fallback FederatedScope server Actor (non-Docker mode)"""
    
    def __init__(self, config: Dict[str, Any], gpu_id: Optional[int] = None):
        self.config = config
        self.gpu_id = gpu_id
        self.process = None
        self.node_ip = ray.util.get_node_ip_address()
        self.server_port = None
        
    def start(self) -> Tuple[str, int]:
        """Start local process server"""
        # Dynamically allocate port
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()
        
        # Update configuration
        self.config['distribute']['server_host'] = self.node_ip
        self.config['distribute']['server_port'] = self.server_port
        self.config['use_gpu'] = False  # Force CPU mode
        
        # Prepare configuration file
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        os.makedirs(config_dir, exist_ok=True)
        config_path = f"{config_dir}/server.yaml"
        
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # Start process
        try:
            # Set up log file
            self.log_file = f"{CONFIG.LOG_DIR}/server.log"
            os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
            
            # Set environment variables (simple way as in original version)
            env = os.environ.copy()
            env['PYTHONPATH'] = '.'
            if self.gpu_id is not None:
                env['CUDA_VISIBLE_DEVICES'] = str(self.gpu_id)
            
            # Start process (using original version approach, including log redirection)
            cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]
            
            with open(self.log_file, 'w') as log_f:
                self.process = subprocess.Popen(
                    cmd, stdout=log_f, stderr=log_f, env=env
                )
            
            return self.node_ip, self.server_port
        except Exception as e:
            print(f"Failed to start server process: {e}")
            return None, None
    
    def get_status(self) -> Dict:
        """Get process status"""
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
        """Stop process"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()

@ray.remote
class FallbackFederatedScopeClient:
    """Fallback FederatedScope client Actor (non-Docker mode)"""
    
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
        """Start local process client"""
        # Dynamically allocate port
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()
        
        # Update client network configuration
        self.config['distribute']['server_host'] = self.server_ip
        self.config['distribute']['server_port'] = self.server_port
        self.config['distribute']['client_host'] = self.node_ip
        self.config['distribute']['client_port'] = self.client_port
        self.config['distribute']['data_idx'] = self.client_id
        
        # Client-specific seed
        self.config['seed'] = 12345 + self.client_id
        
        # GPU mode configuration - all clients use GPU (fractional allocation)
        self.config['use_gpu'] = True
        
        # Prepare configuration and output directories
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        output_dir = f"{CONFIG.OUTPUT_DIR}/client_{self.client_id}_output"
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        
        config_path = f"{config_dir}/client_{self.client_id}.yaml"
        self.config['outdir'] = output_dir
        
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # Start process
        try:
            # Set up log file
            self.log_file = f"{CONFIG.LOG_DIR}/client_{self.client_id}.log"
            os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
            
            # Set environment variables (simple approach following original version)
            env = os.environ.copy()
            env['PYTHONPATH'] = '.'
            # Note: This is FallbackFederatedScopeClient, does not use GPU
            # GPU configuration has been set in self.config
            
            # Start process (using original version approach, including log redirection)
            cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]
            
            with open(self.log_file, 'w') as log_f:
                self.process = subprocess.Popen(
                    cmd, stdout=log_f, stderr=log_f, env=env
                )
            
            return True
        except Exception as e:
            print(f"Failed to start client {self.client_id} process: {e}")
            return False
    
    def get_status(self) -> Dict:
        """Get process status"""
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
        """Stop process"""
        if self.process and self.process.poll() is None:
            self.process.terminate()
            self.process.wait()

@ray.remote
class DockerFederatedScopeServer:
    """Dockerized FederatedScope Server Actor"""
    
    def __init__(self, config: Dict[str, Any], gpu_id: Optional[int] = None):
        self.config = config
        self.gpu_id = gpu_id
        self.container = None
        self.docker_client = docker.from_env()
        self.node_ip = ray.util.get_node_ip_address()
        self.server_port = None
        self.container_name = "fl_server"
        
    def _get_absolute_path(self, path: str) -> str:
        """Safely get absolute path, handling working directory issues in Ray Actor"""
        try:
            # If path is already absolute, return directly
            if os.path.isabs(path):
                return path
            # Try to get absolute path
            return os.path.abspath(path)
        except (OSError, FileNotFoundError):
            # If current working directory doesn't exist, use fixed base directory
            base_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master"
            return os.path.join(base_dir, path)
    
    def start(self) -> Tuple[str, int]:
        """Start Docker server container"""
        # Dynamically allocate port
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()
        
        # Update configuration - Docker mode three-segment address format: bind_IP|report_IP|report_port
        container_bind_ip = '0.0.0.0'  # Container internal bind address
        external_access_ip = self.node_ip  # External access IP
        external_access_port = self.server_port  # Host mapped port
        self.config['distribute']['server_host'] = f"{container_bind_ip}|{external_access_ip}|{external_access_port}"
        self.config['distribute']['server_port'] = 50051  # Container internal port (keep integer type)
        
        if self.gpu_id is not None:
            self.config['device'] = 0  # Container internal GPU ID
            self.config['use_gpu'] = True
        
        # Prepare configuration file
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        os.makedirs(config_dir, exist_ok=True)
        config_path = f"{config_dir}/server.yaml"
        
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # Prepare log directory - use CONFIG.LOG_DIR to ensure path consistency
        log_dir = CONFIG.LOG_DIR  # Use unified log directory
        os.makedirs(log_dir, exist_ok=True)
        
        # Docker container configuration
        container_config = {
            "image": CONFIG.DOCKER_BASE_IMAGE,
            "name": self.container_name,
            "hostname": "fl-server",
            "detach": True,
            "remove": True,  # Automatically remove container after stop
            
            # Port mapping: container 50051 -> host random port
            "ports": {50051: self.server_port},
            
            # Environment variables - simple approach following original version
            "environment": {
                "PYTHONPATH": "/app"
            },
            
            # Volume mounts - fix path issues in Ray Actor, use local storage
            "volumes": {
                self._get_absolute_path(config_path): {"bind": "/app/config.yaml", "mode": "ro"},
                self._get_absolute_path(log_dir): {"bind": "/app/logs", "mode": "rw"},
                self._get_absolute_path("data"): {"bind": "/app/data", "mode": "rw"},
                f"{os.getcwd()}/docker_data/tmp": {"bind": "/tmp", "mode": "rw"}
            },
            
            # Startup command - use shell wrapper to set working directory and environment, redirect logs to mounted directory
            "command": ["sh", "-c", "cd /app && PYTHONPATH=/app python federatedscope/main.py --cfg /app/config.yaml > /app/logs/server.log 2>&1"]
        }
        
        # GPU support
        if self.gpu_id is not None:
            container_config["device_requests"] = [
                docker.types.DeviceRequest(device_ids=[str(self.gpu_id)], capabilities=[['gpu']])
            ]
        
        try:
            # Start container
            self.container = self.docker_client.containers.run(**container_config)
            return self.node_ip, self.server_port
            
        except Exception as e:
            print(f"Failed to start server container: {e}")
            return None, None
    
    def get_status(self) -> Dict:
        """Get Docker container status"""
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
        """Stop Docker container"""
        if self.container:
            try:
                self.container.stop(timeout=10)
            except Exception as e:
                print(f"Failed to stop server container: {e}")

@ray.remote
class DockerFederatedScopeClient:
    """Dockerized FederatedScope Client Actor"""
    
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
        """Safely get absolute path, handling working directory issues in Ray Actor"""
        try:
            # If path is already absolute, return directly
            if os.path.isabs(path):
                return path
            # Try to get absolute path
            return os.path.abspath(path)
        except (OSError, FileNotFoundError):
            # If current working directory doesn't exist, use fixed base directory
            base_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master"
            return os.path.join(base_dir, path)
    
    def start(self) -> bool:
        """Start Docker client container"""
        # Dynamically allocate port
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()
        
        # Apply device-specific configuration
        self._apply_device_constraints()
        
        # Update client network configuration
        self.config['distribute']['server_host'] = self.server_ip
        self.config['distribute']['server_port'] = self.server_port
        # Docker mode three-segment address format: bind_IP|report_IP|report_port
        container_bind_ip = '0.0.0.0'  # Container bind address
        external_access_ip = self.node_ip  # External access IP
        external_access_port = self.client_port  # Host mapped port
        self.config['distribute']['client_host'] = f"{container_bind_ip}|{external_access_ip}|{external_access_port}"
        self.config['distribute']['client_port'] = 50052  # Container internal port (keep integer type)
        self.config['distribute']['data_idx'] = self.client_id
        
        # Client-specific seed
        self.config['seed'] = 12345 + self.client_id
        
        # 🎮 GPU configuration: based on Ray resource allocation, not container detection
        # Cannot detect CUDA before Docker container startup, should decide based on Ray GPU allocation
        # Note: use_gpu will be re-detected by FederatedScope after container startup
        self.config['device'] = 0  # Container default use GPU 0 (if available)
        self.config['use_gpu'] = True  # Temporary setting, will be re-detected inside container
        
        # Client output directory
        self.config['outdir'] = f"/app/output"
        
        # Prepare configuration and output directories
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        output_dir = f"{CONFIG.OUTPUT_DIR}/client_{self.client_id}_output"
        log_dir = CONFIG.LOG_DIR  # Use unified log directory, keep consistent with host
        data_dir = "data"
        
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True) 
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)
        
        config_path = f"{config_dir}/client_{self.client_id}.yaml"
        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)
        
        # Start Docker container
        return self._start_docker_container(config_path, output_dir, log_dir, data_dir)
    
    def _apply_device_constraints(self):
        """Apply configuration constraints based on device characteristics"""
        profile = self.device_profile
        
        # Adjust batch size based on device capability
        if profile.device_type == "iot_device":
            self.config['dataloader']['batch_size'] = 8  # IoT devices use small batches
        elif profile.device_type == "smartphone_low":
            self.config['dataloader']['batch_size'] = 16  # Low-end phones moderate
        else:
            self.config['dataloader']['batch_size'] = 32  # High-end devices use original configuration
        
        # Adjust local update steps based on training speed
        base_steps = self.config['train']['local_update_steps']
        adjusted_steps = max(1, int(base_steps * profile.training_speed_multiplier))
        self.config['train']['local_update_steps'] = adjusted_steps
        
    def _start_docker_container(self, config_path: str, output_dir: str, log_dir: str, data_dir: str) -> bool:
        """Start Docker container"""
        profile = self.device_profile
        
        # Calculate CPU limit (in microseconds)
        cpu_period = 100000  # 100ms
        cpu_quota = int(float(profile.cpu_limit) * cpu_period)
        
        # Docker container configuration
        container_config = {
            "image": profile.docker_image,
            "name": self.container_name,
            "hostname": f"client-{self.client_id}",
            "detach": True,
            "remove": True,
            
            # Resource limits
            "cpu_period": cpu_period,
            "cpu_quota": cpu_quota,
            "mem_limit": profile.memory_limit,
            "memswap_limit": profile.memory_limit,  # Disable swap
            
            # Port mapping
            "ports": {50052: self.client_port},
            
            # Environment variables - simple approach following original version
            "environment": {
                "PYTHONPATH": "/app"
            },
            
            # Volume mounts - fix path issues in Ray Actor, use local storage
            "volumes": {
                self._get_absolute_path(config_path): {"bind": "/app/config.yaml", "mode": "ro"},
                self._get_absolute_path(output_dir): {"bind": "/app/output", "mode": "rw"},
                self._get_absolute_path(log_dir): {"bind": "/app/logs", "mode": "rw"},
                self._get_absolute_path(data_dir): {"bind": "/app/data", "mode": "rw"},
                f"{os.getcwd()}/docker_data/tmp": {"bind": "/tmp", "mode": "rw"}
            },
            
            # Privileged mode (for network control)
            "privileged": True,
            
            # Startup command - use shell wrapper to set working directory and environment, redirect logs to mounted directory
            "command": ["sh", "-c", f"cd /app && PYTHONPATH=/app python federatedscope/main.py --cfg /app/config.yaml > /app/logs/client_{self.client_id}.log 2>&1"]
        }
        
        # GPU support - determine container GPU access based on Ray resource allocation
        # Get GPU allocation information from Actor class resource options
        # This information is determined when Actor is created
        try:
            # Get current Actor's resource allocation (from class variables or instance-saved information)
            actor_options = ray.get_runtime_context().current_actor
            # Since we cannot directly get Actor resource configuration, we use a simpler method:
            # Check if cluster has GPU resources, if yes give container GPU access
            cluster_resources = ray.cluster_resources()
            has_gpu_in_cluster = cluster_resources.get('GPU', 0) > 0
            
            if has_gpu_in_cluster:
                # If cluster has GPU resources, give Docker container GPU access
                # FederatedScope will auto-detect inside container and decide whether to use GPU
                container_config["device_requests"] = [
                    docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])
                ]
                print(f"🎮 Client {self.client_id}: Cluster has GPU resources, enable container GPU access")
            else:
                print(f"💻 Client {self.client_id}: Cluster has no GPU resources, use CPU mode")
                
        except Exception as e:
            # If getting resource info fails, enable GPU access by default (let FederatedScope decide itself)
            container_config["device_requests"] = [
                docker.types.DeviceRequest(count=-1, capabilities=[['gpu']])
            ]
            print(f"🎮 Client {self.client_id}: Cannot get resource information ({e}), default to enable GPU access")
        
        try:
            # Start container
            self.container = self.docker_client.containers.run(**container_config)
            
            # Apply network constraints
            if CONFIG.ENABLE_NETWORK_SIMULATION:
                self._apply_network_constraints()
            
            # Start device behavior simulation (battery, mobility, etc.)
            self._start_device_behavior_simulation()
            
            return True
            
        except Exception as e:
            print(f"Failed to start client {self.client_id} container: {e}")
            return False
    
    def _apply_network_constraints(self):
        """Apply network constraints using tc (traffic control)"""
        if not self.container:
            return
            
        profile = self.device_profile
        
        # Convert bandwidth units
        up_bandwidth = f"{profile.bandwidth_up_kbps}kbit"
        down_bandwidth = f"{profile.bandwidth_down_kbps}kbit"
        delay = f"{profile.latency_ms}ms"
        jitter = f"{profile.jitter_ms}ms" 
        loss = f"{profile.packet_loss_rate * 100}%"
        
        # tc command sequence
        tc_commands = [
            # Clear existing rules
            "tc qdisc del dev eth0 root 2>/dev/null || true",
            
            # Set upstream bandwidth limit
            "tc qdisc add dev eth0 root handle 1: htb default 12",
            f"tc class add dev eth0 parent 1: classid 1:1 htb rate {up_bandwidth} ceil {up_bandwidth}",
            
            # Add network delay and packet loss
            f"tc qdisc add dev eth0 parent 1:1 handle 10: netem delay {delay} {jitter} loss {loss}",
        ]
        
        # Execute tc commands
        for cmd in tc_commands:
            try:
                result = self.container.exec_run(f"sh -c '{cmd}'", privileged=True)
                if result.exit_code != 0:
                    print(f"Network configuration command failed: {cmd}, error: {result.output.decode()}")
            except Exception as e:
                print(f"Failed to execute network command: {cmd}, error: {e}")
                
    def _start_device_behavior_simulation(self):
        """Start device behavior simulation (battery, mobility, etc.)"""
        profile = self.device_profile
        
        if profile.mobility_pattern == "intermittent":
            # Intermittent device simulation
            threading.Thread(target=self._simulate_intermittent_connectivity, daemon=True).start()
            
        if profile.battery_constraint:
            # Battery constraint simulation
            threading.Thread(target=self._simulate_battery_drain, daemon=True).start()
    
    def _simulate_intermittent_connectivity(self):
        """Simulate intermittent connections"""
        if not self.container:
            return
            
        while True:
            try:
                # Decide connection status based on availability ratio
                if random.random() > self.device_profile.availability_ratio:
                    # Device offline
                    offline_duration = random.randint(10, 60)  # 10-60 seconds offline
                    self.container.pause()
                    print(f"📱 Device {self.client_id} offline {offline_duration} seconds")
                    time.sleep(offline_duration)
                    
                    # Back online
                    self.container.unpause() 
                    print(f"📱 Device {self.client_id} back online")
                    
                # Online time
                online_duration = random.randint(60, 300)  # 1-5 minutes online
                time.sleep(online_duration)
                
            except Exception as e:
                print(f"Intermittent connection simulation error: {e}")
                break
                
    def _simulate_battery_drain(self):
        """Simulate battery consumption"""
        if not self.container:
            return
            
        # Simulate battery level (30 minutes until low battery mode)
        battery_life = 30 * 60  # 30 minutes
        low_battery_threshold = 0.2  # 20% battery level
        
        time.sleep(battery_life * (1 - low_battery_threshold))
        
        # Enter low battery mode (reduce performance)
        print(f"🔋 Device {self.client_id} enter low battery mode")
        
        # Simulate performance degradation (reduce CPU limit)
        try:
            # Reduce CPU usage to 70%
            cpu_quota = int(float(self.device_profile.cpu_limit) * 0.7 * 100000)
            self.container.update(cpu_quota=cpu_quota)
        except Exception as e:
            print(f"Failed to update CPU limit: {e}")
            
        # Wait for remaining battery to drain
        time.sleep(battery_life * low_battery_threshold)
        
        # Battery drained, device shutdown
        print(f"🔋 Device {self.client_id} battery drained, device shutdown")
        try:
            self.container.stop()
        except Exception as e:
            print(f"Shutdown failed: {e}")
    
    def get_status(self) -> Dict:
        """Get Docker container status"""
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
        """Stop Docker container"""
        if self.container:
            try:
                self.container.stop(timeout=10)
            except Exception as e:
                print(f"Failed to stop client {self.client_id} container: {e}")
                
    def simulate_device_failure(self, duration: int = 60):
        """Simulate device failure"""
        if self.container:
            self.container.pause()
            print(f"🔴 Device {self.client_id} failure occurred, offline for {duration} seconds")
            
            # Scheduled recovery
            def recover():
                time.sleep(duration)
                try:
                    if self.container.status == "paused":
                        self.container.unpause()
                        print(f"🔄 Device {self.client_id} failure recovery")
                except Exception as e:
                    print(f"Device recovery failed: {e}")
            
            threading.Thread(target=recover, daemon=True).start()

# ============================================================================
# 🚀 Main Execution Classes
# ============================================================================

class RayV2FederatedLearning:
    """Ray V2 Federated Learning Main Controller"""
    
    def __init__(self):
        self.logger = setup_logging()
        self.server_actor = None
        self.client_actors = []
        self.server_info = None
        self.cleanup_performed = False
        
    def _find_available_port(self, start_port: int = 8265, max_attempts: int = 50) -> int:
        """Find an available port starting from start_port"""
        import socket
        
        for port in range(start_port, start_port + max_attempts):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.bind(('localhost', port))
                    self.logger.info(f"🔍 Found available port for Ray dashboard: {port}")
                    return port
            except socket.error:
                continue
        
        # If no port found, fall back to a random port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(('localhost', 0))
            port = sock.getsockname()[1]
            self.logger.info(f"🔍 Using random available port for Ray dashboard: {port}")
            return port
        
    def initialize_ray_cluster(self):
        """Initialize new Ray cluster (not using default cluster)"""
        # Shutdown any existing Ray cluster first
        try:
            ray.shutdown()
        except:
            pass  # Ignore if no cluster is running
        
        # Detect hardware resources
        num_cpus = CONFIG.RAY_MAX_CPUS or psutil.cpu_count()
        num_gpus = CONFIG.RAY_MAX_GPUS
        
        if CONFIG.RAY_AUTO_GPU_DETECTION and num_gpus is None:
            try:
                import torch
                num_gpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
            except ImportError:
                num_gpus = 0
        
        # Create new Ray cluster configuration
        # Use parent directory to avoid Unix socket path length limit (107 bytes)
        current_dir = os.getcwd()
        parent_dir = os.path.dirname(current_dir)
        ray_temp_dir = os.path.join(parent_dir, "ray")
        os.makedirs(ray_temp_dir, exist_ok=True)
        
        ray_config = {
            "num_cpus": num_cpus,
            "num_gpus": num_gpus or 0,
            "ignore_reinit_error": True,
            "_temp_dir": ray_temp_dir,  # Use shorter path to avoid socket path length issues
            "_system_config": {
                "automatic_object_spilling_enabled": True,
                "max_io_workers": 4,
                "min_spilling_size": 100 * 1024 * 1024,  # 100MB
            }
        }
        
        dashboard_port = None
        if CONFIG.ENABLE_RAY_DASHBOARD:
            # Find available port for Ray dashboard
            dashboard_port = self._find_available_port(start_port=8265, max_attempts=50)
            ray_config.update({
                "include_dashboard": True,
                "dashboard_host": "0.0.0.0",
                "dashboard_port": dashboard_port
            })
        
        # Create new Ray cluster (explicitly not using default)
        ray.init(**ray_config)
        
        resources = ray.cluster_resources()
        self.logger.info(f"🚀 Ray cluster initialization completed:")
        self.logger.info(f"   📊 Resources: {dict(resources)}")
        
        if CONFIG.ENABLE_RAY_DASHBOARD and dashboard_port:
            dashboard_url = f"http://127.0.0.1:{dashboard_port}"
            self.logger.info(f"   🌐 Dashboard: {dashboard_url}")
        
    def generate_base_config(self) -> Dict[str, Any]:
        """Generate base configuration"""
        # Check CUDA availability
        use_gpu = False
        try:
            import torch
            use_gpu = torch.cuda.is_available()
            if not use_gpu:
                self.logger.warning("⚠️ CUDA unavailable, all nodes will use CPU mode")
        except ImportError:
            self.logger.warning("⚠️ PyTorch not installed, use CPU mode")
            
        return {
            'use_gpu': use_gpu,
            'device': 0 if use_gpu else -1,  # GPU device ID or CPU mode
            'seed': 12345,  # Will be dynamically overridden
            
            'federate': {
                'client_num': CONFIG.CLIENT_NUM,
                'mode': 'distributed',
                'total_round_num': CONFIG.TOTAL_ROUNDS,
                'sample_client_num': CONFIG.CLIENT_NUM
            },
            
            'distribute': {
                'use': True,
                'server_host': '127.0.0.1',  # Will be dynamically overridden
                'server_port': 50051,        # Will be dynamically overridden
                'client_host': '127.0.0.1',  # Will be dynamically overridden
                'client_port': 50052,        # Will be dynamically overridden
                'role': 'server',            # Will be dynamically overridden
                'data_idx': 0                # Will be dynamically overridden
            },
            
            'data': {
                'root': 'data/',
                'type': CONFIG.DATASET,
                'splits': [0.6, 0.2, 0.2],  # Shakespeare dataset standard splits
                'subsample': 0.5,            # Increased data usage for better learning
                'splitter': 'lda',
                'splitter_args': [{'alpha': CONFIG.DATA_SPLIT_ALPHA}]
            },
            
            'dataloader': {
                'batch_size': CONFIG.BATCH_SIZE
            },
            
            'model': {
                'type': CONFIG.MODEL_TYPE,
                'hidden': CONFIG.MODEL_HIDDEN,
                'in_channels': 80,           # Shakespeare vocab size
                'out_channels': CONFIG.MODEL_OUT_CHANNELS,
                'embed_size': 8,             # Small embedding for testing
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
                'type': 'character_loss'     # Character-level loss for shakespeare
            },
            
            'trainer': {
                'type': 'nlptrainer'         # NLP trainer for text tasks
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
        """Dynamic fractional GPU resource allocation: server uses CPU, all nodes use fractional GPU"""
        cluster_resources = ray.cluster_resources()
        available_gpus = float(cluster_resources.get('GPU', 0))
        
        if available_gpus == 0:
            self.logger.warning("⚠️ No GPU detected, all nodes use CPU mode")
            return [None] + [None] * CONFIG.CLIENT_NUM
        
        # 🖥️ Server fixed to use CPU (no GPU allocation)
        gpu_allocation = [None]  # Server CPU mode
        
        # 🎮 Define GPU resource allocation ratios corresponding to device performance
        device_gpu_ratios = {
            "edge_server": 0.5,        # Edge server - high performance 50%
            "smartphone_high": 0.35,   # High-end phone - medium performance 35% 
            "smartphone_low": 0.25,    # Low-end phone - low performance 25%
            "raspberry_pi": 0.25,      # Raspberry Pi - low performance 25%
            "iot_device": 0.15,        # IoT device - lowest performance 15%
        }
        
        # Calculate total GPU needed by all clients
        total_required_gpu = 0.0
        client_requirements = []
        
        # Only use device types with ratio>0 for GPU allocation
        active_device_types = [dt for dt, ratio in CONFIG.DEVICE_DISTRIBUTION.items() if ratio > 0]
        if not active_device_types:
            active_device_types = ["smartphone_high"]  # Default device type
            
        for i in range(CONFIG.CLIENT_NUM):
            device_type = active_device_types[i % len(active_device_types)]
            required_gpu = device_gpu_ratios.get(device_type, 0.3)  # Default 0.3
            client_requirements.append((device_type, required_gpu))
            total_required_gpu += required_gpu
        
        # 🎯 Smart scaling: always maintain 90-100% GPU utilization
        target_utilization = 0.80  # Target 80% utilization (reduce resource competition)
        target_gpu_usage = available_gpus * target_utilization
        scaling_factor = target_gpu_usage / total_required_gpu
        
        if scaling_factor < 1.0:
            self.logger.warning(f"⚠️ GPU demand ({total_required_gpu:.2f}) exceeds target usage ({target_gpu_usage:.2f}), scaling down ({scaling_factor:.2f})")
        elif scaling_factor > 1.0:
            self.logger.info(f"🚀 GPU sufficient ({total_required_gpu:.2f} < {target_gpu_usage:.2f}), scaling up ({scaling_factor:.2f}) to improve performance")
        else:
            self.logger.info(f"⚡ GPU allocation optimized, reaching target utilization ({target_utilization*100:.0f}%)")
        
        # Allocate GPU resources to each client
        actual_total_gpu = 0.0
        for device_type, required_gpu in client_requirements:
            allocated_gpu = required_gpu * scaling_factor
            
            # Ray constraint: GPU quantities >1 must be whole numbers
            if allocated_gpu > 1.0:
                allocated_gpu = int(allocated_gpu)  # Round down to integer
            
            gpu_allocation.append(allocated_gpu)
            actual_total_gpu += allocated_gpu
        
        # Generate allocation summary
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
        
        self.logger.info(f"🎯 Fractional GPU resource allocation: {gpu_summary}")
        self.logger.info(f"📋 Client detailed allocation: {[f'{alloc:.2f}' if alloc else 'CPU' for alloc in gpu_allocation[1:]]}")
        return gpu_allocation
    
    def _create_diverse_device_fleet(self, num_devices: int) -> List[EdgeDeviceProfile]:
        """Create diverse edge device queue"""
        device_assignments = []
        device_types = list(CONFIG.DEVICE_DISTRIBUTION.keys())
        
        # Allocate device types according to configured distribution ratio
        for device_type, ratio in CONFIG.DEVICE_DISTRIBUTION.items():
            if ratio <= 0:  # Skip device types with ratio 0
                continue
            count = max(1, int(num_devices * ratio))
            
            for i in range(count):
                if len(device_assignments) >= num_devices:
                    break
                    
                # Get base device profile and create variants
                base_profile = EDGE_DEVICE_PROFILES[device_type]
                device_variant = self._create_device_variant(base_profile, len(device_assignments) + 1)
                device_assignments.append(device_variant)
        
        # If not enough devices, add randomly (only choose from device types with ratio>0)
        while len(device_assignments) < num_devices:
            available_types = [dt for dt, ratio in CONFIG.DEVICE_DISTRIBUTION.items() if ratio > 0]
            if not available_types:
                break  # No available device types
            device_type = random.choice(available_types)
            base_profile = EDGE_DEVICE_PROFILES[device_type]
            device_variant = self._create_device_variant(base_profile, len(device_assignments) + 1)
            device_assignments.append(device_variant)
        
        # Record device distribution
        distribution_summary = {}
        for assignment in device_assignments:
            device_type = assignment.device_type
            distribution_summary[device_type] = distribution_summary.get(device_type, 0) + 1
        
        self.logger.info(f"📱 Edge device distribution: {distribution_summary}")
        return device_assignments[:num_devices]
    
    def _create_device_variant(self, base_profile: EdgeDeviceProfile, device_id: int) -> EdgeDeviceProfile:
        """Create device variants (increase realism)"""
        import copy
        variant = copy.deepcopy(base_profile)
        
        # Make device ID unique
        variant.device_id = f"{base_profile.device_id}_{device_id}"
        
        # Add random variations (±20%)
        variation_factor = random.uniform(0.8, 1.2)
        
        # CPU variation
        base_cpu = float(variant.cpu_limit)
        variant.cpu_limit = f"{base_cpu * variation_factor:.1f}"
        
        # Network variation
        variant.bandwidth_up_kbps = int(variant.bandwidth_up_kbps * variation_factor)
        variant.bandwidth_down_kbps = int(variant.bandwidth_down_kbps * variation_factor)
        variant.latency_ms = max(10, int(variant.latency_ms * variation_factor))
        
        # Availability randomization
        variant.availability_ratio *= random.uniform(0.9, 1.0)
        
        return variant
    
    def _get_ray_resources_for_device(self, device_profile: EdgeDeviceProfile) -> Dict[str, Any]:
        """Get Ray resource allocation based on device type"""
        base_cpu = max(0.1, float(device_profile.cpu_limit))
        
        # Parse memory limit from device configuration
        memory_str = device_profile.memory_limit.lower()
        if memory_str.endswith('g'):
            memory_bytes = int(float(memory_str[:-1]) * 1024 * 1024 * 1024)
        elif memory_str.endswith('m'):
            memory_bytes = int(float(memory_str[:-1]) * 1024 * 1024)
        else:
            memory_bytes = 1024 * 1024 * 1024  # Default 1GB
        
        # Adjust CPU resources based on device type
        if device_profile.device_type == "iot":
            return {"num_cpus": 0.2, "memory": memory_bytes}
        elif device_profile.device_type == "smartphone":
            return {"num_cpus": base_cpu, "memory": memory_bytes}
        elif device_profile.device_type == "edge_server":
            return {"num_cpus": base_cpu, "memory": memory_bytes}
        else:  # raspberry_pi, edge_device
            return {"num_cpus": base_cpu, "memory": memory_bytes}
    
    def cleanup_environment(self):
        """Clean environment"""
        if self.cleanup_performed:
            return
            
        self.logger.info("🧹 Clean environment...")
        
        # Stop old processes
        subprocess.run(['pkill', '-9', '-f', 'python.*federatedscope'], 
                      capture_output=True, check=False)
        
        # Clean database files
        try:
            import glob
            db_files = glob.glob('tmp/client_*/client_*_chunks.db')
            for db_file in db_files:
                if os.path.exists(db_file):
                    os.remove(db_file)
        except Exception as e:
            self.logger.debug(f"Failed to clean database files: {e}")
        
        # Clean log directory
        for log_dir in ['connection_logs', 'topology_logs', 'bittorrent_logs']:
            subprocess.run(['rm', '-rf', log_dir], check=False)
        
        # Clean previous Ray temp directory
        subprocess.run(['rm', '-rf', f"{os.getcwd()}/tmp/ray"], check=False)
        
        # Create output and Ray temporary directories
        os.makedirs(CONFIG.OUTPUT_DIR, exist_ok=True)
        os.makedirs(CONFIG.LOG_DIR, exist_ok=True)
        os.makedirs(f"{os.getcwd()}/tmp/ray", exist_ok=True)
        
        time.sleep(1)
        self.cleanup_performed = True
        self.logger.info("✅ Environment cleanup completed")
    
    def start_federated_learning(self):
        """Start federated learning"""
        self.logger.info(f"🚀 Start Ray V2 federated learning")
        self.logger.info(f"📊 Configuration: {CONFIG.CLIENT_NUM} clients, {CONFIG.TOTAL_ROUNDS} rounds training, total nodes: {CONFIG.CLIENT_NUM + 1}")
        
        # Clean environment
        self.cleanup_environment()
        
        # Initialize Docker environment (if enabled)
        if CONFIG.USE_DOCKER:
            self.docker_manager = DockerManager()
            if not self.docker_manager.docker_available:
                self.logger.warning("⚠️  Docker unavailable, switch to non-container mode")
                CONFIG.USE_DOCKER = False
                # Continue using non-Docker mode
            else:
                # Check and ensure Docker images are ready
                self.logger.info("🔍 Check Docker images...")
                if not self.docker_manager.ensure_images_ready():
                    self.logger.warning("⚠️  Docker images not ready, switch to non-container mode")
                    CONFIG.USE_DOCKER = False
                else:
                    # Set up Docker network environment
                    if not self.docker_manager.setup_docker_environment():
                        self.logger.error("❌ Docker environment setup failed, switch to non-container mode")
                        CONFIG.USE_DOCKER = False
                    else:
                        self.logger.info("✅ Docker environment and image initialization successful")
        
        # Initialize Ray
        self.initialize_ray_cluster()
        
        # GPU resource allocation
        gpu_allocation = self.allocate_gpu_resources()
        server_gpu = gpu_allocation[0]
        client_gpus = gpu_allocation[1:]
        
        # Generate configuration
        base_config = self.generate_base_config()
        
        # 🖥️ Start server (fixed to use CPU resources)
        server_config = base_config.copy()
        server_config['distribute']['role'] = 'server'
        server_config['use_gpu'] = False  # Server forced to use CPU
        
        # Server resource configuration: CPU only, no GPU
        server_resources = {"num_cpus": 2}
        # Note: server_gpu is always None, server does not use GPU
        
        # Choose Actor type based on Docker availability
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
        
        self.logger.info(f"✅ Server started: {server_ip}:{server_port}")
        time.sleep(3)
        
        # Create diverse edge devices
        device_assignments = self._create_diverse_device_fleet(CONFIG.CLIENT_NUM)
        
        # Start Docker clients
        successful_clients = 0
        for i, device_profile in enumerate(device_assignments):
            client_id = i + 1
            
            client_config = base_config.copy()
            client_config['distribute']['role'] = 'client'
            
            # Ray resource allocation (based on device type and GPU allocation)
            client_resources = self._get_ray_resources_for_device(device_profile)
            
            # 🎮 Add fractional GPU resource allocation (all clients use GPU)
            client_gpu = client_gpus[i] if i < len(client_gpus) else None
            if client_gpu is not None:
                client_resources["num_gpus"] = client_gpu  # Allocate fractional GPU resources
            
            try:
                # Choose Actor type based on Docker availability
                if CONFIG.USE_DOCKER:
                    client_actor = DockerFederatedScopeClient.options(**client_resources).remote(
                        client_id, client_config, server_ip, server_port, device_profile
                    )
                else:
                    client_actor = FallbackFederatedScopeClient.options(**client_resources).remote(
                        client_id, client_config, server_ip, server_port, device_profile
                    )
                
                self.client_actors.append(client_actor)
                
                # Start client container
                start_result = ray.get(client_actor.start.remote())
                if start_result:
                    successful_clients += 1
                    self.logger.info(f"✅ Client {client_id} ({device_profile.device_type}) started successfully")
                else:
                    self.logger.error(f"❌ Client {client_id} ({device_profile.device_type}) failed to start")
                    
                time.sleep(3)  # Give Docker containers more startup time
                
            except Exception as e:
                self.logger.error(f"❌ Client {client_id} creation failed: {e}")
        
        if successful_clients < CONFIG.CLIENT_NUM * 0.7:  # At least 70% success
            self.logger.error(f"❌ Client startup success rate too low: {successful_clients}/{CONFIG.CLIENT_NUM}")
            return
            
        self.logger.info(f"✅ {successful_clients}/{CONFIG.CLIENT_NUM} Docker clients started successfully")
        
        self.logger.info(f"✅ All {CONFIG.CLIENT_NUM} clients have completed startup")
        
        # Monitor training
        self.monitor_training()
        
    def monitor_training(self):
        """Monitor training progress"""
        self.logger.info(f"📊 Starting training monitoring ({CONFIG.MONITOR_DURATION} seconds)...")
        
        start_time = time.time()
        
        while True:
            elapsed = int(time.time() - start_time)
            
            if elapsed > CONFIG.MONITOR_DURATION:
                self.logger.info("⏰ Monitoring time ended")
                break
            
            # Check status
            server_status = ray.get(self.server_actor.get_status.remote())
            client_statuses = ray.get([
                actor.get_status.remote() for actor in self.client_actors
            ])
            
            running_clients = sum(1 for s in client_statuses if s["status"] == "running")
            
            # Resource usage
            cluster_resources = ray.cluster_resources()
            available_resources = ray.available_resources()
            
            # Calculate actual GPU usage
            total_gpus = float(cluster_resources.get('GPU', 0))
            available_gpus = float(available_resources.get('GPU', 0))
            gpu_used = total_gpus - available_gpus
            
            # Count running clients (all clients use fractional GPU)
            total_clients = sum(1 for s in client_statuses if s["status"] == "running")
            
            self.logger.info(
                f"⏰ {elapsed}s | Server: {server_status['status']} (CPU) | "
                f"Clients: {total_clients} nodes (fractional GPU) | "
                f"Ray GPU usage: {gpu_used:.1f}/{total_gpus:.1f} ({(gpu_used/total_gpus)*100:.1f}%)"
            )
            
            # Check training completion
            if server_status["status"] != "running":
                self.logger.info("🏁 Server training completed")
                break
            
            if running_clients == 0:
                self.logger.info("🏁 All clients training completed")
                break
            
            time.sleep(10)
    
    def stop_all(self):
        """Stop all processes and Docker containers"""
        self.logger.info("🛑 Stop all federated learning processes...")
        
        # Clean Docker environment
        if CONFIG.USE_DOCKER and hasattr(self, 'docker_manager'):
            try:
                self.docker_manager.cleanup_docker_environment()
                self.logger.info("✅ Docker environment cleaned")
            except Exception as e:
                self.logger.warning(f"⚠️  Docker cleanup warning: {e}")
        else:
            # St
            # op Ray Actors
            try:
                if self.server_actor:
                    ray.get(self.server_actor.stop.remote())

                if self.client_actors:
                    stop_futures = [actor.stop.remote() for actor in self.client_actors]
                    ray.get(stop_futures)
            except Exception as e:
                self.logger.warning(f"⚠️  Ray Actors stop warning: {e}")
        
        self.logger.info("✅ All resources stopped")
        
    def generate_results_summary(self):
        """Generate results summary"""
        self.logger.info("📈 Generate results summary...")
        
        # Check log files
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
        
        # Save summary
        summary_file = f"{CONFIG.OUTPUT_DIR}/results_summary.yaml"
        with open(summary_file, 'w') as f:
            yaml.safe_dump(summary, f)
        
        self.logger.info(f"📄 Results summary saved: {summary_file}")
        
        return summary

# ============================================================================
# 🎬 Main Program Entry
# ============================================================================

def display_banner():
    """Display startup banner"""
    docker_status = "✅ Enabled" if CONFIG.USE_DOCKER else "❌ Disabled"
    network_sim_status = "✅ Enabled" if CONFIG.ENABLE_NETWORK_SIMULATION else "❌ Disabled"
    
    banner = f"""
{'='*80}
🚀 Ray-Powered FederatedScope V2 Script (Docker Edition)
{'='*80}
📊 Configuration information:
   • Number of clients: {CONFIG.CLIENT_NUM}
   • Training rounds: {CONFIG.TOTAL_ROUNDS}
   • Dataset: {CONFIG.DATASET}
   • Model: {CONFIG.MODEL_TYPE}
   • Number of chunks: {CONFIG.CHUNK_NUM}
   • Importance method: {CONFIG.IMPORTANCE_METHOD}
   • Monitoring duration: {CONFIG.MONITOR_DURATION}s

🐳 Docker mode: {docker_status}
🌐 Network simulation: {network_sim_status}
📱 Device distribution: {dict(CONFIG.DEVICE_DISTRIBUTION)}

💡 Output directory: {CONFIG.OUTPUT_DIR}
📝 Log directory: {CONFIG.LOG_DIR}
{'='*80}
"""
    print(banner)

def main():
    """Main function"""
    display_banner()
    
    ray_fl = RayV2FederatedLearning()
    
    try:
        # Start federated learning
        ray_fl.start_federated_learning()
        
        # Generate results summary
        summary = ray_fl.generate_results_summary()
        
        print("\n🎉 Ray V2 federated learning completed!")
        print(f"📄 Results summary: {CONFIG.OUTPUT_DIR}/results_summary.yaml")
        print(f"📝 Log files: {CONFIG.LOG_DIR}/")
        
        if CONFIG.ENABLE_RAY_DASHBOARD:
            print(f"🌐 Ray Dashboard: http://127.0.0.1:8265")
            
    except KeyboardInterrupt:
        print("\n👋 Received interrupt signal, cleaning up...")
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean resources
        ray_fl.stop_all()
        ray.shutdown()
        print("🧹 Resource cleanup completed")

if __name__ == "__main__":
    main()