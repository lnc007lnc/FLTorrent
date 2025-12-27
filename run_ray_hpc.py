#!/usr/bin/env python3
"""
Ray-Powered FederatedScope V2 Script (HPC SLURM Version)
=========================================================

Adapted for HPC cluster environments with SLURM job scheduler.
Based on run_ray.py with modifications for SLURM-based deployment.

Usage:
    # Option 1: Submit via generated SLURM sbatch script
    python run_ray_hpc.py --generate-slurm   # Generate slurm_submit.sh
    sbatch slurm_submit.sh                    # Submit to SLURM

    # Option 2: Run directly on allocated SLURM nodes
    srun --nodes=4 --ntasks=4 python run_ray_hpc.py

    # Option 3: Use ray symmetric-run (Ray 2.49+)
    srun --nodes=4 ray symmetric-run --min-nodes 4 -- python run_ray_hpc.py

    # Option 4: Connect to existing Ray cluster
    RAY_ADDRESS=<head_node_ip>:6379 python run_ray_hpc.py

Key differences from run_ray.py:
- Dynamic PROJECT_ROOT detection based on script location
- Uses internal network IP instead of AWS public IP
- SLURM-compatible resource detection and allocation
- Can generate SLURM submission scripts with --generate-slurm

One-click distributed federated learning with:
- Automatic GPU resource management and allocation
- Dynamic IP port allocation
- Real-time monitoring and logging
- HPC cluster extension support

🚀 Unified Aggregator and LR Scheduler Configuration
- Configure aggregator type: fedavg, krum, median, trimmedmean, bulyan, normbounding
- Configure LR scheduler: StepLR, MultiStepLR, CosineAnnealingLR, etc.
- All settings centralized in FLConfig class

🔧 BitTorrent Parameter Completion Strategies
- Configure how missing chunks are completed during aggregation
- Options: 'global_model' (default), 'zeros', 'local_model'

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
import socket
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

# === Project root - dynamically detected for HPC environments ===
# Automatically detects project root based on script location
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


def get_node_ip() -> str:
    """Get node IP address for HPC cluster internal communication.

    For HPC environments, uses internal network IP instead of public IP.
    Tries multiple methods to get the correct IP address.
    """
    # Method 1: Try to get IP that can reach external network (most reliable)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        pass

    # Method 2: Use Ray's internal IP detection
    try:
        return ray.util.get_node_ip_address()
    except Exception:
        pass

    # Method 3: Get hostname IP
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        pass

    # Fallback
    return "127.0.0.1"


# Alias for backward compatibility
def get_public_ip() -> str:
    """Alias for get_node_ip() - maintained for backward compatibility."""
    return get_node_ip()

# ============================================================================
# 🔧 Configuration Area - All settings centralized here for easy modification
# ============================================================================

@dataclass
class SLURMConfig:
    """SLURM job scheduler configuration for HPC environments"""

    # === Basic SLURM Settings ===
    PARTITION: str = "gpu2"                   # SLURM partition name (gpu2 has 4 GPUs, 512GB RAM)
    ACCOUNT: str = ""                         # Account/project for billing (optional)
    QOS: str = ""                             # Quality of Service (optional)

    # === Time Limits ===
    TIME_LIMIT: str = "UNLIMITED"             # Max wall time (no limit)

    # === Resource Requests ===
    NODES: int = 1                            # Number of nodes to request
    TASKS_PER_NODE: int = 1                   # Tasks per node (1 for Ray)
    CPUS_PER_TASK: int = 64                   # CPUs per task (gpu2 has 64 CPUs)
    MEM_PER_NODE: str = "400G"                # Memory per node (gpu2 has 512GB)
    GPUS_PER_NODE: int = 4                    # GPUs per node (gpu2 has 4 GPUs)
    GPU_TYPE: str = ""                        # Specific GPU type (e.g., "v100", "a100")

    # === Environment Settings ===
    CONDA_ENV: str = "flv2"                   # Conda environment name
    MODULE_LOADS: List[str] = field(default_factory=lambda: [])  # Modules to load

    # === Job Naming ===
    JOB_NAME: str = "fl_experiment"           # Job name

    # === Output Settings ===
    SLURM_OUTPUT_DIR: str = "slurm_logs"      # SLURM output directory

    # === Advanced Settings ===
    EXCLUSIVE: bool = False                   # Exclusive node access
    CONSTRAINT: str = ""                      # Node constraint (e.g., "skylake")

    # === Multi-Node / Multi-Partition Settings ===
    # Enable hetjob mode to use multiple partitions with different resources
    USE_MULTI_PARTITION: bool = False          # Enable multi-partition hetjob mode

    # Configuration for each partition in hetjob mode
    # Format: list of dicts with partition-specific settings
    # Node 0 (first partition) will run the server + some clients
    # Other nodes will run only clients
    HETJOB_CONFIG: List[Dict[str, Any]] = field(default_factory=lambda: [
        {
            "partition": "gpu2",
            "nodes": 1,
            "cpus": 64,
            "mem": "400G",
            "gpus": 4,
            "clients": 38,      # Number of clients to run on this node
            "run_server": True  # This node runs the server
        },
        {
            "partition": "gpu1",
            "nodes": 1,
            "cpus": 64,
            "mem": "200G",
            "gpus": 2,
            "clients": 12,      # Number of clients to run on this node
            "run_server": False
        }
    ])


# Create global SLURM configuration instance
SLURM_CONFIG = SLURMConfig()


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
    bandwidth_up_kbps: int = 176610         # Uplink bandwidth (kbps) Ookla Speedtest Global Index spain Fixed Broadband
    bandwidth_down_kbps: int = 241750       # Downlink bandwidth (kbps) Ookla Speedtest Global Index spain Fixed Broadband
    # bandwidth_up_kbps: int = 17661000000000000         # Uplink bandwidth (kbps) Ookla Speedtest Global Index spain Fixed Broadband
    # bandwidth_down_kbps: int = 24175000000000000       # Downlink bandwidth (kbps) Ookla Speedtest Global Index spain Fixed Broadband
    latency_ms: int = 1                   # Network latency (milliseconds)
    packet_loss_rate: float = 0.00        # Packet loss rate (0-1)
    jitter_ms: int = 1                    # Network jitter (milliseconds)
    
    # Device characteristics
    training_speed_multiplier: float = 1.0  # Training speed multiplier
    availability_ratio: float = 0.9        # Availability ratio (0-1)
    battery_constraint: bool = False        # Whether has battery constraint
    mobility_pattern: str = "static"        # Mobility pattern: static, mobile, intermittent

@dataclass
class FLConfig:
    """Federated learning configuration parameters"""

    # === Basic Settings ===
    CLIENT_NUM: int = 50                   # Number of clients for FL experiments
    TOTAL_ROUNDS: int = 100                  # Extended to 100 rounds for two-phase training
    CHUNK_NUM: int = 16                    # 128 chunks: ~343KB each, fits in 425KB socket buffer
    IMPORTANCE_METHOD: str = "fisher"         # Chunk importance method: magnitude, l2_norm, snip, fisher
    SEED: int = 12345                       # Random seed for reproducibility (server uses SEED, clients use SEED + client_id)
    
    # === Dataset Settings ===
    # CNN Settings (current active for ResNet-18)
    DATASET: str = "celeba"                 # CelebA dataset (84x84 RGB, binary classification)
    # DATASET: str = "CIFAR10@torchvision"    # CIFAR-10 dataset (32x32 RGB)
    # DATASET: str = "MNIST@torchvision"      # MNIST dataset (28x28 grayscale)
    BATCH_SIZE: int = 16                    # Standard batch size for CIFAR-10
    # BATCH_SIZE: int = 10                    # Standard batch size for MNIST in FL
    DATA_SPLIT_ALPHA: float = 10000           # Non-IID data split parameter
    
    # Transformer/NLP Settings (commented for future use)
    # DATASET: str = "shakespeare"            # Shakespeare text dataset
    # BATCH_SIZE: int = 8                     # Small batch size for testing
    # DATA_SPLIT_ALPHA: float = 0.5          # LDA data split parameter (less extreme split)
    
    # === Model Settings ===
    # CNN Model Settings (current active - ConvNet2)
    MODEL_TYPE: str = "resnet18"            # ResNet-18 for CIFAR-10 (32x32 RGB, requires 3 channels)
    # MODEL_TYPE: str = "convnet2"            # ConvNet2 for MNIST (28x28 grayscale, supports 1 channel)
    # MODEL_HIDDEN: int = 2048                # Hidden layer size for ConvNet2
    # MODEL_OUT_CHANNELS: int = 2             # Original: For CrossEntropyLoss (2 output neurons for binary)
    MODEL_OUT_CHANNELS: int = 1             # For BCEWithLogitsLoss (1 output neuron, Sigmoid applied internally)
    # MODEL_OUT_CHANNELS: int = 10            # CIFAR-10 classes
    # MODEL_OUT_CHANNELS: int = 10            # MNIST classes (10 digits: 0-9)
    MODEL_DROPOUT: float = 0.0              # Dropout for regularization
    
    # Transformer/NLP Model Settings (commented for future use)
    # MODEL_TYPE: str = "lstm"                # LSTM model for text
    # MODEL_HIDDEN: int = 256                 # Increased hidden size for better capacity
    # MODEL_OUT_CHANNELS: int = 80            # Vocab size for shakespeare
    # MODEL_DROPOUT: float = 0.1             # Add dropout for regularization
    
    # === Container Settings ===
    USE_DOCKER: bool = False              # Enable Docker for containerized execution (requires root)
    USE_PODMAN: bool = True               # 🚀 Enable Podman rootless for HPC (no root required)
    USE_LOCAL_CODE_MOUNT: bool = True     # 🚀 Mount local code instead of rebuilding image
    BASE_CONTAINER_IMAGE: str = "flv2:base"  # Base image (works for both Docker and Podman)
    BASE_DOCKER_IMAGE: str = "flv2:base"  # Alias for backward compatibility
    CONTAINER_NETWORK_NAME: str = "fl_network"      # Container network name
    DOCKER_NETWORK_NAME: str = "fl_network"         # Alias for backward compatibility
    ENABLE_NETWORK_SIMULATION: bool = True          # Enable network simulation

    # === Unix Domain Socket (UDS) Settings ===
    # UDS bypasses TCP buffer limits (rmem_max/wmem_max) - ideal for single-node scenarios
    USE_UDS: bool = True                            # 🚀 Enable UDS for single-node (bypasses 208KB TCP limit)
    UDS_DIR: str = "/tmp/federatedscope_uds"        # Directory for UDS socket files

    # === Device Distribution Configuration ===
    DEVICE_DISTRIBUTION: Dict[str, float] = field(default_factory=lambda: {
        "smartphone_high": 1,    # 20% High-end smartphones
        "smartphone_low": 0,     # 40% Low-end smartphones
        "raspberry_pi": 0,       # 15% Raspberry Pi
        "iot_device": 0,         # 20% IoT devices
        "edge_server": 0         # 5% Edge servers
    })
    
    # === Training Settings ===
    LOCAL_UPDATE_STEPS: int = 1           # 2 epochs for ResNet-18 on CIFAR-10
    # LEARNING_RATE: float = 0.1            # Original: High LR for CIFAR-10 (32×32), too high for CelebA (84×84)
    # LEARNING_RATE: float = 0.01           # Reduced for CelebA with SGD
    # LEARNING_RATE: float = 0.001          # Adam optimizer with lower LR (recommended for CelebA 84×84)
    LEARNING_RATE: float = 0.0001         # Further reduced for CelebA with ResNet+BN in FL (BN instability)
    # OPTIMIZER: str = "SGD"                # Original: SGD optimizer for ResNet (standard choice)
    OPTIMIZER: str = "Adam"               # Adam optimizer (better for CelebA, more stable convergence)
    OPTIMIZER_MOMENTUM: float = 0.90       # SGD momentum for better convergence (not used with Adam)
    WEIGHT_DECAY: float = 5e-4            # Weight decay for ResNet regularization
    GRAD_CLIP: float = 5.0                # Higher gradient clipping for CNN
    
    # === Aggregator Settings ===
    AGGREGATOR_TYPE: str = "fedavg"       # Aggregator type: fedavg, krum, median, trimmedmean, bulyan, normbounding

    # === Personalization Settings (FedBN) ===
    PERSONALIZATION_LOCAL_PARAM: List[str] = field(default_factory=lambda: ['bn'])  # Local parameters not aggregated (e.g., ['bn'] for FedBN)

    # === Learning Rate Scheduler Settings ===
    # Examples:
    # - No scheduler: LR_SCHEDULER_TYPE = ""
    # - Step decay: LR_SCHEDULER_TYPE = "StepLR", LR_SCHEDULER_STEP_SIZE = 30, LR_SCHEDULER_GAMMA = 0.1
    # - Multi-step: LR_SCHEDULER_TYPE = "MultiStepLR", LR_SCHEDULER_MILESTONES = [50, 100], LR_SCHEDULER_GAMMA = 0.1
    # - Cosine: LR_SCHEDULER_TYPE = "CosineAnnealingLR", LR_SCHEDULER_T_MAX = 150, LR_SCHEDULER_ETA_MIN = 1e-6
    LR_SCHEDULER_TYPE: str = "SequentialLR"           # Scheduler type: "", StepLR, MultiStepLR, CosineAnnealingLR, SequentialLR, etc.
    
    # Warmup + Cosine Annealing parameters
    # Phase 1: Linear warmup (rounds 1-10)
    LR_SCHEDULER_PHASE1_START_FACTOR: float = 0.01  # Start at 1% of target LR
    LR_SCHEDULER_PHASE1_TOTAL_ITERS: int = 10       # Warmup for 10 rounds
    
    # Phase 2: Cosine annealing (rounds 11-100) 
    LR_SCHEDULER_PHASE2_T_MAX: int = 90             # Cosine annealing for 40 rounds (11-50)
    LR_SCHEDULER_PHASE2_ETA_MIN: float = 1e-8       # End at very small LR
    
    # Scheduler switch point: after round 10, switch from warmup to cosine annealing
    LR_SCHEDULER_MILESTONES: List[int] = field(default_factory=lambda: [11])
    
    # Legacy parameters (for non-SequentialLR schedulers)
    # LR_SCHEDULER_STEP_SIZE: int = 30      # StepLR: step size (rounds)
    # LR_SCHEDULER_GAMMA: float = 0.1       # StepLR/MultiStepLR: decay factor
    # LR_SCHEDULER_T_MAX: int = 100         # CosineAnnealingLR: total rounds
    # LR_SCHEDULER_ETA_MIN: float = 1e-6    # CosineAnnealingLR: minimum learning rate
    
    # === BitTorrent Settings ===
    BITTORRENT_TIMEOUT: float = 160.0     # BitTorrent timeout 5min
    BT_CHUNK_SELECTION: str = "rarest_first"  # Chunk selection strategy
    BT_MIN_COMPLETION_RATIO: float = 0.8   # Minimum completion ratio
    BT_PARAMETER_COMPLETION: str = "global_model"  # Parameter completion strategy: 'global_model', 'zeros', 'local_model'
    # BT_PARAMETER_COMPLETION: str = "zeros"  # Parameter completion strategy: 'global_model', 'zeros', 'local_model'

    # === Compensation Settings ===
    BT_ENABLE_COMPENSATION: bool = True    # Enable post-aggregation compensation for partial chunk collection
                                           # Set to False for complete collection scenarios to reduce CPU overhead

    # === Chunk Selection Algorithm Parameters ===
    BT_RARITY_WEIGHT: float = 0.01         # tau: rarity weight
    BT_RARITY_ADJUSTMENT: float = 1e-6     # eps: rarity adjustment parameter
    BT_RANDOM_NOISE: float = 1e-4          # gamma: random noise strength

    # === Write Queue Parameters ===
    BT_WRITE_QUEUE_SIZE: int = 500        # ChunkWriteQueue maximum capacity
    
    # === Topology Settings ===
    TOPOLOGY_TYPE: str = "mesh"         # Topology type: star, ring, fully_connected, mesh, random
    TOPOLOGY_TIMEOUT: float = 600.0       # Topology construction timeout
    TOPOLOGY_CONNECTIONS: int = 4         # Connections per node (mesh: exact connections, random: minimum connections)
    
    # === Network Simulation Settings ===
    ENABLE_NETWORK_SIMULATION: bool = True # Enable network simulation
    DOCKER_BASE_IMAGE: str = "flv2:base"  # Docker base image
    
    # === Ray Resource Settings ===
    RAY_AUTO_GPU_DETECTION: bool = True   # Automatic GPU detection
    RAY_MAX_CPUS: Optional[int] = None     # Maximum CPU count (None=auto)
    RAY_MAX_GPUS: Optional[int] = None     # Maximum GPU count (None=auto)
    
    # === Monitoring Settings ===
    MONITOR_DURATION: int = 999999999           # Monitoring duration (seconds)
    LOG_LEVEL: str = "DEBUG"               # Log level
    ENABLE_RAY_DASHBOARD: bool = True     # Enable Ray Dashboard
    
    # === Output Settings ===
    OUTPUT_DIR: str = "ray_v2_output"     # Output directory
    LOG_DIR: str = "logs"                 # Log directory
    
    # === Early Stopping Settings ===
    EARLY_STOP_PATIENCE: int = 1000         # Number of rounds to wait before stopping
    EARLY_STOP_DELTA: float = 0.0         # Minimum improvement threshold
    EARLY_STOP_MODE: str = "best"         # Improvement tracking mode
    
    # === Training Framework Settings ===
    # CRITERION_TYPE: str = "CrossEntropyLoss"  # Original: Multi-class loss (for >2 classes), not optimal for binary classification
    CRITERION_TYPE: str = "BCEWithLogitsLoss"  # Binary Cross-Entropy with Logits (for CelebA binary: male/female)
                                               # BCEWithLogitsLoss = Sigmoid + BCE, numerically stable
                                               # Requires: model output = raw logits (no Sigmoid), labels = [0, 1] float
    TRAINER_TYPE: str = "cvtrainer"           # Trainer type: cvtrainer, nlptrainer
    EVAL_FREQ: int = 1                        # Evaluation frequency
    EVAL_METRICS: List[str] = field(default_factory=lambda: ['acc', 'correct', 'f1'])  # Evaluation metrics: acc, correct, f1 (macro F1 score)
    EVAL_BEST_KEY: str = "val_acc"           # Key to track for best results
    
    # === Data Processing Settings ===
    DATA_ROOT: str = "data/"                  # Data root directory
    DATA_SPLITS: List[float] = field(default_factory=lambda: [0.8, 0.1, 0.1])  # Train/val/test splits (CelebA default)
    DATA_SUBSAMPLE: float = 1.0               # Data subsample ratio - controls total dataset size 1.0 = use full dataset, 0.1 = use only 10% of data
    DATA_SPLITTER: str = "lda"                # Data splitter method
                                              # Supported splitters:
                                              # Generic: 'lda' (non-IID), 'iid' (uniform)
                                              # Graph node-level: 'louvain' (community), 'random'
                                              # Graph link-level: 'rel_type' (relation-based)
                                              # Graph graph-level: 'rand_chunk' (random chunks)
                                              # Molecular: 'scaffold' (structure), 'scaffold_lda' (combined)
                                              # Custom: can register via register_splitter()
                                              # Note: DATA_SPLIT_ALPHA (line 100) is used as the 'alpha' parameter
                                              # for LDA splitter - smaller alpha = more heterogeneous data
    DATA_MERGE_LEAF_BEFORE_SPLIT: bool = True  # For LEAF datasets (femnist, celeba, etc.):
                                              # False: Use original user-based split (1 client = 1 celebrity/writer)
                                              # True: Merge all users' data, then re-split using DATA_SPLITTER
                                              # Only affects LEAF datasets; other datasets unaffected
    DATA_TRANSFORM_MEAN: List[float] = field(default_factory=lambda: [0.485, 0.456, 0.406])  # CelebA/ImageNet normalize mean (RGB channels)
    DATA_TRANSFORM_STD: List[float] = field(default_factory=lambda: [0.229, 0.224, 0.225])   # CelebA/ImageNet normalize std (RGB channels)
    # DATA_TRANSFORM_MEAN: List[float] = field(default_factory=lambda: [0.4914, 0.4822, 0.4465])  # CIFAR-10 normalize mean (RGB channels)
    # DATA_TRANSFORM_STD: List[float] = field(default_factory=lambda: [0.2470, 0.2435, 0.2616])   # CIFAR-10 normalize std (RGB channels)
    # DATA_TRANSFORM_MEAN: List[float] = field(default_factory=lambda: [0.1307])  # MNIST normalize mean (grayscale)
    # DATA_TRANSFORM_STD: List[float] = field(default_factory=lambda: [0.3081])   # MNIST normalize std (grayscale)
    DATA_AUTO_DOWNLOAD: bool = True           # Auto-download dataset
    DATA_SHARE_TEST_DATASET: bool = True      # All clients use same complete test set for global eval
                                              # val dataset remains split for local performance tracking
    DATALOADER_NUM_WORKERS: int = 0           # DataLoader workers (0 for Docker compatibility)
    
    # === Device Performance Settings ===
    DEVICE_GPU_RATIOS: Dict[str, float] = field(default_factory=lambda: {
        "edge_server": 0.5,        # Edge server - high performance 50%
        "smartphone_high": 0.35,   # High-end phone - medium performance 35% 
        "smartphone_low": 0.25,    # Low-end phone - low performance 25%
        "raspberry_pi": 0.25,      # Raspberry Pi - low performance 25%
        "iot_device": 0.15,        # IoT device - lowest performance 15%
    })
    TARGET_UTILIZATION: float = 0.95      # Target GPU utilization (95%)
    
    # === Chunk Database Retention Settings ===
    CHUNK_KEEP_ROUNDS: int = 1             # Number of recent rounds to keep in database
    
    # === Advanced FederatedScope Settings ===
    BACKEND: str = "torch"                 # Backend framework: torch, tensorflow
    FEDERATE_MAKE_GLOBAL_EVAL: bool = False # Enable global evaluation
    FEDERATE_SHARE_LOCAL_MODEL: bool = False # Share local model between clients
    FEDERATE_ONLINE_AGGR: bool = False     # Online aggregation
    FEDERATE_SAVE_TO: str = ""            # Save federated model path
    FEDERATE_RESTORE_FROM: str = ""       # Restore federated model path
    
    # === Asynchronous Settings ===
    ASYN_USE: bool = False                # Enable asynchronous training
    ASYN_MIN_RECEIVED_NUM: int = 2        # Minimum received messages for async (FederatedScope default)
    ASYN_STALENESS_TOLERATION: int = 0    # Staleness tolerance
    ASYN_TIME_BUDGET: int = 0             # Time budget for async training (FederatedScope default)
    
    # === Quantization Settings ===
    QUANTIZATION_METHOD: str = "none"     # Quantization method: none, uniform, etc. (FederatedScope default)
    QUANTIZATION_NBITS: int = 8           # Number of bits for quantization
    
    # === Aggregator Settings ===
    AGGREGATOR_ROBUST_RULE: str = "fedavg" # Robust aggregation rule
    
    # === gRPC Setting ===
    GRPC_KEEPALIVE_TIME_MS: int = 180000           # 3 min 
    GRPC_KEEPALIVE_TIMEOUT_MS: int = 60000         # 60 s 
    GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS: bool = True 
    GRPC_MIN_TIME_BETWEEN_PINGS_MS: int = 60000    
    GRPC_MIN_PING_INTERVAL_WITHOUT_DATA_MS: int = 600000 
    GRPC_MAX_CONNECTION_IDLE_MS: int = 300000      
    GRPC_MAX_CONNECTION_AGE_MS: int = 1200000      
    GRPC_MAX_CONNECTION_AGE_GRACE_MS: int = 60000  
    
    # === Wandb Settings ===
    WANDB_USE: bool = False               # Enable Weights & Biases logging
    WANDB_CLIENT_TRAIN_INFO: bool = False # Log client training information

# Edge device configuration profile library
EDGE_DEVICE_PROFILES = {
    "smartphone_high": EdgeDeviceProfile(
        device_id="smartphone_high",
        device_type="smartphone", 
        docker_image="flv2:base",  # Temporarily use base image
        cpu_limit="1.0", memory_limit="2g", storage_limit="64g",
        bandwidth_up_kbps=9460000000610, bandwidth_down_kbps=2410000000750,
        latency_ms=0, packet_loss_rate=0.0, jitter_ms=0,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        mobility_pattern="static"
    ),
    
    "smartphone_low": EdgeDeviceProfile(
        device_id="smartphone_low", 
        device_type="smartphone",
        docker_image="flv2:base",  # Temporarily use base image
        cpu_limit="1.0", memory_limit="2g", storage_limit="64g",
        bandwidth_up_kbps=94600000000610, bandwidth_down_kbps=240000001750,
        latency_ms=0, packet_loss_rate=0.0, jitter_ms=0,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        battery_constraint=False, mobility_pattern="static"
    ),
    
    "raspberry_pi": EdgeDeviceProfile(
        device_id="raspberry_pi",
        device_type="edge_device",
        docker_image="flv2:base",  # Temporarily use base image
        cpu_limit="0.6", memory_limit="2g", storage_limit="64g",
        bandwidth_up_kbps=1760000000610, bandwidth_down_kbps=2410000000750,
        latency_ms=0, packet_loss_rate=0.0, jitter_ms=0,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        mobility_pattern="static"
    ),
    
    "iot_device": EdgeDeviceProfile(
        device_id="iot_device",
        device_type="iot",
        docker_image="flv2:base",  # Temporarily use base image
        cpu_limit="0.1", memory_limit="2g", storage_limit="2g", 
        bandwidth_up_kbps=1760000000610, bandwidth_down_kbps=2410000000750,
        latency_ms=0, packet_loss_rate=0.0, jitter_ms=0,
        training_speed_multiplier=1.0, availability_ratio=1.0,
        battery_constraint=False, mobility_pattern="static"
    ),
    
    "edge_server": EdgeDeviceProfile(
        device_id="edge_server",
        device_type="edge_server", 
        docker_image="flv2:base",  # Temporarily use base image
        cpu_limit="2.0", memory_limit="2g", storage_limit="100g",
        bandwidth_up_kbps=10000000000, bandwidth_down_kbps=10000000000000,
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
                
                # Remove existing queue rules (both egress and ingress)
                "tc qdisc del dev eth0 root 2>/dev/null || true",
                # "tc qdisc del dev eth0 ingress 2>/dev/null || true",
                
                # 🚀 FIX: Create root HTB queue with correct default class
                "tc qdisc add dev eth0 root handle 1: htb default 1",
                
                # Set total bandwidth limit (upstream) - this becomes the default class
                f"tc class add dev eth0 parent 1: classid 1:1 htb rate {profile.bandwidth_up_kbps}kbit ceil {profile.bandwidth_up_kbps}kbit",
                
                # Add network delay, jitter, and packet loss
                f"tc qdisc add dev eth0 parent 1:1 handle 10: netem delay {profile.latency_ms}ms {profile.jitter_ms}ms loss {profile.packet_loss_rate * 100}%",
                
                # # 🚀 FIX: Add ingress (download) bandwidth limiting
                # f"tc qdisc add dev eth0 ingress",
                # f"tc filter add dev eth0 parent ffff: protocol ip prio 50 u32 match ip src 0.0.0.0/0 police rate {profile.bandwidth_down_kbps}kbit burst 10k drop flowid :1"
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
                "bandwidth_down_kbps": profile.bandwidth_down_kbps,  # 🚀 Added download bandwidth tracking
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
        self.local_docker_root = os.path.join(PROJECT_ROOT, "docker_data")
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
                parent_dir = os.path.dirname(PROJECT_ROOT)
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
        
    def optimize_host_network_settings(self):
        """Optimize host system network settings for ultra-high concurrency"""
        print("🚀 Applying host network optimizations for ultra-high concurrency...")
        
        # System-level network optimizations
        sysctl_settings = {
            # TCP connection optimizations
            "net.core.somaxconn": "65535",
            "net.core.netdev_max_backlog": "30000", 
            "net.core.rmem_default": "16777216",
            "net.core.rmem_max": "134217728",
            "net.core.wmem_default": "16777216",
            "net.core.wmem_max": "134217728",
            "net.ipv4.tcp_rmem": "4096 16777216 134217728", 
            "net.ipv4.tcp_wmem": "4096 16777216 134217728",
            "net.ipv4.tcp_mem": "786432 1048576 26777216",
            "net.ipv4.tcp_max_syn_backlog": "30000",
            "net.ipv4.tcp_fin_timeout": "15",
            "net.ipv4.tcp_keepalive_time": "30",
            "net.ipv4.tcp_keepalive_intvl": "5",
            "net.ipv4.tcp_keepalive_probes": "3",
            "net.ipv4.tcp_tw_reuse": "1",
            "net.ipv4.ip_local_port_range": "1024 65535",
            
            # Connection tracking optimizations
            "net.netfilter.nf_conntrack_max": "1048576",
            "net.netfilter.nf_conntrack_tcp_timeout_established": "600",
            "net.netfilter.nf_conntrack_tcp_timeout_close_wait": "10",
            "net.netfilter.nf_conntrack_tcp_timeout_fin_wait": "10",
            
            # File descriptor limits
            "fs.file-max": "2097152",
            "fs.nr_open": "2097152"
        }
        
        # Apply sysctl settings (non-destructive, will be reset on reboot)
        applied_settings = []
        for key, value in sysctl_settings.items():
            try:
                result = subprocess.run([
                    'sudo', 'sysctl', '-w', f'{key}={value}'
                ], capture_output=True, text=True, timeout=5)
                
                if result.returncode == 0:
                    applied_settings.append(f"{key}={value}")
                else:
                    print(f"⚠️  Warning: Failed to set {key}={value}: {result.stderr.strip()}")
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError) as e:
                print(f"⚠️  Warning: Cannot apply sysctl {key}={value}: {e}")
        
        if applied_settings:
            print(f"✅ Applied {len(applied_settings)} host network optimizations")
        else:
            print("⚠️  Warning: No host network optimizations could be applied (may need sudo access)")
    
    def setup_docker_environment(self):
        """Set up Docker environment"""
        try:
            # Create dedicated network with ultra-high concurrency optimizations
            try:
                self.fl_network = self.client.networks.get(CONFIG.DOCKER_NETWORK_NAME)
                print(f"📶 Using existing Docker network: {CONFIG.DOCKER_NETWORK_NAME}")
            except docker.errors.NotFound:
                self.fl_network = self.client.networks.create(
                    CONFIG.DOCKER_NETWORK_NAME,
                    driver="bridge",
                    options={
                        # Enable inter-container communication and IP masquerade
                        "com.docker.network.bridge.enable_icc": "true",
                        "com.docker.network.bridge.enable_ip_masquerade": "true",
                        
                        # High-concurrency bridge optimizations
                        "com.docker.network.bridge.name": CONFIG.DOCKER_NETWORK_NAME,
                        "com.docker.network.bridge.mtu": "1500",
                        "com.docker.network.bridge.forward_delay": "0",
                        "com.docker.network.bridge.max_age": "0",
                        "com.docker.network.bridge.stp": "false",
                        
                        # Network buffer optimizations
                        "com.docker.network.bridge.host_binding_ipv4": "0.0.0.0",
                        "com.docker.network.bridge.default_bridge": "false",
                        
                        # Advanced bridge settings for high throughput
                        "com.docker.network.bridge.enable_multicast": "true"
                    },
                    ipam=docker.types.IPAMConfig(
                        driver="default",
                        pool_configs=[
                            docker.types.IPAMPool(
                                subnet="172.30.0.0/16",  # Large subnet for many containers
                                gateway="172.30.0.1"
                            )
                        ]
                    )
                )
                print(f"📶 Creating optimized Docker network: {CONFIG.DOCKER_NETWORK_NAME}")
            
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
            
            # Clean up local Docker storage temp files and all mount directories
            try:
                # Clean Docker data directories completely (use PROJECT_ROOT for consistency)
                docker_cleanup_dirs = [
                    f"{self.local_docker_root}/tmp",
                    os.path.join(PROJECT_ROOT, "docker_data/tmp"),
                    os.path.join(PROJECT_ROOT, "docker_data/app_tmp"),
                ]

                # Clean individual client directories in docker_data/app_tmp/client_*/
                app_tmp_dir = os.path.join(PROJECT_ROOT, "docker_data/app_tmp")
                if os.path.exists(app_tmp_dir):
                    try:
                        for item in os.listdir(app_tmp_dir):
                            if item.startswith('client_'):
                                client_dir = os.path.join(app_tmp_dir, item)
                                if os.path.isdir(client_dir):
                                    docker_cleanup_dirs.append(client_dir)
                                    print(f"🗑️  Adding client directory for cleanup: {client_dir}")
                    except Exception as e:
                        print(f"⚠️  Failed to enumerate client directories: {e}")
                
                for cleanup_dir in docker_cleanup_dirs:
                    if os.path.exists(cleanup_dir):
                        try:
                            # Use Docker to clean up root-owned files (no password needed)
                            result = subprocess.run([
                                'docker', 'run', '--rm', '-v', f"{cleanup_dir}:/cleanup",
                                'alpine:latest', 'sh', '-c', 'rm -rf /cleanup/*'
                            ], capture_output=True, text=True, timeout=10)
                            
                            if result.returncode == 0:
                                print(f"🗑️  Docker-cleaned directory: {cleanup_dir}")
                            else:
                                # Fallback to regular rm
                                if cleanup_dir and cleanup_dir != "/":
                                    subprocess.run(f"rm -rf {cleanup_dir}/*", shell=True, check=False)
                                    print(f"🗑️  Cleaned directory (fallback): {cleanup_dir}")
                        except (subprocess.TimeoutExpired, Exception):
                            # If Docker cleanup fails, try regular rm
                            if cleanup_dir and cleanup_dir != "/":
                                subprocess.run(f"rm -rf {cleanup_dir}/*", shell=True, check=False)
                            print(f"🗑️  Cleaned directory (simple): {cleanup_dir}")
                            
            except Exception as e:
                print(f"⚠️  Docker storage cleanup warning: {e}")
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
                CONFIG.DOCKER_BASE_IMAGE,  # "flv2:base"
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
                "tag": "flv2:base",
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
                # Use Docker Python API to build image
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

        # Images are incomplete, auto-build without confirmation
        print("\n🔨 Missing Docker images detected, starting auto-build...")
        print("   This may take 5-10 minutes...")

        return self.build_required_images()


class PodmanManager:
    """
    🚀 Podman rootless container manager for HPC environments.

    Key advantages over Docker:
    - No root privileges required (rootless mode)
    - User namespace isolation with custom sysctl settings
    - Compatible with Docker images and Dockerfiles
    - Better suited for HPC environments
    """

    def __init__(self):
        self.fl_network = None
        self.local_podman_root = os.path.join(PROJECT_ROOT, "podman_data")
        self.podman_available = self._check_podman_availability()
        self.containers = {}  # Track running containers

        if self.podman_available:
            self._setup_local_storage()

    def _check_podman_availability(self) -> bool:
        """Check if Podman is available and working"""
        try:
            result = subprocess.run(['which', 'podman'],
                                  capture_output=True, text=True, timeout=2)
            if result.returncode != 0:
                print("⚠️  Podman command not found")
                return False

            result = subprocess.run(['podman', '--version'],
                                  capture_output=True, text=True, timeout=3)
            if result.returncode != 0:
                print("⚠️  Podman version check failed")
                return False
            print(f"🦭 Podman version: {result.stdout.strip()}")

            # Try a simple podman command to verify it works
            result = subprocess.run(['podman', 'ps', '--format', '{{.ID}}'],
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print("✅ Podman rootless mode available")
                return True
            else:
                # Try podman info as fallback
                result = subprocess.run(['podman', 'info', '--format', 'json'],
                                      capture_output=True, text=True, timeout=15)
                if result.returncode == 0:
                    print("✅ Podman available (info check passed)")
                    return True
                print(f"⚠️  Podman check failed: {result.stderr.strip()[:200]}")
                return False

        except Exception as e:
            print(f"⚠️  Podman not available: {e}")
            return False

    def _setup_local_storage(self):
        """Setup local storage directories for Podman"""
        try:
            storage_dirs = [
                f"{self.local_podman_root}/tmp",
                f"{self.local_podman_root}/volumes",
                f"{self.local_podman_root}/containers"
            ]
            for storage_dir in storage_dirs:
                os.makedirs(storage_dir, exist_ok=True)
            print(f"🦭 Podman storage configured: {self.local_podman_root}")
        except Exception as e:
            print(f"⚠️  Failed to setup Podman storage: {e}")

    def setup_network(self) -> bool:
        """
        Create optimized Podman network for FL training.
        Uses bridge network with custom MTU and options.
        """
        try:
            network_name = CONFIG.CONTAINER_NETWORK_NAME

            # Check if network exists
            result = subprocess.run(
                ['podman', 'network', 'exists', network_name],
                capture_output=True, text=True, timeout=5
            )

            if result.returncode == 0:
                print(f"📶 Using existing Podman network: {network_name}")
                self.fl_network = network_name
                return True

            # Create new network with optimizations
            # Using subnet to avoid conflicts
            create_cmd = [
                'podman', 'network', 'create',
                '--driver', 'bridge',
                '--subnet', '172.30.0.0/16',
                '--gateway', '172.30.0.1',
                '-o', 'mtu=9000',  # Jumbo frames for better throughput
                network_name
            ]

            result = subprocess.run(create_cmd, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                print(f"📶 Created optimized Podman network: {network_name}")
                self.fl_network = network_name
                return True
            else:
                print(f"⚠️  Failed to create network: {result.stderr}")
                return False

        except Exception as e:
            print(f"❌ Network setup failed: {e}")
            return False

    def get_network_sysctl_options(self) -> List[str]:
        """
        Get sysctl options for container network optimization.
        These are applied per-container in user namespace.
        """
        # Note: In rootless mode, only certain sysctls are allowed
        # Focus on network buffer sizes that are permitted
        return [
            '--sysctl', 'net.core.somaxconn=65535',
            '--sysctl', 'net.core.netdev_max_backlog=30000',
            '--sysctl', 'net.ipv4.tcp_max_syn_backlog=30000',
            '--sysctl', 'net.ipv4.tcp_fin_timeout=15',
            '--sysctl', 'net.ipv4.tcp_tw_reuse=1',
        ]

    def get_ulimit_options(self) -> List[str]:
        """Get ulimit options for high-concurrency FL"""
        return [
            '--ulimit', 'nofile=1048576:1048576',
            '--ulimit', 'nproc=65536:65536',
        ]

    def build_image(self, dockerfile_path: str, tag: str, context_path: str = None) -> bool:
        """Build container image using Podman"""
        if context_path is None:
            context_path = PROJECT_ROOT

        try:
            print(f"🔨 Building image {tag} from {dockerfile_path}...")

            build_cmd = [
                'podman', 'build',
                '-f', dockerfile_path,
                '-t', tag,
                '--format', 'docker',  # Docker-compatible format
                context_path
            ]

            result = subprocess.run(
                build_cmd,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutes timeout
            )

            if result.returncode == 0:
                print(f"✅ Successfully built image: {tag}")
                return True
            else:
                print(f"❌ Build failed: {result.stderr[:500]}")
                return False

        except Exception as e:
            print(f"❌ Build error: {e}")
            return False

    def check_image_exists(self, image_name: str) -> bool:
        """Check if image exists"""
        try:
            result = subprocess.run(
                ['podman', 'image', 'exists', image_name],
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except:
            return False

    def ensure_image_ready(self) -> bool:
        """Ensure base image is ready"""
        image_name = CONFIG.BASE_CONTAINER_IMAGE

        if self.check_image_exists(image_name):
            print(f"✅ Podman image exists: {image_name}")
            return True

        print(f"🔨 Building base image: {image_name}")
        dockerfile = os.path.join(PROJECT_ROOT, "docker", "Dockerfile.base")
        return self.build_image(dockerfile, image_name)

    def run_container(self, name: str, image: str, command: List[str],
                     volumes: Dict[str, str] = None,
                     environment: Dict[str, str] = None,
                     gpu_id: int = None,
                     detach: bool = True) -> Optional[str]:
        """
        Run a container with optimized network settings.
        Returns container ID on success.
        """
        try:
            run_cmd = ['podman', 'run']

            if detach:
                run_cmd.append('-d')

            run_cmd.extend(['--name', name])

            # Network settings
            if self.fl_network:
                run_cmd.extend(['--network', self.fl_network])

            # Apply sysctl optimizations
            run_cmd.extend(self.get_network_sysctl_options())

            # Apply ulimits
            run_cmd.extend(self.get_ulimit_options())

            # Volumes
            if volumes:
                for host_path, container_path in volumes.items():
                    run_cmd.extend(['-v', f'{host_path}:{container_path}'])

            # Environment variables
            if environment:
                for key, value in environment.items():
                    run_cmd.extend(['-e', f'{key}={value}'])

            # GPU support (using NVIDIA Container Toolkit)
            if gpu_id is not None:
                run_cmd.extend([
                    '--device', f'nvidia.com/gpu={gpu_id}',
                    '--security-opt', 'label=disable'
                ])

            # Image and command
            run_cmd.append(image)
            run_cmd.extend(command)

            result = subprocess.run(run_cmd, capture_output=True, text=True, timeout=60)

            if result.returncode == 0:
                container_id = result.stdout.strip()
                self.containers[name] = container_id
                return container_id
            else:
                print(f"❌ Container start failed: {result.stderr[:200]}")
                return None

        except Exception as e:
            print(f"❌ Container run error: {e}")
            return None

    def stop_container(self, name: str) -> bool:
        """Stop a running container"""
        try:
            subprocess.run(['podman', 'stop', '-t', '10', name],
                         capture_output=True, timeout=30)
            subprocess.run(['podman', 'rm', '-f', name],
                         capture_output=True, timeout=10)
            self.containers.pop(name, None)
            return True
        except:
            return False

    def cleanup(self):
        """Cleanup all containers and network"""
        print("🧹 Cleaning up Podman environment...")

        # Stop all tracked containers
        for name in list(self.containers.keys()):
            self.stop_container(name)

        # Remove network
        if self.fl_network:
            try:
                subprocess.run(
                    ['podman', 'network', 'rm', '-f', self.fl_network],
                    capture_output=True, timeout=10
                )
                print(f"🗑️  Removed network: {self.fl_network}")
            except:
                pass

        # Clean temp files
        try:
            import shutil
            temp_dir = f"{self.local_podman_root}/tmp"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                os.makedirs(temp_dir, exist_ok=True)
        except:
            pass

        print("✅ Podman cleanup completed")


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

    def __init__(self, config: Dict[str, Any], use_gpu: bool = False):
        self.config = config
        self.use_gpu = use_gpu
        self.process = None
        # Use public IP for cross-region communication
        self.node_ip = get_public_ip()
        self.server_port = None
        
    def start(self) -> Tuple[str, int]:
        """Start local process server"""
        # Dynamically allocate port
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.server_port = sock.getsockname()[1]
        sock.close()

        # Check if UDS mode is enabled
        use_uds = self.config.get('distribute', {}).get('use_uds', False)
        uds_dir = self.config.get('distribute', {}).get('uds_dir', '/tmp/federatedscope_uds')

        if use_uds:
            # UDS mode: use unix socket path as host
            os.makedirs(uds_dir, exist_ok=True)
            uds_path = f"{uds_dir}/server_{self.server_port}.sock"
            # Remove stale socket if exists
            if os.path.exists(uds_path):
                os.remove(uds_path)
            self.config['distribute']['server_host'] = f"unix://{uds_path}"
            self.uds_path = uds_path
            print(f"🔌 Server UDS mode: {uds_path}")
        else:
            # TCP mode: use IP address
            self.config['distribute']['server_host'] = self.node_ip
            self.uds_path = None

        self.config['distribute']['server_port'] = self.server_port
        self.config['use_gpu'] = False  # Force CPU mode

        # Prepare configuration and output directories
        config_dir = f"{CONFIG.OUTPUT_DIR}/configs"
        output_dir = f"{CONFIG.OUTPUT_DIR}/server_output"
        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        # Set correct output directory (not Docker path)
        self.config['outdir'] = output_dir

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
            if self.use_gpu:
                # Get GPU IDs assigned by Ray
                ray_gpu_ids = ray.get_gpu_ids()
                if ray_gpu_ids:
                    env['CUDA_VISIBLE_DEVICES'] = str(int(ray_gpu_ids[0]))

            # 🚀 FIX: Set resource limits for high-concurrency FL (matching Docker ulimits)
            def set_resource_limits():
                """Set resource limits before exec (equivalent to Docker ulimits)"""
                import resource
                try:
                    # nofile: 1048576 (matching Docker ulimit)
                    resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
                except (ValueError, resource.error):
                    try:
                        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                    except:
                        pass
                try:
                    # nproc: 65536 (matching Docker ulimit)
                    resource.setrlimit(resource.RLIMIT_NPROC, (65536, 65536))
                except (ValueError, resource.error):
                    pass

            # Start process (using original version approach, including log redirection)
            cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]

            with open(self.log_file, 'w') as log_f:
                self.process = subprocess.Popen(
                    cmd, stdout=log_f, stderr=log_f, env=env,
                    preexec_fn=set_resource_limits  # 🚀 Apply resource limits
                )

            # Return server address info (IP for TCP, UDS path for UDS mode)
            if self.uds_path:
                return self.config['distribute']['server_host'], self.server_port
            else:
                return self.node_ip, self.server_port
        except Exception as e:
            print(f"Failed to start server process: {e}")
            return None, None

    def get_status(self) -> Dict:
        """Get process status"""
        if self.process is None:
            return {"status": "not_started"}

        if self.process.poll() is None:
            status = {
                "status": "running",
                "node_ip": self.node_ip,
                "server_port": self.server_port,
                "pid": self.process.pid
            }
            if self.uds_path:
                status["uds_path"] = self.uds_path
            return status
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
        # Use public IP for cross-region communication
        self.node_ip = get_public_ip()
        self.client_port = None
        
    def start(self) -> bool:
        """Start local process client"""
        # Dynamically allocate port
        import socket
        sock = socket.socket()
        sock.bind(('', 0))
        self.client_port = sock.getsockname()[1]
        sock.close()

        # Check if UDS mode is enabled
        use_uds = self.config.get('distribute', {}).get('use_uds', False)
        uds_dir = self.config.get('distribute', {}).get('uds_dir', '/tmp/federatedscope_uds')

        # Update client network configuration
        self.config['distribute']['server_host'] = self.server_ip  # May be UDS path from server
        self.config['distribute']['server_port'] = self.server_port

        if use_uds:
            # UDS mode: use unix socket path for client
            os.makedirs(uds_dir, exist_ok=True)
            uds_path = f"{uds_dir}/client_{self.client_id}_{self.client_port}.sock"
            # Remove stale socket if exists
            if os.path.exists(uds_path):
                os.remove(uds_path)
            self.config['distribute']['client_host'] = f"unix://{uds_path}"
            self.uds_path = uds_path
            print(f"🔌 Client {self.client_id} UDS mode: {uds_path}")
        else:
            # TCP mode
            self.config['distribute']['client_host'] = self.node_ip
            self.uds_path = None

        self.config['distribute']['client_port'] = self.client_port
        self.config['distribute']['data_idx'] = self.client_id

        # Client-specific seed (base seed + client_id for reproducibility with different data splits)
        self.config['seed'] = CONFIG.SEED + self.client_id

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

            # 🚀 FIX: Set resource limits for high-concurrency FL (matching Docker ulimits)
            def set_resource_limits():
                """Set resource limits before exec (equivalent to Docker ulimits)"""
                import resource
                try:
                    # nofile: 1048576 (matching Docker ulimit)
                    resource.setrlimit(resource.RLIMIT_NOFILE, (1048576, 1048576))
                except (ValueError, resource.error) as e:
                    # If we can't set that high, try a more modest limit
                    try:
                        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
                        # Try to maximize within system limits
                        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                    except:
                        pass
                try:
                    # nproc: 65536 (matching Docker ulimit)
                    resource.setrlimit(resource.RLIMIT_NPROC, (65536, 65536))
                except (ValueError, resource.error):
                    pass

            # Start process (using original version approach, including log redirection)
            cmd = [sys.executable, 'federatedscope/main.py', '--cfg', config_path]

            with open(self.log_file, 'w') as log_f:
                self.process = subprocess.Popen(
                    cmd, stdout=log_f, stderr=log_f, env=env,
                    preexec_fn=set_resource_limits  # 🚀 Apply resource limits
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

    def __init__(self, config: Dict[str, Any], use_gpu: bool = False):
        self.config = config
        self.use_gpu = use_gpu
        self.container = None
        self.docker_client = docker.from_env()
        # Use public IP for cross-region communication
        self.node_ip = get_public_ip()
        self.server_port = None
        self.container_name = "fl_server"
        
    def _get_absolute_path(self, path: str) -> str:
        """Map relative paths to /home/ubuntu/FLTorrent on all Ray nodes."""
        # If already absolute, return directly
        if os.path.isabs(path):
            return path
        # Otherwise map to PROJECT_ROOT
        return os.path.join(PROJECT_ROOT, path)

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
        
        if self.use_gpu:
            self.config['device'] = 0  # Container internal GPU ID (always 0 inside container)
            self.config['use_gpu'] = True
        
        # === Use absolute paths under PROJECT_ROOT for consistency across all Ray nodes ===
        config_dir = self._get_absolute_path(f"{CONFIG.OUTPUT_DIR}/configs")
        os.makedirs(config_dir, exist_ok=True)
        config_path = os.path.join(config_dir, "server.yaml")

        with open(config_path, 'w') as f:
            yaml.safe_dump(self.config, f)

        log_dir = self._get_absolute_path(CONFIG.LOG_DIR)
        os.makedirs(log_dir, exist_ok=True)

        server_output_dir = self._get_absolute_path(f"{CONFIG.OUTPUT_DIR}/server_output")
        os.makedirs(server_output_dir, exist_ok=True)
        
        # Docker container configuration with ultra-high concurrency optimizations
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
            
            # Volume mounts - use PROJECT_ROOT for consistent paths across all Ray nodes
            "volumes": {
                self._get_absolute_path(config_path): {"bind": "/app/config.yaml", "mode": "ro"},
                self._get_absolute_path(server_output_dir): {"bind": "/app/output", "mode": "rw"},  # Mount server output directory
                self._get_absolute_path(log_dir): {"bind": "/app/logs", "mode": "rw"},
                self._get_absolute_path("data"): {"bind": "/app/data", "mode": "rw"},
                os.path.join(PROJECT_ROOT, "docker_data/tmp"): {"bind": "/tmp", "mode": "rw"},
                os.path.join(PROJECT_ROOT, "docker_data/app_tmp"): {"bind": "/app/tmp", "mode": "rw"},  # Mount for chunk databases
                os.path.join(PROJECT_ROOT, "federatedscope"): {"bind": "/app/federatedscope", "mode": "rw"},  # 🚀 Mount local source code
                os.path.join(PROJECT_ROOT, "materials"): {"bind": "/app/materials", "mode": "ro"},  # Mount materials directory
                os.path.join(PROJECT_ROOT, "scripts"): {"bind": "/app/scripts", "mode": "ro"},  # Mount scripts directory
            },
            
            # Container-safe network optimizations (removing restricted sysctls)
            "sysctls": {
                # TCP connection optimizations (container-safe parameters)
                "net.ipv4.tcp_keepalive_time": "30",
                "net.ipv4.tcp_keepalive_intvl": "5",
                "net.ipv4.tcp_keepalive_probes": "3",
                "net.ipv4.tcp_fin_timeout": "15",
                "net.ipv4.tcp_tw_reuse": "1",
                "net.ipv4.ip_local_port_range": "1024 65535"
                # Note: Removed net.core.* and net.netfilter.* parameters that require host-level privileges
                # These optimizations are applied at host level via optimize_host_network_settings()
            },
            
            # Resource limits for high concurrency
            "ulimits": [
                docker.types.Ulimit(name="nofile", soft=1048576, hard=1048576),  # File descriptors
                docker.types.Ulimit(name="nproc", soft=65536, hard=65536),      # Processes
                docker.types.Ulimit(name="memlock", soft=-1, hard=-1)           # Memory lock
            ],
            
            # Enhanced network settings
            "network_mode": CONFIG.DOCKER_NETWORK_NAME,  # Use custom network
            "dns": ["8.8.8.8", "8.8.4.4"],             # Reliable DNS
            
            # Security optimizations for network performance
            "security_opt": ["apparmor:unconfined"],
            
            # Startup command - use shell wrapper to set working directory and environment, redirect logs to mounted directory
            "command": ["sh", "-c", "cd /app && PYTHONPATH=/app python federatedscope/main.py --cfg /app/config.yaml > /app/logs/server.log 2>&1"]
        }
        
        # GPU support - Use Ray's GPU allocation for correct local GPU ID
        if self.use_gpu:
            ray_gpu_ids = ray.get_gpu_ids()
            if ray_gpu_ids:
                local_gpu_id = int(ray_gpu_ids[0])
                container_config["device_requests"] = [
                    docker.types.DeviceRequest(device_ids=[str(local_gpu_id)], capabilities=[['gpu']])
                ]
                print(f"🎮 Server: Using GPU {local_gpu_id} (Ray assigned: {ray_gpu_ids})")
        
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
                "use_gpu": self.use_gpu,
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
                 server_ip: str, server_port: int, device_profile: EdgeDeviceProfile,
                 use_gpu: bool = False):
        self.client_id = client_id
        self.config = config.copy()
        self.server_ip = server_ip
        self.server_port = server_port
        self.device_profile = device_profile
        self.use_gpu = use_gpu  # Flag indicating whether to use GPU
        self.container = None
        self.docker_client = docker.from_env()
        # Use public IP for cross-region communication
        self.node_ip = get_public_ip()
        self.client_port = None
        self.container_name = f"fl_client_{client_id}"
        
    def _get_absolute_path(self, path: str) -> str:
        """Map relative paths to /home/ubuntu/FLTorrent on all Ray nodes."""
        # If already absolute, return directly
        if os.path.isabs(path):
            return path
        # Otherwise map to PROJECT_ROOT
        return os.path.join(PROJECT_ROOT, path)

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

        # Client-specific seed (base seed + client_id for reproducibility with different data splits)
        self.config['seed'] = CONFIG.SEED + self.client_id
        
        # 🎮 GPU configuration: based on Ray resource allocation, not container detection
        # Cannot detect CUDA before Docker container startup, should decide based on Ray GPU allocation
        # Note: use_gpu will be re-detected by FederatedScope after container startup
        self.config['device'] = 0  # Container default use GPU 0 (if available)
        self.config['use_gpu'] = True  # Temporary setting, will be re-detected inside container
        
        # Client output directory inside container
        self.config['outdir'] = "/app/output"

        # === Use absolute paths under PROJECT_ROOT for consistency across all Ray nodes ===
        config_dir = self._get_absolute_path(f"{CONFIG.OUTPUT_DIR}/configs")
        output_dir = self._get_absolute_path(f"{CONFIG.OUTPUT_DIR}/client_{self.client_id}_output")
        log_dir = self._get_absolute_path(CONFIG.LOG_DIR)
        data_dir = self._get_absolute_path(CONFIG.DATA_ROOT)

        os.makedirs(config_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        os.makedirs(data_dir, exist_ok=True)

        config_path = os.path.join(config_dir, f"client_{self.client_id}.yaml")
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
        
        # Docker container configuration with ultra-high concurrency optimizations
        container_config = {
            "image": profile.docker_image,
            "name": self.container_name,
            "hostname": f"client-{self.client_id}",
            "detach": True,
            "remove": True,
            
            # Resource limits
            # "cpu_period": cpu_period,
            # "cpu_quota": cpu_quota,
            # "mem_limit": profile.memory_limit,
            # "memswap_limit": profile.memory_limit,  # Disable swap
            
            # Port mapping
            "ports": {50052: self.client_port},
            
            # Environment variables - simple approach following original version
            "environment": {
                "PYTHONPATH": "/app"
            },
            
            # Volume mounts - use PROJECT_ROOT for consistent paths across all Ray nodes
            "volumes": {
                self._get_absolute_path(config_path): {"bind": "/app/config.yaml", "mode": "ro"},
                self._get_absolute_path(output_dir): {"bind": "/app/output", "mode": "rw"},
                self._get_absolute_path(log_dir): {"bind": "/app/logs", "mode": "rw"},
                self._get_absolute_path(data_dir): {"bind": "/app/data", "mode": "rw"},
                os.path.join(PROJECT_ROOT, "docker_data/tmp"): {"bind": "/tmp", "mode": "rw"},
                os.path.join(PROJECT_ROOT, "docker_data/app_tmp"): {"bind": "/app/tmp", "mode": "rw"},  # Mount for chunk databases
                os.path.join(PROJECT_ROOT, "federatedscope"): {"bind": "/app/federatedscope", "mode": "rw"},  # 🚀 Mount local source code
                os.path.join(PROJECT_ROOT, "materials"): {"bind": "/app/materials", "mode": "ro"},  # Mount materials directory
                os.path.join(PROJECT_ROOT, "scripts"): {"bind": "/app/scripts", "mode": "ro"},  # Mount scripts directory
            },
            
            # Container-safe network optimizations (removing restricted sysctls)
            "sysctls": {
                # TCP connection optimizations (container-safe parameters)
                "net.ipv4.tcp_keepalive_time": "300",
                "net.ipv4.tcp_keepalive_intvl": "50",
                "net.ipv4.tcp_keepalive_probes": "5",
                "net.ipv4.tcp_fin_timeout": "120",
                "net.ipv4.tcp_tw_reuse": "1",
                "net.ipv4.ip_local_port_range": "1024 65535"
                # Note: Removed net.core.* and net.netfilter.* parameters that require host-level privileges
                # These optimizations are applied at host level via optimize_host_network_settings()
            },
            
            # Resource limits for high concurrency
            "ulimits": [
                docker.types.Ulimit(name="nofile", soft=1048576, hard=1048576),  # File descriptors
                docker.types.Ulimit(name="nproc", soft=65536, hard=65536),      # Processes  
                docker.types.Ulimit(name="memlock", soft=-1, hard=-1)           # Memory lock
            ],
            
            # Enhanced network settings
            "network_mode": CONFIG.DOCKER_NETWORK_NAME,  # Use custom network
            "dns": ["8.8.8.8", "8.8.4.4"],             # Reliable DNS
            
            # Privileged mode (for network control and sysctls)
            "privileged": True,
            
            # Security optimizations for network performance
            "security_opt": ["apparmor:unconfined"],
            
            # Startup command - use shell wrapper to set working directory and environment, redirect logs to mounted directory
            "command": ["sh", "-c", f"cd /app && PYTHONPATH=/app python federatedscope/main.py --cfg /app/config.yaml > /app/logs/client_{self.client_id}.log 2>&1"]
        }
        
        # 🎮 GPU support - Use Ray's GPU allocation for correct local GPU ID
        if self.use_gpu:
            # Get GPU IDs assigned by Ray to this Actor
            ray_gpu_ids = ray.get_gpu_ids()
            if ray_gpu_ids:
                # Use the first GPU assigned by Ray (local device ID on this node)
                local_gpu_id = int(ray_gpu_ids[0])
                container_config["device_requests"] = [
                    docker.types.DeviceRequest(device_ids=[str(local_gpu_id)], capabilities=[['gpu']])
                ]
                # Set CUDA environment variable to limit visible GPUs
                container_config["environment"]["CUDA_VISIBLE_DEVICES"] = str(local_gpu_id)
                print(f"🎮 Client {self.client_id}: Using GPU {local_gpu_id} (Ray assigned: {ray_gpu_ids})")
            else:
                # Ray didn't assign any GPU, fall back to CPU mode
                container_config["environment"]["CUDA_VISIBLE_DEVICES"] = ""
                print(f"⚠️ Client {self.client_id}: use_gpu=True but Ray assigned no GPUs, using CPU mode")
        else:
            # CPU mode
            container_config["environment"]["CUDA_VISIBLE_DEVICES"] = ""
            print(f"💻 Client {self.client_id}: CPU mode, no GPU allocation")
        
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
            # Clear existing rules (both egress and ingress)
            "tc qdisc del dev eth0 root 2>/dev/null || true",
            # "tc qdisc del dev eth0 ingress 2>/dev/null || true",
            
            # 🚀 FIX: Set upstream bandwidth limit with correct default class
            "tc qdisc add dev eth0 root handle 1: htb default 1",
            f"tc class add dev eth0 parent 1: classid 1:1 htb rate {up_bandwidth} ceil {up_bandwidth}",
            
            # Add network delay and packet loss
            f"tc qdisc add dev eth0 parent 1:1 handle 10: netem delay {delay} {jitter} loss {loss}",
            
        #     # 🚀 FIX: Add ingress (download) bandwidth limiting
        #     f"tc qdisc add dev eth0 ingress",
        #     f"tc filter add dev eth0 parent ffff: protocol ip prio 50 u32 match ip src 0.0.0.0/0 police rate {down_bandwidth} burst 10k drop flowid :1",
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
# 🚀 Helper Functions
# ============================================================================

def build_scheduler_config():
    """Build scheduler configuration parameters based on scheduler type"""
    if CONFIG.LR_SCHEDULER_TYPE == "SequentialLR":
        # Warmup + Cosine annealing scheduler
        return {
            'type': 'SequentialLR',
            'schedulers': [
                {
                    'type': 'LinearLR',
                    'start_factor': CONFIG.LR_SCHEDULER_PHASE1_START_FACTOR,
                    'total_iters': CONFIG.LR_SCHEDULER_PHASE1_TOTAL_ITERS
                },
                {
                    'type': 'CosineAnnealingLR', 
                    'T_max': CONFIG.LR_SCHEDULER_PHASE2_T_MAX,
                    'eta_min': CONFIG.LR_SCHEDULER_PHASE2_ETA_MIN
                }
            ],
            'milestones': CONFIG.LR_SCHEDULER_MILESTONES
        }
    else:
        # Standard schedulers - pass through all available parameters
        scheduler_config = {'type': CONFIG.LR_SCHEDULER_TYPE}
        
        # Add all possible scheduler parameters if they exist
        for attr_name in dir(CONFIG):
            if attr_name.startswith('LR_SCHEDULER_') and attr_name != 'LR_SCHEDULER_TYPE':
                attr_value = getattr(CONFIG, attr_name)
                param_name = attr_name.replace('LR_SCHEDULER_', '').lower()
                scheduler_config[param_name] = attr_value
        
        return scheduler_config


@ray.remote
class PodmanFederatedScopeServer:
    """
    🦭 Podman-based FederatedScope Server Actor.
    Runs server in Podman container with optimized network settings.
    """

    def __init__(self, config: Dict[str, Any], use_gpu: bool = False, podman_manager: 'PodmanManager' = None):
        self.config = config
        self.use_gpu = use_gpu
        self.container_name = f"fl_server_{int(time.time())}"
        self.container_id = None
        self.process = None
        # Use provided podman_manager or create a new one
        self.podman_manager = podman_manager if podman_manager else PodmanManager()

    def start(self) -> Tuple[str, int]:
        """Start server in Podman container and return (host, port)"""
        try:
            if not self.podman_manager.podman_available:
                raise RuntimeError("Podman not available")

            # Ensure network is set up
            if not self.podman_manager.setup_network():
                raise RuntimeError("Failed to setup Podman network")

            # Ensure image is ready
            if not self.podman_manager.ensure_image_ready():
                raise RuntimeError("Failed to prepare container image")

            # Prepare config file
            config_path = self._prepare_config()

            # Prepare volumes
            volumes = {
                PROJECT_ROOT: '/app',
                os.path.join(PROJECT_ROOT, 'data'): '/app/data',
                os.path.join(PROJECT_ROOT, 'logs'): '/app/logs',
                self.config.get('output_dir', 'ray_v2_output/server_output'): '/app/output',
            }

            # Prepare environment
            environment = {
                'PYTHONPATH': '/app',
                'PYTHONUNBUFFERED': '1',
            }

            if self.use_gpu:
                environment['CUDA_VISIBLE_DEVICES'] = '0'

            # Run container
            command = ['python', '/app/federatedscope/main.py', '--cfg', f'/app/{config_path}']

            self.container_id = self.podman_manager.run_container(
                name=self.container_name,
                image=CONFIG.BASE_CONTAINER_IMAGE,
                command=command,
                volumes=volumes,
                environment=environment,
                gpu_id=0 if self.use_gpu else None,
                detach=True
            )

            if self.container_id:
                host = self.config.get('distribute', {}).get('server_host', 'localhost')
                port = self.config.get('distribute', {}).get('server_port', 50051)
                return (host, port)
            else:
                raise RuntimeError("Container start failed")

        except Exception as e:
            raise RuntimeError(f"Failed to start Podman server: {e}")

    def _prepare_config(self) -> str:
        """Prepare config file for container"""
        import yaml
        config_dir = os.path.join(PROJECT_ROOT, 'ray_v2_output', 'configs')
        os.makedirs(config_dir, exist_ok=True)
        config_path = os.path.join(config_dir, 'server_podman.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
        return os.path.relpath(config_path, PROJECT_ROOT)

    def get_status(self) -> Dict[str, Any]:
        """Get container status"""
        try:
            if not self.container_id:
                return {"status": "not_started"}

            result = subprocess.run(
                ['podman', 'inspect', '--format', '{{.State.Status}}', self.container_name],
                capture_output=True, text=True, timeout=5
            )
            status = result.stdout.strip() if result.returncode == 0 else "unknown"
            return {"status": status, "container_id": self.container_id}
        except:
            return {"status": "error"}

    def stop(self) -> bool:
        """Stop the container"""
        if self.podman_manager:
            return self.podman_manager.stop_container(self.container_name)
        return False


@ray.remote
class PodmanFederatedScopeClient:
    """
    🦭 Podman-based FederatedScope Client Actor.
    Runs client in Podman container with optimized network settings.
    """

    def __init__(self, client_id: int, config: Dict[str, Any],
                 server_ip: str, server_port: int, device_profile: EdgeDeviceProfile,
                 use_gpu: bool = False, podman_manager: 'PodmanManager' = None):
        self.client_id = client_id
        self.config = config.copy()
        self.server_ip = server_ip
        self.server_port = server_port
        self.device_profile = device_profile
        self.use_gpu = use_gpu
        self.container_name = f"fl_client_{client_id}_{int(time.time())}"
        self.container_id = None
        # Use provided podman_manager or create a new one
        self.podman_manager = podman_manager if podman_manager else PodmanManager()

    def start(self) -> bool:
        """Start client in Podman container"""
        try:
            if not self.podman_manager.podman_available:
                raise RuntimeError("Podman not available")

            # Network should already be set up by server
            self.podman_manager.fl_network = CONFIG.CONTAINER_NETWORK_NAME

            # Prepare config file
            config_path = self._prepare_config()

            # Prepare volumes
            output_dir = os.path.join(PROJECT_ROOT, f'ray_v2_output/client_{self.client_id}_output')
            log_dir = os.path.join(PROJECT_ROOT, 'logs')
            os.makedirs(output_dir, exist_ok=True)
            os.makedirs(log_dir, exist_ok=True)

            volumes = {
                PROJECT_ROOT: '/app',
                os.path.join(PROJECT_ROOT, 'data'): '/app/data',
                log_dir: '/app/logs',
                output_dir: '/app/output',
            }

            # Prepare environment
            environment = {
                'PYTHONPATH': '/app',
                'PYTHONUNBUFFERED': '1',
            }

            if self.use_gpu:
                environment['CUDA_VISIBLE_DEVICES'] = '0'

            # Run container
            command = ['python', '/app/federatedscope/main.py', '--cfg', f'/app/{config_path}']

            self.container_id = self.podman_manager.run_container(
                name=self.container_name,
                image=CONFIG.BASE_CONTAINER_IMAGE,
                command=command,
                volumes=volumes,
                environment=environment,
                gpu_id=0 if self.use_gpu else None,
                detach=True
            )

            if self.container_id:
                return True
            else:
                return False

        except Exception as e:
            print(f"❌ Podman client {self.client_id} start failed: {e}")
            return False

    def _prepare_config(self) -> str:
        """Prepare config file for container"""
        import yaml

        # Update config with server info
        self.config['distribute']['server_host'] = self.server_ip
        self.config['distribute']['server_port'] = self.server_port
        self.config['distribute']['role'] = 'client'

        config_dir = os.path.join(PROJECT_ROOT, 'ray_v2_output', 'configs')
        os.makedirs(config_dir, exist_ok=True)
        config_path = os.path.join(config_dir, f'client_{self.client_id}_podman.yaml')
        with open(config_path, 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)
        return os.path.relpath(config_path, PROJECT_ROOT)

    def get_status(self) -> Dict[str, Any]:
        """Get container status"""
        try:
            if not self.container_id:
                return {"status": "not_started", "client_id": self.client_id}

            result = subprocess.run(
                ['podman', 'inspect', '--format', '{{.State.Status}}', self.container_name],
                capture_output=True, text=True, timeout=5
            )
            status = result.stdout.strip() if result.returncode == 0 else "unknown"
            return {"status": status, "client_id": self.client_id, "container_id": self.container_id}
        except:
            return {"status": "error", "client_id": self.client_id}

    def stop(self) -> bool:
        """Stop the container"""
        if self.podman_manager:
            return self.podman_manager.stop_container(self.container_name)
        return False


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
        self.head_node_id = None
        self.worker_node_ids = []

    def _get_cluster_nodes(self) -> Tuple[str, List[str]]:
        """
        Get Ray cluster node information.
        Returns: (head_node_id, [worker_node_ids])

        The head node is identified as the node running the GCS (Global Control Store).
        """
        nodes = ray.nodes()
        head_node_id = None
        worker_node_ids = []

        for node in nodes:
            if not node["Alive"]:
                continue

            node_id = node["NodeID"]
            # Head node typically has the GCS running on it
            # We can identify it by checking if it's the node with the Ray dashboard or GCS
            # A simpler heuristic: the head node is usually the first one or has specific resources

            # Check node resources and metadata
            resources = node.get("Resources", {})

            # Store node info for logging
            node_ip = node.get("NodeManagerAddress", "unknown")
            self.logger.debug(f"Found node: {node_id[:12]}... IP: {node_ip}, Resources: {resources}")

            worker_node_ids.append(node_id)

        if not worker_node_ids:
            self.logger.error("No alive nodes found in Ray cluster!")
            return None, []

        # The head node is typically the one we're running on (first connected)
        # Get current node's ID
        current_node_ip = ray.util.get_node_ip_address()

        for node in nodes:
            if node["Alive"] and node.get("NodeManagerAddress") == current_node_ip:
                head_node_id = node["NodeID"]
                break

        # If we couldn't find head by IP, use the first node as head
        if head_node_id is None:
            head_node_id = worker_node_ids[0]

        # Remove head from worker list
        worker_node_ids = [nid for nid in worker_node_ids if nid != head_node_id]

        return head_node_id, worker_node_ids

    def _is_single_node(self) -> bool:
        """
        Check if running on a single node (no worker nodes).
        Used to determine if UDS can be enabled (UDS only works on single node).
        """
        try:
            nodes = ray.nodes()
            alive_nodes = [n for n in nodes if n.get("Alive", False)]
            is_single = len(alive_nodes) <= 1
            if is_single:
                self.logger.info("🔌 Single-node mode detected - UDS can be enabled")
            else:
                self.logger.info(f"🌐 Multi-node mode detected ({len(alive_nodes)} nodes) - UDS disabled, using TCP")
            return is_single
        except Exception as e:
            self.logger.warning(f"Failed to detect cluster topology: {e}, assuming single node")
            return True

    def _get_node_scheduling_strategy(self, node_id: str):
        """
        Create a NodeAffinitySchedulingStrategy for the given node.
        """
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
        return NodeAffinitySchedulingStrategy(
            node_id=node_id,
            soft=False  # Hard affinity - must run on this node
        )

    def _assign_clients_to_workers(self, num_clients: int, worker_node_ids: List[str]) -> List[Tuple[str, float]]:
        """
        Assign clients to ALL GPU nodes (including head node) based on GPU MEMORY.

        Returns: List of (node_id, cpu_fraction) for each client

        Distribution is proportional to total GPU memory on each node,
        ensuring each client gets approximately 3GB of GPU memory.
        """
        MEMORY_PER_CLIENT_GB = 3.0  # Each client needs ~3GB GPU memory

        # Build list of all nodes with their GPU info
        all_nodes_with_gpus = []

        # Include head node if it has GPUs
        head_gpus = self._get_node_gpus(self.head_node_id)
        head_memory = self._get_node_gpu_memory(self.head_node_id)
        if head_gpus > 0:
            all_nodes_with_gpus.append((self.head_node_id, head_gpus, head_memory))
            self.logger.info(f"📍 Head node ({self.head_node_id[:12]}...): {head_gpus} GPUs, {head_memory}GB total memory")

        # Include worker nodes
        for node_id in worker_node_ids:
            node_gpus = self._get_node_gpus(node_id)
            node_memory = self._get_node_gpu_memory(node_id)
            if node_gpus > 0:
                all_nodes_with_gpus.append((node_id, node_gpus, node_memory))
                self.logger.info(f"📍 Worker node ({node_id[:12]}...): {node_gpus} GPUs, {node_memory}GB total memory")

        if not all_nodes_with_gpus:
            self.logger.warning("No GPU nodes available, clients will run on head node")
            return [(self.head_node_id, 1.0 / num_clients)] * num_clients

        # Calculate total GPU memory
        total_memory = sum(mem for _, _, mem in all_nodes_with_gpus)
        self.logger.info(f"📊 Total GPU memory across all nodes: {total_memory}GB")
        self.logger.info(f"📊 Memory per client: {MEMORY_PER_CLIENT_GB}GB → Max capacity: {int(total_memory / MEMORY_PER_CLIENT_GB)} clients")

        # Distribute clients proportionally based on GPU MEMORY
        assignments = []
        clients_per_node = {}
        remaining_clients = num_clients

        for i, (node_id, node_gpus, node_memory) in enumerate(all_nodes_with_gpus):
            # Calculate max clients this node can handle
            max_clients_for_node = int(node_memory / MEMORY_PER_CLIENT_GB)

            if i == len(all_nodes_with_gpus) - 1:
                # Last node gets remaining clients
                node_clients = remaining_clients
            else:
                # Proportional distribution based on memory
                node_clients = int(num_clients * node_memory / total_memory)
                remaining_clients -= node_clients

            # Warn if node is overloaded
            if node_clients > max_clients_for_node:
                self.logger.warning(f"⚠️ Node {node_id[:12]}... assigned {node_clients} clients but max capacity is {max_clients_for_node}")

            clients_per_node[node_id] = node_clients
            memory_per_client = node_memory / node_clients if node_clients > 0 else 0
            self.logger.info(f"   Node {node_id[:12]}...: {node_clients} clients ({node_memory}GB / {node_clients} = {memory_per_client:.1f}GB/client)")

        # Build assignment list
        for node_id, node_clients in clients_per_node.items():
            for _ in range(node_clients):
                cpu_fraction = 1.0 / node_clients if node_clients > 0 else 1.0
                assignments.append((node_id, cpu_fraction))

        return assignments

    def _get_node_gpus(self, node_id: str) -> float:
        """Get the number of GPUs available on a specific node."""
        try:
            nodes = ray.nodes()
            for node in nodes:
                if node.get('NodeID') == node_id and node.get('Alive', False):
                    resources = node.get('Resources', {})
                    return resources.get('GPU', 0)
            return 0
        except Exception:
            return 0

    def _get_node_gpu_memory(self, node_id: str) -> float:
        """
        Get total GPU memory (in GB) available on a specific node.
        Uses Ray's accelerator_type resource labels to determine GPU type.
        """
        try:
            nodes = ray.nodes()
            for node in nodes:
                if node.get('NodeID') == node_id and node.get('Alive', False):
                    resources = node.get('Resources', {})
                    num_gpus = resources.get('GPU', 0)
                    if num_gpus == 0:
                        return 0

                    # Detect GPU type from Ray resource labels
                    # Ray exposes 'accelerator_type:XXX' resources
                    gpu_memory_per_gpu = 24.0  # Default

                    # Check for specific accelerator types in this node's resources
                    for resource_name in resources.keys():
                        if resource_name.startswith('accelerator_type:'):
                            gpu_type = resource_name.split(':')[1].upper()
                            if 'A100' in gpu_type:
                                if '80' in gpu_type:
                                    gpu_memory_per_gpu = 80.0
                                else:
                                    gpu_memory_per_gpu = 40.0  # A100-40GB
                            elif 'L4' in gpu_type:
                                gpu_memory_per_gpu = 23.0  # L4-24GB (actually 23GB usable)
                            elif 'L40' in gpu_type:
                                gpu_memory_per_gpu = 48.0
                            elif 'H100' in gpu_type:
                                gpu_memory_per_gpu = 80.0
                            elif 'V100' in gpu_type:
                                if '32' in gpu_type:
                                    gpu_memory_per_gpu = 32.0
                                else:
                                    gpu_memory_per_gpu = 16.0
                            elif 'T4' in gpu_type:
                                gpu_memory_per_gpu = 16.0
                            self.logger.info(f"🎮 Detected GPU type on node {node_id[:12]}...: {gpu_type} -> {gpu_memory_per_gpu}GB per GPU")
                            break

                    total_memory = num_gpus * gpu_memory_per_gpu
                    return total_memory
            return 0
        except Exception as e:
            self.logger.warning(f"Failed to get GPU memory for node {node_id}: {e}")
            return 0
        
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

        # Check if external Ray cluster address is provided
        external_addr = os.environ.get("RAY_ADDRESS")
        if external_addr:
            # Connect to external Ray cluster
            ray.init(address=external_addr, ignore_reinit_error=True)
            resources = ray.cluster_resources()
            self.logger.info(f"🚀 Connected to external Ray cluster: {external_addr}")
            self.logger.info(f"   📊 Resources: {dict(resources)}")
            return

        # ---- If no external address, initialize local Ray cluster ----
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
        parent_dir = os.path.dirname(PROJECT_ROOT)
        ray_temp_dir = os.path.join(parent_dir, "ray")
        os.makedirs(ray_temp_dir, exist_ok=True)
        
        # Calculate available memory (use 90% of system memory)
        total_memory = psutil.virtual_memory().total
        available_memory = int(total_memory * 0.9)  # Use 90% of total memory
        
        ray_config = {
            "num_cpus": num_cpus,
            "num_gpus": num_gpus or 0,
            "object_store_memory": int(available_memory * 0.3),  # 30% for object store
            "ignore_reinit_error": True,
            "_temp_dir": ray_temp_dir,  # Use shorter path to avoid socket path length issues
            "_system_config": {
                "automatic_object_spilling_enabled": True,
                "max_io_workers": 8,
                "min_spilling_size": 512 * 1024 * 1024,  # 1000MB
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
            'seed': CONFIG.SEED,  # Will be dynamically overridden for clients (CONFIG.SEED + client_id)
            
            'federate': {
                'client_num': CONFIG.CLIENT_NUM,
                'mode': 'distributed',
                'total_round_num': CONFIG.TOTAL_ROUNDS,
                'sample_client_num': CONFIG.CLIENT_NUM,
                'method': CONFIG.AGGREGATOR_TYPE,  # Use aggregator type from config
                'make_global_eval': CONFIG.FEDERATE_MAKE_GLOBAL_EVAL,
                'share_local_model': CONFIG.FEDERATE_SHARE_LOCAL_MODEL,
                'online_aggr': CONFIG.FEDERATE_ONLINE_AGGR,
                'save_to': CONFIG.FEDERATE_SAVE_TO,
                'restore_from': CONFIG.FEDERATE_RESTORE_FROM
            },
            
            'distribute': {
                'use': True,
                'server_host': '127.0.0.1',  # Will be dynamically overridden
                'server_port': 50051,        # Will be dynamically overridden
                'client_host': '127.0.0.1',  # Will be dynamically overridden
                'client_port': 50052,        # Will be dynamically overridden
                'role': 'server',            # Will be dynamically overridden
                'data_idx': 0,               # Will be dynamically overridden
                # 🚀 UDS settings - bypasses TCP buffer limits (rmem_max/wmem_max)
                'use_uds': CONFIG.USE_UDS and self._is_single_node(),  # Enable UDS only on single node
                'uds_dir': CONFIG.UDS_DIR,
            },
            
            'data': {
                'root': CONFIG.DATA_ROOT,
                'type': CONFIG.DATASET,
                'splits': CONFIG.DATA_SPLITS,
                'subsample': CONFIG.DATA_SUBSAMPLE,
                'splitter': CONFIG.DATA_SPLITTER,
                'splitter_args': [{'alpha': CONFIG.DATA_SPLIT_ALPHA}],
                'share_test_dataset': CONFIG.DATA_SHARE_TEST_DATASET,
                'merge_leaf_before_split': CONFIG.DATA_MERGE_LEAF_BEFORE_SPLIT,
                'transform': [
                    ['ToTensor'],
                    ['Normalize', {'mean': CONFIG.DATA_TRANSFORM_MEAN, 'std': CONFIG.DATA_TRANSFORM_STD}]
                ],
                'args': [{'download': CONFIG.DATA_AUTO_DOWNLOAD}]
            },
            
            'dataloader': {
                'batch_size': CONFIG.BATCH_SIZE,
                'num_workers': CONFIG.DATALOADER_NUM_WORKERS
            },
            
            'model': {
                'type': CONFIG.MODEL_TYPE,
                # 'hidden': CONFIG.MODEL_HIDDEN,
                'in_channels': 3,            # RGB channels for CIFAR-10
                # 'in_channels': 1,            # Grayscale channel for MNIST
                'out_channels': CONFIG.MODEL_OUT_CHANNELS,
                # 'embed_size': 8,             # Small embedding for testing (NLP only)
                'dropout': CONFIG.MODEL_DROPOUT
            },
            
            'train': {
                'local_update_steps': CONFIG.LOCAL_UPDATE_STEPS,
                'batch_or_epoch': 'epoch',   # Use epoch-based training for stability
                'optimizer': {
                    'lr': CONFIG.LEARNING_RATE,
                    'type': CONFIG.OPTIMIZER,
                    'weight_decay': CONFIG.WEIGHT_DECAY,
                    # 'momentum': CONFIG.OPTIMIZER_MOMENTUM    # Only for SGD, not for Adam
                },
                'scheduler': build_scheduler_config()
            },
            
            'grad': {
                'grad_clip': CONFIG.GRAD_CLIP
            },
            
            'criterion': {
                'type': CONFIG.CRITERION_TYPE
            },
            
            'trainer': {
                'type': CONFIG.TRAINER_TYPE
            },
            
            'eval': {
                'freq': CONFIG.EVAL_FREQ,
                'metrics': CONFIG.EVAL_METRICS,
                'best_res_update_round_wise_key': CONFIG.EVAL_BEST_KEY
            },

            'early_stop': {
                'patience': CONFIG.EARLY_STOP_PATIENCE,
                'delta': CONFIG.EARLY_STOP_DELTA,
                'improve_indicator_mode': CONFIG.EARLY_STOP_MODE
            },

            'personalization': {
                'local_param': CONFIG.PERSONALIZATION_LOCAL_PARAM  # FedBN: keep BN parameters local (not aggregated)
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
                'min_completion_ratio': CONFIG.BT_MIN_COMPLETION_RATIO,  # Use default from cfg_bittorrent.py
                'parameter_completion': CONFIG.BT_PARAMETER_COMPLETION, # Parameter completion strategy
                'enable_compensation': CONFIG.BT_ENABLE_COMPENSATION,   # Enable post-aggregation compensation
                'rarity_weight': CONFIG.BT_RARITY_WEIGHT,        # tau: rarity weight
                'rarity_adjustment': CONFIG.BT_RARITY_ADJUSTMENT, # eps: rarity adjustment parameter
                'random_noise': CONFIG.BT_RANDOM_NOISE,          # gamma: random noise strength
                'write_queue_size': CONFIG.BT_WRITE_QUEUE_SIZE   # ChunkWriteQueue maximum capacity
            },
            
            'chunk': {
                'num_chunks': CONFIG.CHUNK_NUM,
                'importance_method': CONFIG.IMPORTANCE_METHOD
            },
            
            'chunk_keep_rounds': CONFIG.CHUNK_KEEP_ROUNDS,
            
            'chunk_num': CONFIG.CHUNK_NUM,
            'chunk_importance_method': CONFIG.IMPORTANCE_METHOD,
            
            'backend': CONFIG.BACKEND,
            
            'asyn': {
                'use': CONFIG.ASYN_USE,
                'min_received_num': CONFIG.ASYN_MIN_RECEIVED_NUM,
                'staleness_toleration': CONFIG.ASYN_STALENESS_TOLERATION,
                'time_budget': CONFIG.ASYN_TIME_BUDGET
            },
            
            'quantization': {
                'method': CONFIG.QUANTIZATION_METHOD,
                'nbits': CONFIG.QUANTIZATION_NBITS
            },
            
            'aggregator': {
                'robust_rule': CONFIG.AGGREGATOR_ROBUST_RULE
            },
            
            'wandb': {
                'use': CONFIG.WANDB_USE,
                'client_train_info': CONFIG.WANDB_CLIENT_TRAIN_INFO
            },
            
            'outdir': '/app/output'
        }
        
    def allocate_gpu_resources(self, client_node_assignments: List[Tuple[str, float]] = None,
                                clients_per_node: Dict[str, int] = None) -> Tuple[Optional[float], List[Tuple[Optional[float], Optional[int]]]]:
        """
        Dynamic GPU resource allocation with per-node constraints.

        Key insight: GPU allocation must respect per-node limits. If a node has 1 GPU and 9 clients,
        each client can only get 1/9 = 0.111 GPU, not the global average of 6/50 = 0.12 GPU.

        Args:
            client_node_assignments: List of (node_id, _) tuples for each client
            clients_per_node: Dict mapping node_id to number of clients on that node
        """
        cluster_resources = ray.cluster_resources()
        available_gpus = float(cluster_resources.get('GPU', 0))
        num_physical_gpus = int(available_gpus)  # Number of physical GPUs

        if available_gpus == 0:
            self.logger.warning("⚠️ No GPU detected, all nodes use CPU mode")
            return None, [(None, None)] * CONFIG.CLIENT_NUM

        # 🖥️ Server fixed to use CPU (no GPU allocation)
        server_gpu = False  # use_gpu flag for server

        # 🎮 Get per-node GPU resources
        node_gpu_resources = {}
        for node in ray.nodes():
            if node['Alive']:
                node_id = node['NodeID']
                node_gpu = node['Resources'].get('GPU', 0)
                node_gpu_resources[node_id] = node_gpu

        # 🎯 Calculate max GPU per client for each node (considering SINGLE GPU capacity limit)
        # Key insight: Ray distributes clients across GPUs, so we must ensure
        # that even the most loaded GPU doesn't exceed 1.0 total allocation
        import math
        node_gpu_per_client = {}
        if clients_per_node:
            for node_id, num_clients in clients_per_node.items():
                node_gpu = node_gpu_resources.get(node_id, 0)
                if node_gpu > 0 and num_clients > 0:
                    # Calculate max clients that could be on a single GPU (worst case)
                    max_clients_per_single_gpu = math.ceil(num_clients / node_gpu)
                    # Each client's GPU allocation must ensure single GPU doesn't exceed 1.0
                    max_gpu_per_client = (1.0 * CONFIG.TARGET_UTILIZATION) / max_clients_per_single_gpu
                    node_gpu_per_client[node_id] = max_gpu_per_client
                    self.logger.info(f"   Node {node_id[:12]}...: {node_gpu} GPU, {num_clients} clients, "
                                   f"max {max_clients_per_single_gpu} clients/GPU -> {max_gpu_per_client:.4f} GPU/client")

        # Allocate GPU resources to each client based on their assigned node
        client_gpu_assignments = []
        actual_total_gpu = 0.0

        for i in range(CONFIG.CLIENT_NUM):
            # Get the node this client is assigned to
            if client_node_assignments and i < len(client_node_assignments):
                target_node_id = client_node_assignments[i][0]
                max_gpu = node_gpu_per_client.get(target_node_id, 0)
            else:
                # Fallback: use global average if no node assignment info
                max_gpu = (available_gpus * CONFIG.TARGET_UTILIZATION) / CONFIG.CLIENT_NUM

            # Round to 4 decimal places for precision
            allocated_gpu = round(max_gpu, 4)

            # Ray constraint: GPU quantities >1 must be whole numbers
            if allocated_gpu > 1.0:
                allocated_gpu = int(allocated_gpu)  # Round down to integer

            # use_gpu flag: True if any GPU is allocated
            use_gpu = allocated_gpu > 0
            client_gpu_assignments.append((allocated_gpu, use_gpu))
            actual_total_gpu += allocated_gpu
        
        # Generate allocation summary
        gpu_clients = 0
        cpu_clients = 0
        node_allocation_summary = {}

        for i, (allocated_gpu, use_gpu) in enumerate(client_gpu_assignments):
            # Count GPU vs CPU clients
            if use_gpu:
                gpu_clients += 1
            else:
                cpu_clients += 1

            # Per-node summary
            if client_node_assignments and i < len(client_node_assignments):
                node_id = client_node_assignments[i][0][:12]
                if node_id not in node_allocation_summary:
                    node_allocation_summary[node_id] = {'count': 0, 'total_gpu': 0.0}
                node_allocation_summary[node_id]['count'] += 1
                node_allocation_summary[node_id]['total_gpu'] += allocated_gpu

        gpu_summary = {
            "total_available_gpus": available_gpus,
            "total_allocated_gpus": round(actual_total_gpu, 4),
            "utilization_rate": f"{(actual_total_gpu/available_gpus)*100:.2f}%",
            "server": "CPU only",
            "per_node_allocation": node_allocation_summary,
            "gpu_clients": gpu_clients,
            "cpu_clients": cpu_clients
        }

        self.logger.info(f"🎯 GPU allocation summary: {gpu_summary}")
        self.logger.info(f"📋 Client distribution: {gpu_clients} GPU clients, {cpu_clients} CPU clients")
        
        return server_gpu, client_gpu_assignments
    
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
        variation_factor = random.uniform(1, 1.2)
        
        # CPU variation
        base_cpu = float(variant.cpu_limit)
        variant.cpu_limit = f"{base_cpu * variation_factor:.1f}"
        
        # Network variation
        variant.bandwidth_up_kbps = int(variant.bandwidth_up_kbps * variation_factor)
        variant.bandwidth_down_kbps = int(variant.bandwidth_down_kbps * variation_factor)
        variant.latency_ms = max(0, int(variant.latency_ms * variation_factor))
        
        # Availability randomization
        variant.availability_ratio *= random.uniform(0.9, 1.0)
        
        return variant
    
    def _get_node_resources(self, node_id: str) -> Dict[str, float]:
        """Get resources available on a specific node"""
        nodes = ray.nodes()
        for node in nodes:
            if node["NodeID"] == node_id and node["Alive"]:
                return node.get("Resources", {})
        return {}

    def _get_ray_resources_for_device(self, device_profile: EdgeDeviceProfile,
                                       target_node_id: str = None,
                                       clients_on_node: int = 1,
                                       is_head_node: bool = False) -> Dict[str, Any]:
        """
        Get Ray resource allocation based on device type and target node.

        Args:
            device_profile: Device configuration profile
            target_node_id: The node where this client will run (for per-node resource calculation)
            clients_on_node: Number of clients sharing this node
            is_head_node: Whether this is the head node (server runs here, need to reserve resources)
        """
        if target_node_id:
            # Get resources for the specific target node
            node_resources = self._get_node_resources(target_node_id)
            available_cpus = node_resources.get('CPU', 4)  # Default to 4 if unknown
        else:
            # Fallback: use cluster-wide resources (legacy behavior)
            cluster_resources = ray.cluster_resources()
            available_cpus = cluster_resources.get('CPU', 64)

        # Reserve CPU for Ray system overhead (10%)
        usable_cpus = available_cpus * 0.9

        # If this is head node, also reserve CPUs for server (2 CPUs)
        if is_head_node:
            usable_cpus -= 2  # Server uses 2 CPUs

        # Calculate CPU per client based on how many clients share this node
        cpu_per_client = usable_cpus / clients_on_node

        # Ensure minimum viable CPU allocation
        cpu_per_client = max(0.1, cpu_per_client)

        # Parse memory limit from device configuration
        memory_str = device_profile.memory_limit.lower()
        if memory_str.endswith('g'):
            memory_bytes = int(float(memory_str[:-1]) * 1024 * 1024 * 1024)
        elif memory_str.endswith('m'):
            memory_bytes = int(float(memory_str[:-1]) * 1024 * 1024)
        else:
            memory_bytes = 1024 * 1024 * 1024  # Default 1GB

        # Use calculated fractional CPU for all device types
        return {"num_cpus": cpu_per_client, "memory": memory_bytes}

    def _apply_host_network_optimizations(self):
        """
        🚀 Apply host network optimizations for high-concurrency FL scenarios.
        This is CRITICAL for non-Docker mode with many clients (e.g., 50 clients + mesh topology = 2450 connections).
        Without these optimizations, you may experience:
        - TCP buffer overflow → Queue saturation → "Streaming layer rejected request"
        - Connection limit exceeded → Request failures
        - High latency due to insufficient network buffers
        """
        self.logger.info("🚀 Applying host network optimizations for ultra-high concurrency...")

        # System-level network optimizations (same as DockerManager.optimize_host_network_settings)
        sysctl_settings = {
            # TCP connection optimizations - Critical for 50+ concurrent gRPC connections
            "net.core.somaxconn": "65535",                    # Max socket connection backlog
            "net.core.netdev_max_backlog": "30000",           # Network device backlog queue
            "net.core.rmem_default": "16777216",              # Default receive buffer (16MB)
            "net.core.rmem_max": "134217728",                 # Max receive buffer (128MB)
            "net.core.wmem_default": "16777216",              # Default send buffer (16MB)
            "net.core.wmem_max": "134217728",                 # Max send buffer (128MB)
            "net.ipv4.tcp_rmem": "4096 16777216 134217728",   # TCP receive buffer (min, default, max)
            "net.ipv4.tcp_wmem": "4096 16777216 134217728",   # TCP send buffer (min, default, max)
            "net.ipv4.tcp_mem": "786432 1048576 26777216",    # TCP memory limits
            "net.ipv4.tcp_max_syn_backlog": "30000",          # SYN queue size
            "net.ipv4.tcp_fin_timeout": "15",                 # FIN timeout (faster cleanup)
            "net.ipv4.tcp_keepalive_time": "30",              # Keepalive time
            "net.ipv4.tcp_keepalive_intvl": "5",              # Keepalive interval
            "net.ipv4.tcp_keepalive_probes": "3",             # Keepalive probes
            "net.ipv4.tcp_tw_reuse": "1",                     # Reuse TIME_WAIT sockets
            "net.ipv4.ip_local_port_range": "1024 65535",     # Available port range

            # Connection tracking optimizations - Critical for mesh topology
            "net.netfilter.nf_conntrack_max": "1048576",               # Max tracked connections
            "net.netfilter.nf_conntrack_tcp_timeout_established": "600",
            "net.netfilter.nf_conntrack_tcp_timeout_close_wait": "10",
            "net.netfilter.nf_conntrack_tcp_timeout_fin_wait": "10",

            # File descriptor limits - Critical for many gRPC streams
            "fs.file-max": "2097152",
            "fs.nr_open": "2097152"
        }

        # Apply sysctl settings (non-destructive, will be reset on reboot)
        applied_settings = []
        failed_settings = []
        for key, value in sysctl_settings.items():
            try:
                result = subprocess.run([
                    'sudo', 'sysctl', '-w', f'{key}={value}'
                ], capture_output=True, text=True, timeout=5)

                if result.returncode == 0:
                    applied_settings.append(f"{key}={value}")
                else:
                    failed_settings.append((key, result.stderr.strip()))
            except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError) as e:
                failed_settings.append((key, str(e)))

        if applied_settings:
            self.logger.info(f"✅ Applied {len(applied_settings)} host network optimizations")
            self.logger.debug(f"   Applied: {', '.join([s.split('=')[0] for s in applied_settings[:5]])}...")

        if failed_settings:
            self.logger.warning(f"⚠️  {len(failed_settings)} network optimizations could not be applied (may need sudo access)")
            for key, err in failed_settings[:3]:
                self.logger.debug(f"   Failed: {key} - {err[:50]}")

        if not applied_settings:
            self.logger.warning("⚠️  No host network optimizations applied! High-concurrency FL may experience issues.")
            self.logger.warning("   Consider running with sudo or contacting HPC admin to apply these settings.")

    def cleanup_environment(self):
        """Clean environment"""
        if self.cleanup_performed:
            return

        self.logger.info("🧹 Clean environment...")

        # Stop old processes
        subprocess.run(['pkill', '-9', '-f', 'python.*federatedscope'],
                      capture_output=True, check=False)
        
        # Clean chunk database files (both local and Docker mount points)
        try:
            import glob
            
            # Clean local chunk databases
            db_patterns = [
                'tmp/client_*/client_*_chunks.db',           # Local tmp directory
                'tmp/client_*/*.db',                        # Any .db files in client directories
                'docker_data/app_tmp/**/*.db',              # Docker mount point databases
                'docker_data/tmp/**/*.db',                  # Docker temp databases
                f"{CONFIG.OUTPUT_DIR}/**/*.db",             # Output directory databases
                f"{CONFIG.LOG_DIR}/**/*.db",                # Log directory databases
                '/tmp/fl_chunks/**/*.db',                   # tmpfs chunk databases
                '/tmp/fl_chunks/**/chunks/*'                # tmpfs chunk cache files
            ]

            # Clean /tmp/fl_chunks/ directory (tmpfs storage for fast I/O)
            tmpfs_chunk_dir = '/tmp/fl_chunks'
            if os.path.exists(tmpfs_chunk_dir):
                try:
                    import shutil
                    shutil.rmtree(tmpfs_chunk_dir)
                    self.logger.info(f"🗑️  Cleaned tmpfs chunk directory: {tmpfs_chunk_dir}")
                except Exception as e:
                    self.logger.warning(f"Failed to clean {tmpfs_chunk_dir}: {e}")
            
            for pattern in db_patterns:
                db_files = glob.glob(pattern, recursive=True)
                for db_file in db_files:
                    if os.path.exists(db_file):
                        os.remove(db_file)
                        self.logger.debug(f"🗑️  Removed chunk database: {db_file}")
            
            # Clean Docker data directories - use Docker to handle permission issues
            docker_cleanup_dirs = [
                os.path.join(PROJECT_ROOT, "docker_data/tmp"),
                os.path.join(PROJECT_ROOT, "docker_data/app_tmp"),
            ]

            # Clean individual client directories in docker_data/app_tmp/client_*/
            app_tmp_dir = os.path.join(PROJECT_ROOT, "docker_data/app_tmp")
            if os.path.exists(app_tmp_dir):
                try:
                    for item in os.listdir(app_tmp_dir):
                        if item.startswith('client_'):
                            client_dir = os.path.join(app_tmp_dir, item)
                            if os.path.isdir(client_dir):
                                docker_cleanup_dirs.append(client_dir)
                except Exception as e:
                    self.logger.debug(f"Failed to enumerate client directories: {e}")
            
            for cleanup_dir in docker_cleanup_dirs:
                if os.path.exists(cleanup_dir):
                    try:
                        # Use Docker to clean up root-owned files (no password needed)
                        result = subprocess.run([
                            'docker', 'run', '--rm', '-v', f"{cleanup_dir}:/cleanup",
                            'alpine:latest', 'sh', '-c', 'rm -rf /cleanup/*'
                        ], capture_output=True, text=True, timeout=10)
                        
                        if result.returncode == 0:
                            self.logger.debug(f"🗑️  Docker-cleaned mount directory: {cleanup_dir}")
                        else:
                            # Fallback to regular rm
                            if cleanup_dir and cleanup_dir != "/":
                                subprocess.run(f"rm -rf {cleanup_dir}/*", shell=True, check=False)
                                self.logger.debug(f"🗑️  Cleaned mount directory (fallback): {cleanup_dir}")
                    except (subprocess.TimeoutExpired, Exception) as e:
                        # If Docker cleanup fails, try regular rm
                        if cleanup_dir and cleanup_dir != "/":
                            subprocess.run(f"rm -rf {cleanup_dir}/*", shell=True, check=False)
                        self.logger.debug(f"🗑️  Cleaned mount directory (simple): {cleanup_dir}")
                    
        except Exception as e:
            self.logger.debug(f"Failed to clean database files: {e}")
        
        # Clean log directories (use PROJECT_ROOT for consistency)
        for log_dir_name in ['connection_logs', 'topology_logs', 'bittorrent_logs']:
            subprocess.run(['rm', '-rf', os.path.join(PROJECT_ROOT, log_dir_name)], check=False)

        # Clean previous Ray temp directory
        ray_tmp = os.path.join(PROJECT_ROOT, "tmp/ray")
        subprocess.run(['rm', '-rf', ray_tmp], check=False)

        # Create output and Ray temporary directories
        os.makedirs(os.path.join(PROJECT_ROOT, CONFIG.OUTPUT_DIR), exist_ok=True)
        os.makedirs(os.path.join(PROJECT_ROOT, CONFIG.LOG_DIR), exist_ok=True)
        os.makedirs(ray_tmp, exist_ok=True)
        
        time.sleep(1)
        self.cleanup_performed = True
        self.logger.info("✅ Environment cleanup completed")
    
    def start_federated_learning(self):
        """Start federated learning"""
        self.logger.info(f"🚀 Start Ray V2 federated learning")
        self.logger.info(f"📊 Configuration: {CONFIG.CLIENT_NUM} clients, {CONFIG.TOTAL_ROUNDS} rounds training, total nodes: {CONFIG.CLIENT_NUM + 1}")
        
        # Clean environment
        self.cleanup_environment()
        
        # 🚀 Apply host network optimizations for ALL modes (Docker and non-Docker)
        # This is critical for high-concurrency scenarios like 50 clients with mesh topology
        self._apply_host_network_optimizations()

        # Initialize container environment (Podman preferred for HPC, Docker as fallback)
        self.podman_manager = None
        self.docker_manager = None

        if CONFIG.USE_PODMAN:
            # 🦭 Try Podman first (rootless, no sudo required)
            self.logger.info("🦭 Initializing Podman rootless environment...")
            self.podman_manager = PodmanManager()
            if not self.podman_manager.podman_available:
                self.logger.warning("⚠️  Podman unavailable, trying Docker...")
                CONFIG.USE_PODMAN = False
                self.podman_manager = None
            else:
                # Setup Podman network with optimized settings
                if not self.podman_manager.setup_network():
                    self.logger.warning("⚠️  Podman network setup failed, trying Docker...")
                    CONFIG.USE_PODMAN = False
                    self.podman_manager = None
                else:
                    # Ensure image is ready
                    self.logger.info("🔍 Checking Podman images...")
                    if not self.podman_manager.ensure_image_ready():
                        self.logger.warning("⚠️  Failed to build Podman image, trying Docker...")
                        CONFIG.USE_PODMAN = False
                        self.podman_manager = None
                    else:
                        self.logger.info("✅ Podman environment ready with network optimizations")
                        self.logger.info("   📶 Custom network with sysctl: somaxconn=65535, tcp_max_syn_backlog=30000")
                        self.logger.info("   📦 Ulimits: nofile=1048576, nproc=65536")

        if not CONFIG.USE_PODMAN and CONFIG.USE_DOCKER:
            # 🐳 Fallback to Docker if Podman not available
            self.logger.info("🐳 Initializing Docker environment...")
            self.docker_manager = DockerManager()
            if not self.docker_manager.docker_available:
                self.logger.warning("⚠️  Docker unavailable, switch to non-container mode")
                CONFIG.USE_DOCKER = False
            else:
                # Docker-specific setup
                self.logger.info("🔍 Checking Docker images...")
                if not self.docker_manager.check_required_images():
                    if CONFIG.USE_LOCAL_CODE_MOUNT:
                        self.logger.info("🚀 Local code mount enabled - building base image only")
                        if not self.docker_manager.ensure_images_ready():
                            self.logger.warning("⚠️  Failed to build base Docker image, switch to non-container mode")
                            CONFIG.USE_DOCKER = False
                        else:
                            self.logger.info("✅ Base image built successfully for local code mount")
                    else:
                        self.logger.info("🔍 Building all required images...")
                        if not self.docker_manager.ensure_images_ready():
                            self.logger.warning("⚠️  Docker images not ready, switch to non-container mode")
                            CONFIG.USE_DOCKER = False
                else:
                    if CONFIG.USE_LOCAL_CODE_MOUNT:
                        self.logger.info("🚀 Using local code mount - base image ready, skipping rebuild")
                    else:
                        self.logger.info("✅ All Docker images ready")

                # Set up Docker network environment
                if CONFIG.USE_DOCKER:
                    if not self.docker_manager.setup_docker_environment():
                        self.logger.error("❌ Docker environment setup failed, switch to non-container mode")
                        CONFIG.USE_DOCKER = False
                    else:
                        self.logger.info("✅ Docker environment ready")

        # Log final container mode
        if CONFIG.USE_PODMAN:
            self.logger.info("🦭 Using Podman rootless mode (optimized for HPC)")
        elif CONFIG.USE_DOCKER:
            self.logger.info("🐳 Using Docker mode")
        else:
            self.logger.info("📦 Using non-container mode (Fallback)")
        
        # Initialize Ray
        self.initialize_ray_cluster()

        # === 🌐 Get cluster node information for distributed scheduling ===
        self.head_node_id, self.worker_node_ids = self._get_cluster_nodes()

        # Log cluster topology
        self.logger.info(f"🌐 Ray Cluster Topology:")
        self.logger.info(f"   📍 Head node: {self.head_node_id[:12]}... (Server will run here)")
        if self.worker_node_ids:
            self.logger.info(f"   📍 Worker nodes: {len(self.worker_node_ids)} available")
            for i, wid in enumerate(self.worker_node_ids):
                self.logger.info(f"      Worker {i+1}: {wid[:12]}...")
        else:
            self.logger.warning(f"   ⚠️ No worker nodes found! Clients will run on head node (not recommended)")

        # === 🎯 Calculate client-to-worker assignments FIRST (needed for GPU allocation) ===
        client_node_assignments = self._assign_clients_to_workers(CONFIG.CLIENT_NUM, self.worker_node_ids)

        # Count how many clients are on each node (for per-node resource calculation)
        clients_per_node = {}
        for node_id, _ in client_node_assignments:
            clients_per_node[node_id] = clients_per_node.get(node_id, 0) + 1

        # GPU resource allocation (now with node assignment info for proper per-node limits)
        server_gpu, client_gpu_assignments = self.allocate_gpu_resources(client_node_assignments, clients_per_node)

        # Generate configuration
        base_config = self.generate_base_config()

        # === 🖥️ Start server on HEAD node (fixed to use CPU resources) ===
        server_config = base_config.copy()
        server_config['distribute']['role'] = 'server'
        server_config['use_gpu'] = False  # Server forced to use CPU

        # 🚀 Server 不需要加载数据集（除非 make_global_eval=True）
        # 这避免了 server 加载整个数据集导致 OOM
        if not CONFIG.FEDERATE_MAKE_GLOBAL_EVAL:
            server_config['data'] = server_config['data'].copy()  # 避免影响 base_config
            server_config['data']['type'] = ''  # 空字符串跳过数据加载

        # Server resource configuration: CPU only, no GPU
        server_resources = {"num_cpus": 2}

        # 🎯 Add scheduling strategy to pin Server to head node
        if self.head_node_id:
            server_resources["scheduling_strategy"] = self._get_node_scheduling_strategy(self.head_node_id)
            self.logger.info(f"📍 Server scheduled to head node: {self.head_node_id[:12]}...")

        # Choose Actor type based on container runtime availability (priority: Podman > Docker > Fallback)
        if CONFIG.USE_PODMAN and hasattr(self, 'podman_manager') and self.podman_manager and self.podman_manager.podman_available:
            self.server_actor = PodmanFederatedScopeServer.options(**server_resources).remote(
                server_config, server_gpu, self.podman_manager
            )
            self.logger.info("🐳 Using Podman for Server container")
        elif CONFIG.USE_DOCKER:
            self.server_actor = DockerFederatedScopeServer.options(**server_resources).remote(
                server_config, server_gpu
            )
            self.logger.info("🐳 Using Docker for Server container")
        else:
            self.server_actor = FallbackFederatedScopeServer.options(**server_resources).remote(
                server_config, server_gpu
            )
            self.logger.info("📦 Using Fallback (no container) for Server")

        server_ip, server_port = ray.get(self.server_actor.start.remote())
        self.server_info = (server_ip, server_port)

        self.logger.info(f"✅ Server started: {server_ip}:{server_port}")
        time.sleep(15)

        # Create diverse edge devices
        device_assignments = self._create_diverse_device_fleet(CONFIG.CLIENT_NUM)

        self.logger.info(f"📊 Client distribution plan:")
        for i, (node_id, _) in enumerate(client_node_assignments):
            node_type = "head" if node_id == self.head_node_id else "worker"
            node_resources = self._get_node_resources(node_id)
            node_cpus = node_resources.get('CPU', 'unknown')
            clients_on_this_node = clients_per_node[node_id]
            self.logger.info(f"   Client {i+1} -> {node_type} ({node_id[:12]}...), "
                           f"Node CPUs: {node_cpus}, Clients on node: {clients_on_this_node}")

        # Start Docker clients with node affinity scheduling
        successful_clients = 0
        for i, device_profile in enumerate(device_assignments):
            client_id = i + 1

            client_config = base_config.copy()
            client_config['distribute']['role'] = 'client'

            # 🎯 Get target node and calculate resources based on that node
            target_node_id, _ = client_node_assignments[i]
            clients_on_this_node = clients_per_node[target_node_id]
            is_head = (target_node_id == self.head_node_id)

            # Ray resource allocation (based on device type AND target node resources)
            client_resources = self._get_ray_resources_for_device(
                device_profile,
                target_node_id=target_node_id,
                clients_on_node=clients_on_this_node,
                is_head_node=is_head  # Reserve CPU for server on head node
            )

            # 🎮 GPU allocation: Get GPU resources and use_gpu flag
            use_gpu = False
            if i < len(client_gpu_assignments):
                client_gpu_alloc, client_use_gpu = client_gpu_assignments[i]
                client_resources["num_gpus"] = client_gpu_alloc  # Fractional GPU allocation
                use_gpu = client_use_gpu  # Flag for Docker GPU configuration

            # 🎯 Add scheduling strategy to pin Client to assigned worker node
            client_resources["scheduling_strategy"] = self._get_node_scheduling_strategy(target_node_id)
            node_type = "head" if target_node_id == self.head_node_id else "worker"

            # Debug: print resource request for each client
            self.logger.info(f"🔧 Client {client_id} resources: num_cpus={(client_resources.get('num_cpus') or 0):.4f}, "
                           f"num_gpus={(client_resources.get('num_gpus') or 0):.4f}, "
                           f"memory={(client_resources.get('memory') or 0) / (1024**3):.2f}GB")

            try:
                # Choose Actor type based on container runtime availability (priority: Podman > Docker > Fallback)
                if CONFIG.USE_PODMAN and hasattr(self, 'podman_manager') and self.podman_manager and self.podman_manager.podman_available:
                    client_actor = PodmanFederatedScopeClient.options(**client_resources).remote(
                        client_id, client_config, server_ip, server_port, device_profile, use_gpu, self.podman_manager
                    )
                elif CONFIG.USE_DOCKER:
                    client_actor = DockerFederatedScopeClient.options(**client_resources).remote(
                        client_id, client_config, server_ip, server_port, device_profile, use_gpu
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
                    self.logger.info(f"✅ Client {client_id} ({device_profile.device_type}) started on {node_type} node ({target_node_id[:12]}...)")
                else:
                    self.logger.error(f"❌ Client {client_id} ({device_profile.device_type}) failed to start on {node_type} node")

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
            
            # Check status with retry for network resilience
            max_retries = 3
            status_ok = False
            for retry in range(max_retries):
                try:
                    server_status = ray.get(self.server_actor.get_status.remote(), timeout=30)
                    client_statuses = ray.get([
                        actor.get_status.remote() for actor in self.client_actors
                    ], timeout=60)
                    status_ok = True
                    break  # Success, exit retry loop
                except (ray.exceptions.ActorUnavailableError, ray.exceptions.RayTaskError, ray.exceptions.GetTimeoutError) as e:
                    if retry < max_retries - 1:
                        self.logger.warning(f"⚠️ Status check failed (attempt {retry+1}/{max_retries}): {type(e).__name__}, retrying in 10s...")
                        time.sleep(10)
                    else:
                        self.logger.warning(f"⚠️ Status check failed after {max_retries} attempts, skipping this cycle...")

            if not status_ok:
                time.sleep(30)
                continue  # Skip this monitoring cycle

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
            
            # Build GPU usage string (avoid division by zero when no GPUs available)
            if total_gpus > 0:
                gpu_usage_str = f"Ray GPU usage: {gpu_used:.1f}/{total_gpus:.1f} ({(gpu_used/total_gpus)*100:.1f}%)"
            else:
                gpu_usage_str = "Ray GPU usage: N/A (no GPUs)"

            self.logger.info(
                f"⏰ {elapsed}s | Server: {server_status['status']} (CPU) | "
                f"Clients: {total_clients} nodes (fractional GPU) | "
                f"{gpu_usage_str}"
            )
            
            # Check training completion
            if server_status["status"] != "running":
                self.logger.info("🏁 Server training completed")
                break
            
            if running_clients == 0:
                self.logger.info("🏁 All clients training completed")
                break
            
            time.sleep(5)
    
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
            # Stop Ray Actors
            try:
                if self.server_actor:
                    ray.get(self.server_actor.stop.remote())

                if self.client_actors:
                    stop_futures = [actor.stop.remote() for actor in self.client_actors]
                    ray.get(stop_futures)
            except Exception as e:
                self.logger.warning(f"⚠️  Ray Actors stop warning: {e}")
            
            # Clean environment in non-Docker mode too
            try:
                self.cleanup_environment()
                self.logger.info("✅ Environment cleaned")
            except Exception as e:
                self.logger.warning(f"⚠️  Environment cleanup warning: {e}")
        
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
🚀 Ray-Powered FederatedScope V2 Script (Ultra-High Concurrency Edition)
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
📂 Local code mount: {"✅ Enabled (no rebuild needed)" if CONFIG.USE_LOCAL_CODE_MOUNT else "❌ Disabled (image rebuild required)"}
🌐 Network simulation: {network_sim_status}
🚀 Host network optimizations: ✅ Applied (for all modes)
📱 Device distribution: {dict(CONFIG.DEVICE_DISTRIBUTION)}

🔧 High-concurrency features:
   • TCP buffer sizes: 128MB max
   • Connection tracking: 1M connections
   • File descriptors: 1M per container
   • Optimized Docker network bridge

🔗 gRPC Keep-Alive settings:
   • Keep-alive time: {CONFIG.GRPC_KEEPALIVE_TIME_MS//1000}s (ping interval)
   • Keep-alive timeout: {CONFIG.GRPC_KEEPALIVE_TIMEOUT_MS//1000}s (ACK timeout)
   • Min ping interval: {CONFIG.GRPC_MIN_TIME_BETWEEN_PINGS_MS//1000}s
   • Connection idle timeout: {CONFIG.GRPC_MAX_CONNECTION_IDLE_MS//60000}min
   • Connection max age: {CONFIG.GRPC_MAX_CONNECTION_AGE_MS//60000}min
   • System-level network tuning

💡 Output directory: {CONFIG.OUTPUT_DIR}
📝 Log directory: {CONFIG.LOG_DIR}
{'='*80}
"""
    print(banner)

def generate_slurm_script() -> str:
    """Generate SLURM sbatch submission script for HPC cluster.

    Supports both single-partition and multi-partition (hetjob) modes.

    Returns:
        Path to generated script
    """
    os.makedirs(SLURM_CONFIG.SLURM_OUTPUT_DIR, exist_ok=True)

    script_path = os.path.join(PROJECT_ROOT, "slurm_submit.sh")

    # Check if using multi-partition hetjob mode
    if SLURM_CONFIG.USE_MULTI_PARTITION and len(SLURM_CONFIG.HETJOB_CONFIG) > 1:
        return _generate_hetjob_script(script_path)
    else:
        return _generate_single_partition_script(script_path)


def _generate_single_partition_script(script_path: str) -> str:
    """Generate single-partition SLURM script."""

    # Build SLURM header
    slurm_header = f"""#!/bin/bash
#SBATCH --job-name={SLURM_CONFIG.JOB_NAME}
#SBATCH --partition={SLURM_CONFIG.PARTITION}
#SBATCH --nodes={SLURM_CONFIG.NODES}
#SBATCH --ntasks-per-node={SLURM_CONFIG.TASKS_PER_NODE}
#SBATCH --cpus-per-task={SLURM_CONFIG.CPUS_PER_TASK}
#SBATCH --mem={SLURM_CONFIG.MEM_PER_NODE}
#SBATCH --time={SLURM_CONFIG.TIME_LIMIT}
#SBATCH --output={PROJECT_ROOT}/{SLURM_CONFIG.SLURM_OUTPUT_DIR}/%x_%j.out
#SBATCH --error={PROJECT_ROOT}/{SLURM_CONFIG.SLURM_OUTPUT_DIR}/%x_%j.err
"""

    # GPU allocation
    if SLURM_CONFIG.GPUS_PER_NODE > 0:
        if SLURM_CONFIG.GPU_TYPE:
            slurm_header += f"#SBATCH --gres=gpu:{SLURM_CONFIG.GPU_TYPE}:{SLURM_CONFIG.GPUS_PER_NODE}\n"
        else:
            slurm_header += f"#SBATCH --gres=gpu:{SLURM_CONFIG.GPUS_PER_NODE}\n"

    # Optional settings
    if SLURM_CONFIG.ACCOUNT:
        slurm_header += f"#SBATCH --account={SLURM_CONFIG.ACCOUNT}\n"
    if SLURM_CONFIG.QOS:
        slurm_header += f"#SBATCH --qos={SLURM_CONFIG.QOS}\n"
    if SLURM_CONFIG.EXCLUSIVE:
        slurm_header += "#SBATCH --exclusive\n"
    if SLURM_CONFIG.CONSTRAINT:
        slurm_header += f"#SBATCH --constraint={SLURM_CONFIG.CONSTRAINT}\n"

    # Build script body
    script_body = f"""
# ============================================================================
# Environment Setup
# ============================================================================

echo "============================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Nodes: $SLURM_JOB_NODELIST"
echo "Node Count: $SLURM_JOB_NUM_NODES"
echo "Tasks per Node: $SLURM_NTASKS_PER_NODE"
echo "CPUs per Task: $SLURM_CPUS_PER_TASK"
echo "Start Time: $(date)"
echo "============================================"

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate {SLURM_CONFIG.CONDA_ENV}

# Set working directory
cd {PROJECT_ROOT}

# Set Python path
export PYTHONPATH={PROJECT_ROOT}:$PYTHONPATH

# Print environment info
echo "Python: $(which python)"
echo "PyTorch version: $(python -c 'import torch; print(torch.__version__)')"
echo "CUDA available: $(python -c 'import torch; print(torch.cuda.is_available())')"
if [ "$(python -c 'import torch; print(torch.cuda.is_available())')" = "True" ]; then
    echo "GPU count: $(python -c 'import torch; print(torch.cuda.device_count())')"
fi

# ============================================================================
# Single-node mode: Ray will init locally
# ============================================================================

echo "============================================"
echo "Single-node mode: Ray will init locally"
echo "============================================"

# ============================================================================
# Run FL Experiment
# ============================================================================

echo "Starting FL experiment..."
python {PROJECT_ROOT}/run_ray_hpc.py --local

echo "============================================"
echo "Job finished at $(date)"
echo "============================================"

# Cleanup Ray cluster
ray stop --force 2>/dev/null || true
"""

    # Write script
    with open(script_path, 'w') as f:
        f.write(slurm_header + script_body)

    os.chmod(script_path, 0o755)

    print(f"Generated SLURM script: {script_path}")
    print(f"\nTo submit the job, run:")
    print(f"  sbatch {script_path}")

    return script_path


def _generate_hetjob_script(script_path: str) -> str:
    """Generate multi-partition hetjob SLURM script.

    Uses SLURM hetjob feature to span multiple partitions with different resources.
    """
    hetjob_configs = SLURM_CONFIG.HETJOB_CONFIG
    total_clients = sum(cfg.get('clients', 0) for cfg in hetjob_configs)

    # Build SLURM header with hetjob sections
    slurm_header = f"""#!/bin/bash
#SBATCH --job-name={SLURM_CONFIG.JOB_NAME}
#SBATCH --time={SLURM_CONFIG.TIME_LIMIT}
#SBATCH --output={PROJECT_ROOT}/{SLURM_CONFIG.SLURM_OUTPUT_DIR}/%x_%j.out
#SBATCH --error={PROJECT_ROOT}/{SLURM_CONFIG.SLURM_OUTPUT_DIR}/%x_%j.err
"""

    # Add hetjob component sections
    for i, cfg in enumerate(hetjob_configs):
        if i > 0:
            slurm_header += "#SBATCH hetjob\n"

        slurm_header += f"""#SBATCH --partition={cfg['partition']}
#SBATCH --nodes={cfg.get('nodes', 1)}
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cfg.get('cpus', 32)}
#SBATCH --mem={cfg.get('mem', '100G')}
#SBATCH --gres=gpu:{cfg.get('gpus', 1)}
"""

    # Build script body for hetjob
    script_body = f"""
# ============================================================================
# Hetjob Multi-Partition FL Experiment
# ============================================================================

echo "============================================"
echo "Job ID: $SLURM_JOB_ID"
echo "Job Name: $SLURM_JOB_NAME"
echo "Hetjob Size: $SLURM_HET_SIZE partitions"
echo "Start Time: $(date)"
echo "============================================"

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate {SLURM_CONFIG.CONDA_ENV}

# Set working directory
cd {PROJECT_ROOT}
export PYTHONPATH={PROJECT_ROOT}:$PYTHONPATH

# Get head node (first hetjob component) IP (IPv4 only)
HEAD_NODE=$(scontrol show hostnames $SLURM_JOB_NODELIST_HET_GROUP_0 | head -1)
# Use getent ahosts to get IPv4 address, filter out IPv6
HEAD_IP=$(getent ahosts $HEAD_NODE | grep STREAM | head -1 | awk '{{print $1}}')
# Fallback: try hostname -I on the head node
if [ -z "$HEAD_IP" ] || [[ "$HEAD_IP" == *":"* ]]; then
    HEAD_IP=$(srun --het-group=0 --nodes=1 --ntasks=1 hostname -I 2>/dev/null | awk '{{print $1}}')
fi

echo "Head Node: $HEAD_NODE ($HEAD_IP)"
echo "Total clients: {total_clients}"

# Write server IP to shared file for clients
SERVER_INFO_FILE="{PROJECT_ROOT}/.server_info_$SLURM_JOB_ID"
echo "$HEAD_IP" > $SERVER_INFO_FILE

# ============================================================================
# Start Ray cluster across all hetjob components
# ============================================================================

# Function to wait for Ray head to be ready
wait_for_ray_head() {{
    local max_attempts=60
    local attempt=0
    echo "Waiting for Ray head to be ready at $HEAD_IP:6379..."
    while [ $attempt -lt $max_attempts ]; do
        if timeout 2 bash -c "echo > /dev/tcp/$HEAD_IP/6379" 2>/dev/null; then
            echo "Ray head is ready after $attempt seconds!"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
        if [ $((attempt % 10)) -eq 0 ]; then
            echo "  Still waiting... ($attempt seconds)"
        fi
    done
    echo "ERROR: Ray head did not become ready in $max_attempts seconds"
    return 1
}}

# Simple wait for Ray workers to connect
wait_for_ray_workers() {{
    echo "Waiting 20 seconds for Ray workers to connect..."
    sleep 20
    echo "Proceeding with FL experiment..."
}}

# Get worker node hostname
WORKER_NODE=$(scontrol show hostnames $SLURM_JOB_NODELIST_HET_GROUP_1 | head -1)
echo "Worker Node: $WORKER_NODE"

echo "Starting Ray head on $HEAD_NODE..."
# Use srun with --block to keep Ray running as foreground process (backgrounded in script)
srun --het-group=0 --nodes=1 --ntasks=1 --exclusive \\
    bash -c "
        source \\$(conda info --base)/etc/profile.d/conda.sh
        conda activate {SLURM_CONFIG.CONDA_ENV}
        ray stop --force 2>/dev/null || true
        # Use --block to keep the process running
        ray start --head --block --node-ip-address=$HEAD_IP \\
            --port=6379 --dashboard-host=0.0.0.0 \\
            --num-cpus={hetjob_configs[0].get('cpus', 64)} \\
            --num-gpus={hetjob_configs[0].get('gpus', 4)}
    " &

RAY_HEAD_PID=$!
echo "Ray head started with PID $RAY_HEAD_PID"

# Wait for Ray head to be ready before starting workers
wait_for_ray_head || {{ echo "Failed to start Ray head"; kill $RAY_HEAD_PID 2>/dev/null; exit 1; }}

# Start Ray workers via srun
"""

    for i, cfg in enumerate(hetjob_configs[1:], start=1):
        script_body += f"""
echo "Starting Ray worker on hetjob group {i}..."
srun --het-group={i} --nodes=1 --ntasks=1 --exclusive \\
    bash -c "
        source \\$(conda info --base)/etc/profile.d/conda.sh
        conda activate {SLURM_CONFIG.CONDA_ENV}
        ray stop --force 2>/dev/null || true
        for attempt in 1 2 3; do
            echo '  Worker {i} connection attempt '\\$attempt'...'
            ray start --block --address=$HEAD_IP:6379 \\
                --num-cpus={cfg.get('cpus', 64)} \\
                --num-gpus={cfg.get('gpus', 2)} && break
            sleep 5
        done
    " &

RAY_WORKER_{i}_PID=$!
echo "Ray worker {i} started with PID $RAY_WORKER_{i}_PID"
"""

    num_total_components = len(hetjob_configs)
    script_body += f"""
# Wait for all workers to connect to the cluster
wait_for_ray_workers

echo "Ray cluster status:"
ray status

# ============================================================================
# Run FL Experiment with hetjob distribution
# ============================================================================

export RAY_ADDRESS="$HEAD_IP:6379"
export FL_SERVER_HOST="$HEAD_IP"
export FL_TOTAL_CLIENTS={total_clients}

# Pass hetjob configuration to Python script
export FL_HETJOB_CONFIG='{_serialize_hetjob_config(hetjob_configs)}'

echo "Starting FL experiment with hetjob distribution..."
python {PROJECT_ROOT}/run_ray_hpc.py --local --hetjob

echo "============================================"
echo "Job finished at $(date)"
echo "============================================"

# Cleanup
rm -f $SERVER_INFO_FILE
ray stop --force 2>/dev/null || true
"""

    # Write script
    with open(script_path, 'w') as f:
        f.write(slurm_header + script_body)

    os.chmod(script_path, 0o755)

    print(f"Generated Hetjob SLURM script: {script_path}")
    print(f"\nHetjob configuration:")
    for i, cfg in enumerate(hetjob_configs):
        print(f"  Component {i}: {cfg['partition']} - {cfg.get('gpus', 1)} GPUs, {cfg.get('clients', 0)} clients" +
              (" (server)" if cfg.get('run_server') else ""))
    print(f"\nTotal clients: {total_clients}")
    print(f"\nTo submit the job, run:")
    print(f"  sbatch {script_path}")

    return script_path


def _serialize_hetjob_config(configs: List[Dict[str, Any]]) -> str:
    """Serialize hetjob config for passing to subprocess."""
    import json
    return json.dumps(configs)


def is_running_in_slurm() -> bool:
    """Check if currently running inside a SLURM job."""
    return os.environ.get('SLURM_JOB_ID') is not None


def submit_slurm_job(script_path: str) -> Optional[str]:
    """Submit a SLURM job and return the job ID.

    Args:
        script_path: Path to the sbatch script

    Returns:
        Job ID if successful, None otherwise
    """
    try:
        result = subprocess.run(
            ['sbatch', '--parsable', script_path],
            capture_output=True, text=True, check=True
        )
        job_id = result.stdout.strip()
        return job_id
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to submit SLURM job: {e.stderr}")
        return None
    except FileNotFoundError:
        print("❌ sbatch command not found. Is SLURM installed?")
        return None


# ============================================================================
# Batch Experiment Submission Functions
# ============================================================================

def create_code_snapshot(snapshot_dir: str, experiment_name: str) -> str:
    """Create a code snapshot for reproducibility.

    Since SLURM doesn't snapshot code at submission time, we copy the code
    to a separate directory before submission.

    Args:
        snapshot_dir: Base directory for code snapshots
        experiment_name: Name of the experiment

    Returns:
        Path to the snapshot directory
    """
    import shutil
    from datetime import datetime

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    snapshot_path = os.path.join(snapshot_dir, f"{experiment_name}_{timestamp}")

    # Directories and files to copy
    include_dirs = [
        "federatedscope",
    ]
    include_files = [
        "run_ray_hpc.py",
        "run_ray.py",
        "setup.py",
    ]

    # Directories to exclude
    exclude_patterns = [
        "__pycache__",
        "*.pyc",
        ".git",
        "*.egg-info",
        "data",
        "logs",
        "ray_v2_output",
        "experiments_output",
        "experiments_snapshots",
        "slurm_logs",
        ".backup*",
        "node_modules",
    ]

    os.makedirs(snapshot_path, exist_ok=True)

    def should_exclude(path):
        name = os.path.basename(path)
        for pattern in exclude_patterns:
            if pattern.startswith("*"):
                if name.endswith(pattern[1:]):
                    return True
            elif name == pattern or name.startswith(pattern.rstrip("*")):
                return True
        return False

    # Copy directories
    for dir_name in include_dirs:
        src = os.path.join(PROJECT_ROOT, dir_name)
        dst = os.path.join(snapshot_path, dir_name)
        if os.path.exists(src):
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns(*exclude_patterns))

    # Copy files
    for file_name in include_files:
        src = os.path.join(PROJECT_ROOT, file_name)
        dst = os.path.join(snapshot_path, file_name)
        if os.path.exists(src):
            shutil.copy2(src, dst)

    print(f"   Code snapshot created: {snapshot_path}")
    return snapshot_path


def load_experiments_config(config_path: str) -> dict:
    """Load experiments configuration from YAML file.

    Args:
        config_path: Path to experiments.yaml

    Returns:
        Parsed configuration dictionary
    """
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config


def generate_experiment_slurm_script(
    experiment: dict,
    global_config: dict,
    snapshot_path: str,
    output_base_dir: str
) -> str:
    """Generate SLURM script for a specific experiment.

    Args:
        experiment: Experiment configuration
        global_config: Global settings from experiments.yaml
        snapshot_path: Path to code snapshot
        output_base_dir: Base directory for experiment outputs

    Returns:
        Path to generated script
    """
    exp_name = experiment['name']
    exp_description = experiment.get('description', 'No description')
    exp_config = experiment.get('config', {})
    time_limit = experiment.get('time_limit', global_config.get('time_limit', '24:00:00'))

    # Merge settings (experiment overrides global)
    partition = experiment.get('partition', global_config.get('partition', 'gpu3'))
    nodes = experiment.get('nodes', global_config.get('nodes', 1))
    gpus = experiment.get('gpus_per_node', global_config.get('gpus_per_node', 2))
    cpus = experiment.get('cpus_per_task', global_config.get('cpus_per_task', 64))
    mem = experiment.get('mem', global_config.get('mem', '400G'))

    # Output directory for this experiment
    exp_output_dir = os.path.join(output_base_dir, exp_name)
    os.makedirs(exp_output_dir, exist_ok=True)

    # SLURM log directory
    slurm_log_dir = os.path.join(output_base_dir, "slurm_logs")
    os.makedirs(slurm_log_dir, exist_ok=True)

    # Generate config overrides for the experiment
    config_overrides = []
    for key, value in exp_config.items():
        if isinstance(value, str):
            config_overrides.append(f'    CONFIG.{key} = "{value}"')
        elif isinstance(value, bool):
            config_overrides.append(f'    CONFIG.{key} = {value}')
        elif isinstance(value, (int, float)):
            config_overrides.append(f'    CONFIG.{key} = {value}')
        elif isinstance(value, list):
            config_overrides.append(f'    CONFIG.{key} = {value}')

    config_override_str = '\n'.join(config_overrides) if config_overrides else '    pass'

    script_content = f'''#!/bin/bash
#SBATCH --job-name={exp_name}
#SBATCH --partition={partition}
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task={cpus}
#SBATCH --mem={mem}
#SBATCH --time={time_limit}
#SBATCH --output={slurm_log_dir}/{exp_name}_%j.out
#SBATCH --error={slurm_log_dir}/{exp_name}_%j.err
#SBATCH --gres=gpu:{gpus}

# ============================================================================
# Experiment: {exp_name}
# Description: {exp_description}
# Generated: $(date)
# ============================================================================

echo "============================================"
echo "Experiment: {exp_name}"
echo "Job ID: $SLURM_JOB_ID"
echo "Start Time: $(date)"
echo "Code Snapshot: {snapshot_path}"
echo "Output Dir: {exp_output_dir}"
echo "============================================"

# Load modules
module purge
module load nvidia/cuda/toolkit/12.9.1

# Fix LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/lib64

# Activate conda
source $(conda info --base)/etc/profile.d/conda.sh
conda activate flv2

# Set working directory to code snapshot
cd {snapshot_path}
export PYTHONPATH={snapshot_path}:$PYTHONPATH

# Apply experiment-specific configuration
python -c "
import sys
sys.path.insert(0, '{snapshot_path}')
from run_ray_hpc import CONFIG

# Experiment-specific overrides
{config_override_str}

# Set output directory
CONFIG.OUTPUT_DIR = '{exp_output_dir}'
CONFIG.LOG_DIR = '{exp_output_dir}/logs'

# Save effective config
import yaml
import os
os.makedirs('{exp_output_dir}', exist_ok=True)
with open('{exp_output_dir}/experiment_config.yaml', 'w') as f:
    yaml.dump({{
        'experiment_name': '{exp_name}',
        'description': '{exp_description}',
        'config': {{k: getattr(CONFIG, k) for k in dir(CONFIG) if not k.startswith('_') and k.isupper()}}
    }}, f, default_flow_style=False)
print('Experiment config saved')
"

# Run FL experiment
echo "Starting FL experiment..."
srun --ntasks=1 bash -c "
    module purge
    module load nvidia/cuda/toolkit/12.9.1
    export LD_LIBRARY_PATH=/usr/lib64:/usr/local/cuda/lib64:/lib64:$LD_LIBRARY_PATH
    source $(conda info --base)/etc/profile.d/conda.sh
    conda activate flv2
    cd {snapshot_path}
    export PYTHONPATH={snapshot_path}:$PYTHONPATH

    # Run with experiment config
    python -c \\"
import sys
sys.path.insert(0, '{snapshot_path}')
from run_ray_hpc import CONFIG, RayV2FederatedLearning
import ray

# Apply experiment config
{config_override_str}
CONFIG.OUTPUT_DIR = '{exp_output_dir}'
CONFIG.LOG_DIR = '{exp_output_dir}/logs'

# Run experiment
ray_fl = RayV2FederatedLearning()
try:
    ray_fl.start_federated_learning()
    summary = ray_fl.generate_results_summary()
    print('Experiment completed successfully!')
except Exception as e:
    print(f'Experiment failed: {{e}}')
    import traceback
    traceback.print_exc()
finally:
    ray_fl.stop_all()
    ray.shutdown()
\\"
"

echo "============================================"
echo "Experiment {exp_name} finished at $(date)"
echo "============================================"

ray stop --force 2>/dev/null || true
'''

    script_path = os.path.join(output_base_dir, "slurm_scripts", f"{exp_name}.sh")
    os.makedirs(os.path.dirname(script_path), exist_ok=True)

    with open(script_path, 'w') as f:
        f.write(script_content)
    os.chmod(script_path, 0o755)

    return script_path


def submit_batch_experiments(config_path: str, dry_run: bool = False) -> List[Tuple[str, Optional[str]]]:
    """Submit multiple experiments from a configuration file.

    Args:
        config_path: Path to experiments.yaml
        dry_run: If True, generate scripts but don't submit

    Returns:
        List of (experiment_name, job_id) tuples
    """
    from datetime import datetime

    print("\n" + "="*80)
    print("Batch Experiment Submission")
    print("="*80)

    # Load configuration
    config = load_experiments_config(config_path)
    global_config = config.get('global', {})
    experiments = config.get('experiments', [])

    if not experiments:
        print("No experiments found in configuration file")
        return []

    print(f"\nLoaded {len(experiments)} experiments from {config_path}")

    # Setup directories
    snapshot_base_dir = global_config.get('code_snapshot_dir',
        os.path.join(PROJECT_ROOT, "experiments_snapshots"))
    output_base_dir = global_config.get('output_base_dir',
        os.path.join(PROJECT_ROOT, "experiments_output"))

    batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_output_dir = os.path.join(output_base_dir, f"batch_{batch_timestamp}")

    os.makedirs(batch_output_dir, exist_ok=True)

    print(f"\nBatch output directory: {batch_output_dir}")
    print(f"Code snapshot directory: {snapshot_base_dir}")

    # Create single code snapshot for all experiments in this batch
    print("\nCreating code snapshot...")
    snapshot_path = create_code_snapshot(snapshot_base_dir, f"batch_{batch_timestamp}")

    # Save batch configuration
    batch_config_path = os.path.join(batch_output_dir, "batch_config.yaml")
    with open(batch_config_path, 'w') as f:
        yaml.dump({
            'batch_timestamp': batch_timestamp,
            'code_snapshot': snapshot_path,
            'experiments': [e['name'] for e in experiments],
            'config': config
        }, f, default_flow_style=False)

    results = []

    print("\n" + "-"*80)
    print("Generating and submitting experiments:")
    print("-"*80)

    for i, experiment in enumerate(experiments, 1):
        exp_name = experiment['name']
        print(f"\n[{i}/{len(experiments)}] {exp_name}")
        print(f"   Description: {experiment.get('description', 'N/A')}")
        print(f"   Time limit: {experiment.get('time_limit', global_config.get('time_limit', '24:00:00'))}")

        # Generate SLURM script
        script_path = generate_experiment_slurm_script(
            experiment, global_config, snapshot_path, batch_output_dir
        )
        print(f"   Script: {script_path}")

        if dry_run:
            print("   [DRY RUN] Would submit job")
            results.append((exp_name, None))
        else:
            # Submit job
            job_id = submit_slurm_job(script_path)
            if job_id:
                print(f"   Submitted: Job ID {job_id}")
                results.append((exp_name, job_id))
            else:
                print(f"   FAILED to submit")
                results.append((exp_name, None))

    # Summary
    print("\n" + "="*80)
    print("Batch Submission Summary")
    print("="*80)

    submitted = [(name, jid) for name, jid in results if jid]
    failed = [name for name, jid in results if not jid]

    print(f"\nSubmitted: {len(submitted)}/{len(experiments)}")
    if submitted:
        print("\nSubmitted jobs:")
        for name, jid in submitted:
            print(f"  {name}: {jid}")

    if failed and not dry_run:
        print(f"\nFailed: {len(failed)}")
        for name in failed:
            print(f"  {name}")

    print(f"\nOutput directory: {batch_output_dir}")
    print(f"Code snapshot: {snapshot_path}")

    if submitted:
        print("\nMonitor all jobs:")
        job_ids = ','.join(jid for _, jid in submitted)
        print(f"  squeue -j {job_ids}")
        print(f"\nCancel all jobs:")
        print(f"  scancel {job_ids}")

    return results


def main():
    """Main function with SLURM support - one-click submission and execution"""

    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Ray-Powered FederatedScope (HPC SLURM Version)')
    parser.add_argument('--no-submit', action='store_true',
                       help='Generate SLURM script only, do not submit')
    parser.add_argument('--local', action='store_true',
                       help='Run locally without SLURM (same as run_ray.py)')
    parser.add_argument('--hetjob', action='store_true',
                       help='Running in hetjob multi-partition mode')
    parser.add_argument('--partition', type=str, default=None,
                       help='SLURM partition name')
    parser.add_argument('--nodes', type=int, default=None,
                       help='Number of nodes to request')
    parser.add_argument('--time', type=str, default=None,
                       help='Time limit (HH:MM:SS)')
    parser.add_argument('--gpus', type=int, default=None,
                       help='GPUs per node')
    parser.add_argument('--conda-env', type=str, default=None,
                       help='Conda environment name')
    # Batch experiment submission
    parser.add_argument('--batch', type=str, default=None,
                       help='Path to experiments.yaml for batch submission')
    parser.add_argument('--dry-run', action='store_true',
                       help='Generate scripts but do not submit (for --batch mode)')
    args = parser.parse_args()

    # Handle batch experiment submission
    if args.batch:
        submit_batch_experiments(args.batch, dry_run=args.dry_run)
        return

    # Apply command-line overrides to SLURM config
    if args.partition:
        SLURM_CONFIG.PARTITION = args.partition
    if args.nodes:
        SLURM_CONFIG.NODES = args.nodes
    if args.time:
        SLURM_CONFIG.TIME_LIMIT = args.time
    if args.gpus:
        SLURM_CONFIG.GPUS_PER_NODE = args.gpus
    if args.conda_env:
        SLURM_CONFIG.CONDA_ENV = args.conda_env

    # Check if we're already running inside SLURM or user wants local execution
    if is_running_in_slurm() or args.local:
        # Already in SLURM job or local mode - run the FL experiment directly
        if is_running_in_slurm():
            print(f"🖥️  Running inside SLURM job: {os.environ.get('SLURM_JOB_ID')}")
        else:
            print("🖥️  Running in local mode (no SLURM)")

        # Handle hetjob mode - update config from environment
        if args.hetjob or os.environ.get('FL_HETJOB_CONFIG'):
            hetjob_config_str = os.environ.get('FL_HETJOB_CONFIG', '')
            if hetjob_config_str:
                import json
                try:
                    hetjob_configs = json.loads(hetjob_config_str)
                    total_clients = sum(cfg.get('clients', 0) for cfg in hetjob_configs)
                    CONFIG.CLIENT_NUM = total_clients
                    print(f"🔀 Hetjob mode: {len(hetjob_configs)} partitions, {total_clients} total clients")
                    for i, cfg in enumerate(hetjob_configs):
                        print(f"   Partition {i}: {cfg['partition']} - {cfg.get('gpus', 1)} GPUs, {cfg.get('clients', 0)} clients")
                except json.JSONDecodeError as e:
                    print(f"⚠️  Failed to parse hetjob config: {e}")

        # Normal execution
        display_banner()
    else:
        # Not in SLURM - generate script and submit
        print("🚀 HPC SLURM Mode: Generating and submitting job...")

        if SLURM_CONFIG.USE_MULTI_PARTITION and len(SLURM_CONFIG.HETJOB_CONFIG) > 1:
            print("   Mode: Hetjob (multi-partition)")
            total_gpus = sum(cfg.get('gpus', 0) for cfg in SLURM_CONFIG.HETJOB_CONFIG)
            total_clients = sum(cfg.get('clients', 0) for cfg in SLURM_CONFIG.HETJOB_CONFIG)
            print(f"   Partitions: {', '.join(cfg['partition'] for cfg in SLURM_CONFIG.HETJOB_CONFIG)}")
            print(f"   Total GPUs: {total_gpus}")
            print(f"   Total Clients: {total_clients}")
        else:
            print(f"   Partition: {SLURM_CONFIG.PARTITION}")
            print(f"   Nodes: {SLURM_CONFIG.NODES}")
            print(f"   GPUs/Node: {SLURM_CONFIG.GPUS_PER_NODE}")
        print(f"   Time Limit: {SLURM_CONFIG.TIME_LIMIT}")
        print()

        # Generate SLURM script
        script_path = generate_slurm_script()

        if args.no_submit:
            print("\n📝 Script generated. Use 'sbatch slurm_submit.sh' to submit.")
            return

        # Submit job
        print("\n📤 Submitting job to SLURM...")
        job_id = submit_slurm_job(script_path)

        if job_id:
            print(f"\n✅ Job submitted successfully!")
            print(f"   Job ID: {job_id}")
            print(f"\n📊 Monitor with:")
            print(f"   squeue -j {job_id}")
            print(f"   tail -f {SLURM_CONFIG.SLURM_OUTPUT_DIR}/fl_experiment_{job_id}.out")
            print(f"\n🛑 Cancel with:")
            print(f"   scancel {job_id}")
        else:
            print("\n❌ Job submission failed")

        return

    # === FL Experiment Execution (runs inside SLURM or local mode) ===

    ray_fl = RayV2FederatedLearning()

    try:
        # Start federated learning
        ray_fl.start_federated_learning()

        # Generate results summary
        summary = ray_fl.generate_results_summary()

        print("\n🎉 Ray V2 federated learning completed!")
        print(f"📄 Results summary: {CONFIG.OUTPUT_DIR}/results_summary.yaml")
        print(f"📊 Processed {summary['configuration']['client_num']} clients in {summary['configuration']['total_rounds']} rounds")
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