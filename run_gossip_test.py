#!/usr/bin/env python3
"""
Gossip-style DFL Bug Fix Verification Test
===========================================

Small-scale test to verify gossip bug fixes:
  - Bug 1: gossip.iterations support (iterations > 1)
  - Bug 2 & 4: _weighted_avg_two_models fix (key union, no scaling)
  - Bug 3: neighbor_avg/push_sum weight calculation fix

Usage:
    python run_gossip_test.py --generate-slurm
    sbatch slurm_submit.sh

    # Or with explicit partition:
    python run_gossip_test.py --generate-slurm --partition gpu3
"""

import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

# Import and override config before running
from run_ray_hpc import CONFIG, SLURM_CONFIG, FLConfig, SLURMConfig, main

# =============================================================================
# Gossip-DFL Test Configuration Overrides
# =============================================================================

# === SLURM Settings for gpu3 ===
SLURM_CONFIG.PARTITION = "gpu3"
SLURM_CONFIG.CPUS_PER_TASK = 128          # gpu3 has 128 cores
SLURM_CONFIG.MEM_PER_NODE = "1400G"       # gpu3 has 1.5TB
SLURM_CONFIG.GPUS_PER_NODE = 2            # gpu3 has 3 GPUs, use 2
SLURM_CONFIG.JOB_NAME = "gossip_dfl_test"

# === Small-scale FL Settings ===
CONFIG.CLIENT_NUM = 5                      # Small: 5 clients
CONFIG.TOTAL_ROUNDS = 100                  # Extended: 100 rounds to see LR schedule
CONFIG.CHUNK_NUM = 8                       # Fewer chunks
CONFIG.SEED = 42

# === Dataset: CIFAR-10 for testing (in_channels=3) ===
CONFIG.DATASET = "CIFAR10@torchvision"
CONFIG.BATCH_SIZE = 32
CONFIG.DATA_SPLIT_ALPHA = 0.5              # Non-IID

# === Model Settings ===
CONFIG.MODEL_TYPE = "resnet18"              # ResNet-18 for CIFAR-10
CONFIG.MODEL_OUT_CHANNELS = 10              # 10 classes for CIFAR-10

# === Training Settings ===
CONFIG.LOCAL_UPDATE_STEPS = 5
CONFIG.LEARNING_RATE = 0.01
CONFIG.CRITERION_TYPE = "CrossEntropyLoss"  # For CIFAR-10 (10 classes)

# === Topology: Ring for gossip test ===
CONFIG.TOPOLOGY_TYPE = "ring"              # Ring: each node has 2 neighbors
CONFIG.TOPOLOGY_CONNECTIONS = 2
CONFIG.TOPOLOGY_TIMEOUT = 120.0

# === Gossip-style DFL Settings (THE KEY SETTINGS) ===
CONFIG.GOSSIP_ENABLE = True                # Enable gossip mode
CONFIG.GOSSIP_MODE = "neighbor_avg"        # Test neighbor_avg
CONFIG.GOSSIP_ITERATIONS = 2               # Test iterations > 1 (Bug 1)
CONFIG.GOSSIP_MIXING_WEIGHT = 0.5          # Self weight
CONFIG.BT_NEIGHBOR_ONLY = True             # Only collect from neighbors

# === BitTorrent Settings ===
CONFIG.BITTORRENT_TIMEOUT = 60.0
CONFIG.BT_MIN_COMPLETION_RATIO = 0.8
CONFIG.BT_ENABLE_COMPENSATION = True

# === Output ===
CONFIG.OUTPUT_DIR = "exp/gossip_dfl_test"

# === Disable Docker for simplicity ===
CONFIG.ENABLE_NETWORK_SIMULATION = False
CONFIG.USE_DOCKER = False
CONFIG.USE_PODMAN = False

print("=" * 60)
print("Gossip-style DFL Bug Fix Verification Test")
print("=" * 60)
print(f"  Partition:       {SLURM_CONFIG.PARTITION}")
print(f"  Clients:         {CONFIG.CLIENT_NUM}")
print(f"  Rounds:          {CONFIG.TOTAL_ROUNDS}")
print(f"  Topology:        {CONFIG.TOPOLOGY_TYPE}")
print(f"  Gossip Enable:   {CONFIG.GOSSIP_ENABLE}")
print(f"  Gossip Mode:     {CONFIG.GOSSIP_MODE}")
print(f"  Gossip Iters:    {CONFIG.GOSSIP_ITERATIONS}")
print(f"  Mixing Weight:   {CONFIG.GOSSIP_MIXING_WEIGHT}")
print(f"  Neighbor Only:   {CONFIG.BT_NEIGHBOR_ONLY}")
print("=" * 60)

if __name__ == "__main__":
    main()
