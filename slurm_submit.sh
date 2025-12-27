#!/bin/bash
#SBATCH --job-name=fl_experiment
#SBATCH --partition=gpu2
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=400G
#SBATCH --time=UNLIMITED
#SBATCH --output=/home/naicheng_li/FLTorrent/slurm_logs/%x_%j.out
#SBATCH --error=/home/naicheng_li/FLTorrent/slurm_logs/%x_%j.err
#SBATCH --gres=gpu:4

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
conda activate flv2

# Set working directory
cd /home/naicheng_li/FLTorrent

# Set Python path
export PYTHONPATH=/home/naicheng_li/FLTorrent:$PYTHONPATH

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
python /home/naicheng_li/FLTorrent/run_ray_hpc.py --local

echo "============================================"
echo "Job finished at $(date)"
echo "============================================"

# Cleanup Ray cluster
ray stop --force 2>/dev/null || true
