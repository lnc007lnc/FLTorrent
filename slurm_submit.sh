#!/bin/bash
#SBATCH --job-name=gossip_dfl_test
#SBATCH --partition=gpu3
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=128
#SBATCH --mem=1400G
#SBATCH --time=UNLIMITED
#SBATCH --output=./slurm_logs/%x_%j.out
#SBATCH --error=./slurm_logs/%x_%j.err
#SBATCH --gres=gpu:2

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

# Load CUDA modules and set environment
module purge
module load nvidia/cuda/toolkit/12.9.1

# Fix LD_LIBRARY_PATH for CUDA
export LD_LIBRARY_PATH=/usr/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/local/cuda/lib64:$LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/lib64

# Activate conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate flv2

# Set working directory
cd .

# Set Python path
export PYTHONPATH=.:$PYTHONPATH

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
srun python ./run_gossip_test.py --local

echo "============================================"
echo "Job finished at $(date)"
echo "============================================"

# Cleanup Ray cluster
ray stop --force 2>/dev/null || true
