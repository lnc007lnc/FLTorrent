#!/bin/bash
#SBATCH --job-name=test_dup_fix
#SBATCH --partition=gpu3
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=64
#SBATCH --mem=400G
#SBATCH --time=4:00:00
#SBATCH --output=./slurm_logs/test_dup_fix_%j.out
#SBATCH --error=./slurm_logs/test_dup_fix_%j.err
#SBATCH --gres=gpu:2

# ============================================================================
# Test experiment for duplicate chunk fix
# Uses current code with fixes applied
# ============================================================================

echo "============================================"
echo "Test: Duplicate Chunk Fix Verification"
echo "Job ID: $SLURM_JOB_ID"
echo "Start Time: $(date)"
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

# Use current code (with fixes)
cd .
export PYTHONPATH=.:$PYTHONPATH

# Clean up old UDS sockets
rm -rf /tmp/federatedscope_uds/* 2>/dev/null || true
mkdir -p /tmp/federatedscope_uds

# Set Ray temp directory
export RAY_TMPDIR=/tmp/ray_test_dup_fix_$SLURM_JOB_ID

# Print environment
echo "Python: $(which python)"
echo "Working dir: $(pwd)"
echo "PYTHONPATH: $PYTHONPATH"

# Run experiment with current code
echo "Starting FL experiment with duplicate fix..."
srun python ./run_ray_hpc.py --local

echo "============================================"
echo "Job finished at $(date)"
echo "============================================"

# Cleanup
ray stop --force 2>/dev/null || true
