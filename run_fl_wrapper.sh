#!/bin/bash
# Wrapper script for FL experiment (called by srun inside sbatch)

# Load CUDA module
module purge
module load nvidia/cuda/toolkit/12.9.1

# Fix LD_LIBRARY_PATH
export LD_LIBRARY_PATH=/usr/lib64:/usr/local/cuda/lib64:/lib64:$LD_LIBRARY_PATH

# Activate conda
source $(conda info --base)/etc/profile.d/conda.sh
conda activate flv2

# Set Python path
export PYTHONPATH=.:$PYTHONPATH

# Debug output
echo "=== srun Environment Check ==="
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "CUDA_VISIBLE_DEVICES: $CUDA_VISIBLE_DEVICES"
python -c "import torch; print(f'CUDA available: {torch.cuda.is_available()}'); print(f'GPU count: {torch.cuda.device_count() if torch.cuda.is_available() else 0}')"
echo "==============================="

# Run the experiment
cd .
python run_ray_hpc.py --local
