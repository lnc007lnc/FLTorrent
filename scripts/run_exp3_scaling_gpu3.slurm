#!/bin/bash
#SBATCH --job-name=fltorrent_exp3_scaling
#SBATCH --partition=gpu3
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=128G
#SBATCH --time=12:00:00
#SBATCH --output=exp_out/exp3_scaling_%j.out
#SBATCH --error=exp_out/exp3_scaling_%j.err

# Experiment 3: Protocol-only Scaling
# Purpose: Measure protocol overhead without ML computation

echo "============================================="
echo "Experiment 3: Protocol-only Scaling"
echo "Start time: $(date)"
echo "Node: $(hostname)"
echo "============================================="

cd .

# Setup environment
source ~/.bashrc
conda activate fltorrent 2>/dev/null || echo "Using system Python"

# Create output directories
mkdir -p exp_out/scaling_n50 exp_out/scaling_n100 exp_out/scaling_n200

# Fix SSL issues
export PYTHONHTTPSVERIFY=0
export SSL_VERIFY=False

echo ""
echo "=== Running: Protocol-only N=50 ==="
python federatedscope/main.py --cfg configs/exp_scaling_n50.yaml 2>&1 | tee exp_out/scaling_n50/run.log

echo ""
echo "=== Running: Protocol-only N=100 ==="
python federatedscope/main.py --cfg configs/exp_scaling_n100.yaml 2>&1 | tee exp_out/scaling_n100/run.log

echo ""
echo "=== Running: Protocol-only N=200 ==="
python federatedscope/main.py --cfg configs/exp_scaling_n200.yaml 2>&1 | tee exp_out/scaling_n200/run.log

echo ""
echo "============================================="
echo "Experiment 3 completed at: $(date)"
echo "============================================="

# Check for [Protocol-only] markers
echo ""
echo "=== Verifying Protocol-only Mode ==="
grep -c "\[Protocol-only\]" exp_out/scaling_n50/run.log || echo "No markers in n50"
grep -c "\[Protocol-only\]" exp_out/scaling_n100/run.log || echo "No markers in n100"
grep -c "\[Protocol-only\]" exp_out/scaling_n200/run.log || echo "No markers in n200"
