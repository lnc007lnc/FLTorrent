#!/bin/bash
#SBATCH --job-name=fltorrent_exp2_churn
#SBATCH --partition=gpu2
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=exp_out/exp2_churn_%j.out
#SBATCH --error=exp_out/exp2_churn_%j.err

# Experiment 2: Churn Injection
# Purpose: Show timeout+backfill+comp vs wait-for-all

echo "============================================="
echo "Experiment 2: Churn Injection"
echo "Start time: $(date)"
echo "Node: $(hostname)"
echo "============================================="

cd .

# Setup environment
source ~/.bashrc
conda activate fltorrent 2>/dev/null || echo "Using system Python"

# Create output directories
mkdir -p exp_out/churn_p10 exp_out/churn_p20 exp_out/churn_wait_all

# Fix SSL issues
export PYTHONHTTPSVERIFY=0
export SSL_VERIFY=False

echo ""
echo "=== Running: Churn 10% with timeout+comp ==="
python federatedscope/main.py --cfg configs/exp_churn_p10.yaml 2>&1 | tee exp_out/churn_p10/run.log

echo ""
echo "=== Running: Churn 20% with timeout+comp ==="
python federatedscope/main.py --cfg configs/exp_churn_p20.yaml 2>&1 | tee exp_out/churn_p20/run.log

echo ""
echo "=== Running: Churn 10% with wait-for-all (control) ==="
# Note: This will likely timeout or be very slow - that's the point
timeout 7200 python federatedscope/main.py --cfg configs/exp_churn_wait_all.yaml 2>&1 | tee exp_out/churn_wait_all/run.log || echo "Wait-for-all experiment timed out (expected)"

echo ""
echo "============================================="
echo "Experiment 2 completed at: $(date)"
echo "============================================="

# Check for [Churn] markers
echo ""
echo "=== Verifying Churn Injection ==="
grep -c "\[Churn\]" exp_out/churn_p10/run.log || echo "No [Churn] markers in p10"
grep -c "\[Churn\]" exp_out/churn_p20/run.log || echo "No [Churn] markers in p20"
