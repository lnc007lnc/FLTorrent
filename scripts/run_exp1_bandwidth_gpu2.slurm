#!/bin/bash
#SBATCH --job-name=fltorrent_exp1_bandwidth
#SBATCH --partition=gpu2
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --gres=gpu:1
#SBATCH --mem=64G
#SBATCH --time=24:00:00
#SBATCH --output=exp_out/exp1_bandwidth_%j.out
#SBATCH --error=exp_out/exp1_bandwidth_%j.err

# Experiment 1: Bandwidth Heterogeneity + Fairness
# Purpose: (a) time-to-accuracy (b) per-client accuracy boxplot

echo "============================================="
echo "Experiment 1: Bandwidth Heterogeneity"
echo "Start time: $(date)"
echo "Node: $(hostname)"
echo "============================================="

cd .

# Setup environment
source ~/.bashrc
conda activate fltorrent 2>/dev/null || echo "Using system Python"

# Create output directories
mkdir -p exp_out/bandwidth_comp_on exp_out/bandwidth_comp_off

# Fix SSL issues
export PYTHONHTTPSVERIFY=0
export SSL_VERIFY=False

echo ""
echo "=== Running: Bandwidth w/ Compensation ==="
echo "Config: configs/exp_bandwidth_comp_on.yaml"
python federatedscope/main.py --cfg configs/exp_bandwidth_comp_on.yaml 2>&1 | tee exp_out/bandwidth_comp_on/run.log

echo ""
echo "=== Running: Bandwidth w/o Compensation ==="
echo "Config: configs/exp_bandwidth_comp_off.yaml"
python federatedscope/main.py --cfg configs/exp_bandwidth_comp_off.yaml 2>&1 | tee exp_out/bandwidth_comp_off/run.log

echo ""
echo "============================================="
echo "Experiment 1 completed at: $(date)"
echo "============================================="

# Check for [BW-SIM] markers to verify bandwidth simulation worked
echo ""
echo "=== Verifying Bandwidth Simulation ==="
grep -c "\[BW-SIM\]" exp_out/bandwidth_comp_on/run.log || echo "No [BW-SIM] markers found in comp_on"
grep -c "\[BW-SIM\]" exp_out/bandwidth_comp_off/run.log || echo "No [BW-SIM] markers found in comp_off"
