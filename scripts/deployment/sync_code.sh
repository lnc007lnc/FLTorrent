#!/bin/bash
#
# sync_code.sh - Sync FLTorrent core code to all worker nodes
#
# This script synchronizes the essential FLTorrent source code to all configured
# worker nodes. Workers are defined using SSH host aliases in ~/.ssh/config.
#
# Prerequisites:
#   1. Configure ~/.ssh/config with Host entries for each worker
#   2. Update workers.conf with the host aliases
#   3. Ensure proper permissions on pem files and ssh config
#

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKERS_CONF="${SCRIPT_DIR}/workers.conf"
SOURCE_DIR="${SCRIPT_DIR}/FLTorrent"
DEST_DIR="/home/ubuntu/FLTorrent"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# SSH options for connection reliability (authentication handled by ~/.ssh/config)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

# Core code directories and files to sync (based on git history analysis)
# These are the most actively developed and critical components
CORE_DIRS=(
    "federatedscope/core"           # Main core module with BitTorrent, gRPC, chunk management
    "federatedscope/contrib"        # Contributed modules
    "federatedscope/autotune"       # Auto-tuning functionality
    "federatedscope/cv"             # Computer vision models
    "federatedscope/nlp"            # NLP models
    "federatedscope/gfl"            # Graph federated learning
    "federatedscope/mf"             # Matrix factorization
    "federatedscope/vertical_fl"    # Vertical federated learning
    "federatedscope/attack"         # Attack modules
    "federatedscope/register.py"    # Module registration
    "federatedscope/main.py"        # Main entry point
    "federatedscope/__init__.py"    # Package init
)

# Top-level essential files
CORE_FILES=(
    "run_ray.py"                    # Ray-based distributed runner (frequently modified)
    "setup.py"                      # Package setup
    "requirements.txt"              # Dependencies
    "configs"                       # Configuration directory
    "docker"                        # Docker related files
    "environment"                   # Environment setup files
    "materials"                     # Materials for docker container mounting
    "data"                          # Dataset directory (CelebA, CIFAR, etc.)
)

# Files/directories to exclude from sync
EXCLUDES=(
    "__pycache__"
    "*.pyc"
    "*.pyo"
    "*.pyd"
    ".git"
    ".gitignore"
    "*.egg-info"
    "*.egg"
    "build"
    "dist"
    "*.db"
    "*.db-journal"
    "*.db-wal"
    "*.sqlite"
    "*.pkl"
    "*.pickle"
    "*.pth"
    # "*.pt"  # Removed: needed for processed dataset files
    "*.h5"
    "*.hdf5"
    "*.npz"
    "*.npy"
    "*.log"
    "ray_v2_output"
    "docker_data"
    "tmp"
    "logs"
    "wandb"
    ".idea"
    ".vscode"
    "node_modules"
    "*.tar.gz"
    ".DS_Store"
)

# Print functions
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if workers config exists
check_config() {
    if [[ ! -f "$WORKERS_CONF" ]]; then
        print_error "Workers configuration file not found: $WORKERS_CONF"
        print_info "Please create the config file with worker host aliases."
        print_info "Example format:"
        echo "  WORKER1=london-worker"
        echo "  WORKER2=us-worker"
        echo ""
        print_info "And configure ~/.ssh/config with Host entries for each worker."
        exit 1
    fi
}

# Load worker addresses from config
load_workers() {
    WORKERS=()
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        # Only load WORKER entries
        if [[ "$key" =~ ^WORKER[0-9]+$ ]]; then
            # Remove leading/trailing whitespace and quotes
            value=$(echo "$value" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | tr -d '"')
            if [[ -n "$value" && ! "$value" =~ ^\<.*\>$ ]]; then
                WORKERS+=("$value")
            fi
        fi
    done < "$WORKERS_CONF"

    if [[ ${#WORKERS[@]} -eq 0 ]]; then
        print_error "No valid workers found in config file."
        print_info "Please update $WORKERS_CONF with SSH host aliases."
        exit 1
    fi

    print_info "Found ${#WORKERS[@]} worker(s) in configuration"
}

# Check SSH connectivity to a worker
check_connectivity() {
    local worker=$1
    ssh $SSH_OPTS "$worker" "echo 'Connection OK'" &>/dev/null
    return $?
}

# Sync code to a single worker
sync_to_worker() {
    local worker=$1
    local worker_name=$2

    print_info "Syncing to $worker_name ($worker)..."

    # Check connectivity first
    if ! check_connectivity "$worker"; then
        print_error "Cannot connect to $worker"
        print_info "Please check:"
        print_info "  1. ~/.ssh/config has correct Host entry for '$worker'"
        print_info "  2. IdentityFile path is correct"
        print_info "  3. HostName (IP) is correct"
        print_info "  4. Worker node is running"
        return 1
    fi

    # Create destination directory on remote
    ssh $SSH_OPTS "$worker" "mkdir -p $DEST_DIR" || {
        print_error "Failed to create directory on $worker"
        return 1
    }

    # Build rsync exclude arguments
    local exclude_args=""
    for pattern in "${EXCLUDES[@]}"; do
        exclude_args="$exclude_args --exclude='$pattern'"
    done

    # Sync core directories (quiet mode, only show changed files)
    for item in "${CORE_DIRS[@]}"; do
        local src_path="${SOURCE_DIR}/${item}"
        if [[ -e "$src_path" ]]; then
            local parent_dir=$(dirname "$item")
            # Create parent directory on remote if needed
            if [[ "$parent_dir" != "." ]]; then
                ssh $SSH_OPTS "$worker" "mkdir -p ${DEST_DIR}/${parent_dir}" 2>/dev/null
            fi
            # Use --itemize-changes to only show actual changes, suppress stats
            eval "rsync -az --delete --itemize-changes $exclude_args -e 'ssh $SSH_OPTS' '$src_path' '$worker:${DEST_DIR}/${parent_dir}/'" 2>/dev/null || {
                print_warning "Failed to sync $item"
            }
        fi
    done

    # Sync core files
    for item in "${CORE_FILES[@]}"; do
        local src_path="${SOURCE_DIR}/${item}"
        if [[ -e "$src_path" ]]; then
            eval "rsync -az --delete --itemize-changes $exclude_args -e 'ssh $SSH_OPTS' '$src_path' '$worker:${DEST_DIR}/'" 2>/dev/null || {
                print_warning "Failed to sync $item"
            }
        fi
    done

    # Sync federatedscope __init__.py
    rsync -az -e "ssh $SSH_OPTS" "${SOURCE_DIR}/federatedscope/__init__.py" "$worker:${DEST_DIR}/federatedscope/" 2>/dev/null

    print_success "Completed sync to $worker_name"
    return 0
}

# Main sync function (parallel execution)
sync_all() {
    print_info "Starting FLTorrent core code synchronization (parallel)..."
    print_info "Source: $SOURCE_DIR"
    print_info "Destination: $DEST_DIR"
    echo ""

    local worker_num=1
    local pids=()
    local workers_list=()

    # Start all sync jobs in parallel
    for worker in "${WORKERS[@]}"; do
        print_info "Starting sync to WORKER${worker_num} ($worker) in background..."
        sync_to_worker "$worker" "WORKER${worker_num}" &
        pids+=($!)
        workers_list+=("$worker")
        worker_num=$((worker_num + 1))
    done

    print_info "Waiting for ${#pids[@]} sync jobs to complete..."

    # Wait for all jobs and collect results
    local success_count=0
    local fail_count=0
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            success_count=$((success_count + 1))
        else
            print_error "Sync to ${workers_list[$i]} failed"
            fail_count=$((fail_count + 1))
        fi
    done

    echo "========================================"
    print_info "Synchronization complete!"
    print_success "Successful: $success_count"
    if [[ $fail_count -gt 0 ]]; then
        print_error "Failed: $fail_count"
        return 1
    fi
    return 0
}

# Show what will be synced (dry run)
show_sync_plan() {
    print_info "=== Sync Plan ==="
    echo ""
    print_info "Core directories to sync:"
    for dir in "${CORE_DIRS[@]}"; do
        if [[ -e "${SOURCE_DIR}/${dir}" ]]; then
            echo "  ✓ $dir"
        else
            echo "  ✗ $dir (not found)"
        fi
    done
    echo ""
    print_info "Core files to sync:"
    for file in "${CORE_FILES[@]}"; do
        if [[ -e "${SOURCE_DIR}/${file}" ]]; then
            echo "  ✓ $file"
        else
            echo "  ✗ $file (not found)"
        fi
    done
    echo ""
    print_info "Excluded patterns:"
    for pattern in "${EXCLUDES[@]}"; do
        echo "  - $pattern"
    done
    echo ""
    print_info "Target workers (SSH host aliases):"
    for worker in "${WORKERS[@]}"; do
        echo "  → $worker"
    done
}

# Test SSH connectivity to all workers
test_connectivity() {
    print_info "=== Testing SSH Connectivity ==="
    echo ""

    local success_count=0
    local fail_count=0

    for worker in "${WORKERS[@]}"; do
        echo -n "  Testing $worker... "
        if check_connectivity "$worker"; then
            echo -e "${GREEN}OK${NC}"
            success_count=$((success_count + 1))
        else
            echo -e "${RED}FAILED${NC}"
            fail_count=$((fail_count + 1))
        fi
    done

    echo ""
    print_info "Results: $success_count connected, $fail_count failed"

    if [[ $fail_count -gt 0 ]]; then
        echo ""
        print_info "For failed connections, check ~/.ssh/config entries:"
        echo "  Host <worker-alias>"
        echo "      HostName <ip-address>"
        echo "      User ubuntu"
        echo "      IdentityFile /home/ubuntu/worker_pem/<key>.pem"
        return 1
    fi
    return 0
}

# Parse command line arguments
case "${1:-}" in
    --dry-run|-n)
        check_config
        load_workers
        show_sync_plan
        ;;
    --test|-t)
        check_config
        load_workers
        test_connectivity
        ;;
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --dry-run, -n    Show sync plan without executing"
        echo "  --test, -t       Test SSH connectivity to all workers"
        echo "  --help, -h       Show this help message"
        echo ""
        echo "Sync FLTorrent core code to all configured worker nodes."
        echo ""
        echo "Configuration:"
        echo "  Workers file: $WORKERS_CONF"
        echo "  SSH config:   ~/.ssh/config"
        echo ""
        echo "Example ~/.ssh/config entry:"
        echo "  Host london-worker"
        echo "      HostName 35.179.181.30"
        echo "      User ubuntu"
        echo "      IdentityFile /home/ubuntu/worker_pem/worker-london.pem"
        ;;
    *)
        check_config
        load_workers
        sync_all
        ;;
esac
