#!/bin/bash
#
# run_experiment.sh - Complete FLTorrent Distributed Experiment Runner
#
# This script performs the following steps:
# 1. Sync core code to all worker nodes
# 2. Start Ray head node on this machine
# 3. Connect all workers to the Ray cluster
# 4. Run the FLTorrent experiment via run_ray.py
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
FLTORRENT_DIR="${SCRIPT_DIR}/FLTorrent"
SYNC_SCRIPT="${SCRIPT_DIR}/sync_code.sh"

# Ray configuration
RAY_HEAD_PORT=6379
RAY_DASHBOARD_PORT=8265
RAY_OBJECT_MANAGER_PORT=8076
RAY_NODE_MANAGER_PORT=8077

# Conda configuration
# Change CONDA_ENV_NAME if your environment has a different name
CONDA_ENV_NAME="flv2"

# Conda base path (use absolute path, must be the same on head and all workers)
# Common paths: /home/ubuntu/miniconda3, /home/ubuntu/anaconda3, /opt/conda
CONDA_BASE=/home/ubuntu/miniconda3

# Conda initialization command (same for both local and remote, using absolute path)
CONDA_INIT_CMD="source ${CONDA_BASE}/etc/profile.d/conda.sh && conda activate ${CONDA_ENV_NAME}"
REMOTE_CONDA_INIT="source ${CONDA_BASE}/etc/profile.d/conda.sh && conda activate ${CONDA_ENV_NAME}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# SSH options for connection reliability (authentication handled by ~/.ssh/config)
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes"

# Print functions
print_header() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════════${NC}"
    echo ""
}

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

# Get head node IP (this machine's IP that workers can reach)
get_head_ip() {
    local ip=""

    # For cross-region AWS: Try to get public IP first
    # Method 1: AWS metadata service (works on EC2)
    ip=$(curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/public-ipv4 2>/dev/null)

    # Method 2: External service to get public IP
    if [[ -z "$ip" || "$ip" == *"Not Found"* ]]; then
        ip=$(curl -s --connect-timeout 2 http://checkip.amazonaws.com 2>/dev/null)
    fi

    # Method 3: Another external service
    if [[ -z "$ip" ]]; then
        ip=$(curl -s --connect-timeout 2 http://ifconfig.me 2>/dev/null)
    fi

    # Fallback to private IP (for same-VPC deployments)
    if [[ -z "$ip" ]]; then
        ip=$(ip route get 1 2>/dev/null | awk '{print $7; exit}')
    fi

    if [[ -z "$ip" ]]; then
        ip=$(hostname -I 2>/dev/null | awk '{print $1}')
    fi

    echo "$ip"
}

# Load worker addresses from config
load_workers() {
    WORKERS=()
    while IFS='=' read -r key value; do
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        if [[ "$key" =~ ^WORKER[0-9]+$ ]]; then
            value=$(echo "$value" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | tr -d '"')
            if [[ -n "$value" && ! "$value" =~ ^\<.*\>$ ]]; then
                WORKERS+=("$value")
            fi
        fi
    done < "$WORKERS_CONF"
}

# Check prerequisites
check_prerequisites() {
    print_header "Step 0: Checking Prerequisites"

    # Check workers config
    if [[ ! -f "$WORKERS_CONF" ]]; then
        print_error "Workers config not found: $WORKERS_CONF"
        exit 1
    fi
    print_success "Workers config found"

    # Check sync script
    if [[ ! -f "$SYNC_SCRIPT" ]]; then
        print_error "Sync script not found: $SYNC_SCRIPT"
        exit 1
    fi
    print_success "Sync script found"

    # Check FLTorrent directory
    if [[ ! -d "$FLTORRENT_DIR" ]]; then
        print_error "FLTorrent directory not found: $FLTORRENT_DIR"
        exit 1
    fi
    print_success "FLTorrent directory found"

    # Check run_ray.py
    if [[ ! -f "${FLTORRENT_DIR}/run_ray.py" ]]; then
        print_error "run_ray.py not found in FLTorrent directory"
        exit 1
    fi
    print_success "run_ray.py found"

    # Activate conda environment before checking Ray
    print_info "Activating conda environment: $CONDA_ENV_NAME"
    eval "$CONDA_INIT_CMD" || {
        print_error "Failed to activate conda environment: $CONDA_ENV_NAME"
        print_info "Please ensure conda is installed at: $CONDA_BASE"
        exit 1
    }
    print_success "Conda environment activated"

    # Check Ray installation (now in conda environment)
    if ! command -v ray &> /dev/null; then
        print_error "Ray is not installed. Please install with: pip install ray"
        exit 1
    fi
    print_success "Ray is installed: $(ray --version 2>/dev/null | head -1)"

    # Load workers
    load_workers
    if [[ ${#WORKERS[@]} -eq 0 ]]; then
        print_warning "No workers configured. Running in local mode only."
    else
        print_success "Found ${#WORKERS[@]} worker(s): ${WORKERS[*]}"
    fi
}

# Step 1: Sync code to workers
sync_code() {
    print_header "Step 1: Syncing Code to Workers"

    if [[ ${#WORKERS[@]} -eq 0 ]]; then
        print_warning "No workers to sync. Skipping..."
        return 0
    fi

    print_info "Running sync_code.sh..."
    bash "$SYNC_SCRIPT"

    print_success "Code sync completed"
}

# Step 2: Stop any existing Ray processes and Docker containers
cleanup_ray() {
    print_header "Step 2: Cleaning Up Existing Ray Processes and Docker Containers"

    # Stop Ray on head node (with conda)
    print_info "Stopping Ray on head node..."
    eval "$CONDA_INIT_CMD" 2>/dev/null || true
    ray stop --force 2>/dev/null || true

    # Clean up FL Docker containers on head node
    print_info "Cleaning up FL Docker containers on head node..."
    docker ps -a --filter "name=fl_" -q | xargs -r docker rm -f 2>/dev/null || true

    # Clean up temporary experiment files on head node
    print_info "Cleaning up temporary experiment files on head node..."
    sudo rm -rf "${FLTORRENT_DIR}/docker_data/app_tmp/"* 2>/dev/null || true
    sudo rm -rf "${FLTORRENT_DIR}/docker_data/tmp/"* 2>/dev/null || true

    # Stop Ray and clean Docker on all workers
    # Workers are SSH host aliases, ~/.ssh/config handles authentication
    for worker in "${WORKERS[@]}"; do
        print_info "Stopping Ray on $worker..."
        ssh $SSH_OPTS "$worker" "bash -lc '${REMOTE_CONDA_INIT} && ray stop --force'" 2>/dev/null || true

        print_info "Cleaning up FL Docker containers on $worker..."
        ssh $SSH_OPTS "$worker" "docker ps -a --filter 'name=fl_' -q | xargs -r docker rm -f" 2>/dev/null || true

        print_info "Cleaning up temporary experiment files on $worker..."
        ssh $SSH_OPTS "$worker" "sudo rm -rf ${FLTORRENT_DIR}/docker_data/app_tmp/* ${FLTORRENT_DIR}/docker_data/tmp/*" 2>/dev/null || true
    done

    # Wait for cleanup
    sleep 2
    print_success "Ray, Docker, and temp files cleanup completed"
}

# Step 3: Start Ray head node
start_ray_head() {
    print_header "Step 3: Starting Ray Head Node"

    HEAD_IP=$(get_head_ip)
    if [[ -z "$HEAD_IP" ]]; then
        print_error "Could not determine head node IP address"
        exit 1
    fi

    print_info "Head node IP: $HEAD_IP"
    print_info "Activating conda environment: $CONDA_ENV_NAME"
    print_info "Starting Ray head node..."

    # Activate conda and start Ray head with dashboard
    # Use --node-ip-address=0.0.0.0 to accept connections from any interface (needed for cross-region)
    eval "$CONDA_INIT_CMD"

    # Set environment variables for cross-region tolerance (longer timeouts for high-latency networks)
    export RAY_health_check_failure_threshold=20      # Allow 20 missed heartbeats before marking dead
    export RAY_health_check_period_ms=10000           # Check health every 10 seconds
    export RAY_health_check_timeout_ms=60000          # 60 second timeout for health check
    export RAY_num_heartbeats_timeout=30              # Number of heartbeats to wait before timeout
    export RAY_heartbeat_timeout_milliseconds=60000   # 60 second heartbeat timeout
    export RAY_raylet_heartbeat_period_milliseconds=5000  # Raylet sends heartbeat every 5 seconds
    export RAY_gcs_server_request_timeout_seconds=60  # GCS request timeout 60 seconds

    ray start --head \
        --port=$RAY_HEAD_PORT \
        --node-ip-address=0.0.0.0 \
        --dashboard-host=0.0.0.0 \
        --dashboard-port=$RAY_DASHBOARD_PORT \
        --object-manager-port=$RAY_OBJECT_MANAGER_PORT \
        --node-manager-port=$RAY_NODE_MANAGER_PORT \
        --disable-usage-stats

    # Wait for head to be ready
    sleep 3

    # Verify head is running
    if ray status &>/dev/null; then
        print_success "Ray head node started successfully"
        print_info "Dashboard: http://${HEAD_IP}:${RAY_DASHBOARD_PORT}"
    else
        print_error "Failed to start Ray head node"
        exit 1
    fi

    # Export for later use
    export RAY_HEAD_ADDRESS="${HEAD_IP}:${RAY_HEAD_PORT}"
    print_info "Ray cluster address: $RAY_HEAD_ADDRESS"
}

# Step 4: Connect workers to Ray cluster
connect_workers() {
    print_header "Step 4: Connecting Workers to Ray Cluster"

    if [[ ${#WORKERS[@]} -eq 0 ]]; then
        print_warning "No workers to connect. Running on head node only."
        return 0
    fi

    local success_count=0
    local fail_count=0

    # Environment variables for worker nodes (same as head for cross-region tolerance)
    local RAY_ENV_VARS="export RAY_health_check_failure_threshold=20 && \
export RAY_health_check_period_ms=10000 && \
export RAY_health_check_timeout_ms=60000 && \
export RAY_num_heartbeats_timeout=30 && \
export RAY_heartbeat_timeout_milliseconds=60000 && \
export RAY_raylet_heartbeat_period_milliseconds=5000 && \
export RAY_gcs_server_request_timeout_seconds=60"

    for worker in "${WORKERS[@]}"; do
        print_info "Connecting $worker to Ray cluster..."

        # Get worker's public IP for cross-region communication
        local worker_public_ip=$(ssh $SSH_OPTS "$worker" "curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/public-ipv4" 2>/dev/null)
        print_info "Worker $worker public IP: $worker_public_ip"

        # Start Ray worker node with conda environment and timeout settings activated
        # Use --node-ip-address to specify public IP for cross-region communication
        # Worker is SSH host alias, ~/.ssh/config handles User and IdentityFile
        if ssh $SSH_OPTS "$worker" "bash -lc '${REMOTE_CONDA_INIT} && ${RAY_ENV_VARS} && ray start --address=${RAY_HEAD_ADDRESS} --node-ip-address=${worker_public_ip} --disable-usage-stats'" 2>/dev/null; then
            print_success "$worker connected"
            success_count=$((success_count + 1))
        else
            print_error "Failed to connect $worker"
            print_info "Check ~/.ssh/config entry for '$worker'"
            fail_count=$((fail_count + 1))
        fi
    done

    # Wait for workers to register
    sleep 3

    # Create Docker network on all workers (for distributed container deployment)
    print_info "Creating Docker network on worker nodes..."
    for worker in "${WORKERS[@]}"; do
        # Create fl_network on worker if not exists
        ssh $SSH_OPTS "$worker" "docker network inspect fl_network >/dev/null 2>&1 || \
            docker network create --driver bridge \
            --opt com.docker.network.bridge.enable_icc=true \
            --opt com.docker.network.bridge.enable_ip_masquerade=true \
            --subnet=172.30.0.0/16 --gateway=172.30.0.1 \
            fl_network" 2>/dev/null && \
            print_success "Docker network fl_network ready on $worker" || \
            print_warning "Failed to create Docker network on $worker"
    done

    print_info "Connected workers: $success_count, Failed: $fail_count"

    # Show cluster status (conda already activated in start_ray_head)
    print_info "Ray cluster status:"
    ray status
}

# Step 5: Run the experiment
run_experiment() {
    print_header "Step 5: Running FLTorrent Experiment"

    cd "$FLTORRENT_DIR"

    # Ensure conda environment is activated
    eval "$CONDA_INIT_CMD"

    # Set Ray address environment variable
    export RAY_ADDRESS="$RAY_HEAD_ADDRESS"

    print_info "Working directory: $(pwd)"
    print_info "Conda environment: $CONDA_ENV_NAME"
    print_info "RAY_ADDRESS: $RAY_ADDRESS"
    print_info "Starting run_ray.py..."
    echo ""

    # Run the experiment with sudo for Docker access
    sudo -E env "PATH=$PATH" python run_ray.py

    local exit_code=$?

    if [[ $exit_code -eq 0 ]]; then
        print_success "Experiment completed successfully"
    else
        print_error "Experiment failed with exit code: $exit_code"
    fi

    return $exit_code
}

# Cleanup function for graceful shutdown
cleanup() {
    print_header "Cleaning Up"

    print_info "Stopping Ray on all nodes..."

    # Stop workers first (with conda)
    # Workers are SSH host aliases
    for worker in "${WORKERS[@]}"; do
        ssh $SSH_OPTS "$worker" "bash -lc '${REMOTE_CONDA_INIT} && ray stop --force'" 2>/dev/null || true
    done

    # Stop head node (with conda)
    eval "$CONDA_INIT_CMD" 2>/dev/null || true
    ray stop --force 2>/dev/null || true

    print_success "Cleanup completed"
}

# Test SSH connectivity to all workers
test_workers() {
    print_header "Testing Worker Connectivity"

    load_workers

    if [[ ${#WORKERS[@]} -eq 0 ]]; then
        print_warning "No workers configured in $WORKERS_CONF"
        return 0
    fi

    local success_count=0
    local fail_count=0

    for worker in "${WORKERS[@]}"; do
        echo -n "  Testing $worker... "
        if ssh $SSH_OPTS "$worker" "echo 'OK'" &>/dev/null; then
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
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --skip-sync       Skip code synchronization step"
    echo "  --skip-cleanup    Skip initial Ray cleanup step"
    echo "  --local           Run in local mode (no workers)"
    echo "  --stop            Stop all Ray processes and exit"
    echo "  --status          Show Ray cluster status and exit"
    echo "  --test            Test SSH connectivity to all workers"
    echo "  --help, -h        Show this help message"
    echo ""
    echo "Configuration:"
    echo "  Workers file: $WORKERS_CONF"
    echo "  SSH config:   ~/.ssh/config"
    echo ""
    echo "Environment Variables:"
    echo "  RAY_HEAD_PORT     Ray head node port (default: 6379)"
    echo "  RAY_DASHBOARD_PORT Ray dashboard port (default: 8265)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Full run: sync, start Ray, run experiment"
    echo "  $0 --skip-sync        # Skip code sync, useful for re-runs"
    echo "  $0 --local            # Run on head node only"
    echo "  $0 --test             # Test SSH connectivity to workers"
    echo "  $0 --stop             # Stop all Ray processes"
    echo ""
    echo "SSH Configuration:"
    echo "  Workers are defined using SSH host aliases in ~/.ssh/config"
    echo "  Example ~/.ssh/config entry:"
    echo "    Host london-worker"
    echo "        HostName 35.179.181.30"
    echo "        User ubuntu"
    echo "        IdentityFile /home/ubuntu/worker_pem/worker-london.pem"
}

# Main execution
main() {
    local skip_sync=false
    local skip_cleanup=false
    local local_mode=false

    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-sync)
                skip_sync=true
                shift
                ;;
            --skip-cleanup)
                skip_cleanup=true
                shift
                ;;
            --local)
                local_mode=true
                shift
                ;;
            --stop)
                load_workers
                cleanup
                exit 0
                ;;
            --status)
                eval "$CONDA_INIT_CMD"
                ray status
                exit 0
                ;;
            --test)
                test_workers
                exit 0
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done

    # Set trap for cleanup on exit
    trap cleanup EXIT INT TERM

    print_header "FLTorrent Distributed Experiment Runner"
    print_info "Start time: $(date)"

    # Run steps
    check_prerequisites

    if [[ "$local_mode" == true ]]; then
        WORKERS=()  # Clear workers for local mode
    fi

    if [[ "$skip_sync" == false ]]; then
        sync_code
    else
        print_info "Skipping code sync (--skip-sync)"
    fi

    if [[ "$skip_cleanup" == false ]]; then
        cleanup_ray
    else
        print_info "Skipping Ray cleanup (--skip-cleanup)"
    fi

    start_ray_head
    connect_workers
    run_experiment

    print_info "End time: $(date)"
}

# Run main
main "$@"
