#!/bin/bash
# Build FederatedScope edge device simulation Docker images
set -e

echo "🐳 Starting to build FederatedScope edge device simulation images..."

# Color definitions
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Build base image
echo -e "${BLUE}📦 Building base image (federatedscope:base)${NC}"
docker build -f docker/Dockerfile.base -t federatedscope:base .

# Build edge server image
echo -e "${BLUE}🖥️  Building edge server image (federatedscope:edge-server)${NC}"
docker build -f docker/Dockerfile.edge-server -t federatedscope:edge-server .

# Build high-end smartphone image
echo -e "${BLUE}📱 Building high-end smartphone image (federatedscope:smartphone-high)${NC}"
docker build -f docker/Dockerfile.smartphone-high -t federatedscope:smartphone-high .

# Build low-end smartphone image
echo -e "${BLUE}📱 Building low-end smartphone image (federatedscope:smartphone-low)${NC}"
docker build -f docker/Dockerfile.smartphone-low -t federatedscope:smartphone-low .

# Build Raspberry Pi image
echo -e "${BLUE}🥧 Building Raspberry Pi image (federatedscope:raspberry-pi)${NC}"
docker build -f docker/Dockerfile.raspberry-pi -t federatedscope:raspberry-pi .

# Build IoT minimal image
echo -e "${BLUE}🔌 Building IoT minimal image (federatedscope:iot-minimal)${NC}"
docker build -f docker/Dockerfile.iot-minimal -t federatedscope:iot-minimal .

echo -e "${GREEN}✅ All images built successfully!${NC}"
echo ""
echo -e "${YELLOW}📋 Available images:${NC}"
echo "  • federatedscope:base           - Base image"
echo "  • federatedscope:edge-server    - Edge server (high performance, multi-GPU)"
echo "  • federatedscope:smartphone-high - High-end smartphone (GPU support)"
echo "  • federatedscope:smartphone-low  - Low-end smartphone (CPU version)"
echo "  • federatedscope:raspberry-pi    - Raspberry Pi (ARM optimized)"
echo "  • federatedscope:iot-minimal     - IoT device (minimal resources)"
echo ""
echo -e "${YELLOW}🚀 Running example:${NC}"
echo "  python run_ray.py --config_path configs/docker_simulation.yaml"
echo ""
echo -e "${YELLOW}📊 View images:${NC}"
echo "  docker images | grep federatedscope"