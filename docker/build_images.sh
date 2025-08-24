#!/bin/bash
# 构建FederatedScope边缘设备仿真Docker镜像
set -e

echo "🐳 开始构建FederatedScope边缘设备仿真镜像..."

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 构建基础镜像
echo -e "${BLUE}📦 构建基础镜像 (federatedscope:base)${NC}"
docker build -f docker/Dockerfile.base -t federatedscope:base .

# 构建边缘服务器镜像
echo -e "${BLUE}🖥️  构建边缘服务器镜像 (federatedscope:edge-server)${NC}"
docker build -f docker/Dockerfile.edge-server -t federatedscope:edge-server .

# 构建高端智能手机镜像
echo -e "${BLUE}📱 构建高端智能手机镜像 (federatedscope:smartphone-high)${NC}"
docker build -f docker/Dockerfile.smartphone-high -t federatedscope:smartphone-high .

# 构建低端智能手机镜像
echo -e "${BLUE}📱 构建低端智能手机镜像 (federatedscope:smartphone-low)${NC}"
docker build -f docker/Dockerfile.smartphone-low -t federatedscope:smartphone-low .

# 构建树莓派镜像
echo -e "${BLUE}🥧 构建树莓派镜像 (federatedscope:raspberry-pi)${NC}"
docker build -f docker/Dockerfile.raspberry-pi -t federatedscope:raspberry-pi .

# 构建IoT最小化镜像
echo -e "${BLUE}🔌 构建IoT最小化镜像 (federatedscope:iot-minimal)${NC}"
docker build -f docker/Dockerfile.iot-minimal -t federatedscope:iot-minimal .

echo -e "${GREEN}✅ 所有镜像构建完成!${NC}"
echo ""
echo -e "${YELLOW}📋 可用镜像:${NC}"
echo "  • federatedscope:base           - 基础镜像"
echo "  • federatedscope:edge-server    - 边缘服务器 (高性能，多GPU)"
echo "  • federatedscope:smartphone-high - 高端智能手机 (GPU支持)"
echo "  • federatedscope:smartphone-low  - 低端智能手机 (CPU版本)"
echo "  • federatedscope:raspberry-pi    - 树莓派 (ARM优化)"
echo "  • federatedscope:iot-minimal     - IoT设备 (最小化资源)"
echo ""
echo -e "${YELLOW}🚀 运行示例:${NC}"
echo "  python run_ray.py --config_path configs/docker_simulation.yaml"
echo ""
echo -e "${YELLOW}📊 查看镜像:${NC}"
echo "  docker images | grep federatedscope"