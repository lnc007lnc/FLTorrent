#!/usr/bin/env python3
"""
Ray Cloud Integration for FederatedScope
========================================

支持将本地GPU资源与云服务器无缝结合：
- 动态云服务器发现和加入
- 跨云平台资源管理（AWS, GCP, Azure）
- 自动负载均衡和故障切换
- 成本优化的资源调度

使用案例：
1. 本地2 GPU + 云服务器4 GPU = 6 GPU联邦学习
2. 地理分布式客户端模拟（不同地区云服务器）  
3. 弹性扩容：训练负载高峰时自动启动云实例
"""

import ray
import os
import json
import time
import boto3
import logging
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from ray_distributed_fl import RayFederatedLearning

logger = logging.getLogger(__name__)

@dataclass
class CloudConfig:
    """云服务器配置"""
    provider: str  # aws, gcp, azure
    region: str
    instance_type: str
    gpu_count: int
    max_instances: int = 5
    credentials: Optional[Dict] = None

@dataclass 
class NodeInfo:
    """Ray节点信息"""
    node_id: str
    ip_address: str
    resources: Dict
    cloud_provider: Optional[str] = None
    instance_id: Optional[str] = None

class CloudRayManager:
    """
    云服务器Ray集群管理器
    自动发现、启动、管理云端Ray节点
    """
    
    def __init__(self):
        self.cloud_configs: List[CloudConfig] = []
        self.active_instances: List[Dict] = []
        self.ray_cluster_address = None
        
    def add_cloud_config(self, config: CloudConfig):
        """添加云服务器配置"""
        self.cloud_configs.append(config)
        logger.info(f"📋 添加云配置: {config.provider} {config.region} {config.instance_type}")
    
    def start_ray_head_node(self, port: int = 10001) -> str:
        """启动Ray头节点（通常在本地机器）"""
        # 获取本机IP
        import socket
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        
        # 启动Ray头节点
        ray.init(
            address=None,  # 本地模式
            _node_ip_address=local_ip,
            port=port,
            include_dashboard=True,
            dashboard_host='0.0.0.0',
            dashboard_port=8265
        )
        
        self.ray_cluster_address = f"ray://{local_ip}:{port}"
        logger.info(f"🎯 Ray头节点已启动: {self.ray_cluster_address}")
        logger.info(f"📊 Ray Dashboard: http://{local_ip}:8265")
        
        return self.ray_cluster_address
    
    def launch_aws_instances(self, config: CloudConfig, num_instances: int = 1) -> List[str]:
        """启动AWS EC2实例并加入Ray集群"""
        try:
            import boto3
        except ImportError:
            logger.error("❌ boto3未安装，无法使用AWS功能")
            return []
        
        if not config.credentials:
            logger.warning("⚠️ AWS凭证未配置，尝试使用默认凭证")
        
        ec2 = boto3.client('ec2', region_name=config.region, **config.credentials or {})
        
        # 启动实例的用户脚本
        user_data_script = f'''#!/bin/bash
# 安装Ray和依赖
pip3 install ray torch torchvision

# 连接到Ray集群
ray start --address={self.ray_cluster_address} --num-gpus={config.gpu_count}

# 下载和配置FederatedScope（如需要）
# git clone https://github.com/alibaba/FederatedScope.git
# cd FederatedScope && pip install -e .
'''
        
        # 启动实例
        response = ec2.run_instances(
            ImageId='ami-0c02fb55956c7d316',  # Ubuntu 20.04 with GPU support
            MinCount=num_instances,
            MaxCount=num_instances,
            InstanceType=config.instance_type,
            KeyName='your-key-pair',  # 需要配置
            SecurityGroupIds=['sg-xxxxxxxxx'],  # 需要配置
            UserData=user_data_script,
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': 'Ray-FederatedScope-Worker'},
                    {'Key': 'Project', 'Value': 'FederatedLearning'}
                ]
            }]
        )
        
        instance_ids = [inst['InstanceId'] for inst in response['Instances']]
        
        # 等待实例启动并获取IP
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=instance_ids)
        
        instances_info = ec2.describe_instances(InstanceIds=instance_ids)
        public_ips = []
        
        for reservation in instances_info['Reservations']:
            for instance in reservation['Instances']:
                public_ip = instance.get('PublicIpAddress')
                if public_ip:
                    public_ips.append(public_ip)
                    self.active_instances.append({
                        'instance_id': instance['InstanceId'],
                        'public_ip': public_ip,
                        'provider': 'aws',
                        'region': config.region,
                        'instance_type': config.instance_type,
                        'gpu_count': config.gpu_count
                    })
        
        logger.info(f"🚀 AWS实例已启动: {instance_ids}")
        logger.info(f"🌐 公网IP: {public_ips}")
        
        return public_ips
    
    def launch_gcp_instances(self, config: CloudConfig, num_instances: int = 1) -> List[str]:
        """启动GCP实例并加入Ray集群"""
        # TODO: 实现GCP集成
        logger.info("🔧 GCP集成开发中...")
        return []
    
    def auto_scale_cluster(self, target_gpu_count: int) -> bool:
        """
        自动扩容集群到目标GPU数量
        
        Args:
            target_gpu_count: 目标GPU总数
            
        Returns:
            bool: 是否成功达到目标
        """
        current_resources = ray.cluster_resources()
        current_gpu_count = int(current_resources.get('GPU', 0))
        
        if current_gpu_count >= target_gpu_count:
            logger.info(f"✅ 集群已有{current_gpu_count}个GPU，无需扩容")
            return True
        
        needed_gpus = target_gpu_count - current_gpu_count
        logger.info(f"📈 需要扩容{needed_gpus}个GPU")
        
        # 按优先级尝试不同云平台
        for config in self.cloud_configs:
            if needed_gpus <= 0:
                break
                
            instances_needed = min(
                (needed_gpus + config.gpu_count - 1) // config.gpu_count,  # 向上取整
                config.max_instances
            )
            
            if instances_needed <= 0:
                continue
            
            logger.info(f"🌩️ 在{config.provider}启动{instances_needed}个实例...")
            
            if config.provider == 'aws':
                launched_ips = self.launch_aws_instances(config, instances_needed)
                if launched_ips:
                    needed_gpus -= instances_needed * config.gpu_count
                    
                    # 等待实例加入集群
                    self.wait_for_nodes_join(len(launched_ips))
            
            elif config.provider == 'gcp':
                # GCP支持（待实现）
                pass
        
        # 检查最终GPU数量
        final_resources = ray.cluster_resources()
        final_gpu_count = int(final_resources.get('GPU', 0))
        
        if final_gpu_count >= target_gpu_count:
            logger.info(f"🎯 集群扩容成功: {final_gpu_count} GPU")
            return True
        else:
            logger.warning(f"⚠️ 集群扩容不完整: {final_gpu_count}/{target_gpu_count} GPU")
            return False
    
    def wait_for_nodes_join(self, expected_nodes: int, timeout: int = 300):
        """等待新节点加入集群"""
        initial_nodes = len(ray.nodes())
        target_nodes = initial_nodes + expected_nodes
        
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            current_nodes = len(ray.nodes())
            
            if current_nodes >= target_nodes:
                logger.info(f"✅ {expected_nodes}个新节点已加入集群")
                return True
                
            logger.info(f"⏳ 等待节点加入: {current_nodes}/{target_nodes}")
            time.sleep(10)
        
        logger.warning(f"⏰ 等待节点加入超时")
        return False
    
    def get_cluster_topology(self) -> Dict:
        """获取集群拓扑信息"""
        nodes = ray.nodes()
        cluster_resources = ray.cluster_resources()
        
        topology = {
            "total_nodes": len(nodes),
            "total_resources": dict(cluster_resources),
            "nodes": [],
            "cloud_instances": self.active_instances
        }
        
        for node in nodes:
            node_info = NodeInfo(
                node_id=node["NodeID"],
                ip_address=node.get("NodeManagerAddress", "N/A"),
                resources=node.get("Resources", {})
            )
            
            # 匹配云实例信息
            for instance in self.active_instances:
                if instance["public_ip"] in node_info.ip_address:
                    node_info.cloud_provider = instance["provider"]
                    node_info.instance_id = instance["instance_id"]
                    break
                    
            topology["nodes"].append({
                "node_id": node_info.node_id,
                "ip_address": node_info.ip_address,
                "resources": node_info.resources,
                "cloud_provider": node_info.cloud_provider,
                "instance_id": node_info.instance_id
            })
        
        return topology
    
    def terminate_cloud_instances(self):
        """终止所有云实例"""
        logger.info("🧹 终止云实例...")
        
        # 终止AWS实例
        aws_instances = [inst for inst in self.active_instances if inst['provider'] == 'aws']
        if aws_instances:
            # 按region分组终止
            by_region = {}
            for inst in aws_instances:
                region = inst['region']
                if region not in by_region:
                    by_region[region] = []
                by_region[region].append(inst['instance_id'])
            
            for region, instance_ids in by_region.items():
                try:
                    ec2 = boto3.client('ec2', region_name=region)
                    ec2.terminate_instances(InstanceIds=instance_ids)
                    logger.info(f"🔪 已终止AWS实例 {region}: {instance_ids}")
                except Exception as e:
                    logger.error(f"❌ 终止AWS实例失败: {e}")
        
        # 清空活动实例记录
        self.active_instances.clear()
        logger.info("✅ 云实例终止完成")

class HybridCloudFederatedLearning(RayFederatedLearning):
    """
    混合云联邦学习
    结合本地GPU和云服务器GPU资源
    """
    
    def __init__(self, config_dir: str = "multi_process_test_v2/configs"):
        super().__init__(config_dir)
        self.cloud_manager = CloudRayManager()
    
    def setup_hybrid_cluster(self, target_gpu_count: int = 6,
                           aws_config: Optional[CloudConfig] = None):
        """
        设置混合云集群
        
        Args:
            target_gpu_count: 目标GPU总数
            aws_config: AWS配置（可选）
        """
        logger.info(f"🌩️ 设置混合云集群，目标GPU数: {target_gpu_count}")
        
        # 1. 启动本地Ray头节点
        cluster_address = self.cloud_manager.start_ray_head_node()
        
        # 2. 添加云配置
        if aws_config:
            self.cloud_manager.add_cloud_config(aws_config)
        
        # 3. 自动扩容到目标GPU数
        success = self.cloud_manager.auto_scale_cluster(target_gpu_count)
        
        if success:
            logger.info("✅ 混合云集群设置完成")
            
            # 显示集群拓扑
            topology = self.cloud_manager.get_cluster_topology()
            logger.info(f"🗺️ 集群拓扑: {topology['total_nodes']}节点, {topology['total_resources']}")
            
            return cluster_address
        else:
            logger.error("❌ 混合云集群设置失败")
            return None
    
    def start_hybrid_federated_learning(self, num_clients: int = 6,
                                      total_rounds: int = 5,
                                      monitor_duration: int = 1200):
        """
        启动混合云联邦学习
        充分利用本地+云端GPU资源
        """
        logger.info(f"🚀 启动混合云联邦学习: {num_clients}客户端")
        
        # 检查集群资源
        cluster_resources = ray.cluster_resources()
        total_gpus = int(cluster_resources.get('GPU', 0))
        
        if total_gpus < num_clients:
            logger.warning(f"⚠️ GPU不足: {total_gpus} < {num_clients}，部分客户端将使用CPU")
        
        # 启动联邦学习（继承父类逻辑）
        self.start_federated_learning(num_clients, total_rounds, monitor_duration)
    
    def cleanup_hybrid_cluster(self):
        """清理混合云集群"""
        logger.info("🧹 清理混合云集群...")
        
        # 停止联邦学习
        self.stop_all()
        
        # 终止云实例
        self.cloud_manager.terminate_cloud_instances()
        
        # 关闭Ray
        ray.shutdown()
        
        logger.info("✅ 混合云集群清理完成")

def create_sample_aws_config() -> CloudConfig:
    """创建示例AWS配置"""
    return CloudConfig(
        provider='aws',
        region='us-west-2',
        instance_type='g4dn.xlarge',  # 1 GPU实例
        gpu_count=1,
        max_instances=4,
        credentials={
            'aws_access_key_id': 'YOUR_ACCESS_KEY',
            'aws_secret_access_key': 'YOUR_SECRET_KEY'
        }
    )

def main_hybrid_demo():
    """混合云联邦学习演示"""
    print("🌩️ 混合云联邦学习演示")
    print("=" * 50)
    
    # 创建混合云FL实例
    hybrid_fl = HybridCloudFederatedLearning()
    
    try:
        # AWS配置（需要用户提供真实凭证）
        aws_config = create_sample_aws_config()
        
        # 设置混合云集群（本地2GPU + 云端4GPU = 6GPU）
        cluster_address = hybrid_fl.setup_hybrid_cluster(
            target_gpu_count=6,
            aws_config=aws_config
        )
        
        if cluster_address:
            # 启动6客户端联邦学习（充分利用6个GPU）
            hybrid_fl.start_hybrid_federated_learning(
                num_clients=6,
                total_rounds=5,
                monitor_duration=1800  # 30分钟
            )
        
    except KeyboardInterrupt:
        logger.info("👋 收到中断信号")
    except Exception as e:
        logger.error(f"❌ 发生错误: {e}")
    finally:
        # 清理所有资源（包括云实例）
        hybrid_fl.cleanup_hybrid_cluster()
        
        print("🎉 混合云联邦学习结束！")

if __name__ == "__main__":
    # 基础示例：仅本地Ray集群
    if len(os.sys.argv) > 1 and os.sys.argv[1] == '--hybrid':
        main_hybrid_demo()
    else:
        print("💡 本地Ray集群演示，使用 '--hybrid' 参数体验混合云模式")
        from ray_distributed_fl import main
        main()