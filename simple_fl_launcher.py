#!/usr/bin/env python3
"""
简化的FederatedScope启动器
用于Ray Actor中启动federatedscope进程
"""

import os
import sys
import yaml
import time
import argparse

def setup_environment():
    """设置运行环境"""
    # 确保当前目录在Python路径中
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    
    # 设置环境变量
    os.environ['PYTHONPATH'] = current_dir

def load_config(config_path):
    """加载配置文件"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def start_federatedscope_server(config):
    """启动FederatedScope服务器"""
    print(f"🖥️ 启动FederatedScope服务器")
    print(f"   服务器地址: {config['distribute']['server_host']}:{config['distribute']['server_port']}")
    print(f"   GPU模式: {config.get('use_gpu', False)}")
    
    # 模拟服务器运行
    print("✅ 服务器启动成功，开始等待客户端连接...")
    
    # 这里应该调用真正的FederatedScope服务器启动代码
    # 为了测试，我们模拟运行60秒
    for i in range(60):
        print(f"⏰ 服务器运行中... {i+1}/60秒", flush=True)
        time.sleep(1)
    
    print("🏁 服务器运行完成")

def start_federatedscope_client(config):
    """启动FederatedScope客户端"""
    client_id = config['distribute'].get('data_idx', 0)
    print(f"📱 启动FederatedScope客户端 {client_id}")
    print(f"   连接服务器: {config['distribute']['server_host']}:{config['distribute']['server_port']}")
    print(f"   GPU模式: {config.get('use_gpu', False)}")
    
    # 模拟客户端连接和训练
    print("✅ 客户端启动成功，开始联邦学习...")
    
    # 这里应该调用真正的FederatedScope客户端启动代码
    # 为了测试，我们模拟运行30秒
    for i in range(30):
        print(f"⏰ 客户端{client_id}训练中... {i+1}/30秒", flush=True)
        time.sleep(1)
    
    print(f"🏁 客户端{client_id}训练完成")

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='简化的FederatedScope启动器')
    parser.add_argument('--cfg', required=True, help='配置文件路径')
    args = parser.parse_args()
    
    try:
        # 设置环境
        setup_environment()
        
        # 加载配置
        config = load_config(args.cfg)
        
        # 根据角色启动相应组件
        role = config['distribute'].get('role', 'server')
        
        if role == 'server':
            start_federatedscope_server(config)
        else:
            start_federatedscope_client(config)
            
    except Exception as e:
        print(f"❌ 启动失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()