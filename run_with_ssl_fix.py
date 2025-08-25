#!/usr/bin/env python3
"""
SSL修复包装器 - 启动FederatedScope时修复SSL问题
"""

import ssl
import sys
import subprocess

# 修复SSL证书验证问题
ssl._create_default_https_context = ssl._create_unverified_context

# 运行原始命令
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python run_with_ssl_fix.py <python_script> [args...]")
        sys.exit(1)
    
    # 运行传入的Python脚本
    cmd = [sys.executable] + sys.argv[1:]
    subprocess.run(cmd)