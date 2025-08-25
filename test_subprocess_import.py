#!/usr/bin/env python3
"""
测试子进程模块导入问题
"""

import os
import subprocess
import sys

def test_subprocess_import():
    """测试子进程中的federatedscope导入"""
    
    print("🧪 测试子进程模块导入")
    print(f"当前目录: {os.getcwd()}")
    print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', '未设置')}")
    
    # 测试1: 不设置PYTHONPATH
    print("\n1️⃣ 测试不设置PYTHONPATH:")
    try:
        result = subprocess.run([
            'python', '-c', 
            'import federatedscope; print("✅ federatedscope导入成功")'
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print("✅ 成功:", result.stdout.strip())
        else:
            print("❌ 失败:", result.stderr.strip())
    except Exception as e:
        print("💥 异常:", e)
    
    # 测试2: 设置PYTHONPATH
    print("\n2️⃣ 测试设置PYTHONPATH:")
    try:
        env = os.environ.copy()
        current_dir = os.getcwd()
        env['PYTHONPATH'] = current_dir
        
        result = subprocess.run([
            'python', '-c', 
            'import federatedscope; print("✅ federatedscope导入成功")'
        ], capture_output=True, text=True, env=env, timeout=10)
        
        if result.returncode == 0:
            print("✅ 成功:", result.stdout.strip())
        else:
            print("❌ 失败:", result.stderr.strip())
    except Exception as e:
        print("💥 异常:", e)
    
    # 测试3: 测试federatedscope main.py
    print("\n3️⃣ 测试federatedscope main.py:")
    try:
        env = os.environ.copy()
        current_dir = os.getcwd()
        env['PYTHONPATH'] = current_dir
        
        result = subprocess.run([
            'python', 'federatedscope/main.py', '--help'
        ], capture_output=True, text=True, env=env, timeout=10)
        
        if result.returncode == 0:
            print("✅ 成功: federatedscope main.py可以运行")
            print("输出前几行:", result.stdout.split('\n')[:3])
        else:
            print("❌ 失败:", result.stderr.strip()[:200])
    except Exception as e:
        print("💥 异常:", e)

if __name__ == "__main__":
    test_subprocess_import()