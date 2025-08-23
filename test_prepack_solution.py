#!/usr/bin/env python3
"""
测试预打包解决方案：在传输前将chunk_data序列化为bytes
避免网络传输过程中的numpy→list转换
"""

import numpy as np
import hashlib
import pickle
import base64
import sys
import os

# 添加FederatedScope路径
sys.path.append(os.path.abspath('.'))

from federatedscope.core.message import Message

def test_prepack_solution():
    """测试预打包解决方案"""
    print("🔧 测试预打包解决方案：避免网络传输中的数据变化")
    
    # 1. 原始chunk_data（numpy数组）
    original_data = np.array([1.0, 2.5, 3.7, 0.0, 5.2], dtype=np.float32)
    print(f"原始chunk_data: {original_data} (type: {type(original_data)})")
    
    # 2. 方案A：pickle + base64编码（推荐）
    print(f"\n=== 方案A：pickle + base64编码 ===")
    
    # 发送端预处理
    pickled_data = pickle.dumps(original_data)
    encoded_data = base64.b64encode(pickled_data).decode('utf-8')
    original_hash = hashlib.sha256(pickled_data).hexdigest()
    
    print(f"预处理后数据: {encoded_data[:50]}... (type: {type(encoded_data)})")
    print(f"预处理后哈希: {original_hash[:16]}...")
    
    # 模拟网络传输
    msg = Message(msg_type='piece', sender=1, receiver=[2], content={
        'data': encoded_data,  # 预序列化的字符串
        'checksum': original_hash
    })
    
    # 应用transform_to_list（字符串不会被转换）
    transmitted_data = msg.content['data']
    print(f"传输后数据: {transmitted_data[:50]}... (type: {type(transmitted_data)})")
    print(f"数据是否一致: {encoded_data == transmitted_data}")
    
    # 接收端反序列化
    try:
        decoded_data = base64.b64decode(transmitted_data.encode('utf-8'))
        recovered_data = pickle.loads(decoded_data)
        recovered_hash = hashlib.sha256(decoded_data).hexdigest()
        
        print(f"恢复后数据: {recovered_data} (type: {type(recovered_data)})")
        print(f"恢复后哈希: {recovered_hash[:16]}...")
        print(f"数据完整性: {np.array_equal(original_data, recovered_data)}")
        print(f"哈希一致性: {original_hash == recovered_hash}")
    except Exception as e:
        print(f"❌ 反序列化失败: {e}")
    
    # 3. 方案B：直接使用bytes（更简单）
    print(f"\n=== 方案B：直接使用pickle bytes ===")
    
    # 发送端预处理
    bytes_data = pickle.dumps(original_data)
    bytes_hash = hashlib.sha256(bytes_data).hexdigest()
    
    print(f"预处理后数据: {bytes_data[:30]}... (type: {type(bytes_data)}, 长度: {len(bytes_data)})")
    print(f"预处理后哈希: {bytes_hash[:16]}...")
    
    # 模拟网络传输（bytes会被保持原样）
    msg_bytes = Message(msg_type='piece', sender=1, receiver=[2], content={
        'data': bytes_data,  # 直接使用bytes
        'checksum': bytes_hash
    })
    
    # bytes类型不会被transform_to_list转换
    transmitted_bytes = msg_bytes.content['data']
    print(f"传输后数据: {transmitted_bytes[:30]}... (type: {type(transmitted_bytes)})")
    print(f"数据是否一致: {bytes_data == transmitted_bytes}")
    
    # 接收端反序列化
    try:
        recovered_bytes_data = pickle.loads(transmitted_bytes)
        recovered_bytes_hash = hashlib.sha256(transmitted_bytes).hexdigest()
        
        print(f"恢复后数据: {recovered_bytes_data} (type: {type(recovered_bytes_data)})")
        print(f"恢复后哈希: {recovered_bytes_hash[:16]}...")
        print(f"数据完整性: {np.array_equal(original_data, recovered_bytes_data)}")
        print(f"哈希一致性: {bytes_hash == recovered_bytes_hash}")
    except Exception as e:
        print(f"❌ 反序列化失败: {e}")
    
    # 4. 测试空数组
    print(f"\n=== 测试空数组情况 ===")
    empty_data = np.array([], dtype=np.float32)
    empty_bytes = pickle.dumps(empty_data)
    empty_hash = hashlib.sha256(empty_bytes).hexdigest()
    
    # 模拟传输
    transmitted_empty = empty_bytes
    recovered_empty = pickle.loads(transmitted_empty)
    recovered_empty_hash = hashlib.sha256(transmitted_empty).hexdigest()
    
    print(f"空数组数据完整性: {np.array_equal(empty_data, recovered_empty)}")
    print(f"空数组哈希一致性: {empty_hash == recovered_empty_hash}")
    print(f"空数组哈希: {empty_hash[:16]}...")

if __name__ == "__main__":
    test_prepack_solution()