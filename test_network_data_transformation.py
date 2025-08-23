#!/usr/bin/env python3
"""
测试网络传输过程中chunk_data的数据类型变化
验证numpy array → list → numpy array的转换过程对哈希值的影响
"""

import numpy as np
import hashlib
import pickle
import sys
import os

# 添加FederatedScope路径
sys.path.append(os.path.abspath('.'))

from federatedscope.core.message import Message

def calculate_hash_methods(data, label):
    """计算不同哈希方法的结果"""
    print(f"\n=== {label} ===")
    print(f"数据类型: {type(data)}")
    print(f"数据内容: {data}")
    if hasattr(data, 'dtype'):
        print(f"数据dtype: {data.dtype}")
    if hasattr(data, 'shape'):
        print(f"数据shape: {data.shape}")
    
    # 方法1: pickle序列化后计算哈希（存储时使用）
    try:
        pickled = pickle.dumps(data)
        hash1 = hashlib.sha256(pickled).hexdigest()
        print(f"方法1 (pickle.dumps): {hash1[:16]}... (长度:{len(pickled)}字节)")
    except Exception as e:
        print(f"方法1 失败: {e}")
        hash1 = None
    
    # 方法2: 直接哈希（发送时错误使用）
    try:
        hash2 = hashlib.sha256(data).hexdigest()
        print(f"方法2 (直接哈希): {hash2[:16]}...")
    except Exception as e:
        print(f"方法2 失败: {e}")
        hash2 = None
    
    # 方法3: 复杂转换后哈希（接收时使用）
    try:
        if hasattr(data, 'tobytes'):
            chunk_bytes = data.tobytes()
        elif isinstance(data, bytes):
            chunk_bytes = data
        elif isinstance(data, (list, tuple)) and all(isinstance(x, int) and 0 <= x <= 255 for x in data):
            chunk_bytes = bytes(data)
        else:
            chunk_bytes = str(data).encode('utf-8')
        
        hash3 = hashlib.sha256(chunk_bytes).hexdigest()
        print(f"方法3 (复杂转换): {hash3[:16]}... (长度:{len(chunk_bytes)}字节)")
    except Exception as e:
        print(f"方法3 失败: {e}")
        hash3 = None
    
    return hash1, hash2, hash3

def test_network_transformation():
    """测试网络传输中的数据变化"""
    print("🔍 测试网络传输过程中chunk_data的数据类型变化")
    
    # 1. 创建原始numpy数组（模拟chunk_data）
    original_data = np.array([1.0, 2.5, 3.7, 0.0, 5.2], dtype=np.float32)
    print(f"原始chunk_data: {original_data} (type: {type(original_data)})")
    
    # 2. 发送端 - 计算哈希
    h1_orig, h2_orig, h3_orig = calculate_hash_methods(original_data, "发送端 - 原始numpy数组")
    
    # 3. 模拟网络传输 - Message.transform_to_list()
    print(f"\n📡 模拟网络传输过程")
    
    # 创建一个piece消息
    msg = Message(msg_type='piece', sender=1, receiver=[2], content={
        'data': original_data,
        'checksum': h1_orig  # 假设使用方法1计算的checksum
    })
    
    # 应用transform_to_list（模拟网络序列化）
    print(f"传输前content['data']: {msg.content['data']} (type: {type(msg.content['data'])})")
    
    # 检查是否有tolist方法
    if hasattr(msg.content['data'], 'tolist'):
        print(f"数据具有tolist方法，将被转换为list")
        if msg.msg_type == 'model_para':
            # model_para消息使用param_serializer
            transformed_data = msg.param_serializer(msg.content['data'])
            print(f"model_para消息：使用param_serializer")
        else:
            # 其他消息使用tolist()
            transformed_data = msg.content['data'].tolist()
            print(f"非model_para消息：使用tolist()")
    else:
        transformed_data = msg.content['data']
    
    print(f"传输后数据: {transformed_data} (type: {type(transformed_data)})")
    
    # 4. 接收端 - 接收到的数据类型
    h1_recv, h2_recv, h3_recv = calculate_hash_methods(transformed_data, "接收端 - 转换后的数据")
    
    # 5. 尝试转换回numpy数组
    if isinstance(transformed_data, list):
        try:
            restored_array = np.array(transformed_data, dtype=np.float32)
            print(f"\n🔄 转换回numpy数组: {restored_array} (type: {type(restored_array)})")
            h1_rest, h2_rest, h3_rest = calculate_hash_methods(restored_array, "转换回numpy数组后")
        except Exception as e:
            print(f"❌ 无法转换回numpy数组: {e}")
            restored_array = None
    
    # 6. 比较哈希结果
    print(f"\n📊 哈希对比结果:")
    print(f"方法1 (pickle): 原始={h1_orig and h1_orig[:16]} | 接收={h1_recv and h1_recv[:16]} | 恢复={h1_rest and h1_rest[:16] if 'h1_rest' in locals() else 'N/A'}")
    print(f"方法2 (直接):   原始={h2_orig and h2_orig[:16]} | 接收={h2_recv and h2_recv[:16]} | 恢复={h2_rest and h2_rest[:16] if 'h2_rest' in locals() else 'N/A'}")  
    print(f"方法3 (复杂):   原始={h3_orig and h3_orig[:16]} | 接收={h3_recv and h3_recv[:16]} | 恢复={h3_rest and h3_rest[:16] if 'h3_rest' in locals() else 'N/A'}")
    
    # 7. 验证结果
    print(f"\n🎯 验证结果:")
    if 'h1_rest' in locals():
        if h1_orig == h1_rest:
            print(f"✅ 方法1 (pickle): 原始 == 恢复 ✓")
        else:
            print(f"❌ 方法1 (pickle): 原始 != 恢复 ✗")
        
        if h3_orig == h3_rest:
            print(f"✅ 方法3 (复杂): 原始 == 恢复 ✓")
        else:
            print(f"❌ 方法3 (复杂): 原始 != 恢复 ✗")
    
    # 8. 测试空数组情况
    print(f"\n🔍 测试空数组情况:")
    empty_data = np.array([], dtype=np.float32)
    h1_empty, h2_empty, h3_empty = calculate_hash_methods(empty_data, "空numpy数组")
    
    empty_list = empty_data.tolist()
    h1_empty_list, h2_empty_list, h3_empty_list = calculate_hash_methods(empty_list, "空list")
    
    empty_restored = np.array(empty_list, dtype=np.float32)
    h1_empty_restored, h2_empty_restored, h3_empty_restored = calculate_hash_methods(empty_restored, "恢复的空numpy数组")
    
    print(f"空数组哈希一致性 (方法1): {h1_empty == h1_empty_restored}")

if __name__ == "__main__":
    test_network_transformation()