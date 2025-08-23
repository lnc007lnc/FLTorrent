#!/usr/bin/env python3
"""
调试chunk哈希计算不一致问题
"""

import hashlib
import pickle
import numpy as np

def test_hash_inconsistency():
    """测试不同哈希计算方式的结果"""
    
    print("🔍 测试chunk哈希计算不一致问题\n")
    
    # 模拟一个典型的chunk数据（numpy数组）
    chunk_data = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float32)
    print(f"原始chunk数据: {chunk_data}")
    print(f"数据类型: {type(chunk_data)}")
    print(f"数据形状: {chunk_data.shape}")
    
    print(f"\n📊 不同哈希计算方式的结果:")
    
    # 1. 存储时的哈希计算方式 (chunk_manager.py:233-234)
    storage_bytes = pickle.dumps(chunk_data)
    storage_hash = hashlib.sha256(storage_bytes).hexdigest()
    print(f"1. 存储时哈希 (pickle.dumps): {storage_hash[:16]}... (长度: {len(storage_bytes)} bytes)")
    
    # 2. 发送时的哈希计算方式 (bittorrent_manager.py:493)
    # 注意：这里有个问题，chunk_data是numpy数组，不能直接传给sha256
    try:
        send_hash = hashlib.sha256(chunk_data).hexdigest()
        print(f"2. 发送时哈希 (直接chunk_data): {send_hash[:16]}...")
    except Exception as e:
        print(f"2. 发送时哈希 (直接chunk_data): ❌ 错误 - {e}")
    
    # 3. 接收时的哈希计算方式 (bittorrent_manager.py:157-167)
    # 模拟接收端的转换逻辑
    if hasattr(chunk_data, 'tobytes'):
        receive_bytes = chunk_data.tobytes()
        receive_hash = hashlib.sha256(receive_bytes).hexdigest()
        print(f"3. 接收时哈希 (tobytes): {receive_hash[:16]}... (长度: {len(receive_bytes)} bytes)")
    
    print(f"\n🔍 哈希值对比:")
    print(f"存储哈希: {storage_hash}")
    print(f"接收哈希: {receive_hash if 'receive_hash' in locals() else 'N/A'}")
    print(f"哈希匹配: {storage_hash == receive_hash if 'receive_hash' in locals() else False}")
    
    print(f"\n📋 数据内容对比:")
    print(f"pickle.dumps长度: {len(storage_bytes)} bytes")
    print(f"tobytes长度: {len(receive_bytes)} bytes")
    print(f"pickle.dumps前16字节: {storage_bytes[:16]}")
    print(f"tobytes前16字节: {receive_bytes[:16] if len(receive_bytes) >= 16 else receive_bytes}")
    
    # 4. 测试空数组的情况
    print(f"\n🔍 测试空数组:")
    empty_chunk = np.array([], dtype=np.float32)
    
    empty_storage_bytes = pickle.dumps(empty_chunk)
    empty_storage_hash = hashlib.sha256(empty_storage_bytes).hexdigest()
    
    empty_receive_bytes = empty_chunk.tobytes()
    empty_receive_hash = hashlib.sha256(empty_receive_bytes).hexdigest()
    
    print(f"空数组存储哈希: {empty_storage_hash[:16]}... (长度: {len(empty_storage_bytes)})")
    print(f"空数组接收哈希: {empty_receive_hash[:16]}... (长度: {len(empty_receive_bytes)})")
    print(f"空数组哈希匹配: {empty_storage_hash == empty_receive_hash}")

if __name__ == "__main__":
    test_hash_inconsistency()