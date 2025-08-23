#!/usr/bin/env python3
"""
测试哈希修复后的一致性
"""

import hashlib
import pickle
import numpy as np

def test_hash_consistency_fix():
    """测试修复后的哈希一致性"""
    
    print("🔍 测试修复后的哈希一致性\n")
    
    # 测试数据
    test_cases = [
        ("非空数组", np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float32)),
        ("空数组", np.array([], dtype=np.float32)),
        ("单元素数组", np.array([42.0], dtype=np.float32)),
    ]
    
    for case_name, chunk_data in test_cases:
        print(f"🧪 测试: {case_name}")
        print(f"   数据: {chunk_data}")
        print(f"   形状: {chunk_data.shape}")
        
        # 1. 存储时的哈希计算 (chunk_manager.py - 不变)
        storage_bytes = pickle.dumps(chunk_data)
        storage_hash = hashlib.sha256(storage_bytes).hexdigest()
        
        # 2. 发送时的哈希计算 (修复后的 bittorrent_manager.py:_send_piece)
        send_bytes = pickle.dumps(chunk_data)
        send_hash = hashlib.sha256(send_bytes).hexdigest()
        
        # 3. 接收时的哈希计算 (修复后的 bittorrent_manager.py:handle_piece)
        receive_bytes = pickle.dumps(chunk_data)
        receive_hash = hashlib.sha256(receive_bytes).hexdigest()
        
        print(f"   存储哈希: {storage_hash[:16]}...")
        print(f"   发送哈希: {send_hash[:16]}...")
        print(f"   接收哈希: {receive_hash[:16]}...")
        
        # 验证一致性
        all_match = (storage_hash == send_hash == receive_hash)
        print(f"   ✅ 哈希一致: {all_match}")
        
        if not all_match:
            print(f"   ❌ 哈希不匹配!")
            print(f"      存储: {storage_hash}")
            print(f"      发送: {send_hash}")  
            print(f"      接收: {receive_hash}")
        
        print()

def test_serialization_consistency():
    """测试pickle序列化的一致性"""
    
    print("🔍 测试pickle序列化一致性\n")
    
    chunk_data = np.array([1.0, 2.0, 3.0], dtype=np.float32)
    
    # 多次序列化，确保结果一致
    serializations = []
    for i in range(5):
        serialized = pickle.dumps(chunk_data)
        hash_value = hashlib.sha256(serialized).hexdigest()
        serializations.append(hash_value)
        print(f"序列化{i+1}: {hash_value[:16]}...")
    
    all_same = len(set(serializations)) == 1
    print(f"\n✅ pickle序列化一致性: {all_same}")
    
    if not all_same:
        print("❌ 序列化结果不一致!")
        for i, h in enumerate(serializations):
            print(f"   {i+1}: {h}")

if __name__ == "__main__":
    test_hash_consistency_fix()
    test_serialization_consistency()