#!/usr/bin/env python3

import numpy as np
import pickle
import hashlib

# 模拟发送端的数据
original_chunk = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float32)
print(f"Original chunk: {original_chunk}")

# 发送端哈希计算
send_bytes = pickle.dumps(original_chunk)  
send_hash = hashlib.sha256(send_bytes).hexdigest()
print(f"Send hash: {send_hash}")

# 模拟网络传输 - numpy数组被转换为列表
transmitted_data = original_chunk.tolist()
print(f"Transmitted data (list): {transmitted_data}")

# 接收端处理 - 按照新的修复方法
received_chunk = transmitted_data
if isinstance(received_chunk, list):
    received_chunk = np.array(received_chunk, dtype=np.float32)

print(f"Received chunk after conversion: {received_chunk}")

# 接收端哈希计算
receive_bytes = pickle.dumps(received_chunk)
receive_hash = hashlib.sha256(receive_bytes).hexdigest()
print(f"Receive hash: {receive_hash}")

print(f"Hash match: {send_hash == receive_hash}")
print(f"Data match: {np.array_equal(original_chunk, received_chunk)}")