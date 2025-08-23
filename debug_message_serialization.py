#!/usr/bin/env python3

import numpy as np
import pickle
import hashlib
from federatedscope.core.message import Message

# 模拟chunk数据
chunk_data = np.array([1, 2, 3, 4, 5], dtype=np.float32)
print(f"Original chunk_data: {chunk_data}")
print(f"Original type: {type(chunk_data)}")

# 计算原始哈希
original_bytes = pickle.dumps(chunk_data)
original_hash = hashlib.sha256(original_bytes).hexdigest()
print(f"Original hash: {original_hash}")

# 创建piece消息
msg = Message(msg_type='piece', 
              sender=1, 
              receiver=[2], 
              content={
                  'chunk_id': 1,
                  'data': chunk_data,
                  'checksum': original_hash
              })

print(f"Message content before transform: {msg.content}")

# 模拟网络传输过程 - 调用transform_to_list
msg.content = msg.transform_to_list(msg.content)
print(f"Message content after transform: {msg.content}")

# 检查transform后的数据
transformed_data = msg.content['data']
print(f"Transformed data: {transformed_data}")
print(f"Transformed type: {type(transformed_data)}")

# 计算transform后的哈希
if hasattr(transformed_data, 'tolist'):
    # 如果仍是numpy数组
    transformed_bytes = pickle.dumps(transformed_data)
else:
    # 如果变成了列表，需要转回numpy数组再计算哈希
    # 这里模拟接收端的处理
    if isinstance(transformed_data, list):
        # 尝试重建numpy数组
        reconstructed = np.array(transformed_data, dtype=np.float32)
        transformed_bytes = pickle.dumps(reconstructed)
    else:
        transformed_bytes = pickle.dumps(transformed_data)

transformed_hash = hashlib.sha256(transformed_bytes).hexdigest()
print(f"Transformed hash: {transformed_hash}")

print(f"Hash match: {original_hash == transformed_hash}")