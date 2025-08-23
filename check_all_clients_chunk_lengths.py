#!/usr/bin/env python3
"""
检查所有客户端数据库中chunk的长度分布
"""

import sqlite3
import pickle
import numpy as np
import os

def check_client_chunks(client_id):
    """检查指定客户端的chunk长度"""
    
    db_path = f"/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_{client_id}/client_{client_id}_chunks.db"
    
    if not os.path.exists(db_path):
        print(f"❌ 客户端{client_id}数据库不存在")
        return {}
    
    print(f"\n🔍 客户端{client_id}的chunk长度分布:")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    chunk_lengths = {}
    
    try:
        cursor.execute('''
            SELECT cm.chunk_id, cd.data FROM chunk_metadata cm
            JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
            WHERE cm.round_num = 0
            ORDER BY cm.chunk_id
        ''')
        
        for row in cursor.fetchall():
            chunk_id, raw_data = row
            try:
                chunk_data = pickle.loads(raw_data)
                if isinstance(chunk_data, np.ndarray):
                    length = len(chunk_data)
                    size = chunk_data.size
                    shape = chunk_data.shape
                    print(f"  chunk_{chunk_id}: len={length}, size={size}, shape={shape}")
                    chunk_lengths[chunk_id] = length
                else:
                    length = len(chunk_data) if hasattr(chunk_data, '__len__') else 0
                    print(f"  chunk_{chunk_id}: len={length}, type={type(chunk_data)}")
                    chunk_lengths[chunk_id] = length
            except Exception as e:
                print(f"  chunk_{chunk_id}: 解析失败 - {e}")
                chunk_lengths[chunk_id] = -1
                
    except Exception as e:
        print(f"❌ 查询客户端{client_id}出错: {e}")
    finally:
        conn.close()
    
    return chunk_lengths

def main():
    print("🔍 检查所有客户端的chunk长度分布\n")
    
    all_clients_data = {}
    
    # 检查客户端1, 2, 3
    for client_id in [1, 2, 3]:
        chunk_lengths = check_client_chunks(client_id)
        all_clients_data[client_id] = chunk_lengths
    
    # 汇总分析
    print(f"\n📊 汇总分析:")
    
    # 找出所有有长度>0的chunks
    print(f"\n✅ 长度>0的chunks:")
    for client_id, chunks in all_clients_data.items():
        non_empty_chunks = {chunk_id: length for chunk_id, length in chunks.items() if length > 0}
        if non_empty_chunks:
            print(f"  客户端{client_id}: {non_empty_chunks}")
        else:
            print(f"  客户端{client_id}: 没有长度>0的chunks")
    
    # 找出所有长度=0的chunks
    print(f"\n❌ 长度=0的chunks:")
    for client_id, chunks in all_clients_data.items():
        empty_chunks = [chunk_id for chunk_id, length in chunks.items() if length == 0]
        if empty_chunks:
            print(f"  客户端{client_id}: {empty_chunks}")
        else:
            print(f"  客户端{client_id}: 没有长度=0的chunks")
    
    # 统计每个chunk_id在不同客户端的长度分布
    print(f"\n📈 按chunk_id统计:")
    chunk_ids = set()
    for chunks in all_clients_data.values():
        chunk_ids.update(chunks.keys())
    
    for chunk_id in sorted(chunk_ids):
        lengths = []
        for client_id in [1, 2, 3]:
            if chunk_id in all_clients_data[client_id]:
                lengths.append(f"C{client_id}:{all_clients_data[client_id][chunk_id]}")
        print(f"  chunk_{chunk_id}: {', '.join(lengths)}")

if __name__ == "__main__":
    main()