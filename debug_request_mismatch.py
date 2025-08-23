#!/usr/bin/env python3
"""
调试BitTorrent request消息不匹配问题
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '.'))

from federatedscope.core.chunk_manager import ChunkManager
import sqlite3

def debug_request_mismatch():
    """调试request不匹配问题"""
    print("🐛 调试BitTorrent request消息不匹配问题...")
    
    # 检查不同客户端的chunk可用性
    for client_id in [1, 2, 3]:
        print(f"\n📊 检查Client {client_id}的chunk情况:")
        
        chunk_manager = ChunkManager(client_id=client_id)
        
        # 检查第0轮的数据
        conn = sqlite3.connect(chunk_manager.db_path)
        cursor = conn.cursor()
        
        # 查询第0轮的chunk_id分布
        cursor.execute('SELECT chunk_id FROM chunk_metadata WHERE round_num = 0 ORDER BY chunk_id')
        round0_chunks = [row[0] for row in cursor.fetchall()]
        
        print(f"  第0轮chunks: {round0_chunks}")
        
        # 测试JOIN查询是否能找到数据
        cursor.execute('''
            SELECT cm.chunk_id, cm.chunk_hash, LENGTH(cd.data) as data_length 
            FROM chunk_metadata cm
            JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
            WHERE cm.round_num = 0
            ORDER BY cm.chunk_id
        ''')
        join_results = cursor.fetchall()
        
        print(f"  JOIN查询结果数量: {len(join_results)}")
        if join_results:
            print(f"  第一个JOIN结果: chunk_id={join_results[0][0]}, hash={join_results[0][1][:8]}..., data_len={join_results[0][2]}")
            print(f"  最后一个JOIN结果: chunk_id={join_results[-1][0]}, hash={join_results[-1][1][:8]}..., data_len={join_results[-1][2]}")
        
        # 测试get_chunk_data方法
        print(f"  测试get_chunk_data:")
        for test_chunk_id in [0, 1, 2, 5, 9]:
            chunk_data = chunk_manager.get_chunk_data(round_num=0, source_client_id=client_id, chunk_id=test_chunk_id)
            found = "✅" if chunk_data is not None else "❌"
            print(f"    Client {client_id}, Chunk {test_chunk_id}: {found}")
        
        conn.close()
        
        # 检查bitfield
        bitfield = chunk_manager.get_global_bitfield(round_num=0)
        available_chunks = []
        for key, has_chunk in bitfield.items():
            if has_chunk and len(key) == 3:
                round_num, source_id, chunk_id = key
                if round_num == 0 and source_id == client_id:
                    available_chunks.append(chunk_id)
        
        print(f"  Bitfield中可用chunks: {sorted(available_chunks)}")
        
        # 对比JOIN结果和bitfield结果
        join_chunk_ids = [r[0] for r in join_results]
        if sorted(join_chunk_ids) == sorted(available_chunks):
            print("  ✅ JOIN查询与bitfield一致")
        else:
            print("  ❌ JOIN查询与bitfield不一致")
            print(f"    JOIN但不在bitfield: {set(join_chunk_ids) - set(available_chunks)}")
            print(f"    Bitfield但不在JOIN: {set(available_chunks) - set(join_chunk_ids)}")

if __name__ == "__main__":
    debug_request_mismatch()