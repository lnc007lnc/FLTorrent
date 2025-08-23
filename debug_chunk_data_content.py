#!/usr/bin/env python3
"""
检查chunk_data的实际内容和长度
"""

import sqlite3
import pickle
import numpy as np

def check_chunk_data_content():
    """检查chunk数据的实际内容"""
    
    db_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_1/client_1_chunks.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 测试具体的chunk查询
        test_cases = [2, 3, 4, 8, 0, 1]  # 日志中报错的chunk_id
        
        for chunk_id in test_cases:
            print(f"\n🔍 检查chunk_id={chunk_id}:")
            
            cursor.execute('''
                SELECT cd.data FROM chunk_metadata cm
                JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                WHERE cm.round_num = ? AND cm.chunk_id = ?
            ''', (0, chunk_id))
            
            result = cursor.fetchone()
            if result:
                raw_data = result[0]
                print(f"  原始数据大小: {len(raw_data)} bytes")
                
                try:
                    chunk_data = pickle.loads(raw_data)
                    print(f"  反序列化后类型: {type(chunk_data)}")
                    
                    if isinstance(chunk_data, np.ndarray):
                        print(f"  numpy数组形状: {chunk_data.shape}")
                        print(f"  numpy数组大小: {chunk_data.size}")
                        print(f"  numpy数组dtype: {chunk_data.dtype}")
                        print(f"  numpy数组内容预览: {chunk_data}")
                        
                        # 关键检查: len(chunk_data)
                        print(f"  len(chunk_data): {len(chunk_data)}")
                        print(f"  len(chunk_data) > 0: {len(chunk_data) > 0}")
                        
                        # 模拟bittorrent_manager.py的检查
                        condition_result = chunk_data is not None and len(chunk_data) > 0
                        print(f"  BitTorrent条件检查结果: {condition_result}")
                        
                        if not condition_result:
                            print(f"  ⚠️ 这就是问题所在! chunk_data长度为0!")
                        else:
                            print(f"  ✅ 条件检查通过")
                            
                    else:
                        print(f"  len(chunk_data): {len(chunk_data) if hasattr(chunk_data, '__len__') else 'N/A'}")
                        
                except Exception as e:
                    print(f"  ❌ 反序列化失败: {e}")
            else:
                print(f"  ❌ 查询无结果")
        
        # 额外检查: 看看所有chunk的长度分布
        print(f"\n🔍 所有chunk的长度分布:")
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
                    print(f"  chunk_{chunk_id}: shape={chunk_data.shape}, size={chunk_data.size}, len={len(chunk_data)}")
            except:
                print(f"  chunk_{chunk_id}: 解析失败")
                
    except Exception as e:
        print(f"❌ 检查过程中出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_chunk_data_content()