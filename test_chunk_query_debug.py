#!/usr/bin/env python3
"""
使用相同代码逻辑查询客户端3数据库中的chunk 3:3
模拟bittorrent_manager.py中get_chunk_data的查询逻辑
"""

import sqlite3
import pickle
import os

def test_chunk_query():
    """模拟ChunkManager.get_chunk_data的查询逻辑"""
    
    # 客户端3的数据库路径
    db_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_3/client_3_chunks.db"
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return
    
    print(f"🔍 正在查询数据库: {db_path}")
    
    # 查询参数 (模拟WARNING中的参数)
    round_num = 0
    source_client_id = 3  # Client 3 requesting chunk from itself
    chunk_id = 3
    client_id = 3  # 模拟self.client_id
    
    print(f"查询参数: round_num={round_num}, source_client_id={source_client_id}, chunk_id={chunk_id}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. 首先查看数据库中有哪些表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📋 数据库中的表: {[table[0] for table in tables]}")
        
        # 2. 查看chunk_metadata表的内容
        print("\n📊 chunk_metadata表内容:")
        cursor.execute("SELECT round_num, chunk_id, chunk_hash, flat_size FROM chunk_metadata ORDER BY round_num, chunk_id")
        metadata_results = cursor.fetchall()
        for row in metadata_results:
            print(f"  round={row[0]}, chunk_id={row[1]}, hash={row[2][:10]}..., size={row[3]}")
        
        # 3. 使用相同的查询逻辑 (source_client_id == client_id的情况)
        print(f"\n🔍 执行相同的查询逻辑 (source_client_id == client_id):")
        if source_client_id == client_id:
            print("查询本地chunks (chunk_metadata表)")
            cursor.execute('''
                SELECT cd.data FROM chunk_metadata cm
                JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                WHERE cm.round_num = ? AND cm.chunk_id = ?
            ''', (round_num, chunk_id))
        else:
            print("查询BitTorrent chunks (bt_chunks表)")
            cursor.execute('''
                SELECT cd.data FROM bt_chunks bc
                JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
                WHERE bc.round_num = ? AND bc.source_client_id = ? 
                AND bc.chunk_id = ? AND bc.holder_client_id = ?
            ''', (round_num, source_client_id, chunk_id, client_id))
        
        result = cursor.fetchone()
        print(f"📋 查询结果: {result is not None}")
        
        if result:
            try:
                chunk_data = pickle.loads(result[0])
                print(f"✅ 成功解析chunk数据，大小: {len(result[0])} bytes")
                print(f"✅ 数据类型: {type(chunk_data)}")
            except Exception as e:
                print(f"❌ 解析chunk数据失败: {e}")
        else:
            print("❌ 未找到匹配的chunk数据")
            
        # 4. 检查是否存在chunk_id=3的记录
        print(f"\n🔎 专门检查round_num={round_num}, chunk_id={chunk_id}的记录:")
        cursor.execute("SELECT * FROM chunk_metadata WHERE round_num = ? AND chunk_id = ?", (round_num, chunk_id))
        specific_result = cursor.fetchone()
        if specific_result:
            print(f"✅ 找到对应记录: {specific_result}")
        else:
            print("❌ 没有找到对应的chunk_metadata记录")
            
        # 5. 检查chunk_data表是否有对应的数据
        if metadata_results:
            print(f"\n🔎 检查chunk_data表:")
            cursor.execute("SELECT COUNT(*) FROM chunk_data")
            data_count = cursor.fetchone()[0]
            print(f"chunk_data表中总共有 {data_count} 条记录")
            
    except Exception as e:
        print(f"❌ 查询过程中出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_chunk_query()