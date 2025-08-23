#!/usr/bin/env python3
"""
针对客户端1的chunk查询进行精确测试
基于日志中的实际查询参数进行复现
"""

import sqlite3
import pickle
import os

def test_client1_chunks():
    """测试客户端1数据库中的实际chunk查询"""
    
    db_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_1/client_1_chunks.db"
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return
    
    print(f"🔍 正在查询客户端1数据库: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. 查看数据库中的所有chunk_metadata
        print("\n📊 客户端1 chunk_metadata表内容:")
        cursor.execute("SELECT round_num, chunk_id, chunk_hash, flat_size FROM chunk_metadata ORDER BY round_num, chunk_id")
        metadata_results = cursor.fetchall()
        for row in metadata_results:
            print(f"  round={row[0]}, chunk_id={row[1]}, hash={row[2][:10]}..., size={row[3]}")
        
        if not metadata_results:
            print("  ❌ chunk_metadata表为空!")
        
        # 2. 测试日志中失败的具体查询
        failed_queries = [
            (0, 1, 2),  # 日志line 630-631
            (0, 1, 3),  # 日志line 709-710 
            (0, 1, 4),  # 日志line 693-694
            (0, 1, 8),  # 日志line 780-781
            (0, 1, 0),  # 日志line 819-820
            (0, 1, 1),  # 日志line 977-978
        ]
        
        print(f"\n🔍 测试日志中失败的查询:")
        for round_num, source_client_id, chunk_id in failed_queries:
            print(f"\n  测试查询: round={round_num}, source_client={source_client_id}, chunk_id={chunk_id}")
            
            # 使用完全相同的查询语句
            cursor.execute('''
                SELECT cd.data FROM chunk_metadata cm
                JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                WHERE cm.round_num = ? AND cm.chunk_id = ?
            ''', (round_num, chunk_id))
            
            result = cursor.fetchone()
            if result:
                try:
                    chunk_data = pickle.loads(result[0])
                    print(f"    ✅ 找到数据: {len(result[0])} bytes, 类型: {type(chunk_data)}")
                except Exception as e:
                    print(f"    ⚠️ 数据损坏: {e}")
            else:
                print(f"    ❌ 没有找到数据 - 这与日志中的错误一致!")
                
                # 详细诊断
                cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE round_num = ? AND chunk_id = ?", (round_num, chunk_id))
                metadata_count = cursor.fetchone()[0]
                print(f"       chunk_metadata匹配数: {metadata_count}")
                
                if metadata_count > 0:
                    cursor.execute("SELECT chunk_hash FROM chunk_metadata WHERE round_num = ? AND chunk_id = ?", (round_num, chunk_id))
                    chunk_hash = cursor.fetchone()[0]
                    print(f"       chunk_hash: {chunk_hash}")
                    
                    cursor.execute("SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
                    data_count = cursor.fetchone()[0]
                    print(f"       chunk_data匹配数: {data_count}")
                    
                    if data_count == 0:
                        print(f"       🔍 问题根源: chunk_metadata存在但chunk_data不存在!")
        
        # 3. 检查chunk_data表的完整性
        print(f"\n🔍 检查chunk_data表:")
        cursor.execute("SELECT COUNT(*) FROM chunk_data")
        total_data = cursor.fetchone()[0]
        print(f"  chunk_data总记录数: {total_data}")
        
        cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
        total_metadata = cursor.fetchone()[0]
        print(f"  chunk_metadata总记录数: {total_metadata}")
        
        # 4. 检查是否有孤儿记录
        cursor.execute('''
            SELECT cm.round_num, cm.chunk_id, cm.chunk_hash 
            FROM chunk_metadata cm 
            LEFT JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash 
            WHERE cd.chunk_hash IS NULL
        ''')
        orphaned_metadata = cursor.fetchall()
        
        if orphaned_metadata:
            print(f"\n⚠️ 发现孤儿metadata记录（没有对应的chunk_data）:")
            for row in orphaned_metadata:
                print(f"    round={row[0]}, chunk_id={row[1]}, hash={row[2][:10]}...")
        else:
            print(f"\n✅ 没有孤儿metadata记录")
        
    except Exception as e:
        print(f"❌ 查询过程中出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_client1_chunks()