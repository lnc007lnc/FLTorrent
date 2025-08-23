#!/usr/bin/env python3
"""
测试所有可能的查询参数组合，找出为什么chunk 3:3查不到
"""

import sqlite3
import pickle
import os

def test_all_possible_queries():
    """测试所有可能导致查不到chunk的情况"""
    
    db_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_3/client_3_chunks.db"
    
    print(f"🔍 测试数据库: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. 查看所有可能的round_num值
        cursor.execute("SELECT DISTINCT round_num FROM chunk_metadata ORDER BY round_num")
        rounds = [row[0] for row in cursor.fetchall()]
        print(f"📊 数据库中的轮次: {rounds}")
        
        # 2. 查看所有chunk_id值
        cursor.execute("SELECT DISTINCT chunk_id FROM chunk_metadata ORDER BY chunk_id")
        chunk_ids = [row[0] for row in cursor.fetchall()]
        print(f"📊 数据库中的chunk_id: {chunk_ids}")
        
        # 3. 测试不同round_num下的chunk 3查询
        for round_num in [-1, 0, 1]:  # 测试可能的round值
            print(f"\n🔍 测试 round_num={round_num}, chunk_id=3:")
            
            # 模拟get_chunk_data的查询
            cursor.execute('''
                SELECT cd.data FROM chunk_metadata cm
                JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                WHERE cm.round_num = ? AND cm.chunk_id = ?
            ''', (round_num, 3))
            
            result = cursor.fetchone()
            if result:
                print(f"  ✅ 找到数据，大小: {len(result[0])} bytes")
            else:
                print(f"  ❌ 没有找到数据")
                
        # 4. 检查可能的空数据问题
        print(f"\n🔍 检查chunk数据内容:")
        cursor.execute('''
            SELECT cm.round_num, cm.chunk_id, cm.chunk_hash, LENGTH(cd.data) as data_size
            FROM chunk_metadata cm
            JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
            WHERE cm.chunk_id = 3
        ''')
        results = cursor.fetchall()
        for row in results:
            print(f"  round={row[0]}, chunk_id={row[1]}, hash={row[2][:10]}..., size={row[3]} bytes")
            
        # 5. 检查JOIN是否正常工作
        print(f"\n🔍 测试JOIN连接:")
        cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE chunk_id = 3")
        metadata_count = cursor.fetchone()[0]
        print(f"  chunk_metadata中chunk_id=3的记录数: {metadata_count}")
        
        if metadata_count > 0:
            cursor.execute("SELECT chunk_hash FROM chunk_metadata WHERE chunk_id = 3")
            chunk_hash = cursor.fetchone()[0]
            print(f"  chunk_hash: {chunk_hash}")
            
            cursor.execute("SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
            data_count = cursor.fetchone()[0]
            print(f"  chunk_data中对应hash的记录数: {data_count}")
            
        # 6. 模拟实际的错误情况 - 可能是不同的round_num
        print(f"\n🔍 模拟可能的错误查询:")
        error_cases = [
            (1, 3, 3),  # 可能server已经进入round 1
            (-1, 3, 3), # 可能传入了错误的round
            (0, 4, 3),  # 可能source_client_id不对
        ]
        
        for round_num, source_client_id, chunk_id in error_cases:
            print(f"  测试参数: round={round_num}, source_client={source_client_id}, chunk_id={chunk_id}")
            
            if source_client_id == 3:  # 模拟client_id = 3
                cursor.execute('''
                    SELECT cd.data FROM chunk_metadata cm
                    JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                    WHERE cm.round_num = ? AND cm.chunk_id = ?
                ''', (round_num, chunk_id))
            else:
                cursor.execute('''
                    SELECT cd.data FROM bt_chunks bc
                    JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
                    WHERE bc.round_num = ? AND bc.source_client_id = ? 
                    AND bc.chunk_id = ? AND bc.holder_client_id = ?
                ''', (round_num, source_client_id, chunk_id, 3))
                
            result = cursor.fetchone()
            if result:
                print(f"    ✅ 找到数据")
            else:
                print(f"    ❌ 没有找到数据 <- 可能的错误原因")
            
    except Exception as e:
        print(f"❌ 查询过程中出错: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_all_possible_queries()