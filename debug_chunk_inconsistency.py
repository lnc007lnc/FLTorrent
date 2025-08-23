#!/usr/bin/env python3
"""
调试chunk不一致性问题：为什么bitfield显示有chunk，但get_chunk_data找不到？
"""

import sqlite3
import os

def debug_client_chunks(client_id):
    """调试指定客户端的chunk一致性问题"""
    db_path = f"tmp/client_{client_id}/client_{client_id}_chunks.db"
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库不存在: {db_path}")
        return
    
    print(f"🔍 调试客户端 {client_id} 的chunk一致性")
    print(f"📁 数据库路径: {db_path}")
    print("=" * 60)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. 检查chunk_metadata表
    print("📊 1. chunk_metadata表内容:")
    try:
        cursor.execute("SELECT round_num, chunk_id, chunk_hash, chunk_size FROM chunk_metadata ORDER BY round_num, chunk_id")
        rows = cursor.fetchall()
        print(f"   总共 {len(rows)} 条记录")
        for row in rows:
            round_num, chunk_id, chunk_hash, chunk_size = row
            print(f"   Round {round_num}, Chunk {chunk_id}: hash={chunk_hash[:8]}..., size={chunk_size}")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    
    print()
    
    # 2. 检查chunk_data表
    print("📊 2. chunk_data表内容:")
    try:
        cursor.execute("SELECT chunk_hash, LENGTH(data) FROM chunk_data")
        rows = cursor.fetchall()
        print(f"   总共 {len(rows)} 条数据记录")
        for row in rows:
            chunk_hash, data_size = row
            print(f"   Hash {chunk_hash[:8]}...: data_size={data_size}")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    
    print()
    
    # 3. 检查JOIN查询（模拟get_chunk_data的逻辑）
    print("🔍 3. 模拟get_chunk_data查询:")
    
    # 测试round_num=1（最新轮次）的chunk
    test_round = 1
    test_chunk_id = 4  # 从错误日志中看到的chunk_id
    
    print(f"   测试查询: Round {test_round}, Source {client_id}, Chunk {test_chunk_id}")
    
    try:
        # 模拟get_chunk_data的本地chunk查询
        cursor.execute('''
            SELECT cm.chunk_id, cm.chunk_hash, cd.data IS NOT NULL as has_data
            FROM chunk_metadata cm
            LEFT JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
            WHERE cm.round_num = ? AND cm.chunk_id = ?
        ''', (test_round, test_chunk_id))
        
        result = cursor.fetchone()
        if result:
            chunk_id, chunk_hash, has_data = result
            print(f"   ✅ 元数据找到: chunk_id={chunk_id}, hash={chunk_hash[:8]}..., has_data={bool(has_data)}")
        else:
            print(f"   ❌ 元数据未找到")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    
    print()
    
    # 4. 检查bitfield生成逻辑
    print("🎯 4. 模拟bitfield生成:")
    try:
        cursor.execute("SELECT round_num, chunk_id FROM chunk_metadata WHERE round_num = ?", (test_round,))
        local_chunks = cursor.fetchall()
        print(f"   Round {test_round} 本地chunks: {len(local_chunks)} 个")
        for round_num, chunk_id in local_chunks:
            print(f"   - ({test_round}, {client_id}, {chunk_id}) -> 应该在bitfield中为True")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    
    print()
    
    # 5. 检查数据完整性
    print("🔍 5. 数据完整性检查:")
    try:
        # 检查是否有元数据但没有数据的情况
        cursor.execute('''
            SELECT cm.round_num, cm.chunk_id, cm.chunk_hash
            FROM chunk_metadata cm
            LEFT JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
            WHERE cd.chunk_hash IS NULL
        ''')
        orphaned = cursor.fetchall()
        if orphaned:
            print(f"   ⚠️ 发现 {len(orphaned)} 个孤立的元数据记录（有元数据但无数据）:")
            for round_num, chunk_id, chunk_hash in orphaned:
                print(f"     Round {round_num}, Chunk {chunk_id}: hash={chunk_hash[:8]}...")
        else:
            print(f"   ✅ 所有元数据都有对应的数据")
    except Exception as e:
        print(f"   ❌ 查询失败: {e}")
    
    conn.close()
    print("=" * 60)

def main():
    """主函数"""
    print("🐛 Chunk一致性调试工具")
    print("分析为什么bitfield显示有chunk，但get_chunk_data返回None")
    print()
    
    # 调试所有客户端
    for client_id in [1, 2, 3]:
        debug_client_chunks(client_id)
        print()

if __name__ == "__main__":
    main()