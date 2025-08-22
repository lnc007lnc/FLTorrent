#!/usr/bin/env python3
"""
Chunk数据库完整性测试程序
测试chunk存储和检索功能，诊断"Chunk X:Y not found"问题
"""

import sys
import os
import sqlite3
import logging

# 添加FederatedScope路径
sys.path.append('/mnt/g/FLtorrent_combine/FederatedScope-master')

from federatedscope.core.chunk_manager import ChunkManager

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_chunk_database(client_id):
    """测试指定客户端的chunk数据库"""
    print(f"\n🔍 测试Client {client_id}的chunk数据库...")
    
    # 初始化ChunkManager
    db_path = f"/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_{client_id}/client_{client_id}_chunks.db"
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return
    
    print(f"📁 数据库路径: {db_path}")
    
    # 创建ChunkManager实例
    chunk_manager = ChunkManager(client_id=client_id)
    
    # 1. 直接查询数据库内容
    print("\n1️⃣ 数据库直接查询:")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 查看chunk_metadata表
    cursor.execute("SELECT round_num, chunk_id, chunk_hash FROM chunk_metadata ORDER BY round_num, chunk_id")
    metadata_rows = cursor.fetchall()
    print(f"📊 chunk_metadata表中有 {len(metadata_rows)} 条记录:")
    for row in metadata_rows:
        print(f"   Round {row[0]}, Chunk {row[1]}, Hash {row[2][:10]}...")
    
    # 查看chunk_data表
    cursor.execute("SELECT chunk_hash, LENGTH(data) FROM chunk_data")
    data_rows = cursor.fetchall()
    print(f"📊 chunk_data表中有 {len(data_rows)} 条记录:")
    for row in data_rows:
        print(f"   Hash {row[0][:10]}..., Size {row[1]} bytes")
    
    conn.close()
    
    # 2. 测试get_global_bitfield
    print("\n2️⃣ 测试get_global_bitfield:")
    for round_num in [0, 1, 2]:
        bitfield = chunk_manager.get_global_bitfield(round_num)
        owned_chunks = [(k, v) for k, v in bitfield.items() if v]
        print(f"   Round {round_num}: 拥有 {len(owned_chunks)} 个chunks")
        for chunk_key, has_chunk in owned_chunks:
            r, src, cid = chunk_key
            print(f"     - ({r}, {src}, {cid})")
    
    # 3. 测试get_chunk_data - 模拟log中的失败案例
    print("\n3️⃣ 测试get_chunk_data:")
    test_cases = [
        (0, client_id, 0),  # 应该存在
        (0, client_id, 3),  # log中显示不存在
        (0, client_id, 8),  # log中显示不存在
        (0, client_id, 9),  # 应该存在
    ]
    
    for round_num, source_id, chunk_id in test_cases:
        print(f"   查询 Round {round_num}, Source {source_id}, Chunk {chunk_id}:")
        chunk_data = chunk_manager.get_chunk_data(round_num, source_id, chunk_id)
        if chunk_data is not None:
            chunk_size = len(chunk_data) if hasattr(chunk_data, '__len__') else 0
            if chunk_size > 0:
                print(f"     ✅ 找到非空chunk数据 ({chunk_size} bytes)")
            else:
                print(f"     ✅ 找到空chunk数据 ({chunk_size} bytes)")
        else:
            print(f"     ❌ chunk数据不存在于数据库中")
            
            # 详细调试：检查数据库查询
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT cm.chunk_hash FROM chunk_metadata cm
                WHERE cm.round_num = ? AND cm.chunk_id = ?
            ''', (round_num, chunk_id))
            metadata_result = cursor.fetchone()
            
            if metadata_result:
                chunk_hash = metadata_result[0]
                print(f"     🔍 metadata表中找到hash: {chunk_hash[:10]}...")
                
                cursor.execute('SELECT LENGTH(data) FROM chunk_data WHERE chunk_hash = ?', (chunk_hash,))
                data_result = cursor.fetchone()
                
                if data_result:
                    print(f"     🔍 data表中找到数据: {data_result[0]} bytes")
                else:
                    print(f"     ❌ data表中未找到对应hash的数据")
            else:
                print(f"     ❌ metadata表中未找到记录")
            
            conn.close()

def main():
    """主测试函数"""
    print("🧪 Chunk数据库完整性测试")
    print("=" * 50)
    
    # 检查可用的数据库文件
    tmp_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    if not os.path.exists(tmp_dir):
        print(f"❌ tmp目录不存在: {tmp_dir}")
        return
    
    clients = []
    for item in os.listdir(tmp_dir):
        if item.startswith("client_") and os.path.isdir(os.path.join(tmp_dir, item)):
            client_id = int(item.split("_")[1])
            clients.append(client_id)
    
    clients.sort()
    print(f"🔍 发现客户端: {clients}")
    
    # 测试每个客户端
    for client_id in clients:
        try:
            test_chunk_database(client_id)
        except Exception as e:
            print(f"❌ Client {client_id} 测试失败: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n✅ 测试完成")

if __name__ == "__main__":
    main()