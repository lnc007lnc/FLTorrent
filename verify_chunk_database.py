#!/usr/bin/env python3
"""
验证本地tmp数据库中chunk数据的完整性和可读性
检查每个轮次是否存储了所有节点的chunk信息
"""

import sqlite3
import os
import json
from collections import defaultdict, Counter

def connect_to_database(db_path):
    """连接到SQLite数据库"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"❌ 无法连接到数据库 {db_path}: {e}")
        return None

def get_database_schema(conn):
    """获取数据库表结构"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        schema_info = {}
        for table in tables:
            table_name = table[0]
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            schema_info[table_name] = columns
            
        return schema_info
    except Exception as e:
        print(f"❌ 获取数据库结构失败: {e}")
        return {}

def analyze_chunk_data(conn, client_id):
    """分析chunk数据的完整性"""
    try:
        cursor = conn.cursor()
        
        # 获取所有chunk记录
        cursor.execute("""
            SELECT round_num, client_id, chunk_id, action, chunk_hash, 
                   chunk_size, timestamp, chunk_data 
            FROM chunks 
            ORDER BY round_num, client_id, chunk_id
        """)
        
        records = cursor.fetchall()
        print(f"\n📊 客户端 {client_id} 数据库分析:")
        print(f"   总记录数: {len(records)}")
        
        if not records:
            print("   ⚠️ 数据库为空，没有chunk记录")
            return {}
        
        # 按轮次组织数据
        round_data = defaultdict(lambda: defaultdict(list))
        client_distribution = defaultdict(Counter)
        action_distribution = defaultdict(Counter)
        
        for record in records:
            round_num, src_client_id, chunk_id, action, chunk_hash, chunk_size, timestamp, chunk_data = record
            
            round_data[round_num][src_client_id].append({
                'chunk_id': chunk_id,
                'action': action,
                'chunk_hash': chunk_hash,
                'chunk_size': chunk_size,
                'timestamp': timestamp,
                'has_data': chunk_data is not None and len(chunk_data) > 0
            })
            
            client_distribution[round_num][src_client_id] += 1
            action_distribution[round_num][action] += 1
        
        # 输出轮次分析
        print(f"\n🔍 轮次分析:")
        for round_num in sorted(round_data.keys()):
            round_chunks = round_data[round_num]
            total_chunks = sum(len(chunks) for chunks in round_chunks.values())
            unique_clients = len(round_chunks.keys())
            
            print(f"   轮次 {round_num}:")
            print(f"     - 总chunk数: {total_chunks}")
            print(f"     - 涉及客户端数: {unique_clients}")
            print(f"     - 客户端分布: {dict(client_distribution[round_num])}")
            print(f"     - 动作分布: {dict(action_distribution[round_num])}")
            
            # 验证是否有其他客户端的chunk
            other_clients = [cid for cid in round_chunks.keys() if cid != client_id]
            if other_clients:
                print(f"     ✅ 包含其他客户端数据: {other_clients}")
            else:
                print(f"     ⚠️ 只有本客户端({client_id})的数据")
        
        return round_data
        
    except Exception as e:
        print(f"❌ 分析chunk数据失败: {e}")
        return {}

def verify_data_integrity(conn):
    """验证数据完整性"""
    try:
        cursor = conn.cursor()
        
        # 检查数据一致性
        print(f"\n🔧 数据完整性检查:")
        
        # 1. 检查NULL值
        cursor.execute("SELECT COUNT(*) FROM chunks WHERE chunk_hash IS NULL")
        null_hashes = cursor.fetchone()[0]
        print(f"   空chunk_hash数量: {null_hashes}")
        
        # 2. 检查chunk大小
        cursor.execute("SELECT MIN(chunk_size), MAX(chunk_size), AVG(chunk_size) FROM chunks WHERE chunk_size > 0")
        size_stats = cursor.fetchone()
        if size_stats[0] is not None:
            print(f"   Chunk大小统计: 最小={size_stats[0]}, 最大={size_stats[1]}, 平均={size_stats[2]:.2f}")
        
        # 3. 检查timestamp范围
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM chunks")
        time_range = cursor.fetchone()
        print(f"   时间戳范围: {time_range[0]} - {time_range[1]}")
        
        # 4. 检查重复记录
        cursor.execute("""
            SELECT round_num, client_id, chunk_id, COUNT(*) as cnt
            FROM chunks 
            GROUP BY round_num, client_id, chunk_id 
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()
        print(f"   重复记录数量: {len(duplicates)}")
        if duplicates:
            print(f"     重复记录: {duplicates[:5]}...")  # 只显示前5个
        
        return True
        
    except Exception as e:
        print(f"❌ 数据完整性检查失败: {e}")
        return False

def test_data_readability(conn):
    """测试数据可读性"""
    try:
        cursor = conn.cursor()
        
        print(f"\n📖 数据可读性测试:")
        
        # 读取一些样本数据
        cursor.execute("""
            SELECT round_num, client_id, chunk_id, action, chunk_hash, 
                   chunk_size, LENGTH(chunk_data) as data_length
            FROM chunks 
            LIMIT 5
        """)
        
        samples = cursor.fetchall()
        print(f"   样本数据 (前5条记录):")
        for i, sample in enumerate(samples, 1):
            round_num, client_id, chunk_id, action, chunk_hash, chunk_size, data_length = sample
            print(f"     {i}. 轮次={round_num}, 客户端={client_id}, 块ID={chunk_id}")
            print(f"        动作={action}, 哈希={chunk_hash[:16] if chunk_hash else 'None'}...")
            print(f"        大小={chunk_size}, 数据长度={data_length}")
        
        return True
        
    except Exception as e:
        print(f"❌ 数据可读性测试失败: {e}")
        return False

def cross_client_analysis(all_data):
    """跨客户端数据分析"""
    print(f"\n🌐 跨客户端数据分析:")
    
    # 统计每个轮次在各客户端中的数据分布
    all_rounds = set()
    for client_data in all_data.values():
        all_rounds.update(client_data.keys())
    
    print(f"   发现的轮次: {sorted(all_rounds)}")
    
    for round_num in sorted(all_rounds):
        print(f"\n   轮次 {round_num} 跨客户端分析:")
        
        round_summary = {}
        for client_id, client_data in all_data.items():
            if round_num in client_data:
                round_chunks = client_data[round_num]
                total_chunks = sum(len(chunks) for chunks in round_chunks.values())
                unique_sources = list(round_chunks.keys())
                round_summary[f"客户端{client_id}"] = {
                    'total_chunks': total_chunks,
                    'source_clients': unique_sources
                }
        
        # 检查数据一致性
        all_source_clients = set()
        for client_info in round_summary.values():
            all_source_clients.update(client_info['source_clients'])
        
        print(f"     所有客户端都应该有来自这些源的数据: {sorted(all_source_clients)}")
        
        for client, info in round_summary.items():
            missing_sources = all_source_clients - set(info['source_clients'])
            if missing_sources:
                print(f"     ⚠️ {client} 缺少来自客户端 {sorted(missing_sources)} 的数据")
            else:
                print(f"     ✅ {client} 拥有所有源客户端的数据")

def main():
    """主函数"""
    print("🔍 开始验证chunk数据库完整性和可读性")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    all_client_data = {}
    
    # 分析每个客户端的数据库
    for client_id, db_path in client_dbs.items():
        print(f"\n{'='*60}")
        print(f"🔍 分析客户端 {client_id} 数据库: {db_path}")
        
        if not os.path.exists(db_path):
            print(f"❌ 数据库文件不存在: {db_path}")
            continue
        
        conn = connect_to_database(db_path)
        if not conn:
            continue
        
        try:
            # 获取数据库结构
            schema = get_database_schema(conn)
            print(f"📋 数据库表结构: {list(schema.keys())}")
            for table, columns in schema.items():
                print(f"   表 {table}: {[col[1] for col in columns]}")
            
            # 分析chunk数据
            client_data = analyze_chunk_data(conn, client_id)
            all_client_data[client_id] = client_data
            
            # 验证数据完整性
            verify_data_integrity(conn)
            
            # 测试数据可读性
            test_data_readability(conn)
            
        finally:
            conn.close()
    
    # 跨客户端分析
    if all_client_data:
        cross_client_analysis(all_client_data)
    
    print(f"\n{'='*60}")
    print("✅ chunk数据库验证完成!")

if __name__ == "__main__":
    main()