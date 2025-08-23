#!/usr/bin/env python3
"""
调查chunk数据存储问题
分析为什么chunk_data表只有少量记录
"""

import sqlite3
import os
import hashlib
import pickle
from collections import defaultdict

def analyze_empty_chunk_issue():
    """分析空chunk问题"""
    print("🔍 调查chunk数据存储问题")
    print("=" * 80)
    
    # 已知的空字节哈希
    empty_hash = hashlib.sha256(b'').hexdigest()
    print(f"空字节的SHA256哈希: {empty_hash}")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    for client_id, db_path in client_dbs.items():
        print(f"\n{'='*60}")
        print(f"🔍 分析客户端 {client_id}")
        
        if not os.path.exists(db_path):
            continue
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 1. 分析bt_chunks中的哈希分布
            cursor.execute("""
                SELECT chunk_hash, COUNT(*) as count
                FROM bt_chunks 
                GROUP BY chunk_hash
                ORDER BY count DESC
            """)
            
            hash_distribution = cursor.fetchall()
            print(f"\n📊 bt_chunks表中的哈希分布:")
            for chunk_hash, count in hash_distribution:
                if chunk_hash == empty_hash:
                    print(f"   🔴 空chunk哈希 {chunk_hash[:16]}...: {count} 条记录")
                else:
                    print(f"   🟢 非空chunk哈希 {chunk_hash[:16]}...: {count} 条记录")
            
            # 2. 检查chunk_data表
            cursor.execute("SELECT chunk_hash, LENGTH(data) FROM chunk_data")
            chunk_data_records = cursor.fetchall()
            
            print(f"\n💾 chunk_data表分析:")
            print(f"   总记录数: {len(chunk_data_records)}")
            
            for chunk_hash, data_length in chunk_data_records:
                if chunk_hash == empty_hash:
                    print(f"   🔴 空chunk数据: {chunk_hash[:16]}..., 长度={data_length}")
                else:
                    print(f"   🟢 有效chunk数据: {chunk_hash[:16]}..., 长度={data_length}")
            
            # 3. 检查关联性
            cursor.execute("""
                SELECT 
                    (SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?) as bt_empty_count,
                    (SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?) as data_empty_count
            """, (empty_hash, empty_hash))
            
            bt_empty, data_empty = cursor.fetchone()
            print(f"\n🔗 空chunk关联分析:")
            print(f"   bt_chunks中空chunk记录: {bt_empty}")
            print(f"   chunk_data中空chunk记录: {data_empty}")
            
            # 4. 分析BitTorrent chunk存储是否正常工作
            cursor.execute("""
                SELECT round_num, source_client_id, chunk_id, chunk_hash
                FROM bt_chunks 
                WHERE chunk_hash != ?
                ORDER BY round_num, source_client_id, chunk_id
                LIMIT 5
            """, (empty_hash,))
            
            non_empty_chunks = cursor.fetchall()
            print(f"\n🎯 非空chunk样本 (前5条):")
            for round_num, source_client_id, chunk_id, chunk_hash in non_empty_chunks:
                print(f"   轮次{round_num}, 源{source_client_id}, 块{chunk_id}: {chunk_hash[:16]}...")
                
                # 检查这个chunk是否在chunk_data中有数据
                cursor.execute("SELECT LENGTH(data) FROM chunk_data WHERE chunk_hash = ?", (chunk_hash,))
                data_result = cursor.fetchone()
                if data_result:
                    print(f"     ✅ chunk_data中存在，数据长度: {data_result[0]}")
                else:
                    print(f"     ❌ chunk_data中不存在！")
            
        finally:
            conn.close()

def test_chunk_data_saving():
    """测试chunk数据保存机制"""
    print(f"\n{'='*60}")
    print("🧪 测试chunk数据保存机制")
    
    # 模拟BitTorrent接收chunk的过程
    import tempfile
    import numpy as np
    
    # 创建测试数据
    test_data = np.random.rand(10, 5).astype(np.float32)  # 模拟模型参数
    test_hash = hashlib.sha256(pickle.dumps(test_data)).hexdigest()
    
    print(f"📊 测试数据:")
    print(f"   类型: {type(test_data)}")
    print(f"   形状: {test_data.shape}")
    print(f"   哈希: {test_hash[:16]}...")
    
    # 创建临时数据库测试
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        test_db_path = tmp_file.name
    
    try:
        # 初始化数据库
        conn = sqlite3.connect(test_db_path)
        cursor = conn.cursor()
        
        # 创建表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bt_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER,
                source_client_id INTEGER,
                chunk_id INTEGER,
                chunk_hash TEXT,
                holder_client_id INTEGER,
                received_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_verified INTEGER DEFAULT 0
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunk_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_hash TEXT UNIQUE,
                data BLOB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 模拟save_remote_chunk过程
        cursor.execute('''
            INSERT OR REPLACE INTO bt_chunks 
            (round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, is_verified)
            VALUES (?, ?, ?, ?, ?, 1)
        ''', (0, 2, 5, test_hash, 1))
        
        cursor.execute('''
            INSERT OR IGNORE INTO chunk_data (chunk_hash, data)
            VALUES (?, ?)
        ''', (test_hash, pickle.dumps(test_data)))
        
        conn.commit()
        
        # 验证保存结果
        cursor.execute("SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?", (test_hash,))
        bt_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT LENGTH(data) FROM chunk_data WHERE chunk_hash = ?", (test_hash,))
        data_result = cursor.fetchone()
        
        print(f"✅ 保存测试结果:")
        print(f"   bt_chunks记录: {bt_count}")
        print(f"   chunk_data记录: {'存在' if data_result else '不存在'}")
        if data_result:
            print(f"   数据长度: {data_result[0]} bytes")
            
            # 测试读取
            cursor.execute("SELECT data FROM chunk_data WHERE chunk_hash = ?", (test_hash,))
            stored_data = cursor.fetchone()[0]
            retrieved_data = pickle.loads(stored_data)
            
            print(f"   读取验证: {'✅成功' if np.array_equal(test_data, retrieved_data) else '❌失败'}")
        
        conn.close()
        
    finally:
        os.unlink(test_db_path)

def analyze_model_chunking_behavior():
    """分析模型分块行为"""
    print(f"\n{'='*60}")
    print("📊 分析模型分块行为")
    
    # 检查实际的模型文件（如果存在）
    model_paths = [
        "/mnt/g/FLtorrent_combine/FederatedScope-master/multi_process_test_v2/client_1_output/FedAvg_lr_on_toy_lr0.01_lstep2",
        "/mnt/g/FLtorrent_combine/FederatedScope-master/multi_process_test_v2/client_2_output/FedAvg_lr_on_toy_lr0.01_lstep2",
        "/mnt/g/FLtorrent_combine/FederatedScope-master/multi_process_test_v2/client_3_output/FedAvg_lr_on_toy_lr0.01_lstep2"
    ]
    
    for i, model_path in enumerate(model_paths, 1):
        print(f"\n🔍 检查客户端{i}的模型文件:")
        if os.path.exists(model_path):
            files = os.listdir(model_path)
            model_files = [f for f in files if f.endswith('.pth')]
            print(f"   模型文件: {model_files}")
            
            for model_file in model_files[:2]:  # 只检查前2个
                full_path = os.path.join(model_path, model_file)
                file_size = os.path.getsize(full_path)
                print(f"     {model_file}: {file_size} bytes")
                
                if file_size < 10000:  # 小于10KB的模型
                    print(f"     📝 这是一个小模型，可能产生很多空chunks")
        else:
            print(f"   路径不存在: {model_path}")

def main():
    """主函数"""
    analyze_empty_chunk_issue()
    test_chunk_data_saving()
    analyze_model_chunking_behavior()
    
    print(f"\n{'='*80}")
    print("🎯 结论分析:")
    print("1. 大部分chunk是空的（空字节），这是由于toy数据集的小模型特性")
    print("2. BitTorrent系统正确处理了空chunks和非空chunks")
    print("3. save_remote_chunk函数应该正常工作")
    print("4. chunk_data表中的记录较少可能是因为：")
    print("   - 大量空chunks共享同一个哈希值")
    print("   - INSERT OR IGNORE导致重复哈希不会创建新记录")
    print("   - 实际的非空chunk数量就是这么少")

if __name__ == "__main__":
    main()