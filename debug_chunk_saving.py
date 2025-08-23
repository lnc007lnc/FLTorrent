#!/usr/bin/env python3
"""
调试chunk保存问题
重现BitTorrent chunk接收和保存过程
"""

import sqlite3
import os
import hashlib
import pickle
import numpy as np
import base64

def simulate_chunk_reception():
    """模拟BitTorrent chunk接收过程"""
    print("🔧 模拟BitTorrent chunk接收过程")
    
    # 检查实际数据库中缺失的chunk
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_1_db = os.path.join(base_path, "client_1", "client_1_chunks.db")
    
    conn = sqlite3.connect(client_1_db)
    cursor = conn.cursor()
    
    # 获取一个在bt_chunks中但不在chunk_data中的非空chunk
    empty_hash = hashlib.sha256(b'').hexdigest()
    cursor.execute("""
        SELECT bc.round_num, bc.source_client_id, bc.chunk_id, bc.chunk_hash
        FROM bt_chunks bc
        LEFT JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
        WHERE bc.chunk_hash != ? AND cd.chunk_hash IS NULL
        LIMIT 1
    """, (empty_hash,))
    
    missing_chunk = cursor.fetchone()
    conn.close()
    
    if not missing_chunk:
        print("❌ 没有找到缺失的chunk进行测试")
        return
    
    round_num, source_client_id, chunk_id, chunk_hash = missing_chunk
    print(f"🎯 测试缺失的chunk: 轮次={round_num}, 源={source_client_id}, 块={chunk_id}")
    print(f"   哈希: {chunk_hash}")
    
    # 生成模拟的chunk数据
    test_data = np.random.rand(5).astype(np.float32)
    
    # 计算哈希，看是否匹配
    test_hash = hashlib.sha256(pickle.dumps(test_data)).hexdigest()
    print(f"   测试数据哈希: {test_hash}")
    print(f"   哈希匹配: {'✅' if test_hash == chunk_hash else '❌'}")
    
    # 如果不匹配，尝试不同的数据类型
    if test_hash != chunk_hash:
        print("   🔍 尝试不同的数据结构...")
        
        # 尝试空数据
        empty_data = np.array([])
        empty_test_hash = hashlib.sha256(pickle.dumps(empty_data)).hexdigest()
        print(f"   空numpy数组哈希: {empty_test_hash}")
        
        # 尝试字典结构
        dict_data = {'layer1': np.random.rand(3), 'layer2': np.random.rand(2)}
        dict_hash = hashlib.sha256(pickle.dumps(dict_data)).hexdigest()
        print(f"   字典数据哈希: {dict_hash}")
        
        # 尝试纯Python数据
        list_data = [1.0, 2.0, 3.0]
        list_hash = hashlib.sha256(pickle.dumps(list_data)).hexdigest()
        print(f"   列表数据哈希: {list_hash}")

def test_save_remote_chunk_edge_cases():
    """测试save_remote_chunk的边界情况"""
    print(f"\n🧪 测试save_remote_chunk边界情况")
    
    # 读取实际的chunk_manager代码，看是否有问题
    chunk_manager_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/federatedscope/core/chunk_manager.py"
    
    # 模拟各种可能的问题场景
    test_scenarios = [
        ("正常数据", np.random.rand(5)),
        ("空numpy数组", np.array([])),
        ("None值", None),
        ("空字典", {}),
        ("复杂嵌套", {'weights': np.random.rand(3), 'bias': np.array([1.0])})
    ]
    
    import tempfile
    
    for scenario_name, test_data in test_scenarios:
        print(f"\n📊 测试场景: {scenario_name}")
        print(f"   数据类型: {type(test_data)}")
        
        if test_data is not None:
            try:
                # 计算哈希
                if hasattr(test_data, '__len__') and len(test_data) == 0:
                    print("   数据长度: 0")
                else:
                    print(f"   数据内容: {str(test_data)[:50]}...")
                
                serialized = pickle.dumps(test_data)
                data_hash = hashlib.sha256(serialized).hexdigest()
                print(f"   序列化长度: {len(serialized)}")
                print(f"   哈希: {data_hash[:16]}...")
                
                # 测试保存到数据库
                with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
                    test_db = tmp_file.name
                
                try:
                    conn = sqlite3.connect(test_db)
                    cursor = conn.cursor()
                    
                    # 创建表
                    cursor.execute('''
                        CREATE TABLE bt_chunks (
                            round_num INTEGER, source_client_id INTEGER, 
                            chunk_id INTEGER, chunk_hash TEXT, 
                            holder_client_id INTEGER, is_verified INTEGER
                        )
                    ''')
                    cursor.execute('''
                        CREATE TABLE chunk_data (
                            chunk_hash TEXT UNIQUE, data BLOB
                        )
                    ''')
                    
                    # 执行INSERT OR IGNORE
                    cursor.execute('''
                        INSERT OR REPLACE INTO bt_chunks 
                        (round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, is_verified)
                        VALUES (?, ?, ?, ?, ?, 1)
                    ''', (0, 2, 1, data_hash, 1))
                    
                    cursor.execute('''
                        INSERT OR IGNORE INTO chunk_data (chunk_hash, data)
                        VALUES (?, ?)
                    ''', (data_hash, serialized))
                    
                    # 检查是否成功插入
                    cursor.execute("SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = ?", (data_hash,))
                    count = cursor.fetchone()[0]
                    
                    print(f"   数据库写入: {'✅成功' if count > 0 else '❌失败'}")
                    
                    if count > 0:
                        # 测试读取
                        cursor.execute("SELECT data FROM chunk_data WHERE chunk_hash = ?", (data_hash,))
                        retrieved_data = pickle.loads(cursor.fetchone()[0])
                        print(f"   读取验证: ✅成功")
                    
                    conn.close()
                    
                finally:
                    os.unlink(test_db)
                    
            except Exception as e:
                print(f"   ❌ 处理异常: {e}")
        else:
            print("   跳过None值测试")

def analyze_actual_chunk_data():
    """分析实际的chunk数据内容"""
    print(f"\n🔍 分析实际chunk数据内容")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    for client_id in [1, 2, 3]:
        db_path = os.path.join(base_path, f"client_{client_id}", f"client_{client_id}_chunks.db")
        print(f"\n📋 客户端{client_id}:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查chunk_data中实际存储的是什么
        cursor.execute("SELECT chunk_hash, LENGTH(data), data FROM chunk_data LIMIT 2")
        records = cursor.fetchall()
        
        for i, (chunk_hash, data_length, data_blob) in enumerate(records, 1):
            print(f"   记录{i}: 哈希={chunk_hash[:16]}..., 长度={data_length}")
            try:
                unpickled_data = pickle.loads(data_blob)
                print(f"     数据类型: {type(unpickled_data)}")
                if hasattr(unpickled_data, 'shape'):
                    print(f"     数据形状: {unpickled_data.shape}")
                elif hasattr(unpickled_data, '__len__'):
                    print(f"     数据长度: {len(unpickled_data)}")
                print(f"     数据样本: {str(unpickled_data)[:100]}...")
            except Exception as e:
                print(f"     ❌ 反序列化失败: {e}")
        
        conn.close()

def main():
    """主函数"""
    print("🔍 调试BitTorrent chunk保存问题")
    print("=" * 80)
    
    simulate_chunk_reception()
    test_save_remote_chunk_edge_cases() 
    analyze_actual_chunk_data()
    
    print(f"\n{'='*80}")
    print("🎯 调试结论:")
    print("1. save_remote_chunk函数的逻辑应该是正确的")
    print("2. 问题可能在于：")
    print("   - BitTorrent接收的chunk数据与实际发送的不匹配")
    print("   - 哈希计算在发送和接收时不一致")
    print("   - chunk数据在网络传输中被错误处理")
    print("3. 需要检查BitTorrent消息处理和数据序列化过程")

if __name__ == "__main__":
    main()