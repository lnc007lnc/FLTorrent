#!/usr/bin/env python3
"""
验证本地tmp数据库中chunk数据的完整性和可读性 (更新版本)
基于实际的数据库结构：chunk_metadata, chunk_data, bt_chunks, bt_exchange_status, bt_sessions
"""

import sqlite3
import os
import json
from collections import defaultdict, Counter
from datetime import datetime

def connect_to_database(db_path):
    """连接到SQLite数据库"""
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"❌ 无法连接到数据库 {db_path}: {e}")
        return None

def analyze_chunk_metadata(conn, client_id):
    """分析chunk_metadata表"""
    try:
        cursor = conn.cursor()
        
        # 获取chunk元数据
        cursor.execute("""
            SELECT round_num, chunk_id, chunk_hash, parts_info, flat_size, created_at
            FROM chunk_metadata 
            ORDER BY round_num, chunk_id
        """)
        
        records = cursor.fetchall()
        print(f"\n📊 客户端 {client_id} chunk_metadata 分析:")
        print(f"   总记录数: {len(records)}")
        
        if not records:
            print("   ⚠️ chunk_metadata表为空")
            return {}
        
        # 按轮次统计
        round_stats = defaultdict(lambda: {'chunks': [], 'total_size': 0})
        
        for record in records:
            round_num, chunk_id, chunk_hash, parts_info, flat_size, created_at = record
            
            round_stats[round_num]['chunks'].append({
                'chunk_id': chunk_id,
                'chunk_hash': chunk_hash,
                'flat_size': flat_size,
                'created_at': created_at
            })
            round_stats[round_num]['total_size'] += flat_size if flat_size else 0
        
        for round_num in sorted(round_stats.keys()):
            stats = round_stats[round_num]
            print(f"   轮次 {round_num}: {len(stats['chunks'])} chunks, 总大小: {stats['total_size']} bytes")
        
        return round_stats
        
    except Exception as e:
        print(f"❌ 分析chunk_metadata失败: {e}")
        return {}

def analyze_chunk_data(conn, client_id):
    """分析chunk_data表"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT chunk_hash, LENGTH(data) as data_length, created_at
            FROM chunk_data 
            ORDER BY created_at
        """)
        
        records = cursor.fetchall()
        print(f"\n💾 客户端 {client_id} chunk_data 分析:")
        print(f"   存储的chunk数据条数: {len(records)}")
        
        if records:
            total_size = sum(record[1] for record in records)
            print(f"   总存储数据大小: {total_size} bytes")
            print(f"   平均chunk大小: {total_size/len(records):.2f} bytes")
        
        return records
        
    except Exception as e:
        print(f"❌ 分析chunk_data失败: {e}")
        return []

def analyze_bt_chunks(conn, client_id):
    """分析bt_chunks表 - 这是关键的BitTorrent交换记录"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT round_num, source_client_id, chunk_id, chunk_hash, 
                   holder_client_id, received_time, is_verified
            FROM bt_chunks 
            ORDER BY round_num, source_client_id, chunk_id
        """)
        
        records = cursor.fetchall()
        print(f"\n🔄 客户端 {client_id} bt_chunks 分析 (BitTorrent交换记录):")
        print(f"   交换记录总数: {len(records)}")
        
        if not records:
            print("   ⚠️ 没有BitTorrent交换记录")
            return {}
        
        # 按轮次和源客户端统计
        round_data = defaultdict(lambda: defaultdict(list))
        source_distribution = defaultdict(Counter)
        
        for record in records:
            round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, received_time, is_verified = record
            
            round_data[round_num][source_client_id].append({
                'chunk_id': chunk_id,
                'chunk_hash': chunk_hash,
                'holder_client_id': holder_client_id,
                'received_time': received_time,
                'is_verified': is_verified
            })
            
            source_distribution[round_num][source_client_id] += 1
        
        print(f"   涉及轮次: {sorted(round_data.keys())}")
        
        for round_num in sorted(round_data.keys()):
            round_chunks = round_data[round_num]
            total_chunks = sum(len(chunks) for chunks in round_chunks.values())
            source_clients = sorted(round_chunks.keys())
            
            print(f"\n   轮次 {round_num}:")
            print(f"     - 交换的chunk总数: {total_chunks}")
            print(f"     - 来源客户端: {source_clients}")
            print(f"     - 各源分布: {dict(source_distribution[round_num])}")
            
            # 检查是否有来自其他客户端的数据
            other_sources = [src for src in source_clients if src != client_id]
            if other_sources:
                print(f"     ✅ 成功接收来自其他客户端的数据: {other_sources}")
            else:
                print(f"     ⚠️ 只有本客户端({client_id})自己的数据，可能没有进行BitTorrent交换")
        
        return round_data
        
    except Exception as e:
        print(f"❌ 分析bt_chunks失败: {e}")
        return {}

def analyze_bt_exchange_status(conn, client_id):
    """分析bt_exchange_status表"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT round_num, peer_id, chunk_key, status, 
                   request_time, complete_time, retry_count, size
            FROM bt_exchange_status 
            ORDER BY round_num, peer_id, request_time
        """)
        
        records = cursor.fetchall()
        print(f"\n📊 客户端 {client_id} bt_exchange_status 分析:")
        print(f"   交换状态记录数: {len(records)}")
        
        if records:
            status_counts = Counter()
            for record in records:
                status_counts[record[3]] += 1
            
            print(f"   状态分布: {dict(status_counts)}")
        
        return records
        
    except Exception as e:
        print(f"❌ 分析bt_exchange_status失败: {e}")
        return []

def analyze_bt_sessions(conn, client_id):
    """分析bt_sessions表"""
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT round_num, start_time, end_time, status, 
                   total_chunks_expected, total_chunks_received, 
                   bytes_uploaded, bytes_downloaded
            FROM bt_sessions 
            ORDER BY round_num
        """)
        
        records = cursor.fetchall()
        print(f"\n🎯 客户端 {client_id} bt_sessions 分析:")
        print(f"   BitTorrent会话数: {len(records)}")
        
        for record in records:
            round_num, start_time, end_time, status, expected, received, uploaded, downloaded = record
            print(f"   轮次 {round_num}: 状态={status}, 期望={expected}, 接收={received}, 上传={uploaded}B, 下载={downloaded}B")
        
        return records
        
    except Exception as e:
        print(f"❌ 分析bt_sessions失败: {e}")
        return []

def cross_client_bt_analysis(all_bt_data):
    """跨客户端BitTorrent数据分析"""
    print(f"\n🌐 跨客户端BitTorrent数据分析:")
    
    # 收集所有轮次
    all_rounds = set()
    for client_data in all_bt_data.values():
        all_rounds.update(client_data.keys())
    
    if not all_rounds:
        print("   ⚠️ 没有发现任何BitTorrent交换数据")
        return
    
    print(f"   发现的轮次: {sorted(all_rounds)}")
    
    for round_num in sorted(all_rounds):
        print(f"\n   轮次 {round_num} 跨客户端数据验证:")
        
        # 检查每个客户端在这轮是否都有来自其他客户端的数据
        expected_clients = set(all_bt_data.keys())
        
        for client_id, client_data in all_bt_data.items():
            if round_num in client_data:
                round_sources = set(client_data[round_num].keys())
                other_sources = round_sources - {client_id}
                missing_sources = expected_clients - round_sources
                
                print(f"     客户端 {client_id}:")
                print(f"       - 拥有来源: {sorted(round_sources)}")
                if other_sources:
                    print(f"       ✅ 成功从其他客户端获取数据: {sorted(other_sources)}")
                else:
                    print(f"       ⚠️ 没有来自其他客户端的数据")
                    
                if missing_sources:
                    print(f"       ⚠️ 缺少来自客户端的数据: {sorted(missing_sources)}")
            else:
                print(f"     客户端 {client_id}: ❌ 该轮次无数据")

def verify_data_consistency(conn, client_id):
    """验证数据一致性"""
    try:
        cursor = conn.cursor()
        print(f"\n🔧 客户端 {client_id} 数据一致性验证:")
        
        # 1. 检查chunk_metadata和chunk_data的一致性
        cursor.execute("""
            SELECT COUNT(*) FROM chunk_metadata cm
            LEFT JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
            WHERE cd.chunk_hash IS NULL
        """)
        missing_data = cursor.fetchone()[0]
        print(f"   metadata中有但data中缺失的chunks: {missing_data}")
        
        # 2. 检查bt_chunks和chunk_metadata的一致性
        cursor.execute("""
            SELECT COUNT(*) FROM bt_chunks bc
            LEFT JOIN chunk_metadata cm ON bc.chunk_hash = cm.chunk_hash
            WHERE cm.chunk_hash IS NULL
        """)
        inconsistent_bt = cursor.fetchone()[0]
        print(f"   bt_chunks中有但metadata中缺失的chunks: {inconsistent_bt}")
        
        # 3. 检查verification状态
        cursor.execute("SELECT COUNT(*), SUM(is_verified) FROM bt_chunks")
        total_bt, verified_bt = cursor.fetchone()
        if total_bt > 0:
            print(f"   BitTorrent chunks验证状态: {verified_bt}/{total_bt} ({verified_bt/total_bt*100:.1f}%)")
        
        return True
        
    except Exception as e:
        print(f"❌ 数据一致性验证失败: {e}")
        return False

def main():
    """主函数"""
    print("🔍 开始验证chunk数据库完整性和可读性 (更新版本)")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    all_bt_data = {}
    
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
            # 分析各个表
            analyze_chunk_metadata(conn, client_id)
            analyze_chunk_data(conn, client_id)
            bt_data = analyze_bt_chunks(conn, client_id)  # 这是最关键的表
            all_bt_data[client_id] = bt_data
            analyze_bt_exchange_status(conn, client_id)
            analyze_bt_sessions(conn, client_id)
            
            # 验证数据一致性
            verify_data_consistency(conn, client_id)
            
        finally:
            conn.close()
    
    # 跨客户端BitTorrent分析
    if all_bt_data:
        cross_client_bt_analysis(all_bt_data)
    
    print(f"\n{'='*60}")
    print("✅ 更新版chunk数据库验证完成!")

if __name__ == "__main__":
    main()