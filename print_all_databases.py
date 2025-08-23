#!/usr/bin/env python3
"""
直接打印所有节点数据库的完整内容
"""

import sqlite3
import os
import hashlib
import pickle
from datetime import datetime

def print_all_databases():
    """打印所有数据库的完整内容"""
    print("📋 所有节点数据库完整内容")
    print("=" * 100)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for client_id, db_path in client_dbs.items():
        print(f"\n{'='*100}")
        print(f"🔍 客户端 {client_id} 数据库: {db_path}")
        print('='*100)
        
        if not os.path.exists(db_path):
            print("❌ 数据库文件不存在")
            continue
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 1. 数据库schema
            print("\n📊 数据库表结构:")
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                print(f"\n📋 表: {table_name}")
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                for col in columns:
                    print(f"   - {col[1]} {col[2]} {'PRIMARY KEY' if col[5] else ''}")
            
            # 2. chunk_metadata表
            print(f"\n{'='*60}")
            print("📋 chunk_metadata 表内容:")
            print('='*60)
            
            cursor.execute("""
                SELECT round_num, chunk_id, chunk_hash, created_at, flat_size
                FROM chunk_metadata 
                ORDER BY round_num, chunk_id
            """)
            
            metadata_records = cursor.fetchall()
            print(f"总记录数: {len(metadata_records)}")
            
            for round_num, chunk_id, chunk_hash, created_at, flat_size in metadata_records:
                try:
                    time_str = datetime.fromtimestamp(created_at).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                except:
                    time_str = str(created_at)
                
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                print(f"   轮次{round_num:2d} chunk{chunk_id:2d}: {chunk_type} {chunk_hash[:16]}... 大小:{flat_size:6d} 时间:{time_str}")
            
            # 3. bt_chunks表
            print(f"\n{'='*60}")
            print("📋 bt_chunks 表内容:")
            print('='*60)
            
            cursor.execute("""
                SELECT round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, received_time, is_verified
                FROM bt_chunks 
                ORDER BY round_num, source_client_id, chunk_id
            """)
            
            bt_records = cursor.fetchall()
            print(f"总记录数: {len(bt_records)}")
            
            for round_num, source_client, chunk_id, chunk_hash, holder_client, received_time, is_verified in bt_records:
                try:
                    time_str = datetime.fromtimestamp(received_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                except:
                    time_str = str(received_time)
                
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                verified_str = "✅已验证" if is_verified else "⏳未验证"
                print(f"   轮次{round_num:2d} 来源客户端{source_client} chunk{chunk_id:2d}: {chunk_type} {chunk_hash[:16]}... 持有者:{holder_client} 时间:{time_str} {verified_str}")
            
            # 4. chunk_data表
            print(f"\n{'='*60}")
            print("📋 chunk_data 表内容:")
            print('='*60)
            
            cursor.execute("""
                SELECT chunk_hash, created_at, length(data) as data_length
                FROM chunk_data 
                ORDER BY created_at
            """)
            
            data_records = cursor.fetchall()
            print(f"总记录数: {len(data_records)}")
            
            for i, (chunk_hash, created_at, data_length) in enumerate(data_records, 1):
                try:
                    time_str = datetime.fromtimestamp(created_at).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                except:
                    time_str = str(created_at)
                
                chunk_type = "🗂️空" if chunk_hash == empty_hash else "📦非空"
                
                # 检查这个hash在metadata和bt_chunks中的引用
                cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = ?", (chunk_hash,))
                metadata_refs = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?", (chunk_hash,))
                bt_refs = cursor.fetchone()[0]
                
                print(f"   记录{i:2d}: {chunk_type} {chunk_hash[:16]}... 数据长度:{data_length:4d}字节 时间:{time_str} 引用:(本地{metadata_refs}, BT{bt_refs})")
            
            # 5. bt_sessions表
            print(f"\n{'='*60}")
            print("📋 bt_sessions 表内容:")
            print('='*60)
            
            cursor.execute("""
                SELECT round_num, start_time, end_time, status, total_chunks_received, total_chunks_expected
                FROM bt_sessions 
                ORDER BY round_num
            """)
            
            session_records = cursor.fetchall()
            print(f"总记录数: {len(session_records)}")
            
            for round_num, start_time, end_time, status, total_chunks_received, total_chunks_expected in session_records:
                try:
                    start_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    end_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if end_time else "进行中"
                    duration = f"{end_time - start_time:.2f}秒" if end_time else "进行中"
                except:
                    start_str = str(start_time)
                    end_str = str(end_time) if end_time else "进行中"
                    duration = "未知"
                
                print(f"   轮次{round_num:2d}: {start_str} - {end_str} ({duration}) 状态:{status} 进度:{total_chunks_received}/{total_chunks_expected}")
            
            # 6. bt_exchange_status表
            print(f"\n{'='*60}")
            print("📋 bt_exchange_status 表内容:")
            print('='*60)
            
            cursor.execute("""
                SELECT round_num, peer_id, chunk_key, status, request_time, complete_time, retry_count, error_msg, size
                FROM bt_exchange_status 
                ORDER BY round_num, peer_id
            """)
            
            status_records = cursor.fetchall()
            print(f"总记录数: {len(status_records)}")
            
            for round_num, peer_id, chunk_key, status, request_time, complete_time, retry_count, error_msg, size in status_records:
                try:
                    request_str = datetime.fromtimestamp(request_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if request_time else "未知"
                    complete_str = datetime.fromtimestamp(complete_time).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] if complete_time else "未完成"
                except:
                    request_str = str(request_time) if request_time else "未知"
                    complete_str = str(complete_time) if complete_time else "未完成"
                
                error_str = f" 错误:{error_msg}" if error_msg else ""
                print(f"   轮次{round_num:2d} 节点{peer_id}: {chunk_key} 状态:{status} 请求:{request_str} 完成:{complete_str} 重试:{retry_count} 大小:{size}{error_str}")
            
        except Exception as e:
            print(f"❌ 读取数据库时出错: {e}")
        finally:
            conn.close()
    
    # 统计总结
    print(f"\n{'='*100}")
    print("📊 数据库统计总结")
    print('='*100)
    
    total_metadata = 0
    total_bt_chunks = 0
    total_chunk_data = 0
    total_sessions = 0
    
    for client_id, db_path in client_dbs.items():
        if not os.path.exists(db_path):
            continue
            
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
            metadata_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM bt_chunks")
            bt_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM chunk_data")
            data_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM bt_sessions")
            session_count = cursor.fetchone()[0]
            
            print(f"客户端{client_id}: chunk_metadata({metadata_count}) + bt_chunks({bt_count}) + chunk_data({data_count}) + bt_sessions({session_count})")
            
            total_metadata += metadata_count
            total_bt_chunks += bt_count
            total_chunk_data += data_count
            total_sessions += session_count
            
        finally:
            conn.close()
    
    print(f"\n🎯 总计: chunk_metadata({total_metadata}) + bt_chunks({total_bt_chunks}) + chunk_data({total_chunk_data}) + bt_sessions({total_sessions})")

if __name__ == "__main__":
    print_all_databases()