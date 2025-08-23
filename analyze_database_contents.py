#!/usr/bin/env python3
"""
分析tmp目录中的chunk数据库内容
检查实际存储的数据和轮次信息
"""
import sqlite3
import os
from typing import Dict, List, Any

def analyze_client_database(db_path: str, client_name: str):
    """分析单个客户端的chunk数据库"""
    print(f"\n{'='*50}")
    print(f"📊 分析 {client_name} 数据库: {db_path}")
    print(f"{'='*50}")
    
    if not os.path.exists(db_path):
        print(f"❌ 数据库文件不存在: {db_path}")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 获取表结构
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📋 数据库中的表: {[table[0] for table in tables]}")
        
        # 分析chunks表
        if ('chunks',) in tables:
            print(f"\n🔍 分析chunks表结构:")
            cursor.execute("PRAGMA table_info(chunks)")
            columns = cursor.fetchall()
            for col in columns:
                print(f"   列: {col[1]} ({col[2]})")
            
            # 获取所有chunk数据
            print(f"\n📦 所有chunk条目:")
            cursor.execute("SELECT * FROM chunks")
            rows = cursor.fetchall()
            
            if not rows:
                print("   ❌ 表为空")
            else:
                print(f"   总条目数: {len(rows)}")
                
                # 按轮次分组统计
                round_stats = {}
                chunk_stats = {}
                
                for i, row in enumerate(rows):
                    print(f"   条目 {i+1}: {row}")
                    
                    # 尝试解析chunk_id (假设格式为 round:node)
                    chunk_id = row[0]  # 假设第一列是chunk_id
                    if ':' in str(chunk_id):
                        parts = str(chunk_id).split(':')
                        if len(parts) >= 2:
                            round_num = parts[0]
                            node_id = parts[1]
                            
                            if round_num not in round_stats:
                                round_stats[round_num] = 0
                            round_stats[round_num] += 1
                            
                            if node_id not in chunk_stats:
                                chunk_stats[node_id] = 0
                            chunk_stats[node_id] += 1
                
                # 打印统计信息
                if round_stats:
                    print(f"\n📈 按轮次统计:")
                    for round_num, count in sorted(round_stats.items()):
                        print(f"   Round {round_num}: {count} chunks")
                
                if chunk_stats:
                    print(f"\n🏷️ 按节点ID统计:")
                    for node_id, count in sorted(chunk_stats.items()):
                        print(f"   Node {node_id}: {count} chunks")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 分析数据库时出错: {e}")

def main():
    """主函数：分析所有客户端数据库"""
    print("🔍 分析FederatedScope临时数据库内容")
    print("目的：查看实际存储的chunk数据和轮次信息")
    
    tmp_dir = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    # 检查tmp目录
    if not os.path.exists(tmp_dir):
        print(f"❌ tmp目录不存在: {tmp_dir}")
        return
    
    # 分析每个客户端的数据库
    for client_dir in sorted(os.listdir(tmp_dir)):
        client_path = os.path.join(tmp_dir, client_dir)
        if os.path.isdir(client_path):
            # 查找数据库文件
            db_files = [f for f in os.listdir(client_path) if f.endswith('.db')]
            if db_files:
                for db_file in db_files:
                    db_path = os.path.join(client_path, db_file)
                    analyze_client_database(db_path, f"{client_dir}/{db_file}")
            else:
                print(f"\n⚠️ {client_dir}: 没有找到数据库文件")
    
    print(f"\n{'='*60}")
    print("✅ 数据库分析完成")

if __name__ == "__main__":
    main()