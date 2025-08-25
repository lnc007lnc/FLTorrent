#!/usr/bin/env python3
"""
检查chunk数据库的实际表结构
"""

import sqlite3
import os

def inspect_chunk_database():
    """检查chunk数据库结构"""
    
    print("🔍 检查chunk数据库结构")
    print("=" * 50)
    
    # 检查所有客户端数据库
    for i in range(1, 4):
        db_path = f"tmp/client_{i}/client_{i}_chunks.db"
        if os.path.exists(db_path):
            print(f"\n--- 客户端 {i} 数据库: {db_path} ---")
            
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # 获取所有表名
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                print(f"📊 表名: {[t[0] for t in tables]}")
                
                # 检查每个表的结构和数据
                for table in tables:
                    table_name = table[0]
                    print(f"\n  📋 表: {table_name}")
                    
                    # 获取表结构
                    cursor.execute(f"PRAGMA table_info({table_name});")
                    columns = cursor.fetchall()
                    print(f"     列信息:")
                    for col in columns:
                        print(f"       {col[1]} ({col[2]})")
                    
                    # 获取数据数量
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                    count = cursor.fetchone()[0]
                    print(f"     数据行数: {count}")
                    
                    # 显示前几行数据
                    if count > 0:
                        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
                        rows = cursor.fetchall()
                        print(f"     示例数据 (前5行):")
                        for row in rows:
                            print(f"       {row}")
                
                conn.close()
                
            except Exception as e:
                print(f"❌ 检查客户端{i}数据库时出错: {e}")
        else:
            print(f"❌ 客户端{i}数据库不存在: {db_path}")

if __name__ == "__main__":
    inspect_chunk_database()