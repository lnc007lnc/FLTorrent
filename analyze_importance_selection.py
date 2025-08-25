#!/usr/bin/env python3
"""
分析chunk数据库中的重要度选择策略验证
"""

import sqlite3
import pandas as pd
from pathlib import Path

def analyze_chunk_importance_selection():
    """分析chunk数据库中的重要度选择记录"""
    
    print("🔍 分析BitTorrent重要度选择策略验证...")
    print("=" * 80)
    
    # 数据库文件路径
    db_files = [
        "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_1/client_1_chunks.db",
        "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_2/client_2_chunks.db", 
        "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_3/client_3_chunks.db"
    ]
    
    for db_file in db_files:
        if not Path(db_file).exists():
            print(f"❌ 数据库文件不存在: {db_file}")
            continue
            
        print(f"\n📊 分析数据库: {db_file}")
        print("-" * 60)
        
        try:
            with sqlite3.connect(db_file) as conn:
                # 检查表结构
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                print(f"📋 可用表: {[t[0] for t in tables]}")
                
                # 分析本地chunks (chunk_metadata表)
                if any('chunk_metadata' in str(table) for table in tables):
                    print(f"\n🏠 本地chunks分析 (chunk_metadata表):")
                    df_local = pd.read_sql_query("""
                        SELECT round_num, chunk_id, importance_score, pruning_method
                        FROM chunk_metadata 
                        ORDER BY round_num, importance_score DESC
                    """, conn)
                    
                    if not df_local.empty:
                        print(df_local.to_string(index=False))
                        
                        # 分析重要度分布
                        for round_num in sorted(df_local['round_num'].unique()):
                            round_data = df_local[df_local['round_num'] == round_num]
                            print(f"\n📈 第{round_num}轮重要度分布:")
                            print(f"   最高重要度: {round_data['importance_score'].max():.4f}")
                            print(f"   最低重要度: {round_data['importance_score'].min():.4f}")
                            print(f"   平均重要度: {round_data['importance_score'].mean():.4f}")
                            print(f"   重要度标准差: {round_data['importance_score'].std():.4f}")
                            
                            # 显示chunks按重要度排序
                            print("   Chunks按重要度排序:")
                            for idx, row in round_data.sort_values('importance_score', ascending=False).iterrows():
                                print(f"     Chunk {row['chunk_id']}: 重要度={row['importance_score']:.4f}")
                    else:
                        print("   无本地chunk数据")
                
                # 分析BitTorrent交换chunks (bt_chunks表)
                if any('bt_chunks' in str(table) for table in tables):
                    print(f"\n📥 BitTorrent chunks交换分析:")
                    df_bt = pd.read_sql_query("""
                        SELECT bt.round_num, bt.source_client_id, bt.chunk_id, bt.holder_client_id,
                               bt.received_time, bt.is_verified, cm.importance_score
                        FROM bt_chunks bt
                        LEFT JOIN chunk_metadata cm 
                        ON bt.round_num = cm.round_num AND bt.chunk_id = cm.chunk_id
                        ORDER BY bt.round_num, bt.received_time
                    """, conn)
                    
                    if not df_bt.empty:
                        print(df_bt.to_string(index=False))
                        
                        # 分析接收顺序是否符合重要度优先
                        for round_num in sorted(df_bt['round_num'].unique()):
                            round_data = df_bt[df_bt['round_num'] == round_num]
                            print(f"\n📊 第{round_num}轮BitTorrent接收chunks验证:")
                            
                            # 按接收时间排序看实际接收顺序
                            by_time = round_data.sort_values('received_time')
                            print("   实际接收顺序 (按时间):")
                            for idx, row in by_time.iterrows():
                                importance = row['importance_score'] if pd.notna(row['importance_score']) else 0.0
                                print(f"     时间{row['received_time']:.1f}: Chunk {row['chunk_id']} "
                                      f"from Client {row['source_client_id']} → Client {row['holder_client_id']}, "
                                      f"重要度={importance:.4f}")
                            
                            # 按重要度排序看理想顺序
                            by_importance = round_data[pd.notna(round_data['importance_score'])].sort_values('importance_score', ascending=False)
                            if not by_importance.empty:
                                print("   理想接收顺序 (按重要度):")
                                for idx, row in by_importance.iterrows():
                                    print(f"     Chunk {row['chunk_id']} from Client {row['source_client_id']}: "
                                          f"重要度={row['importance_score']:.4f}")
                    else:
                        print("   无BitTorrent交换数据")
                
                # 分析交换会话信息
                if any('bt_sessions' in str(table) for table in tables):
                    print(f"\n📡 BitTorrent会话分析:")
                    df_sessions = pd.read_sql_query("""
                        SELECT round_num, peer_id, downloaded_chunks, uploaded_chunks,
                               start_time, end_time
                        FROM bt_sessions 
                        ORDER BY round_num, start_time
                    """, conn)
                    
                    if not df_sessions.empty:
                        print(df_sessions.to_string(index=False))
                    else:
                        print("   无BitTorrent会话数据")
                
        except Exception as e:
            print(f"❌ 分析数据库失败: {e}")
    
    print(f"\n" + "=" * 80)
    print("✅ 重要度选择策略分析完成")

if __name__ == "__main__":
    analyze_chunk_importance_selection()