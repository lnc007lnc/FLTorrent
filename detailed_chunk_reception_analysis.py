#!/usr/bin/env python3
"""
详细分析chunk接收顺序，验证重要度驱动选择策略
"""

import sqlite3
import pandas as pd
from pathlib import Path

def detailed_chunk_reception_analysis():
    """详细分析chunk接收顺序"""
    
    print("🔍 详细chunk接收顺序分析")
    print("=" * 100)
    
    # 数据库文件路径
    db_files = {
        1: "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_1/client_1_chunks.db",
        2: "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_2/client_2_chunks.db", 
        3: "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp/client_3/client_3_chunks.db"
    }
    
    for client_id, db_file in db_files.items():
        if not Path(db_file).exists():
            print(f"❌ 数据库文件不存在: {db_file}")
            continue
            
        print(f"\n" + "=" * 100)
        print(f"📊 Client {client_id} 详细接收分析: {Path(db_file).name}")
        print("=" * 100)
        
        try:
            with sqlite3.connect(db_file) as conn:
                
                # 获取完整的chunk接收记录，包含重要度信息
                df_complete = pd.read_sql_query("""
                    SELECT 
                        bt.round_num,
                        bt.source_client_id,
                        bt.chunk_id,
                        bt.holder_client_id,
                        bt.received_time,
                        bt.is_verified,
                        cm.importance_score,
                        cm.pruning_method
                    FROM bt_chunks bt
                    LEFT JOIN chunk_metadata cm 
                        ON bt.round_num = cm.round_num AND bt.chunk_id = cm.chunk_id
                    ORDER BY bt.round_num, bt.received_time, bt.source_client_id, bt.chunk_id
                """, conn)
                
                if df_complete.empty:
                    print(f"❌ Client {client_id}: 无chunk接收数据")
                    continue
                
                print(f"📋 Client {client_id}: 总共接收了 {len(df_complete)} 个chunks")
                
                # 按轮次分析
                for round_num in sorted(df_complete['round_num'].unique()):
                    round_data = df_complete[df_complete['round_num'] == round_num]
                    
                    print(f"\n🔄 ===== 第{round_num}轮接收详情 =====")
                    print(f"轮次{round_num}: 接收了 {len(round_data)} 个chunks")
                    
                    # 按接收时间排序，显示完整接收顺序
                    by_time = round_data.sort_values(['received_time', 'source_client_id', 'chunk_id'])
                    
                    print(f"\n📥 实际接收顺序 (按时间戳):")
                    print("-" * 90)
                    print(f"{'序号':<4} {'时间戳':<12} {'来源':<6} {'Chunk':<6} {'重要度':<10} {'验证':<4} {'说明'}")
                    print("-" * 90)
                    
                    previous_time = None
                    batch_number = 0
                    for idx, (_, row) in enumerate(by_time.iterrows(), 1):
                        importance = row['importance_score'] if pd.notna(row['importance_score']) else 0.0
                        current_time = row['received_time']
                        
                        # 检测批量接收（相同时间戳）
                        if previous_time != current_time:
                            batch_number += 1
                            time_marker = f"批次{batch_number}"
                        else:
                            time_marker = f"    └─"
                        
                        status = "✓" if row['is_verified'] else "✗"
                        
                        # 根据重要度给出解释
                        if importance >= 0.25:
                            explanation = "🔥最高重要度"
                        elif importance >= 0.08:
                            explanation = "⭐次高重要度"
                        elif importance >= 0.07:
                            explanation = "📊中等重要度"
                        else:
                            explanation = "📉低重要度"
                        
                        print(f"{idx:<4} {current_time:<12.0f} C{row['source_client_id']:<5.0f} {row['chunk_id']:<6.0f} "
                              f"{importance:<10.4f} {status:<4} {explanation}")
                        
                        previous_time = current_time
                    
                    # 分析重要度分布和选择模式
                    print(f"\n📈 轮次{round_num} 重要度分析:")
                    importance_data = round_data[pd.notna(round_data['importance_score'])]
                    if not importance_data.empty:
                        print(f"   最高重要度: {importance_data['importance_score'].max():.4f}")
                        print(f"   最低重要度: {importance_data['importance_score'].min():.4f}")
                        print(f"   平均重要度: {importance_data['importance_score'].mean():.4f}")
                        print(f"   重要度标准差: {importance_data['importance_score'].std():.4f}")
                        
                        # 分析高重要度chunks的接收优先级
                        high_importance = importance_data[importance_data['importance_score'] >= 0.1]
                        if not high_importance.empty:
                            print(f"   高重要度chunks(≥0.1): {len(high_importance)}个")
                            print("   高重要度chunks接收顺序:")
                            for idx, (_, row) in enumerate(high_importance.sort_values('received_time').iterrows(), 1):
                                print(f"      {idx}. Chunk {row['chunk_id']:.0f} from C{row['source_client_id']:.0f}: "
                                      f"重要度={row['importance_score']:.4f}, 时间={row['received_time']:.0f}")
                    
                    # 验证重要度优先选择假设
                    print(f"\n🎯 轮次{round_num} 重要度优先选择验证:")
                    
                    # 检查是否高重要度chunks优先接收
                    if not importance_data.empty:
                        sorted_by_importance = importance_data.sort_values('importance_score', ascending=False)
                        sorted_by_time = importance_data.sort_values('received_time')
                        
                        # 计算前几个接收的chunks的平均重要度
                        first_quarter = len(sorted_by_time) // 4 + 1
                        first_chunks_avg = sorted_by_time.head(first_quarter)['importance_score'].mean()
                        last_chunks_avg = sorted_by_time.tail(first_quarter)['importance_score'].mean()
                        
                        print(f"   前25%接收chunks平均重要度: {first_chunks_avg:.4f}")
                        print(f"   后25%接收chunks平均重要度: {last_chunks_avg:.4f}")
                        
                        if first_chunks_avg > last_chunks_avg * 1.1:  # 10%差异阈值
                            print("   ✅ 验证通过：高重要度chunks优先接收")
                        else:
                            print("   ❓ 需要检查：接收顺序未完全按重要度优先")
                    
                    print("\n" + "─" * 90)
                
        except Exception as e:
            print(f"❌ 分析Client {client_id}数据库失败: {e}")
    
    print(f"\n" + "=" * 100)
    print("✅ 详细chunk接收顺序分析完成")

if __name__ == "__main__":
    detailed_chunk_reception_analysis()