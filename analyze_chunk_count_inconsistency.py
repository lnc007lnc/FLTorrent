#!/usr/bin/env python3
"""
分析为什么不同轮次的本地chunk数量不一致
理论上每轮次应该生成相同数量的chunk
"""

import sqlite3
import os
import hashlib
from datetime import datetime

def analyze_chunk_count_inconsistency():
    """分析chunk数量不一致的原因"""
    print("🔍 分析：为什么不同轮次的本地chunk数量不一致？")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for client_id, db_path in client_dbs.items():
        print(f"\n{'='*70}")
        print(f"🔍 客户端 {client_id} - 本地chunk生成分析")
        print("=" * 70)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 1. 分析每个轮次的本地chunk生成情况
            print("📊 每轮次本地chunk详细统计:")
            cursor.execute("""
                SELECT round_num, 
                       COUNT(*) as total_chunks,
                       SUM(CASE WHEN chunk_hash = ? THEN 1 ELSE 0 END) as empty_chunks,
                       SUM(CASE WHEN chunk_hash != ? THEN 1 ELSE 0 END) as non_empty_chunks
                FROM chunk_metadata 
                GROUP BY round_num 
                ORDER BY round_num
            """, (empty_hash, empty_hash))
            
            local_stats = cursor.fetchall()
            
            for round_num, total, empty, non_empty in local_stats:
                print(f"   轮次{round_num}: 总计{total}个chunk (空:{empty}, 非空:{non_empty})")
            
            # 2. 详细查看每个轮次的chunk_metadata记录
            print(f"\n📋 chunk_metadata表详细记录:")
            cursor.execute("""
                SELECT round_num, chunk_id, chunk_hash, created_at, 
                       CASE WHEN chunk_hash = ? THEN '空chunk' ELSE '非空chunk' END as chunk_type
                FROM chunk_metadata 
                ORDER BY round_num, chunk_id
            """, (empty_hash,))
            
            metadata_records = cursor.fetchall()
            
            current_round = None
            for round_num, chunk_id, chunk_hash, created_at, chunk_type in metadata_records:
                if round_num != current_round:
                    print(f"\n   【轮次 {round_num}】")
                    current_round = round_num
                
                try:
                    created_time = datetime.fromtimestamp(created_at).strftime('%H:%M:%S')
                except:
                    created_time = str(created_at)
                
                print(f"     chunk_{chunk_id}: {chunk_type} | 哈希:{chunk_hash[:16]}... | 时间:{created_time}")
            
            # 3. 检查chunk_data表中对应的数据
            print(f"\n💾 chunk_data表中对应的本地chunk:")
            cursor.execute("""
                SELECT cd.chunk_hash, cd.created_at, length(cd.data) as data_size,
                       COUNT(cm.chunk_hash) as metadata_refs
                FROM chunk_data cd
                INNER JOIN chunk_metadata cm ON cd.chunk_hash = cm.chunk_hash
                GROUP BY cd.chunk_hash
                ORDER BY cd.created_at
            """)
            
            data_records = cursor.fetchall()
            
            for chunk_hash, created_at, data_size, refs in data_records:
                try:
                    created_time = datetime.fromtimestamp(created_at).strftime('%H:%M:%S')
                except:
                    created_time = str(created_at)
                
                chunk_type = "空chunk" if chunk_hash == empty_hash else "非空chunk"
                print(f"   {chunk_type}: 哈希:{chunk_hash[:16]}... | 大小:{data_size}字节 | 引用:{refs}次 | 时间:{created_time}")
            
            # 4. 检查是否有chunk_metadata记录但没有对应chunk_data的情况
            print(f"\n❓ 检查数据完整性:")
            cursor.execute("""
                SELECT cm.round_num, cm.chunk_id, cm.chunk_hash
                FROM chunk_metadata cm
                LEFT JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                WHERE cd.chunk_hash IS NULL
                ORDER BY cm.round_num, cm.chunk_id
            """)
            
            missing_data = cursor.fetchall()
            
            if missing_data:
                print("   ❌ 发现chunk_metadata有记录但chunk_data缺失的情况:")
                for round_num, chunk_id, chunk_hash in missing_data:
                    print(f"     轮次{round_num} chunk_{chunk_id}: {chunk_hash[:16]}...")
            else:
                print("   ✅ 所有chunk_metadata记录都有对应的chunk_data")
            
            # 5. 分析清理策略的影响
            print(f"\n🧹 清理策略分析:")
            cursor.execute("""
                SELECT COUNT(*) as total_chunks
                FROM chunk_metadata
            """)
            total_metadata = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) as total_data
                FROM chunk_data cd
                INNER JOIN chunk_metadata cm ON cd.chunk_hash = cm.chunk_hash
            """)
            preserved_data = cursor.fetchone()[0]
            
            print(f"   chunk_metadata总记录: {total_metadata}")
            print(f"   chunk_data中保留的本地数据: {len(data_records)}")
            print(f"   数据保留率: {len(data_records)/total_metadata*100:.1f}%")
            
        finally:
            conn.close()

    # 6. 检查清理规则的具体实现
    print(f"\n{'='*70}")
    print("🔍 检查清理规则实现")
    print("=" * 70)
    
    chunk_manager_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/federatedscope/core/chunk_manager.py"
    try:
        with open(chunk_manager_path, 'r') as f:
            content = f.read()
        
        # 查找cleanup相关的代码
        import re
        cleanup_methods = re.findall(r'def (cleanup.*?)\(.*?\):', content)
        
        print(f"发现清理方法: {cleanup_methods}")
        
        # 查找保留轮次的逻辑
        retention_pattern = r'(retain_rounds|keep_rounds|rounds_to_keep|max_rounds).*?=.*?(\d+)'
        retention_matches = re.findall(retention_pattern, content, re.IGNORECASE)
        
        if retention_matches:
            print(f"发现轮次保留配置: {retention_matches}")
        else:
            print("未找到明确的轮次保留配置")
            
    except Exception as e:
        print(f"读取chunk_manager.py失败: {e}")

if __name__ == "__main__":
    analyze_chunk_count_inconsistency()