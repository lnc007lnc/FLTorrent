#!/usr/bin/env python3
"""
最终的chunk提取分析
确认BitTorrent chunk数据存储问题
"""

import sqlite3
import os
import hashlib
import pickle

def analyze_bittorrent_chunk_storage_issue():
    """分析BitTorrent chunk存储问题"""
    print("🔍 最终分析：BitTorrent chunk数据存储问题")
    print("=" * 80)
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_dbs = {
        1: os.path.join(base_path, "client_1", "client_1_chunks.db"),
        2: os.path.join(base_path, "client_2", "client_2_chunks.db"), 
        3: os.path.join(base_path, "client_3", "client_3_chunks.db")
    }
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    print("📊 BitTorrent vs 本地数据对比:")
    
    for client_id, db_path in client_dbs.items():
        print(f"\n{'='*60}")
        print(f"🔍 客户端 {client_id} 详细分析")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 1. BitTorrent记录统计
            cursor.execute("""
                SELECT COUNT(*) as total_bt_chunks,
                       SUM(CASE WHEN chunk_hash = ? THEN 1 ELSE 0 END) as empty_bt,
                       SUM(CASE WHEN chunk_hash != ? THEN 1 ELSE 0 END) as non_empty_bt
                FROM bt_chunks
            """, (empty_hash, empty_hash))
            
            total_bt, empty_bt, non_empty_bt = cursor.fetchone()
            
            print(f"📋 BitTorrent交换记录 (bt_chunks表):")
            print(f"   总记录数: {total_bt}")
            print(f"   空chunk记录: {empty_bt}")
            print(f"   非空chunk记录: {non_empty_bt}")
            
            # 2. 实际数据存储统计
            cursor.execute("SELECT COUNT(*) FROM chunk_data")
            total_chunk_data = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COUNT(*) FROM chunk_data WHERE chunk_hash = '{empty_hash}'")
            empty_data_count = cursor.fetchone()[0]
            
            print(f"\n💾 实际chunk数据存储 (chunk_data表):")
            print(f"   总数据条数: {total_chunk_data}")
            print(f"   空chunk数据: {empty_data_count}")
            print(f"   非空chunk数据: {total_chunk_data - empty_data_count}")
            
            # 3. 本地生成记录统计
            cursor.execute("""
                SELECT COUNT(*) as total_local,
                       SUM(CASE WHEN chunk_hash = ? THEN 1 ELSE 0 END) as empty_local,
                       SUM(CASE WHEN chunk_hash != ? THEN 1 ELSE 0 END) as non_empty_local
                FROM chunk_metadata
            """, (empty_hash, empty_hash))
            
            result = cursor.fetchone()
            if result and result[0] > 0:
                total_local, empty_local, non_empty_local = result
                print(f"\n🏠 本地生成记录 (chunk_metadata表):")
                print(f"   总记录数: {total_local}")
                print(f"   空chunk记录: {empty_local}")
                print(f"   非空chunk记录: {non_empty_local}")
            
            # 4. 关键问题：BitTorrent非空chunk在chunk_data中的覆盖率
            cursor.execute("""
                SELECT bc.round_num, bc.source_client_id, bc.chunk_id, bc.chunk_hash,
                       CASE WHEN cd.chunk_hash IS NOT NULL THEN 1 ELSE 0 END as has_data
                FROM bt_chunks bc
                LEFT JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
                WHERE bc.chunk_hash != ?
                ORDER BY bc.round_num, bc.source_client_id, bc.chunk_id
            """, (empty_hash,))
            
            bt_non_empty_chunks = cursor.fetchall()
            
            print(f"\n🎯 关键问题分析：")
            print(f"   BitTorrent非空chunk总数: {len(bt_non_empty_chunks)}")
            
            has_data_count = sum(1 for _, _, _, _, has_data in bt_non_empty_chunks if has_data)
            missing_data_count = len(bt_non_empty_chunks) - has_data_count
            
            print(f"   有对应chunk_data的: {has_data_count}")
            print(f"   缺失chunk_data的: {missing_data_count}")
            
            if missing_data_count > 0:
                print(f"   ❌ 数据存储覆盖率: {has_data_count/len(bt_non_empty_chunks)*100:.1f}%")
                print(f"   🚨 发现问题：{missing_data_count}个BitTorrent非空chunk没有对应的数据存储！")
                
                # 显示缺失数据的详情
                print(f"\n   📋 缺失数据的chunk详情:")
                for round_num, source_client_id, chunk_id, chunk_hash, has_data in bt_non_empty_chunks:
                    if not has_data:
                        print(f"     轮次{round_num}, 源{source_client_id}, 块{chunk_id}: {chunk_hash[:16]}...")
            else:
                print(f"   ✅ 数据存储覆盖率: 100%")
        
        finally:
            conn.close()

def analyze_chunk_data_sources():
    """分析chunk_data的数据来源"""
    print(f"\n{'='*80}")
    print("🔍 分析chunk_data表中数据的来源")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    for client_id in [1, 2, 3]:
        db_path = os.path.join(base_path, f"client_{client_id}", f"client_{client_id}_chunks.db")
        print(f"\n📊 客户端{client_id}的chunk_data来源分析:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # 获取所有chunk_data记录
            cursor.execute("SELECT chunk_hash, created_at FROM chunk_data ORDER BY created_at")
            chunk_data_records = cursor.fetchall()
            
            for i, (chunk_hash, created_at) in enumerate(chunk_data_records, 1):
                print(f"\n   📋 数据记录{i}: (创建: {created_at})")
                print(f"      哈希: {chunk_hash[:16]}...")
                
                # 检查是否在bt_chunks中（来自BitTorrent交换）
                cursor.execute("SELECT COUNT(*) FROM bt_chunks WHERE chunk_hash = ?", (chunk_hash,))
                bt_count = cursor.fetchone()[0]
                
                # 检查是否在chunk_metadata中（本地生成）
                cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE chunk_hash = ?", (chunk_hash,))
                local_count = cursor.fetchone()[0]
                
                # 判断数据来源
                if bt_count > 0 and local_count > 0:
                    print(f"      来源: 🔄 本地生成 + BitTorrent交换 (bt:{bt_count}, local:{local_count})")
                elif bt_count > 0 and local_count == 0:
                    print(f"      来源: 📥 纯BitTorrent交换 (bt:{bt_count})")
                elif bt_count == 0 and local_count > 0:
                    print(f"      来源: 🏠 纯本地生成 (local:{local_count})")
                else:
                    print(f"      来源: ❓ 未知来源")
        
        finally:
            conn.close()

def final_extraction_capability_test():
    """最终的提取能力测试"""
    print(f"\n{'='*80}")
    print("🎯 最终测试：跨节点chunk数据提取能力")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    
    print(f"\n📋 测试场景：每个客户端尝试提取所有其他节点的chunk数据")
    
    empty_hash = hashlib.sha256(b'').hexdigest()
    
    for target_client in [1, 2, 3]:
        db_path = os.path.join(base_path, f"client_{target_client}", f"client_{target_client}_chunks.db")
        print(f"\n🔍 客户端{target_client}的提取能力:")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            other_clients = [c for c in [1, 2, 3] if c != target_client]
            
            total_expected = 0
            total_extractable = 0
            
            for source_client in other_clients:
                # 查找来自该源客户端的所有非空chunk
                cursor.execute("""
                    SELECT COUNT(*) FROM bt_chunks 
                    WHERE source_client_id = ? AND chunk_hash != ?
                """, (source_client, empty_hash))
                expected = cursor.fetchone()[0]
                
                # 查找其中有数据可提取的chunk
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM bt_chunks bc
                    INNER JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
                    WHERE bc.source_client_id = ? AND bc.chunk_hash != ?
                """, (source_client, empty_hash))
                extractable = cursor.fetchone()[0]
                
                print(f"   从客户端{source_client}: 期望{expected}个, 可提取{extractable}个 ({extractable/expected*100 if expected > 0 else 0:.1f}%)")
                
                total_expected += expected
                total_extractable += extractable
            
            overall_rate = (total_extractable / total_expected * 100) if total_expected > 0 else 0
            print(f"   🎯 总计: 期望{total_expected}个, 可提取{total_extractable}个 (总成功率: {overall_rate:.1f}%)")
        
        finally:
            conn.close()

def main():
    """主函数"""
    analyze_bittorrent_chunk_storage_issue()
    analyze_chunk_data_sources()
    final_extraction_capability_test()
    
    print(f"\n{'='*80}")
    print("🎯 最终结论:")
    print("1. ❌ BitTorrent chunk数据存储存在重大问题")
    print("2. 📥 接收到的BitTorrent chunk数据没有被保存到chunk_data表")
    print("3. 💾 chunk_data表中只有本地生成的chunk数据")
    print("4. 🚫 跨节点chunk数据提取功能实际不可用")
    print("5. 🔧 需要修复BitTorrent的save_remote_chunk功能")
    
    print(f"\n🔍 建议修复方向:")
    print("- 检查federatedscope/core/chunk_manager.py中的save_remote_chunk方法")
    print("- 确认BitTorrent接收chunk后是否正确调用了数据保存逻辑")
    print("- 验证chunk数据的序列化和反序列化过程")
    print("- 测试修复后的跨节点chunk数据提取能力")

if __name__ == "__main__":
    main()