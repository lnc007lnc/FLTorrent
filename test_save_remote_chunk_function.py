#!/usr/bin/env python3
"""
测试save_remote_chunk函数
模拟chunk数据，验证函数能否成功写入
"""

import sqlite3
import os
import hashlib
import pickle
import numpy as np
import sys
import tempfile
from pathlib import Path

# 添加项目路径以导入ChunkManager
sys.path.append('/mnt/g/FLtorrent_combine/FederatedScope-master')

def test_save_remote_chunk_with_real_manager():
    """使用真实的ChunkManager测试save_remote_chunk"""
    print("🧪 使用真实ChunkManager测试save_remote_chunk")
    print("=" * 80)
    
    try:
        from federatedscope.core.chunk_manager import ChunkManager
        
        # 初始化ChunkManager (它会自动创建数据库)
        chunk_manager = ChunkManager(client_id=999)  # 使用特殊ID避免冲突
        test_db_path = chunk_manager.db_path
        
        print(f"📊 测试数据库: {test_db_path}")
        
        # 测试各种类型的chunk数据
        test_scenarios = [
            ("空numpy数组", np.array([])),
            ("小numpy数组", np.random.rand(5).astype(np.float32)),
            ("大numpy数组", np.random.rand(100, 50).astype(np.float32)),
            ("整数数组", np.array([1, 2, 3, 4, 5])),
            ("字典数据", {'weights': np.random.rand(3), 'bias': np.array([1.0])}),
            ("列表数据", [1.0, 2.0, 3.0, 4.0]),
            ("字符串数据", "test_chunk_data"),
            ("None数据", None)
        ]
        
        successful_saves = 0
        
        for i, (scenario_name, test_data) in enumerate(test_scenarios, 1):
            print(f"\n🔍 测试场景 {i}: {scenario_name}")
            print(f"   数据类型: {type(test_data)}")
            
            if test_data is not None and hasattr(test_data, 'shape'):
                print(f"   数据形状: {test_data.shape}")
            elif test_data is not None and hasattr(test_data, '__len__'):
                print(f"   数据长度: {len(test_data)}")
            
            try:
                # 调用save_remote_chunk
                chunk_manager.save_remote_chunk(
                    round_num=0,
                    source_client_id=2, 
                    chunk_id=i,
                    chunk_data=test_data
                )
                
                print(f"   ✅ 调用成功")
                
                # 验证是否真的保存到数据库
                conn = sqlite3.connect(test_db_path)
                cursor = conn.cursor()
                
                # 检查bt_chunks表
                cursor.execute("""
                    SELECT chunk_hash FROM bt_chunks 
                    WHERE round_num=0 AND source_client_id=2 AND chunk_id=?
                """, (i,))
                bt_result = cursor.fetchone()
                
                if bt_result:
                    chunk_hash = bt_result[0]
                    print(f"   📋 bt_chunks记录: ✅存在 (哈希: {chunk_hash[:16]}...)")
                    
                    # 检查chunk_data表
                    cursor.execute("SELECT data FROM chunk_data WHERE chunk_hash=?", (chunk_hash,))
                    data_result = cursor.fetchone()
                    
                    if data_result:
                        print(f"   💾 chunk_data记录: ✅存在 (大小: {len(data_result[0])} bytes)")
                        
                        # 尝试反序列化验证
                        try:
                            if test_data is not None:
                                restored_data = pickle.loads(data_result[0])
                                print(f"   🔄 反序列化: ✅成功 (类型: {type(restored_data)})")
                                
                                # 数据一致性检查
                                if isinstance(test_data, np.ndarray) and isinstance(restored_data, np.ndarray):
                                    is_equal = np.array_equal(test_data, restored_data)
                                    print(f"   📊 数据一致性: {'✅一致' if is_equal else '❌不一致'}")
                                elif test_data == restored_data:
                                    print(f"   📊 数据一致性: ✅一致")
                                else:
                                    print(f"   📊 数据一致性: ❌不一致")
                            else:
                                # None数据的特殊处理
                                restored_data = pickle.loads(data_result[0])
                                print(f"   🔄 反序列化: ✅成功 (None数据)")
                                print(f"   📊 数据一致性: {'✅一致' if restored_data is None else '❌不一致'}")
                            
                            successful_saves += 1
                            
                        except Exception as e:
                            print(f"   🔄 反序列化: ❌失败 ({e})")
                    else:
                        print(f"   💾 chunk_data记录: ❌不存在！")
                else:
                    print(f"   📋 bt_chunks记录: ❌不存在！")
                
                conn.close()
                
            except Exception as e:
                print(f"   ❌ 调用失败: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\n📈 测试结果统计:")
        print(f"   成功保存: {successful_saves}/{len(test_scenarios)}")
        print(f"   成功率: {successful_saves/len(test_scenarios)*100:.1f}%")
        
        # 清理测试数据库和目录
        if os.path.exists(test_db_path):
            os.unlink(test_db_path)
            print(f"🧹 清理测试数据库")
        
        # 清理测试目录
        test_dir = os.path.dirname(test_db_path)
        if os.path.exists(test_dir) and "client_999" in test_dir:
            import shutil
            shutil.rmtree(test_dir)
            print(f"🧹 清理测试目录")
        
    except ImportError as e:
        print(f"❌ 导入ChunkManager失败: {e}")
        return False
        
    return successful_saves > 0

def test_with_actual_database():
    """使用实际数据库测试save_remote_chunk"""
    print(f"\n{'='*80}")
    print("🔍 使用实际数据库测试save_remote_chunk")
    
    base_path = "/mnt/g/FLtorrent_combine/FederatedScope-master/tmp"
    client_1_db = os.path.join(base_path, "client_1", "client_1_chunks.db")
    
    if not os.path.exists(client_1_db):
        print(f"❌ 实际数据库不存在: {client_1_db}")
        return
    
    # 备份原数据库
    backup_db = client_1_db + ".backup"
    import shutil
    shutil.copy2(client_1_db, backup_db)
    print(f"📋 已备份数据库到: {backup_db}")
    
    try:
        from federatedscope.core.chunk_manager import ChunkManager
        
        # 使用实际数据库路径，但我们需要手动设置
        # 注意：这是一个hack，正常ChunkManager会自动生成路径
        chunk_manager = ChunkManager(client_id=1)
        chunk_manager.db_path = client_1_db  # 强制设置为实际数据库路径
        
        # 生成测试chunk数据
        test_chunk_data = np.random.rand(10, 5).astype(np.float32)
        test_hash = hashlib.sha256(pickle.dumps(test_chunk_data)).hexdigest()
        
        print(f"🧪 测试chunk数据:")
        print(f"   类型: {type(test_chunk_data)}")
        print(f"   形状: {test_chunk_data.shape}")  
        print(f"   预期哈希: {test_hash[:16]}...")
        
        # 记录保存前的状态
        conn = sqlite3.connect(client_1_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM bt_chunks")
        bt_count_before = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM chunk_data")
        data_count_before = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"📊 保存前状态:")
        print(f"   bt_chunks记录数: {bt_count_before}")
        print(f"   chunk_data记录数: {data_count_before}")
        
        # 调用save_remote_chunk
        print(f"\n🔧 调用save_remote_chunk...")
        chunk_manager.save_remote_chunk(
            round_num=99,  # 使用特殊轮次避免与现有数据冲突
            source_client_id=99,
            chunk_id=99,
            chunk_data=test_chunk_data
        )
        
        # 检查保存后的状态
        conn = sqlite3.connect(client_1_db)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM bt_chunks")
        bt_count_after = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM chunk_data")
        data_count_after = cursor.fetchone()[0]
        
        print(f"\n📊 保存后状态:")
        print(f"   bt_chunks记录数: {bt_count_after} (增加: {bt_count_after - bt_count_before})")
        print(f"   chunk_data记录数: {data_count_after} (增加: {data_count_after - data_count_before})")
        
        # 验证具体记录
        cursor.execute("""
            SELECT chunk_hash FROM bt_chunks 
            WHERE round_num=99 AND source_client_id=99 AND chunk_id=99
        """)
        bt_result = cursor.fetchone()
        
        if bt_result:
            actual_hash = bt_result[0]
            print(f"✅ bt_chunks记录已保存")
            print(f"   实际哈希: {actual_hash}")
            print(f"   哈希匹配: {'✅是' if actual_hash == test_hash else '❌否'}")
            
            # 检查chunk_data
            cursor.execute("SELECT data FROM chunk_data WHERE chunk_hash=?", (actual_hash,))
            data_result = cursor.fetchone()
            
            if data_result:
                print(f"✅ chunk_data记录已保存")
                print(f"   数据大小: {len(data_result[0])} bytes")
                
                # 验证数据完整性
                try:
                    restored_data = pickle.loads(data_result[0])
                    is_equal = np.array_equal(test_chunk_data, restored_data)
                    print(f"   数据完整性: {'✅正确' if is_equal else '❌损坏'}")
                    
                    if is_equal:
                        print(f"🎉 save_remote_chunk函数工作正常！")
                    else:
                        print(f"❌ 数据保存存在问题")
                        
                except Exception as e:
                    print(f"❌ 数据反序列化失败: {e}")
            else:
                print(f"❌ chunk_data记录未保存！")
        else:
            print(f"❌ bt_chunks记录未保存！")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ 测试过程出错: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 恢复原数据库
        if os.path.exists(backup_db):
            shutil.move(backup_db, client_1_db)
            print(f"🔄 已恢复原数据库")

def main():
    """主函数"""
    print("🧪 测试save_remote_chunk函数")
    
    # 测试1: 使用临时数据库
    success = test_save_remote_chunk_with_real_manager()
    
    if success:
        # 测试2: 使用实际数据库
        test_with_actual_database()
    
    print(f"\n{'='*80}")
    print("🎯 测试结论:")
    if success:
        print("✅ save_remote_chunk函数逻辑正常")
        print("🔍 如果实际运行时数据未保存，问题可能在于:")
        print("   1. 传入的chunk_data参数为空或无效")
        print("   2. BitTorrent接收过程中数据丢失")  
        print("   3. 函数实际未被调用（尽管bt_chunks有记录）")
        print("   4. 数据库写入权限或锁定问题")
    else:
        print("❌ save_remote_chunk函数存在问题")

if __name__ == "__main__":
    main()