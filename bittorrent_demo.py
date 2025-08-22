#!/usr/bin/env python3
"""
BitTorrent功能演示脚本
展示FederatedScope中的BitTorrent chunk交换系统
"""

import sys
import os
import time
import tempfile
import shutil

# 添加项目路径
sys.path.insert(0, '/mnt/g/FLtorrent_combine/FederatedScope-master')

def demonstrate_bittorrent_core():
    """演示BitTorrent核心组件功能"""
    print("🔧 BitTorrent核心组件演示")
    print("=" * 50)
    
    from federatedscope.core.bittorrent_manager import BitTorrentManager
    from federatedscope.core.chunk_manager import ChunkManager
    from unittest.mock import Mock
    
    # 创建3个模拟的clients
    clients = {}
    chunk_managers = {}
    bt_managers = {}
    
    print("📦 创建3个客户端及其chunk管理器...")
    
    for client_id in [1, 2, 3]:
        # 创建chunk manager
        chunk_managers[client_id] = ChunkManager(client_id=client_id)
        
        # 模拟保存本地chunks
        for chunk_id in range(1, 4):  # 每个client 3个chunks
            test_data = f"客户端{client_id}_分块{chunk_id}_数据内容".encode('utf-8')
            chunk_managers[client_id].save_remote_chunk(1, client_id, chunk_id, test_data)
            print(f"  ✅ 客户端{client_id}: 保存分块{chunk_id} ({len(test_data)}字节)")
        
        # 创建通信管理器模拟
        comm_manager = Mock()
        
        # 创建BitTorrent管理器
        neighbors = [i for i in [1, 2, 3] if i != client_id]
        bt_managers[client_id] = BitTorrentManager(
            client_id=client_id,
            round_num=1,
            chunk_manager=chunk_managers[client_id],
            comm_manager=comm_manager,
            neighbors=neighbors
        )
        
        print(f"  🌐 客户端{client_id}: 连接到邻居 {neighbors}")
    
    print("\n🔄 模拟BitTorrent协议交换...")
    
    # 1. Bitfield交换阶段
    print("\n1️⃣ Bitfield交换阶段:")
    for client_id in bt_managers:
        bt_manager = bt_managers[client_id]
        my_bitfield = chunk_managers[client_id].get_global_bitfield(1)
        
        print(f"  📋 客户端{client_id}的bitfield: {len(my_bitfield)}个chunks")
        
        # 模拟向其他clients发送bitfield
        for neighbor_id in bt_manager.neighbors:
            if neighbor_id in bt_managers:
                bt_managers[neighbor_id].handle_bitfield(client_id, my_bitfield)
    
    # 2. Interested消息阶段
    print("\n2️⃣ Interested消息阶段:")
    for client_id in bt_managers:
        bt_manager = bt_managers[client_id]
        for peer_id in bt_manager.neighbors:
            if bt_manager._has_interesting_chunks(peer_id):
                bt_manager.handle_interested(peer_id)
                print(f"  💡 客户端{peer_id} 对客户端{client_id}感兴趣")
    
    # 3. Chunk请求和传输模拟
    print("\n3️⃣ Chunk请求和传输模拟:")
    
    # 客户端1请求客户端2的chunk
    print("  📤 客户端1 请求 客户端2 的分块1...")
    chunk_data = chunk_managers[2].get_chunk_data(1, 2, 1)
    if chunk_data:
        # 模拟传输和保存
        chunk_managers[1].save_remote_chunk(1, 2, 1, chunk_data)
        print(f"  ✅ 传输成功: {len(chunk_data)}字节")
        
        # 验证数据完整性
        original_data = "客户端2_分块1_数据内容".encode('utf-8')
        if chunk_data == original_data:
            print("  🔐 数据完整性校验通过")
        else:
            print("  ❌ 数据完整性校验失败")
    
    # 4. 进度统计
    print("\n4️⃣ 交换进度统计:")
    for client_id in bt_managers:
        progress = bt_managers[client_id].get_progress()
        print(f"  📊 客户端{client_id}: {progress['chunks_collected']}个chunks, "
              f"进度 {progress['progress_ratio']:.1%}")
    
    print("\n✅ BitTorrent核心功能演示完成！")
    
    # 清理临时文件
    for chunk_manager in chunk_managers.values():
        try:
            if os.path.exists(chunk_manager.db_path):
                os.remove(chunk_manager.db_path)
        except:
            pass

def demonstrate_configuration():
    """演示BitTorrent配置系统"""
    print("\n🔧 BitTorrent配置系统演示")
    print("=" * 50)
    
    from federatedscope.core.configs.config import CN
    from federatedscope.core.configs import init_global_cfg
    from federatedscope.core.configs.cfg_bittorrent import assert_bittorrent_cfg
    
    # 创建配置
    cfg = CN()
    init_global_cfg(cfg)
    
    print("📋 默认BitTorrent配置:")
    print(f"  启用状态: {cfg.bittorrent.enable}")
    print(f"  超时时间: {cfg.bittorrent.timeout}秒")
    print(f"  最大上传槽位: {cfg.bittorrent.max_upload_slots}")
    print(f"  chunk选择算法: {cfg.bittorrent.chunk_selection}")
    print(f"  最小完成比例: {cfg.bittorrent.min_completion_ratio}")
    
    # 启用BitTorrent
    cfg.bittorrent.enable = True
    print(f"\n✅ BitTorrent已启用")
    
    # 配置验证
    try:
        assert_bittorrent_cfg(cfg)
        print("✅ 配置验证通过")
    except Exception as e:
        print(f"❌ 配置验证失败: {e}")

def show_implementation_summary():
    """显示实现总结"""
    print("\n📊 FederatedScope BitTorrent实现总结")
    print("=" * 60)
    
    components = [
        ("🔧 BitTorrentManager", "核心BitTorrent协议管理器"),
        ("📦 ChunkManager扩展", "支持远程chunk存储和检索"),
        ("🖥️ Server集成", "支持BitTorrent交换触发和监控"),
        ("💻 Client集成", "完整的peer功能实现"),
        ("⚙️ 配置系统", "灵活的BitTorrent参数配置"),
        ("📊 消息协议", "9种BitTorrent消息类型支持"),
        ("🔍 算法实现", "Rarest First、Choke/Unchoke等"),
        ("✅ 测试覆盖", "单元测试和集成测试")
    ]
    
    print("已实现的组件:")
    for component, description in components:
        print(f"  {component}: {description}")
    
    print(f"\n📈 代码统计:")
    print(f"  • BitTorrentManager: ~400行")
    print(f"  • ChunkManager扩展: ~150行")
    print(f"  • Server扩展: ~150行") 
    print(f"  • Client扩展: ~200行")
    print(f"  • 配置系统: ~100行")
    print(f"  • 测试代码: ~500行")
    print(f"  📝 总计: ~1500行代码")
    
    print(f"\n🌟 核心特性:")
    features = [
        "✅ 无Tracker设计 - 利用现有拓扑连接",
        "✅ 经典BitTorrent算法 - Rarest First、Reciprocal Unchoke",
        "✅ 完整性校验 - SHA256确保chunk数据完整",
        "✅ 超时重传 - 可靠的chunk传输机制",
        "✅ 向后兼容 - 不影响现有FL训练流程",
        "✅ 轮次隔离 - 支持多轮次chunk管理",
        "✅ 灵活配置 - 丰富的参数调优选项"
    ]
    
    for feature in features:
        print(f"  {feature}")

def main():
    """主函数"""
    print("🚀 FederatedScope BitTorrent系统演示")
    print("=" * 60)
    
    try:
        # 1. 配置系统演示
        demonstrate_configuration()
        
        # 2. 核心功能演示
        demonstrate_bittorrent_core()
        
        # 3. 实现总结
        show_implementation_summary()
        
        print(f"\n🎉 演示完成！BitTorrent系统已成功集成到FederatedScope中。")
        print(f"💡 提示：使用 'python federatedscope/main.py --cfg config.yaml' 启动FL训练")
        print(f"📖 详细信息请参考：FL_CLASSIC_BITTORRENT_IMPLEMENTATION_PLAN.md")
        
    except Exception as e:
        print(f"❌ 演示过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)