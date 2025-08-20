#!/usr/bin/env python3
"""
测试基于您算法的分块权重保存数据库系统
验证ChunkManager的功能和节点特定数据库存储
"""

import sys
import os
sys.path.insert(0, os.getcwd())

import torch
import torch.nn as nn
import numpy as np
import shutil
import logging
from federatedscope.core.chunk_manager import ChunkManager

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestModel(nn.Module):
    """测试用的简单模型"""
    def __init__(self):
        super().__init__()
        self.fc1 = nn.Linear(10, 20)
        self.fc2 = nn.Linear(20, 15)
        self.fc3 = nn.Linear(15, 5)
        self.dropout = nn.Dropout(0.1)
        
    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = self.dropout(x)
        x = torch.relu(self.fc2(x))
        x = self.fc3(x)
        return x


def cleanup_test_data():
    """清理测试数据"""
    test_dir = os.path.join(os.getcwd(), "tmp")
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
        print("🧹 清理了之前的测试数据")


def test_model_splitting():
    """测试模型分割算法"""
    print("\n🧪 测试模型分割算法...")
    
    model = TestModel()
    params = ChunkManager.model_to_params(model)
    
    # 计算总参数数量
    total_elements = sum(np.prod(p.shape) for p in params.values())
    print(f"   模型总参数数量: {total_elements}")
    
    # 显示各参数的详细信息
    print("   参数详情:")
    for name, param in params.items():
        print(f"     {name}: shape={param.shape}, elements={np.prod(param.shape)}")
    
    # 测试不同的chunk数量
    for num_chunks in [3, 5, 8]:
        print(f"\n   分割为 {num_chunks} 个chunks:")
        chunks = ChunkManager.split_model(params, num_chunks)
        
        total_chunk_elements = 0
        for chunk in chunks:
            print(f"     Chunk {chunk['chunk_id']}: {chunk['flat_size']} 个元素")
            print(f"       包含参数: {list(chunk['parts'].keys())}")
            total_chunk_elements += chunk['flat_size']
            
        assert len(chunks) == num_chunks, f"应该有 {num_chunks} 个chunks"
        assert total_chunk_elements == total_elements, "chunk总元素数应等于模型总元素数"
        
    print("   ✅ 模型分割测试通过")
    return True


def test_chunk_extraction():
    """测试chunk数据提取"""
    print("\n🧪 测试chunk数据提取...")
    
    model = TestModel()
    params = ChunkManager.model_to_params(model)
    chunks = ChunkManager.split_model(params, num_chunks=5)
    
    chunk_manager = ChunkManager(client_id=999)  # 临时测试用
    
    total_extracted = 0
    for chunk_info in chunks:
        chunk_data = chunk_manager.extract_chunk_data(params, chunk_info)
        print(f"   Chunk {chunk_info['chunk_id']}: 提取了 {len(chunk_data)} 个元素")
        assert len(chunk_data) == chunk_info['flat_size'], "提取的数据大小应匹配chunk定义"
        total_extracted += len(chunk_data)
        
    total_elements = sum(np.prod(p.shape) for p in params.values())
    assert total_extracted == total_elements, "总提取元素数应等于模型总元素数"
    
    print("   ✅ chunk数据提取测试通过")
    return True


def test_node_specific_databases():
    """测试节点特定数据库创建和存储"""
    print("\n🧪 测试节点特定数据库...")
    
    # 测试多个客户端的数据库创建
    client_ids = [1, 2, 3]
    chunk_managers = {}
    
    for client_id in client_ids:
        chunk_manager = ChunkManager(client_id=client_id)
        chunk_managers[client_id] = chunk_manager
        
        # 验证数据库路径
        expected_path = os.path.join(os.getcwd(), "tmp", f"client_{client_id}", f"client_{client_id}_chunks.db")
        assert chunk_manager.db_path == expected_path, f"客户端 {client_id} 数据库路径不正确"
        assert os.path.exists(chunk_manager.db_path), f"客户端 {client_id} 数据库文件应该存在"
        
        print(f"   ✅ 客户端 {client_id} 数据库创建成功: {chunk_manager.db_path}")
    
    print("   ✅ 节点特定数据库测试通过")
    return chunk_managers


def test_chunk_storage_and_retrieval():
    """测试chunk存储和检索"""
    print("\n🧪 测试chunk存储和检索...")
    
    # 为每个客户端创建略有不同的模型
    models = {}
    for client_id in [1, 2, 3]:
        model = TestModel()
        # 为每个客户端的模型添加不同的变化
        with torch.no_grad():
            for param in model.parameters():
                param.data += torch.randn_like(param) * 0.01 * client_id
        models[client_id] = model
    
    # 保存多轮次的chunks
    for round_num in range(1, 4):  # 3轮训练
        print(f"\n   第 {round_num} 轮训练:")
        
        for client_id in [1, 2, 3]:
            chunk_manager = ChunkManager(client_id=client_id)
            model = models[client_id]
            
            # 模拟训练后的模型变化
            with torch.no_grad():
                for param in model.parameters():
                    param.data += torch.randn_like(param) * 0.001
            
            # 保存chunks
            saved_hashes = chunk_manager.save_model_chunks(
                model=model,
                round_num=round_num,
                num_chunks=5
            )
            
            print(f"     客户端 {client_id}: 保存了 {len(saved_hashes)} 个chunks")
            assert len(saved_hashes) == 5, f"应该保存5个chunks"
            
            # 测试检索
            chunks = chunk_manager.load_chunks_by_round(round_num)
            assert len(chunks) == 5, f"应该检索到5个chunks"
            
            # 测试按ID获取chunk
            for chunk_id in range(5):
                chunk_result = chunk_manager.get_chunk_by_id(round_num, chunk_id)
                assert chunk_result is not None, f"应该能获取到chunk {chunk_id}"
    
    print("   ✅ chunk存储和检索测试通过")
    return True


def test_storage_statistics():
    """测试存储统计功能"""
    print("\n🧪 测试存储统计...")
    
    for client_id in [1, 2, 3]:
        chunk_manager = ChunkManager(client_id=client_id)
        stats = chunk_manager.get_storage_stats()
        
        print(f"   客户端 {client_id} 存储统计:")
        print(f"     数据库路径: {stats['db_path']}")
        print(f"     元数据条目: {stats['total_metadata_entries']}")
        print(f"     唯一chunks: {stats['unique_chunks']}")
        print(f"     存储大小: {stats['storage_size_mb']:.3f} MB")
        print(f"     轮次范围: {stats['round_range']}")
        
        # 验证统计数据
        assert stats['client_id'] == client_id
        assert stats['total_metadata_entries'] == 15  # 3轮 × 5chunks
        assert stats['round_range'] == (1, 3)
        
    print("   ✅ 存储统计测试通过")
    return True


def test_model_reconstruction():
    """测试从chunks重构模型"""
    print("\n🧪 测试模型重构...")
    
    client_id = 1
    chunk_manager = ChunkManager(client_id=client_id)
    
    # 创建原始模型
    original_model = TestModel()
    original_params = {name: param.clone() for name, param in original_model.named_parameters()}
    
    # 保存模型chunks
    chunk_manager.save_model_chunks(original_model, round_num=99, num_chunks=6)
    
    # 创建新模型并用随机权重初始化
    reconstructed_model = TestModel()
    with torch.no_grad():
        for param in reconstructed_model.parameters():
            param.data = torch.randn_like(param)
    
    # 从chunks重构模型
    success = chunk_manager.reconstruct_model_from_chunks(round_num=99, target_model=reconstructed_model)
    assert success, "模型重构应该成功"
    
    # 验证重构的准确性
    for name, original_param in original_params.items():
        reconstructed_param = dict(reconstructed_model.named_parameters())[name]
        diff = torch.abs(original_param - reconstructed_param).max().item()
        print(f"   参数 {name} 最大差异: {diff:.10f}")
        assert diff < 1e-6, f"参数 {name} 重构差异过大: {diff}"
    
    print("   ✅ 模型重构测试通过")
    return True


def test_cleanup_functionality():
    """测试清理功能"""
    print("\n🧪 测试清理功能...")
    
    client_id = 1
    chunk_manager = ChunkManager(client_id=client_id)
    model = TestModel()
    
    # 保存多轮数据
    for round_num in range(1, 11):  # 10轮
        chunk_manager.save_model_chunks(model, round_num=round_num, num_chunks=3)
    
    # 获取清理前的统计
    stats_before = chunk_manager.get_storage_stats()
    print(f"   清理前: {stats_before['total_metadata_entries']} 条元数据")
    
    # 执行清理（保留最近3轮）
    chunk_manager.cleanup_old_rounds(keep_rounds=3)
    
    # 获取清理后的统计
    stats_after = chunk_manager.get_storage_stats()
    print(f"   清理后: {stats_after['total_metadata_entries']} 条元数据")
    print(f"   轮次范围: {stats_after['round_range']}")
    
    # 验证清理效果
    assert stats_after['total_metadata_entries'] <= 9, "清理后应该只剩下最近3轮的数据"
    assert stats_after['round_range'][0] >= 8, "最小轮次应该是8或更大"
    
    print("   ✅ 清理功能测试通过")
    return True


def test_aggregation_scenario():
    """测试聚合场景：从多个节点数据库读取相同chunk进行聚合"""
    print("\n🧪 测试聚合场景...")
    
    # 模拟3个客户端，每个都保存了第1轮的chunks
    chunk_managers = {1: ChunkManager(1), 2: ChunkManager(2), 3: ChunkManager(3)}
    
    # 从每个客户端获取第1轮的chunk 0数据
    chunk_0_data = {}
    for client_id, chunk_manager in chunk_managers.items():
        chunk_result = chunk_manager.get_chunk_by_id(round_num=1, chunk_id=0)
        if chunk_result:
            chunk_info, chunk_data = chunk_result
            chunk_0_data[client_id] = chunk_data
            print(f"   客户端 {client_id} chunk 0: {len(chunk_data)} 个元素")
    
    # 进行简单的平均聚合
    if len(chunk_0_data) >= 2:
        aggregated_chunk = np.mean(list(chunk_0_data.values()), axis=0)
        print(f"   聚合后的chunk 0: {len(aggregated_chunk)} 个元素")
        print("   ✅ 可以成功从多个节点数据库获取相同chunk进行聚合")
    else:
        print("   ⚠️ 没有足够的chunk数据进行聚合测试")
    
    return True


def main():
    """运行所有测试"""
    print("=" * 60)
    print("🧪 分块权重保存数据库系统测试")
    print("=" * 60)
    
    # 清理之前的测试数据
    cleanup_test_data()
    
    tests = [
        ("模型分割算法", test_model_splitting),
        ("Chunk数据提取", test_chunk_extraction),
        ("节点特定数据库", test_node_specific_databases),
        ("Chunk存储和检索", test_chunk_storage_and_retrieval),
        ("存储统计", test_storage_statistics),
        ("模型重构", test_model_reconstruction),
        ("清理功能", test_cleanup_functionality),
        ("聚合场景", test_aggregation_scenario),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            print(f"\n{'='*20} {test_name} {'='*20}")
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"\n❌ 测试失败: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))
    
    # 打印测试总结
    print("\n" + "=" * 60)
    print("📊 测试总结")
    print("=" * 60)
    
    for test_name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"   {test_name}: {status}")
    
    total_tests = len(results)
    passed_tests = sum(1 for _, success in results if success)
    
    print(f"\n   总计: {passed_tests}/{total_tests} 个测试通过")
    
    if passed_tests == total_tests:
        print("\n🎉 所有测试通过!")
        print("✅ 分块权重保存数据库系统运行正常")
        print("\n📝 系统特性:")
        print("   1. 使用您的算法进行模型分块")
        print("   2. 按节点名创建独立数据库文件")
        print("   3. 支持chunk的存储、检索和重构")
        print("   4. 提供存储统计和清理功能")
        print("   5. 支持多节点聚合场景")
    else:
        print(f"\n❌ {total_tests - passed_tests} 个测试失败")


if __name__ == "__main__":
    main()