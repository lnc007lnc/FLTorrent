#!/usr/bin/env python3
"""
测试chunk恢复功能
验证保存的chunks能否完全恢复原始模型
"""

import os
import sys
import torch
import torch.nn as nn
import numpy as np

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from federatedscope.core.chunk_manager import ChunkManager
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestModel(nn.Module):
    """测试用的神经网络模型"""
    def __init__(self):
        super(TestModel, self).__init__()
        self.conv1 = nn.Conv2d(3, 16, 3, padding=1)
        self.conv2 = nn.Conv2d(16, 32, 3, padding=1)
        self.fc1 = nn.Linear(32 * 8 * 8, 128)
        self.fc2 = nn.Linear(128, 64)
        self.fc3 = nn.Linear(64, 10)
        self.dropout = nn.Dropout(0.5)
        self.bn1 = nn.BatchNorm2d(16)
        self.bn2 = nn.BatchNorm2d(32)
        
    def forward(self, x):
        x = torch.relu(self.bn1(self.conv1(x)))
        x = torch.max_pool2d(x, 2)
        x = torch.relu(self.bn2(self.conv2(x)))
        x = torch.max_pool2d(x, 2)
        x = x.view(x.size(0), -1)
        x = torch.relu(self.fc1(x))
        x = self.dropout(x)
        x = torch.relu(self.fc2(x))
        x = self.fc3(x)
        return x


def compare_models(model1, model2, tolerance=1e-6):
    """比较两个模型的参数是否完全一致"""
    params1 = ChunkManager.model_to_params(model1)
    params2 = ChunkManager.model_to_params(model2)
    
    if set(params1.keys()) != set(params2.keys()):
        print("❌ 模型参数名称不匹配")
        return False, {}
    
    differences = {}
    all_match = True
    
    for name in params1.keys():
        p1, p2 = params1[name], params2[name]
        
        if p1.shape != p2.shape:
            print(f"❌ 参数 {name} 形状不匹配: {p1.shape} vs {p2.shape}")
            all_match = False
            differences[name] = {'error': 'shape_mismatch', 'shapes': (p1.shape, p2.shape)}
            continue
        
        # 计算绝对差异
        abs_diff = np.abs(p1 - p2)
        max_diff = np.max(abs_diff)
        mean_diff = np.mean(abs_diff)
        
        differences[name] = {
            'max_diff': max_diff,
            'mean_diff': mean_diff,
            'shape': p1.shape,
            'total_elements': p1.size
        }
        
        if max_diff > tolerance:
            print(f"❌ 参数 {name} 差异超过容忍度: max_diff={max_diff:.2e} > {tolerance:.2e}")
            all_match = False
    
    return all_match, differences


def test_chunk_recovery_accuracy():
    """测试chunk恢复的准确性"""
    print("🧪 测试chunk恢复准确性...")
    print("=" * 60)
    
    try:
        # 创建并初始化原始模型
        original_model = TestModel()
        
        # 用固定的随机种子初始化，确保可重现
        torch.manual_seed(42)
        with torch.no_grad():
            for param in original_model.parameters():
                param.data.normal_(0, 0.1)
        
        print(f"📊 原始模型参数统计:")
        total_params = sum(p.numel() for p in original_model.parameters())
        print(f"   总参数数量: {total_params:,}")
        
        for name, param in original_model.named_parameters():
            print(f"   {name}: {param.shape} ({param.numel():,} 个元素)")
        
        # 创建ChunkManager并保存chunks
        chunk_manager = ChunkManager(client_id=100)
        
        print(f"\n🔪 测试不同chunk数量的恢复效果:")
        
        for num_chunks in [3, 5, 8, 12]:
            print(f"\n--- 测试 {num_chunks} 个chunks ---")
            
            # 保存模型为chunks
            saved_hashes = chunk_manager.save_model_chunks(
                model=original_model,
                round_num=num_chunks,  # 使用chunk数量作为轮次区分
                num_chunks=num_chunks
            )
            
            print(f"✅ 保存了 {len(saved_hashes)} 个chunks")
            
            # 创建新模型用于恢复
            recovered_model = TestModel()
            
            # 从chunks恢复模型
            success = chunk_manager.reconstruct_model_from_chunks(
                round_num=num_chunks,
                target_model=recovered_model
            )
            
            if not success:
                print(f"❌ {num_chunks} chunks恢复失败")
                continue
            
            # 比较原始模型和恢复模型
            models_match, differences = compare_models(original_model, recovered_model)
            
            if models_match:
                print(f"✅ {num_chunks} chunks完美恢复 - 所有参数完全一致")
            else:
                print(f"⚠️ {num_chunks} chunks恢复有差异:")
                for name, diff in differences.items():
                    if 'error' in diff:
                        print(f"   {name}: {diff['error']}")
                    else:
                        print(f"   {name}: max_diff={diff['max_diff']:.2e}, mean_diff={diff['mean_diff']:.2e}")
            
            # 验证模型功能性 - 使用相同输入测试前向传播
            test_input = torch.randn(1, 3, 32, 32)
            
            with torch.no_grad():
                original_output = original_model(test_input)
                recovered_output = recovered_model(test_input)
                
                output_diff = torch.abs(original_output - recovered_output).max().item()
                
                if output_diff < 1e-6:
                    print(f"✅ 前向传播输出完全一致 (diff: {output_diff:.2e})")
                else:
                    print(f"⚠️ 前向传播输出有差异 (max_diff: {output_diff:.2e})")
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_individual_chunk_integrity():
    """测试单个chunk的完整性"""
    print(f"\n🔍 测试单个chunk完整性...")
    print("=" * 60)
    
    try:
        # 创建模型
        model = TestModel()
        torch.manual_seed(123)
        with torch.no_grad():
            for param in model.parameters():
                param.data.normal_(0, 0.05)
        
        chunk_manager = ChunkManager(client_id=200)
        
        # 保存为6个chunks
        num_chunks = 6
        saved_hashes = chunk_manager.save_model_chunks(model, round_num=1, num_chunks=num_chunks)
        
        print(f"✅ 保存了 {len(saved_hashes)} 个chunks")
        
        # 获取原始参数
        original_params = ChunkManager.model_to_params(model)
        chunk_infos = ChunkManager.split_model(original_params, num_chunks)
        
        print(f"\n📋 验证每个chunk的数据完整性:")
        
        total_recovered_elements = 0
        
        for i in range(num_chunks):
            # 从数据库获取chunk
            chunk_data = chunk_manager.get_chunk_by_id(round_num=1, chunk_id=i)
            
            if chunk_data is None:
                print(f"❌ Chunk {i} 未找到")
                continue
                
            chunk_info, chunk_array = chunk_data
            
            # 验证chunk信息
            expected_info = chunk_infos[i]
            
            if chunk_info['chunk_id'] != expected_info['chunk_id']:
                print(f"❌ Chunk {i} ID不匹配")
                continue
                
            if chunk_info['flat_size'] != expected_info['flat_size']:
                print(f"❌ Chunk {i} 大小不匹配: {chunk_info['flat_size']} vs {expected_info['flat_size']}")
                continue
            
            # 重新提取原始数据进行比较
            expected_data = chunk_manager.extract_chunk_data(original_params, expected_info)
            
            if np.allclose(chunk_array, expected_data, atol=1e-8):
                print(f"✅ Chunk {i}: 数据完整 ({len(chunk_array)} 个元素)")
                total_recovered_elements += len(chunk_array)
            else:
                max_diff = np.max(np.abs(chunk_array - expected_data))
                print(f"❌ Chunk {i}: 数据不匹配 (max_diff: {max_diff:.2e})")
        
        # 验证总元素数量
        total_original_elements = sum(np.prod(v.shape) for v in original_params.values())
        print(f"\n📊 元素数量验证:")
        print(f"   原始模型总元素: {total_original_elements:,}")
        print(f"   恢复的总元素: {total_recovered_elements:,}")
        
        if total_recovered_elements == total_original_elements:
            print(f"✅ 元素数量完全匹配")
            return True
        else:
            print(f"❌ 元素数量不匹配")
            return False
            
    except Exception as e:
        print(f"❌ 单个chunk完整性测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_storage_persistence():
    """测试存储持久性 - 重启后能否恢复"""
    print(f"\n💾 测试存储持久性...")
    print("=" * 60)
    
    try:
        # 第一阶段：保存数据
        print("阶段1: 保存数据...")
        
        model1 = TestModel()
        torch.manual_seed(456)
        with torch.no_grad():
            for param in model1.parameters():
                param.data.uniform_(-0.1, 0.1)
        
        chunk_manager1 = ChunkManager(client_id=300)
        saved_hashes = chunk_manager1.save_model_chunks(model1, round_num=1, num_chunks=4)
        
        print(f"✅ 第一个ChunkManager保存了 {len(saved_hashes)} 个chunks")
        
        # 获取存储统计
        stats1 = chunk_manager1.get_storage_stats()
        print(f"   存储大小: {stats1.get('storage_size_mb', 0):.3f} MB")
        
        # 第二阶段：模拟重启，创建新的ChunkManager实例
        print("\n阶段2: 模拟重启...")
        
        chunk_manager2 = ChunkManager(client_id=300)  # 相同的client_id
        
        # 尝试加载数据
        loaded_chunks = chunk_manager2.load_chunks_by_round(round_num=1)
        print(f"✅ 新的ChunkManager实例加载了 {len(loaded_chunks)} 个chunks")
        
        # 恢复模型
        model2 = TestModel()
        success = chunk_manager2.reconstruct_model_from_chunks(round_num=1, target_model=model2)
        
        if not success:
            print("❌ 模型恢复失败")
            return False
        
        # 比较模型
        models_match, differences = compare_models(model1, model2)
        
        if models_match:
            print("✅ 重启后完美恢复 - 存储持久性验证成功")
            return True
        else:
            print("❌ 重启后恢复有差异")
            for name, diff in differences.items():
                if 'error' not in diff:
                    print(f"   {name}: max_diff={diff['max_diff']:.2e}")
            return False
            
    except Exception as e:
        print(f"❌ 存储持久性测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """运行所有恢复测试"""
    print("🚀 Chunk恢复功能测试套件")
    print("=" * 80)
    
    tests = [
        ("Chunk恢复准确性", test_chunk_recovery_accuracy),
        ("单个Chunk完整性", test_individual_chunk_integrity),
        ("存储持久性", test_storage_persistence)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} - 通过")
            else:
                failed += 1
                print(f"❌ {test_name} - 失败")
        except Exception as e:
            failed += 1
            print(f"❌ {test_name} - 异常: {e}")
    
    print("\n" + "=" * 80)
    print(f"🏁 测试结果: {passed} 通过, {failed} 失败")
    
    if failed == 0:
        print("🎉 所有chunk恢复测试通过! 系统可以完美恢复模型!")
    else:
        print("💥 部分测试失败，请检查chunk恢复逻辑")
    
    return failed == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)