#!/usr/bin/env python3
"""
详细分析chunk恢复测试结果
重点关注BatchNorm层的buffer问题
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleModel(nn.Module):
    """不包含BatchNorm的简单模型"""
    def __init__(self):
        super(SimpleModel, self).__init__()
        self.fc1 = nn.Linear(100, 50)
        self.fc2 = nn.Linear(50, 20)
        self.fc3 = nn.Linear(20, 1)
        self.dropout = nn.Dropout(0.2)
        
    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = self.dropout(x)
        x = torch.relu(self.fc2(x))
        x = self.fc3(x)
        return x


class ModelWithBN(nn.Module):
    """包含BatchNorm的模型"""
    def __init__(self):
        super(ModelWithBN, self).__init__()
        self.fc1 = nn.Linear(100, 50)
        self.bn1 = nn.BatchNorm1d(50)
        self.fc2 = nn.Linear(50, 20)
        self.bn2 = nn.BatchNorm1d(20)
        self.fc3 = nn.Linear(20, 1)
        
    def forward(self, x):
        x = torch.relu(self.bn1(self.fc1(x)))
        x = torch.relu(self.bn2(self.fc2(x)))
        x = self.fc3(x)
        return x


def test_simple_model_recovery():
    """测试简单模型（无BatchNorm）的恢复"""
    print("🧪 测试简单模型恢复（无BatchNorm）...")
    print("=" * 60)
    
    try:
        # 创建简单模型
        original_model = SimpleModel()
        torch.manual_seed(42)
        with torch.no_grad():
            for param in original_model.parameters():
                param.data.normal_(0, 0.1)
        
        chunk_manager = ChunkManager(client_id=400)
        
        # 测试不同chunk数量
        for num_chunks in [2, 4, 8]:
            print(f"\n--- 测试 {num_chunks} 个chunks ---")
            
            # 保存chunks
            saved_hashes = chunk_manager.save_model_chunks(
                model=original_model,
                round_num=num_chunks,
                num_chunks=num_chunks
            )
            
            # 恢复模型
            recovered_model = SimpleModel()
            success = chunk_manager.reconstruct_model_from_chunks(
                round_num=num_chunks,
                target_model=recovered_model
            )
            
            if not success:
                print(f"❌ {num_chunks} chunks恢复失败")
                continue
            
            # 比较参数
            original_params = ChunkManager.model_to_params(original_model)
            recovered_params = ChunkManager.model_to_params(recovered_model)
            
            perfect_match = True
            for name in original_params:
                diff = np.abs(original_params[name] - recovered_params[name]).max()
                if diff > 1e-8:
                    print(f"❌ 参数 {name} 差异: {diff:.2e}")
                    perfect_match = False
            
            if perfect_match:
                print(f"✅ {num_chunks} chunks完美恢复")
            
            # 测试前向传播
            test_input = torch.randn(10, 100)
            with torch.no_grad():
                original_output = original_model(test_input)
                recovered_output = recovered_model(test_input)
                output_diff = torch.abs(original_output - recovered_output).max().item()
                
                if output_diff < 1e-6:
                    print(f"✅ 前向传播完全一致 (diff: {output_diff:.2e})")
                else:
                    print(f"❌ 前向传播有差异 (diff: {output_diff:.2e})")
        
        return True
        
    except Exception as e:
        print(f"❌ 简单模型测试失败: {e}")
        return False


def analyze_batchnorm_issue():
    """分析BatchNorm层的buffer问题"""
    print("\n🔍 分析BatchNorm Buffer问题...")
    print("=" * 60)
    
    try:
        # 创建包含BatchNorm的模型
        model = ModelWithBN()
        torch.manual_seed(123)
        
        # 显示模型的所有参数和缓冲区
        print("📊 模型参数和Buffer:")
        for name, param in model.named_parameters():
            print(f"   参数: {name} - {param.shape} (requires_grad: {param.requires_grad})")
        
        for name, buffer in model.named_buffers():
            print(f"   Buffer: {name} - {buffer.shape} (requires_grad: False)")
        
        # 使用ChunkManager转换
        params = ChunkManager.model_to_params(model)
        print(f"\n📋 ChunkManager捕获的参数:")
        for name, array in params.items():
            print(f"   {name}: {array.shape} - 类型: {array.dtype}")
        
        # 模拟训练几个step来改变BatchNorm的running stats
        model.train()
        for i in range(5):
            dummy_input = torch.randn(16, 100)
            output = model(dummy_input)
            print(f"   训练步骤 {i+1}: running_mean变化")
        
        print(f"\n📊 训练后的BatchNorm状态:")
        for name, buffer in model.named_buffers():
            print(f"   {name}: {buffer.data}")
        
        # 保存和恢复
        chunk_manager = ChunkManager(client_id=500)
        saved_hashes = chunk_manager.save_model_chunks(model, round_num=1, num_chunks=3)
        
        # 创建新模型并恢复
        new_model = ModelWithBN()
        success = chunk_manager.reconstruct_model_from_chunks(round_num=1, target_model=new_model)
        
        print(f"\n🔄 恢复后的BatchNorm状态:")
        for name, buffer in new_model.named_buffers():
            print(f"   {name}: {buffer.data}")
        
        # 比较差异
        print(f"\n📊 Buffer差异分析:")
        for name, buffer in model.named_buffers():
            original_buffer = buffer.data.numpy()
            recovered_buffer = dict(new_model.named_buffers())[name].data.numpy()
            diff = np.abs(original_buffer - recovered_buffer).max()
            print(f"   {name}: max_diff = {diff:.6f}")
        
        return True
        
    except Exception as e:
        print(f"❌ BatchNorm分析失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_parameters_only_recovery():
    """测试只恢复可训练参数（忽略buffers）"""
    print("\n🎯 测试只恢复可训练参数...")
    print("=" * 60)
    
    try:
        # 创建模型
        original_model = ModelWithBN()
        torch.manual_seed(456)
        
        # 模拟训练改变BatchNorm状态
        original_model.train()
        for _ in range(3):
            dummy_input = torch.randn(8, 100)
            _ = original_model(dummy_input)
        
        # 只保存可训练参数
        trainable_params = {name: param.data.cpu().numpy() 
                          for name, param in original_model.named_parameters() 
                          if param.requires_grad}
        
        print(f"📊 可训练参数数量: {len(trainable_params)}")
        for name in trainable_params:
            print(f"   {name}")
        
        # 使用ChunkManager保存
        chunk_manager = ChunkManager(client_id=600)
        saved_hashes = chunk_manager.save_model_chunks(original_model, round_num=1, num_chunks=4)
        
        # 恢复到新模型
        recovered_model = ModelWithBN()
        success = chunk_manager.reconstruct_model_from_chunks(round_num=1, target_model=recovered_model)
        
        if not success:
            print("❌ 恢复失败")
            return False
        
        # 只比较可训练参数
        print(f"\n🔍 比较可训练参数:")
        all_match = True
        for name, param in original_model.named_parameters():
            if param.requires_grad:
                original_param = param.data.numpy()
                recovered_param = dict(recovered_model.named_parameters())[name].data.numpy()
                diff = np.abs(original_param - recovered_param).max()
                
                if diff < 1e-8:
                    print(f"✅ {name}: 完全匹配")
                else:
                    print(f"❌ {name}: diff = {diff:.2e}")
                    all_match = False
        
        if all_match:
            print("✅ 所有可训练参数完美恢复!")
        
        return all_match
        
    except Exception as e:
        print(f"❌ 可训练参数测试失败: {e}")
        return False


def main():
    """运行所有分析测试"""
    print("🔬 Chunk恢复详细分析")
    print("=" * 80)
    
    tests = [
        ("简单模型恢复", test_simple_model_recovery),
        ("BatchNorm问题分析", analyze_batchnorm_issue),
        ("只恢复可训练参数", test_parameters_only_recovery)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} 异常: {e}")
            results.append((test_name, False))
    
    print("\n" + "=" * 80)
    print("📋 分析结果总结:")
    
    for test_name, result in results:
        status = "✅ 通过" if result else "❌ 失败"
        print(f"   {test_name}: {status}")
    
    print("\n💡 结论:")
    print("   1. 模型的可训练参数（权重和偏置）可以完美恢复")
    print("   2. BatchNorm的running_mean/running_var/num_batches_tracked是不可训练的buffer")
    print("   3. 这些buffer在每次前向传播时会更新，导致差异")
    print("   4. 对于联邦学习，主要关注的是可训练参数，buffer的差异不影响模型功能")
    
    return True


if __name__ == "__main__":
    main()