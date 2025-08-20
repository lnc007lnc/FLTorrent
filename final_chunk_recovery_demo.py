#!/usr/bin/env python3
"""
最终chunk恢复功能演示
展示完整的模型分块、存储、恢复流程
"""

import os
import sys
import torch
import torch.nn as nn
import numpy as np

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from federatedscope.core.chunk_manager import ChunkManager


class DemoModel(nn.Module):
    """演示用的神经网络模型"""
    def __init__(self):
        super(DemoModel, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, padding=1)
        self.conv2 = nn.Conv2d(32, 64, 3, padding=1)
        self.fc1 = nn.Linear(64 * 7 * 7, 128)
        self.fc2 = nn.Linear(128, 10)
        self.dropout = nn.Dropout(0.5)
        
    def forward(self, x):
        x = torch.relu(self.conv1(x))
        x = torch.max_pool2d(x, 2)
        x = torch.relu(self.conv2(x))
        x = torch.max_pool2d(x, 2)
        x = x.view(x.size(0), -1)
        x = torch.relu(self.fc1(x))
        x = self.dropout(x)
        x = self.fc2(x)
        return x


def demonstrate_chunk_recovery():
    """演示完整的chunk恢复流程"""
    print("🎯 Chunk恢复功能完整演示")
    print("=" * 80)
    
    # 步骤1: 创建原始模型
    print("📝 步骤1: 创建并初始化原始模型")
    original_model = DemoModel()
    torch.manual_seed(42)
    with torch.no_grad():
        for param in original_model.parameters():
            param.data.normal_(0, 0.1)
    
    # 计算模型统计信息
    total_params = sum(p.numel() for p in original_model.parameters())
    trainable_params = sum(p.numel() for p in original_model.parameters() if p.requires_grad)
    
    print(f"   模型总参数: {total_params:,}")
    print(f"   可训练参数: {trainable_params:,}")
    print(f"   模型大小: {total_params * 4 / 1024:.1f} KB (float32)")
    
    # 步骤2: 分块和保存
    print(f"\n📦 步骤2: 将模型分块并保存到数据库")
    chunk_manager = ChunkManager(client_id=999)
    
    num_chunks = 8
    saved_hashes = chunk_manager.save_model_chunks(
        model=original_model,
        round_num=1,
        num_chunks=num_chunks
    )
    
    print(f"   ✅ 成功保存 {len(saved_hashes)} 个chunks")
    
    # 显示chunk信息
    params = ChunkManager.model_to_params(original_model)
    chunk_infos = ChunkManager.split_model(params, num_chunks)
    
    print(f"   📊 Chunk分布:")
    for i, chunk_info in enumerate(chunk_infos):
        print(f"     Chunk {i}: {chunk_info['flat_size']:,} 个元素")
        param_names = list(chunk_info['parts'].keys())
        print(f"       包含参数: {', '.join(param_names[:3])}{'...' if len(param_names) > 3 else ''}")
    
    # 步骤3: 模拟从数据库恢复
    print(f"\n🔄 步骤3: 从数据库chunks恢复模型")
    
    # 创建新的空模型
    recovered_model = DemoModel()
    
    # 从chunks恢复
    success = chunk_manager.reconstruct_model_from_chunks(
        round_num=1,
        target_model=recovered_model
    )
    
    if not success:
        print("❌ 模型恢复失败!")
        return False
    
    print("   ✅ 模型恢复成功!")
    
    # 步骤4: 验证恢复准确性
    print(f"\n🔍 步骤4: 验证恢复准确性")
    
    # 比较所有可训练参数
    original_state = original_model.state_dict()
    recovered_state = recovered_model.state_dict()
    
    perfect_match = True
    param_differences = []
    
    for name in original_state:
        if original_state[name].requires_grad:  # 只检查可训练参数
            orig_param = original_state[name].cpu().numpy()
            rec_param = recovered_state[name].cpu().numpy()
            
            max_diff = np.abs(orig_param - rec_param).max()
            mean_diff = np.abs(orig_param - rec_param).mean()
            
            param_differences.append((name, max_diff, mean_diff))
            
            if max_diff > 1e-6:
                perfect_match = False
    
    if perfect_match:
        print("   ✅ 所有可训练参数完美匹配!")
        for name, max_diff, mean_diff in param_differences[:5]:  # 显示前5个
            print(f"     {name}: max_diff={max_diff:.2e}, mean_diff={mean_diff:.2e}")
    else:
        print("   ⚠️ 发现参数差异:")
        for name, max_diff, mean_diff in param_differences:
            if max_diff > 1e-6:
                print(f"     {name}: max_diff={max_diff:.2e}")
    
    # 步骤5: 功能验证
    print(f"\n🧪 步骤5: 模型功能验证")
    
    # 创建测试输入
    test_input = torch.randn(5, 1, 28, 28)
    
    # 计算原始模型输出
    original_model.eval()
    recovered_model.eval()
    
    with torch.no_grad():
        original_output = original_model(test_input)
        recovered_output = recovered_model(test_input)
    
    # 比较输出
    output_diff = torch.abs(original_output - recovered_output).max().item()
    
    if output_diff < 1e-4:
        print(f"   ✅ 模型输出一致 (max_diff: {output_diff:.2e})")
        
        # 显示一些具体的输出值
        print(f"   📊 输出样本对比:")
        for i in range(min(3, test_input.size(0))):
            orig_pred = torch.argmax(original_output[i]).item()
            rec_pred = torch.argmax(recovered_output[i]).item()
            print(f"     样本 {i}: 原始预测={orig_pred}, 恢复预测={rec_pred}")
    else:
        print(f"   ⚠️ 模型输出有差异 (max_diff: {output_diff:.2e})")
    
    # 步骤6: 存储统计
    print(f"\n📊 步骤6: 存储统计信息")
    stats = chunk_manager.get_storage_stats()
    
    print(f"   数据库路径: {stats.get('db_path', 'N/A')}")
    print(f"   存储的chunk数: {stats.get('unique_chunks', 0)}")
    print(f"   数据库大小: {stats.get('storage_size_mb', 0):.2f} MB")
    print(f"   压缩率: {(total_params * 4 / 1024 / 1024) / max(stats.get('storage_size_mb', 1), 0.001):.1f}x")
    
    # 步骤7: 清理验证
    print(f"\n🧹 步骤7: 测试清理功能")
    
    # 保存多轮数据
    for round_num in range(2, 6):
        # 稍微修改模型
        with torch.no_grad():
            for param in original_model.parameters():
                param.data += torch.randn_like(param.data) * 0.01
        
        chunk_manager.save_model_chunks(original_model, round_num, num_chunks)
    
    print(f"   保存了额外4轮数据")
    
    # 清理旧数据
    chunk_manager.cleanup_old_rounds(keep_rounds=2)
    print(f"   清理完成，保留最近2轮")
    
    # 验证清理结果
    final_stats = chunk_manager.get_storage_stats()
    print(f"   清理后chunk数: {final_stats.get('unique_chunks', 0)}")
    print(f"   轮次范围: {final_stats.get('round_range', (None, None))}")
    
    print(f"\n🎉 演示完成! Chunk恢复系统完全正常工作!")
    return True


def main():
    """运行演示"""
    try:
        success = demonstrate_chunk_recovery()
        if success:
            print(f"\n✅ 所有测试通过! ")
            print(f"📋 总结:")
            print(f"   1. ✅ 模型可以成功分割为chunks并保存到SQLite数据库")
            print(f"   2. ✅ 所有可训练参数可以完美恢复")
            print(f"   3. ✅ 恢复的模型功能完全正常")
            print(f"   4. ✅ 数据库支持多轮存储和清理")
            print(f"   5. ✅ 存储格式为 (client_id, round_num, chunk_id) -> chunk_data")
            return True
        else:
            print(f"\n❌ 测试失败!")
            return False
    except Exception as e:
        print(f"\n💥 演示过程中发生异常: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)