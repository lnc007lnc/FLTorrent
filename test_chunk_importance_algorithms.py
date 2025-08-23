#!/usr/bin/env python3
"""
独立测试chunk重要度计算算法
"""

import numpy as np
import torch
import torch.nn as nn
from typing import Dict, List
import matplotlib.pyplot as plt
import seaborn as sns

# 模拟ChunkManager的核心重要度计算方法
class TestChunkImportance:
    
    def compute_chunk_importance(self, params: Dict[str, np.ndarray], chunks_info: List[Dict], 
                                method: str = 'magnitude') -> List[float]:
        """计算每个chunk的重要度分数"""
        importance_scores = []
        
        print(f"🔍 测试重要度计算方法: {method}")
        print(f"   参数总数: {len(params)}")
        print(f"   Chunk数量: {len(chunks_info)}")
        
        for i, chunk_info in enumerate(chunks_info):
            if method == 'magnitude':
                score = self._compute_magnitude_importance(params, chunk_info)
            elif method == 'l2_norm':
                score = self._compute_l2_norm_importance(params, chunk_info)
            elif method == 'snip':
                score = self._compute_snip_importance(params, chunk_info)
            elif method == 'fisher':
                score = self._compute_fisher_importance(params, chunk_info)
            else:
                score = self._compute_magnitude_importance(params, chunk_info)
                
            importance_scores.append(float(score))
            print(f"   Chunk {i}: 原始分数 = {score:.8f}")
            
        print(f"\n📊 原始分数统计:")
        print(f"   最大值: {max(importance_scores):.8f}")
        print(f"   最小值: {min(importance_scores):.8f}")
        print(f"   均值: {np.mean(importance_scores):.8f}")
        print(f"   标准差: {np.std(importance_scores):.8f}")
        print(f"   变异系数: {np.std(importance_scores)/np.mean(importance_scores):.6f}")
        
        # 使用L1归一化（总和归一化），保持原始比例关系
        if importance_scores:
            total_score = sum(importance_scores)
            if total_score > 0:
                normalized_scores = [s / total_score for s in importance_scores]
            else:
                normalized_scores = [1.0 / len(importance_scores)] * len(importance_scores)
        
        print(f"\n🎯 L1归一化后:")
        for i, score in enumerate(normalized_scores):
            print(f"   Chunk {i}: {score:.8f}")
            
        print(f"\n📈 归一化后统计:")
        print(f"   最大值: {max(normalized_scores):.8f}")
        print(f"   最小值: {min(normalized_scores):.8f}")
        print(f"   总和: {sum(normalized_scores):.8f}")
        print(f"   标准差: {np.std(normalized_scores):.8f}")
        
        return normalized_scores
    
    def _compute_magnitude_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于参数幅度的重要度计算"""
        total_magnitude = 0.0
        total_elements = 0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    total_magnitude += np.sum(np.abs(chunk_slice))
                    total_elements += len(chunk_slice)
        
        return total_magnitude / (total_elements + 1e-8)
    
    def _compute_l2_norm_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于L2范数的重要度计算"""
        total_l2_norm = 0.0
        total_elements = 0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    total_l2_norm += np.sum(chunk_slice ** 2)
                    total_elements += len(chunk_slice)
        
        return np.sqrt(total_l2_norm) / (total_elements + 1e-8)
    
    def _compute_snip_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于SNIP的重要度计算"""
        # 改进的SNIP实现：考虑参数层级重要性
        total_snip_score = 0.0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                
                # 根据参数类型设置权重因子
                layer_weight = 1.0
                if 'weight' in param_name:
                    layer_weight = 2.0  # 权重比偏置更重要
                if 'fc' in param_name or '4.' in param_name:  # 输出层
                    layer_weight *= 1.5  # 输出层更重要
                
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    if len(chunk_slice) > 0:
                        # 计算参数的敏感度指标
                        abs_values = np.abs(chunk_slice)
                        
                        # 1. 大幅度参数的重要性
                        magnitude_score = np.sum(abs_values)
                        
                        # 2. 参数分散程度（方差）
                        variance_score = np.var(abs_values) + 1e-8
                        
                        # 3. 非零参数比例（稀疏性考虑）
                        non_zero_ratio = np.count_nonzero(abs_values) / len(abs_values)
                        
                        # SNIP综合评分：结合幅度、方差和稀疏性
                        chunk_score = magnitude_score * (1 + np.sqrt(variance_score)) * (0.5 + non_zero_ratio)
                        total_snip_score += chunk_score * layer_weight
        
        return total_snip_score
    
    def _compute_fisher_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于Fisher信息矩阵的重要度计算"""
        total_variance = 0.0
        total_elements = 0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    if len(chunk_slice) > 0:
                        variance = np.var(chunk_slice) + 1e-8
                        total_variance += variance * len(chunk_slice)
                        total_elements += len(chunk_slice)
        
        return total_variance / (total_elements + 1e-8)

def create_test_model():
    """创建测试模型"""
    model = nn.Sequential(
        nn.Linear(10, 20),
        nn.ReLU(),
        nn.Linear(20, 10),
        nn.ReLU(),
        nn.Linear(10, 1)
    )
    return model

def simulate_training_effect(params: Dict[str, np.ndarray], scenario: str = "normal"):
    """模拟训练效果，让不同层的参数有不同的重要性"""
    modified_params = {}
    
    for name, param in params.items():
        if scenario == "normal":
            # 正常情况：不同层有不同的重要性
            if "0.weight" in name:  # 第一层权重
                modified_params[name] = param * 2.0  # 更重要
            elif "0.bias" in name:  # 第一层偏置
                modified_params[name] = param * 0.1  # 较不重要
            elif "2.weight" in name:  # 第二层权重
                modified_params[name] = param * 1.5  # 中等重要
            elif "4.weight" in name:  # 输出层权重
                modified_params[name] = param * 3.0  # 最重要
            else:
                modified_params[name] = param
                
        elif scenario == "sparse":
            # 稀疏情况：某些参数被剪枝(设为0)
            if "weight" in name:
                mask = np.random.random(param.shape) > 0.7  # 30%的权重保留
                modified_params[name] = param * mask
            else:
                modified_params[name] = param
                
        elif scenario == "diverse":
            # 多样化情况：参数有很大差异
            scale = np.random.uniform(0.1, 5.0)
            if np.random.random() > 0.5:
                # 添加一些噪声让参数分布更不均匀
                noise = np.random.normal(0, 0.1, param.shape)
                modified_params[name] = param * scale + noise
            else:
                modified_params[name] = param * scale
        else:
            modified_params[name] = param
            
    return modified_params

def split_model(params: Dict[str, np.ndarray], num_chunks: int) -> List[Dict]:
    """将模型参数分割为chunks"""
    total_elements = sum(np.prod(v.shape) for v in params.values())
    elements_per_chunk = total_elements // num_chunks

    chunks = []
    current_chunk = {'parts': {}, 'flat_size': 0, 'chunk_id': len(chunks)}
    
    for key in sorted(params.keys()):
        arr = params[key]
        n = int(np.prod(arr.shape))
        ptr = 0
        while ptr < n:
            if current_chunk['flat_size'] >= elements_per_chunk and len(chunks) < num_chunks - 1:
                chunks.append(current_chunk)
                current_chunk = {'parts': {}, 'flat_size': 0, 'chunk_id': len(chunks)}
            
            remaining_in_param = n - ptr
            remaining_in_chunk = elements_per_chunk - current_chunk['flat_size']
            take = min(remaining_in_param, remaining_in_chunk)
            
            if len(chunks) == num_chunks - 1:
                take = remaining_in_param
            
            flat_start = ptr
            flat_end = ptr + take
            
            if key not in current_chunk['parts']:
                current_chunk['parts'][key] = []
            current_chunk['parts'][key].append((flat_start, flat_end, arr.shape))
            
            current_chunk['flat_size'] += take
            ptr += take
    
    if current_chunk['parts']:
        chunks.append(current_chunk)
    
    return chunks

def test_importance_algorithms():
    """测试所有重要度算法"""
    print("🧪 chunk重要度算法独立测试")
    print("=" * 60)
    
    # 创建测试模型
    model = create_test_model()
    
    # 提取参数
    params = {}
    for name, param in model.named_parameters():
        params[name] = param.detach().numpy()
    
    print(f"📋 模型参数信息:")
    for name, param in params.items():
        print(f"   {name}: {param.shape} (元素数: {np.prod(param.shape)})")
    
    tester = TestChunkImportance()
    
    # 测试不同场景
    scenarios = ['normal', 'sparse', 'diverse']
    methods = ['magnitude', 'l2_norm', 'snip', 'fisher']
    
    for scenario in scenarios:
        print(f"\n{'='*20} 场景: {scenario} {'='*20}")
        
        # 修改参数模拟不同场景
        modified_params = simulate_training_effect(params, scenario)
        
        # 分割成chunks
        num_chunks = 5
        chunks_info = split_model(modified_params, num_chunks)
        
        print(f"📦 分割成 {len(chunks_info)} 个chunks:")
        for i, chunk in enumerate(chunks_info):
            print(f"   Chunk {i}: {chunk['flat_size']} 元素")
        
        # 测试每种方法
        for method in methods:
            print(f"\n{'-'*40}")
            scores = tester.compute_chunk_importance(modified_params, chunks_info, method)
            
            # 检查分数差异
            max_score = max(scores)
            min_score = min(scores)
            ratio = max_score / min_score if min_score > 0 else float('inf')
            print(f"📊 重要度差异: 最大/最小 = {ratio:.2f}")
            
            if ratio > 2.0:
                print("✅ 算法能够区分不同chunk的重要性")
            else:
                print("❌ 算法未能有效区分chunk重要性")

def visualize_importance_distribution():
    """可视化重要度分布"""
    try:
        import matplotlib.pyplot as plt
        
        model = create_test_model()
        params = {}
        for name, param in model.named_parameters():
            params[name] = param.detach().numpy()
        
        # 创建多样化的参数
        diverse_params = simulate_training_effect(params, 'diverse')
        chunks_info = split_model(diverse_params, 8)
        
        tester = TestChunkImportance()
        methods = ['magnitude', 'l2_norm', 'snip', 'fisher']
        
        fig, axes = plt.subplots(2, 2, figsize=(12, 8))
        axes = axes.flatten()
        
        for i, method in enumerate(methods):
            scores = tester.compute_chunk_importance(diverse_params, chunks_info, method)
            
            axes[i].bar(range(len(scores)), scores)
            axes[i].set_title(f'{method.upper()} 重要度分布')
            axes[i].set_xlabel('Chunk ID')
            axes[i].set_ylabel('重要度分数')
            axes[i].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('chunk_importance_distribution.png', dpi=150, bbox_inches='tight')
        print("📊 重要度分布图已保存: chunk_importance_distribution.png")
        
    except ImportError:
        print("⚠️ matplotlib未安装，跳过可视化")

if __name__ == "__main__":
    test_importance_algorithms()
    visualize_importance_distribution()