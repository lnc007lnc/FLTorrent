"""
基于您提供算法的模型分块管理系统
使用扁平索引记录参数chunk信息，按节点名建立本地数据库存储chunk
支持实时变化监控和chunk信息上报
"""

import os
import json
import hashlib
import pickle
import sqlite3
import time
import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Callable
import numpy as np
import torch
import torch.nn as nn
from datetime import datetime
import logging

# Import ChunkInfo for change monitoring
from federatedscope.core.chunk_tracker import ChunkInfo, ChunkAction

logger = logging.getLogger(__name__)


class ChunkManager:
    """
    统一管理模型分块逻辑，使用扁平索引记录参数chunk信息。
    每个chunk的定义格式为：
      {
          'chunk_id': int,
          'parts': { key: [ (flat_start, flat_end, shape), ... ] },
          'flat_size': int
      }
    """
    
    def __init__(self, client_id: int, change_callback: Optional[Callable[[ChunkInfo], None]] = None):
        """
        初始化ChunkManager，为指定客户端创建独立的数据库
        
        Args:
            client_id: 客户端ID，用于创建节点特定的数据库文件
            change_callback: 数据库变化时的回调函数，用于向服务器报告chunk变化
        """
        self.client_id = client_id
        self.change_callback = change_callback
        
        # 按节点名创建数据库文件路径: /tmp/client_X/client_X_chunks.db
        client_name = f"client_{client_id}"
        db_dir = os.path.join(os.getcwd(), "tmp", client_name)
        os.makedirs(db_dir, exist_ok=True)
        
        self.db_path = os.path.join(db_dir, f"{client_name}_chunks.db")
        self._init_database()
        
        # 变化监控相关
        self.monitoring_enabled = False
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
        self.last_db_mtime = 0
        
        logger.info(f"📊 初始化节点 {client_id} 的chunk数据库: {self.db_path}")
        
        # 如果提供了回调函数，启动监控
        if change_callback:
            self.start_monitoring()
        
    def _get_optimized_connection(self):
        """获取优化的数据库连接"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        cursor = conn.cursor()
        
        # 启用优化设置
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL") 
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA busy_timeout=30000")  # 30秒超时
        
        return conn
        
    def _init_database(self):
        """初始化SQLite数据库表结构"""
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        # 创建chunk元数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunk_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER NOT NULL,
                chunk_id INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                parts_info TEXT NOT NULL,
                flat_size INTEGER NOT NULL,
                importance_score REAL DEFAULT 0.0,
                pruning_method TEXT DEFAULT 'magnitude',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(round_num, chunk_id)
            )
        ''')
        
        # 升级现有表结构（如果需要）
        try:
            cursor.execute("ALTER TABLE chunk_metadata ADD COLUMN importance_score REAL DEFAULT 0.0")
            logger.info("[ChunkManager] Added importance_score column to chunk_metadata table")
        except sqlite3.OperationalError:
            # 列已存在，忽略
            pass
            
        try:
            cursor.execute("ALTER TABLE chunk_metadata ADD COLUMN pruning_method TEXT DEFAULT 'magnitude'")
            logger.info("[ChunkManager] Added pruning_method column to chunk_metadata table")
        except sqlite3.OperationalError:
            # 列已存在，忽略
            pass
        
        # 创建chunk数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunk_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_hash TEXT UNIQUE NOT NULL,
                data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 创建索引以提高查询性能
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_chunk_metadata_round 
            ON chunk_metadata(round_num)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_chunk_metadata_hash 
            ON chunk_metadata(chunk_hash)
        ''')
        
        conn.commit()
        conn.close()
    
    @staticmethod
    def model_to_params(model: nn.Module) -> Dict[str, np.ndarray]:
        """将模型中所有参数及缓冲区转为 numpy 数组"""
        params = {name: param.data.cpu().numpy() for name, param in model.named_parameters()}
        for name, buffer in model.named_buffers():
            params[name] = buffer.data.cpu().numpy()
        return params

    @staticmethod
    def params_to_model(params: Dict[str, np.ndarray], model: nn.Module):
        """将参数字典加载回模型"""
        for name, param in model.named_parameters():
            if name in params:
                param.data = torch.from_numpy(params[name]).to(param.device)
    
    def compute_chunk_importance(self, params: Dict[str, np.ndarray], chunks_info: List[Dict], 
                                method: str = 'magnitude') -> List[float]:
        """
        计算每个chunk的重要度分数
        
        Args:
            params: 模型参数字典
            chunks_info: chunk信息列表
            method: 重要度计算方法 ('magnitude', 'l2_norm', 'gradient_norm', 'snip')
            
        Returns:
            List[float]: 每个chunk的重要度分数
        """
        importance_scores = []
        
        for chunk_info in chunks_info:
            if method == 'magnitude':
                score = self._compute_magnitude_importance(params, chunk_info)
            elif method == 'l2_norm':
                score = self._compute_l2_norm_importance(params, chunk_info)
            elif method == 'gradient_norm':
                score = self._compute_gradient_norm_importance(params, chunk_info)
            elif method == 'snip':
                score = self._compute_snip_importance(params, chunk_info)
            elif method == 'fisher':
                score = self._compute_fisher_importance(params, chunk_info)
            else:
                logger.warning(f"[ChunkManager] Unknown importance method: {method}, using magnitude")
                score = self._compute_magnitude_importance(params, chunk_info)
                
            importance_scores.append(float(score))
            
        # 使用L1归一化（总和归一化），保持原始比例关系
        if importance_scores:
            total_score = sum(importance_scores)
            if total_score > 0:
                importance_scores = [s / total_score for s in importance_scores]
            else:
                importance_scores = [1.0 / len(importance_scores)] * len(importance_scores)  # 平均分配
                
        logger.info(f"[ChunkManager] Computed chunk importance scores: {[f'{s:.4f}' for s in importance_scores]}")
        return importance_scores
    
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
        
        return total_magnitude / max(total_elements, 1)
    
    def _compute_l2_norm_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于L2范数的重要度计算"""
        total_l2_norm = 0.0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    total_l2_norm += np.linalg.norm(chunk_slice) ** 2
        
        return np.sqrt(total_l2_norm)
    
    def _compute_gradient_norm_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于梯度范数的重要度计算（需要梯度信息）"""
        # 注意：这里简化实现，实际应用中需要梯度信息
        # 作为fallback使用magnitude方法
        return self._compute_magnitude_importance(params, chunk_info)
    
    def _compute_snip_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """基于SNIP (Single-shot Network Pruning)的重要度计算"""
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
        # Fisher信息矩阵的简化版本：使用参数方差作为重要性指标
        total_variance = 0.0
        total_chunks = 0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    if len(chunk_slice) > 1:
                        total_variance += np.var(chunk_slice)
                        total_chunks += 1
        
        return total_variance / max(total_chunks, 1)
    
    def get_chunk_importance_scores(self, round_num: int) -> Dict[int, Dict]:
        """
        获取指定轮次所有chunk的重要度分数
        
        Args:
            round_num: 目标轮次
            
        Returns:
            Dict[chunk_id, {'importance_score': float, 'pruning_method': str, 'flat_size': int}]
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT chunk_id, importance_score, pruning_method, flat_size, chunk_hash
                FROM chunk_metadata 
                WHERE round_num = ?
                ORDER BY chunk_id
            ''', (round_num,))
            
            results = cursor.fetchall()
            conn.close()
            
            chunk_scores = {}
            for chunk_id, importance_score, pruning_method, flat_size, chunk_hash in results:
                chunk_scores[chunk_id] = {
                    'importance_score': float(importance_score) if importance_score is not None else 0.0,
                    'pruning_method': pruning_method or 'unknown',
                    'flat_size': flat_size,
                    'chunk_hash': chunk_hash[:8] + '...'  # 显示简短hash
                }
            
            return chunk_scores
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to get chunk importance scores for round {round_num}: {e}")
            return {}

    @staticmethod
    def split_model(params: Dict[str, np.ndarray], num_chunks: int) -> List[Dict]:
        """
        将模型参数均匀分割为指定数量的chunk，记录每个chunk中各参数的扁平索引区间。
        返回列表，每个元素格式为：
          {
              'chunk_id': int,
              'parts': { key: [ (flat_start, flat_end, shape), ... ] },
              'flat_size': int
          }
        """
        total_elements = sum(np.prod(v.shape) for v in params.values())
        elements_per_chunk = total_elements // num_chunks

        chunks = []
        current_chunk = {'parts': {}, 'flat_size': 0, 'chunk_id': len(chunks)}
        
        # 对每个参数，按照扁平顺序进行切分
        for key in sorted(params.keys()):
            arr = params[key]
            n = int(np.prod(arr.shape))
            ptr = 0
            while ptr < n:
                # 检查是否需要开始新的chunk
                if current_chunk['flat_size'] >= elements_per_chunk and len(chunks) < num_chunks - 1:
                    chunks.append(current_chunk)
                    current_chunk = {'parts': {}, 'flat_size': 0, 'chunk_id': len(chunks)}
                
                # 计算可以放入当前chunk的元素数量
                if len(chunks) < num_chunks - 1:
                    remaining = elements_per_chunk - current_chunk['flat_size']
                    take = min(remaining, n - ptr)
                else:
                    # 最后一个chunk包含所有剩余元素
                    take = n - ptr
                    
                # 为当前chunk中参数 key 添加这一段信息
                if key not in current_chunk['parts']:
                    current_chunk['parts'][key] = []
                current_chunk['parts'][key].append((int(ptr), int(ptr + take), arr.shape))
                current_chunk['flat_size'] += take
                ptr += take
                
        # 添加最后一个chunk
        if current_chunk['flat_size'] > 0:
            chunks.append(current_chunk)
            
        return chunks
    
    def extract_chunk_data(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> np.ndarray:
        """
        根据chunk信息从模型参数中提取对应的数据
        
        Args:
            params: 模型参数字典
            chunk_info: chunk的元数据信息
            
        Returns:
            扁平化的chunk数据数组
        """
        chunk_data = []
        
        for key, parts in chunk_info['parts'].items():
            if key not in params:
                logger.warning(f"⚠️ 参数 {key} 在模型中未找到")
                continue
                
            arr_flat = params[key].flatten()
            for flat_start, flat_end, shape in parts:
                chunk_data.append(arr_flat[flat_start:flat_end])
                
        if chunk_data:
            return np.concatenate(chunk_data)
        else:
            return np.array([])
    
    def save_model_chunks(self, model: nn.Module, round_num: int, num_chunks: int = 10, keep_rounds: int = 2, 
                         importance_method: str = 'magnitude') -> List[str]:
        """
        将模型分割成chunks并保存到节点特定的数据库
        
        Args:
            model: PyTorch模型
            round_num: 训练轮次
            num_chunks: 分割的chunk数量
            keep_rounds: 保留最近几轮的数据，默认2轮
            importance_method: chunk重要度计算方法 ('magnitude', 'l2_norm', 'snip', 'fisher')
            
        Returns:
            保存的chunk哈希列表
        """
        try:
            # 将模型转换为参数字典
            params = self.model_to_params(model)
            
            # 分割模型
            chunks_info = self.split_model(params, num_chunks)
            
            # 🧠 计算chunk重要度分数
            logger.info(f"[ChunkManager] Computing chunk importance using method: {importance_method}")
            importance_scores = self.compute_chunk_importance(params, chunks_info, importance_method)
            
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            saved_hashes = []
            
            for i, chunk_info in enumerate(chunks_info):
                # 提取chunk数据
                chunk_data = self.extract_chunk_data(params, chunk_info)
                
                # 计算chunk哈希
                chunk_bytes = pickle.dumps(chunk_data)
                chunk_hash = hashlib.sha256(chunk_bytes).hexdigest()
                
                # 保存chunk数据（如果不存在）
                cursor.execute(
                    "INSERT OR IGNORE INTO chunk_data (chunk_hash, data) VALUES (?, ?)",
                    (chunk_hash, chunk_bytes)
                )
                
                # 保存chunk元数据（包含重要度分数）
                parts_json = json.dumps(chunk_info['parts'])
                importance_score = importance_scores[i] if i < len(importance_scores) else 0.0
                
                cursor.execute('''
                    INSERT OR REPLACE INTO chunk_metadata 
                    (round_num, chunk_id, chunk_hash, parts_info, flat_size, importance_score, pruning_method)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (round_num, chunk_info['chunk_id'], chunk_hash, 
                     parts_json, chunk_info['flat_size'], importance_score, importance_method))
                
                saved_hashes.append(chunk_hash)
                
                # 报告chunk变化
                if self.change_callback:
                    self.report_chunk_change(
                        round_num=round_num,
                        chunk_id=chunk_info['chunk_id'],
                        action=ChunkAction.ADD.value,
                        chunk_hash=chunk_hash,
                        chunk_size=chunk_info['flat_size']
                    )
                
            conn.commit()
            conn.close()
            
            # 自动清理旧轮次数据，保留最近几轮
            self.cleanup_old_rounds(keep_rounds=keep_rounds)
            
            logger.debug(f"💾 节点 {self.client_id}: 第{round_num}轮保存了 {len(saved_hashes)} 个chunks")
            return saved_hashes
            
        except Exception as e:
            logger.error(f"❌ 保存模型chunks失败: {e}")
            return []
    
    def load_chunks_by_round(self, round_num: int) -> List[Tuple[Dict, np.ndarray]]:
        """
        加载指定轮次的所有chunks
        
        Args:
            round_num: 训练轮次
            
        Returns:
            (chunk_info, chunk_data) 元组列表
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 查询chunk元数据
            cursor.execute('''
                SELECT chunk_id, chunk_hash, parts_info, flat_size
                FROM chunk_metadata
                WHERE round_num = ?
                ORDER BY chunk_id
            ''', (round_num,))
            
            metadata_rows = cursor.fetchall()
            
            chunks = []
            for chunk_id, chunk_hash, parts_json, flat_size in metadata_rows:
                # 加载chunk数据
                cursor.execute(
                    "SELECT data FROM chunk_data WHERE chunk_hash = ?",
                    (chunk_hash,)
                )
                data_row = cursor.fetchone()
                
                if data_row:
                    chunk_data = pickle.loads(data_row[0])
                    chunk_info = {
                        'chunk_id': chunk_id,
                        'parts': json.loads(parts_json),
                        'flat_size': flat_size
                    }
                    chunks.append((chunk_info, chunk_data))
                    
            conn.close()
            return chunks
            
        except Exception as e:
            logger.error(f"❌ 加载chunks失败: {e}")
            return []
    
    def get_chunk_by_id(self, round_num: int, chunk_id: int) -> Optional[Tuple[Dict, np.ndarray]]:
        """
        获取指定轮次和chunk_id的chunk数据
        
        Args:
            round_num: 训练轮次
            chunk_id: chunk ID
            
        Returns:
            (chunk_info, chunk_data) 元组，如果不存在则返回None
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT chunk_hash, parts_info, flat_size
                FROM chunk_metadata
                WHERE round_num = ? AND chunk_id = ?
            ''', (round_num, chunk_id))
            
            row = cursor.fetchone()
            if row:
                chunk_hash, parts_json, flat_size = row
                
                # 加载chunk数据
                cursor.execute(
                    "SELECT data FROM chunk_data WHERE chunk_hash = ?",
                    (chunk_hash,)
                )
                data_row = cursor.fetchone()
                
                if data_row:
                    chunk_data = pickle.loads(data_row[0])
                    chunk_info = {
                        'chunk_id': chunk_id,
                        'parts': json.loads(parts_json),
                        'flat_size': flat_size
                    }
                    conn.close()
                    return (chunk_info, chunk_data)
                    
            conn.close()
            return None
            
        except Exception as e:
            logger.error(f"❌ 获取chunk失败: {e}")
            return None
    
    def get_storage_stats(self) -> Dict:
        """获取数据库存储统计信息"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 统计chunk元数据
            cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
            total_metadata = cursor.fetchone()[0]
            
            # 统计chunk数据
            cursor.execute("SELECT COUNT(*) FROM chunk_data")
            total_chunks = cursor.fetchone()[0]
            
            # 统计存储大小
            cursor.execute("SELECT SUM(LENGTH(data)) FROM chunk_data")
            total_size = cursor.fetchone()[0] or 0
            
            # 统计轮次范围
            cursor.execute("SELECT MIN(round_num), MAX(round_num) FROM chunk_metadata")
            min_round, max_round = cursor.fetchone()
            
            conn.close()
            
            return {
                'client_id': self.client_id,
                'db_path': self.db_path,
                'total_metadata_entries': total_metadata,
                'unique_chunks': total_chunks,
                'storage_size_bytes': total_size,
                'storage_size_mb': total_size / (1024 * 1024) if total_size else 0,
                'round_range': (min_round, max_round) if min_round is not None else (None, None)
            }
            
        except Exception as e:
            logger.error(f"❌ 获取存储统计失败: {e}")
            return {}
    
    def cleanup_old_rounds(self, keep_rounds: int = 2):
        """
        清理旧轮次的chunks，只保留最近的几轮
        
        Args:
            keep_rounds: 保留最近几轮的数据，默认只保留2轮
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 找到要保留的最小轮次（同时考虑本地和接收的chunk）
            cursor.execute("SELECT MAX(round_num) FROM chunk_metadata")
            max_local_round = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(round_num) FROM bt_chunks")
            max_bt_round = cursor.fetchone()[0]
            
            # 使用两个表中较大的轮次作为基准
            max_round = max_local_round
            if max_bt_round is not None:
                if max_round is None:
                    max_round = max_bt_round
                else:
                    max_round = max(max_round, max_bt_round)
            
            if max_round is not None:
                min_keep_round = max_round - keep_rounds + 1
                
                # 删除旧的本地chunk元数据
                deleted_local = cursor.execute(
                    "DELETE FROM chunk_metadata WHERE round_num < ?",
                    (min_keep_round,)
                ).rowcount
                
                # 🔧 新增：删除旧的BitTorrent chunk记录
                deleted_bt = cursor.execute(
                    "DELETE FROM bt_chunks WHERE round_num < ?",
                    (min_keep_round,)
                ).rowcount
                
                # 删除不再被引用的chunk数据
                # 现在bt_chunks表也会被清理，所以不会有永久引用的问题
                deleted_data = cursor.execute('''
                    DELETE FROM chunk_data 
                    WHERE chunk_hash NOT IN (
                        SELECT DISTINCT chunk_hash FROM chunk_metadata
                        UNION
                        SELECT DISTINCT chunk_hash FROM bt_chunks
                    )
                ''').rowcount
                
                conn.commit()
                logger.info(f"🧹 节点 {self.client_id}: 清理了第{min_keep_round}轮之前的数据")
                logger.info(f"   - 删除本地chunk元数据: {deleted_local}条")
                logger.info(f"   - 删除接收chunk记录: {deleted_bt}条") 
                logger.info(f"   - 删除无引用chunk数据: {deleted_data}条")
                
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ 清理旧chunks失败: {e}")
    
    def reconstruct_model_from_chunks(self, round_num: int, target_model: nn.Module) -> bool:
        """
        从chunks重构模型参数
        
        Args:
            round_num: 要重构的轮次
            target_model: 目标模型，参数将被重构的值替换
            
        Returns:
            是否重构成功
        """
        try:
            chunks = self.load_chunks_by_round(round_num)
            if not chunks:
                logger.warning(f"⚠️ 第{round_num}轮没有找到chunks")
                return False
            
            # 获取目标模型的参数形状信息
            model_params = self.model_to_params(target_model)
            
            # 初始化重构参数字典
            reconstructed_params = {}
            for param_name, param_array in model_params.items():
                reconstructed_params[param_name] = np.zeros_like(param_array)
            
            # 从chunks重构参数
            for chunk_info, chunk_data in chunks:
                data_ptr = 0
                
                for param_name, parts in chunk_info['parts'].items():
                    if param_name not in reconstructed_params:
                        logger.warning(f"⚠️ 参数 {param_name} 在目标模型中不存在")
                        continue
                        
                    for flat_start, flat_end, shape in parts:
                        chunk_size = flat_end - flat_start
                        part_data = chunk_data[data_ptr:data_ptr + chunk_size]
                        
                        # 直接对扁平化的参数数组进行赋值
                        param_flat = reconstructed_params[param_name].reshape(-1)
                        param_flat[flat_start:flat_end] = part_data
                        
                        data_ptr += chunk_size
            
            # 将重构的参数加载到模型
            self.params_to_model(reconstructed_params, target_model)
            
            logger.info(f"📦 节点 {self.client_id}: 成功从chunks重构第{round_num}轮的模型")
            return True
            
        except Exception as e:
            logger.error(f"❌ 重构模型失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def start_monitoring(self):
        """启动数据库变化监控"""
        if self.monitoring_enabled:
            logger.warning(f"⚠️ 节点 {self.client_id}: 监控已经启动")
            return
            
        self.monitoring_enabled = True
        self.stop_monitoring.clear()
        self.last_db_mtime = self._get_db_mtime()
        
        self.monitoring_thread = threading.Thread(
            target=self._monitor_database_changes,
            daemon=True,
            name=f"ChunkMonitor-{self.client_id}"
        )
        self.monitoring_thread.start()
        
        logger.info(f"🔍 节点 {self.client_id}: 启动chunk数据库变化监控")
    
    def stop_monitoring_thread(self):
        """停止数据库变化监控"""
        if not self.monitoring_enabled:
            return
            
        self.monitoring_enabled = False
        self.stop_monitoring.set()
        
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=2.0)
            
        logger.info(f"🛑 节点 {self.client_id}: 停止chunk数据库变化监控")
    
    def _get_db_mtime(self) -> float:
        """获取数据库文件的修改时间"""
        try:
            return os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0
        except OSError:
            return 0
    
    def _monitor_database_changes(self):
        """监控数据库变化的后台线程"""
        logger.debug(f"🔍 节点 {self.client_id}: 开始监控数据库变化")
        
        while not self.stop_monitoring.is_set():
            try:
                current_mtime = self._get_db_mtime()
                
                if current_mtime > self.last_db_mtime:
                    # 数据库发生变化，检测具体变化
                    self._detect_and_report_changes()
                    self.last_db_mtime = current_mtime
                
                # 每秒检查一次
                self.stop_monitoring.wait(1.0)
                
            except Exception as e:
                logger.error(f"❌ 节点 {self.client_id}: 监控数据库变化失败: {e}")
                self.stop_monitoring.wait(5.0)  # 错误后等待5秒再重试
        
        logger.debug(f"🔍 节点 {self.client_id}: 数据库变化监控线程退出")
    
    def _detect_and_report_changes(self):
        """检测并报告数据库变化"""
        if not self.change_callback:
            return
            
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 获取最近添加的chunk信息（基于创建时间）
            cursor.execute('''
                SELECT round_num, chunk_id, chunk_hash, flat_size, created_at
                FROM chunk_metadata
                ORDER BY created_at DESC
                LIMIT 10
            ''')
            
            recent_chunks = cursor.fetchall()
            conn.close()
            
            # 报告最近的变化
            for round_num, chunk_id, chunk_hash, flat_size, created_at in recent_chunks:
                chunk_info = ChunkInfo(
                    client_id=self.client_id,
                    round_num=round_num,
                    chunk_id=chunk_id,
                    action=ChunkAction.ADD.value,
                    chunk_hash=chunk_hash,
                    chunk_size=flat_size,
                    timestamp=time.time()
                )
                
                # 调用回调函数报告变化
                try:
                    self.change_callback(chunk_info)
                    logger.debug(f"📤 节点 {self.client_id}: 报告chunk变化 - 轮次{round_num}, chunk{chunk_id}")
                except Exception as e:
                    logger.error(f"❌ 节点 {self.client_id}: 报告chunk变化失败: {e}")
                    
        except Exception as e:
            logger.error(f"❌ 节点 {self.client_id}: 检测数据库变化失败: {e}")
    
    def report_chunk_change(self, round_num: int, chunk_id: int, action: str, chunk_hash: str, chunk_size: int):
        """手动报告chunk变化"""
        if not self.change_callback:
            return
            
        chunk_info = ChunkInfo(
            client_id=self.client_id,
            round_num=round_num,
            chunk_id=chunk_id,
            action=action,
            chunk_hash=chunk_hash,
            chunk_size=chunk_size,
            timestamp=time.time()
        )
        
        try:
            self.change_callback(chunk_info)
            logger.debug(f"📤 节点 {self.client_id}: 手动报告chunk变化 - {action} 轮次{round_num}, chunk{chunk_id}")
        except Exception as e:
            logger.error(f"❌ 节点 {self.client_id}: 手动报告chunk变化失败: {e}")
    
    def set_change_callback(self, callback: Callable[[ChunkInfo], None]):
        """设置变化回调函数"""
        self.change_callback = callback
        
        # 如果监控未启动且设置了回调，启动监控
        if callback and not self.monitoring_enabled:
            self.start_monitoring()
        # 如果取消回调，停止监控
        elif not callback and self.monitoring_enabled:
            self.stop_monitoring_thread()
            
        logger.info(f"🔄 节点 {self.client_id}: 更新变化回调函数")
    
    def get_all_chunks_info(self) -> List[ChunkInfo]:
        """获取所有chunk信息用于初始化报告"""
        chunk_infos = []
        
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT round_num, chunk_id, chunk_hash, flat_size, created_at
                FROM chunk_metadata
                ORDER BY round_num, chunk_id
            ''')
            
            rows = cursor.fetchall()
            conn.close()
            
            for round_num, chunk_id, chunk_hash, flat_size, created_at in rows:
                chunk_info = ChunkInfo(
                    client_id=self.client_id,
                    round_num=round_num,
                    chunk_id=chunk_id,
                    action=ChunkAction.ADD.value,
                    chunk_hash=chunk_hash,
                    chunk_size=flat_size,
                    timestamp=time.time()
                )
                chunk_infos.append(chunk_info)
                
        except Exception as e:
            logger.error(f"❌ 节点 {self.client_id}: 获取所有chunk信息失败: {e}")
            
        return chunk_infos
    
    # =================== BitTorrent扩展方法 ===================
    
    def _init_bittorrent_tables(self):
        """初始化BitTorrent相关的数据库表"""
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        # 创建BitTorrent chunks表（独立于原有表，避免冲突）
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bt_chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER NOT NULL,
                source_client_id INTEGER NOT NULL,
                chunk_id INTEGER NOT NULL,
                chunk_hash TEXT NOT NULL,
                holder_client_id INTEGER NOT NULL,
                received_time REAL DEFAULT (strftime('%s', 'now')),
                is_verified INTEGER DEFAULT 0,
                UNIQUE(round_num, source_client_id, chunk_id, holder_client_id)
            )
        ''')
        
        # 创建BitTorrent交换状态表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bt_exchange_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER NOT NULL,
                peer_id INTEGER NOT NULL,
                chunk_key TEXT NOT NULL,
                status TEXT NOT NULL,
                request_time REAL,
                complete_time REAL,
                retry_count INTEGER DEFAULT 0,
                error_msg TEXT,
                size INTEGER,
                UNIQUE(round_num, peer_id, chunk_key)
            )
        ''')
        
        # 创建BitTorrent会话表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bt_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round_num INTEGER NOT NULL,
                start_time REAL NOT NULL,
                end_time REAL,
                status TEXT NOT NULL,
                total_chunks_expected INTEGER,
                total_chunks_received INTEGER DEFAULT 0,
                bytes_uploaded INTEGER DEFAULT 0,
                bytes_downloaded INTEGER DEFAULT 0,
                UNIQUE(round_num)
            )
        ''')
        
        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bt_round_holder ON bt_chunks(round_num, holder_client_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bt_source ON bt_chunks(round_num, source_client_id, chunk_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_bt_hash ON bt_chunks(chunk_hash)')
        
        conn.commit()
        conn.close()
        logger.debug(f"[ChunkManager] BitTorrent tables initialized for client {self.client_id}")
    
    def get_global_bitfield(self, round_num=None):
        """
        🔧 修复：兼容旧代码，支持可选round_num参数
        获取指定轮次的全局chunk拥有情况的bitfield
        """
        # 如果没有传入round_num，使用当前轮次
        if round_num is None:
            round_num = getattr(self, 'current_round', 0)
            
        bitfield = {}
        
        # 查询本地chunks（原有表）
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        try:
            # 查询本地保存的chunks
            cursor.execute('''
                SELECT chunk_id FROM chunk_metadata
                WHERE round_num = ?
            ''', (round_num,))
            
            local_chunks = cursor.fetchall()
            logger.debug(f"[ChunkManager] Client {self.client_id}: Found {len(local_chunks)} local chunks for round {round_num}")
            
            for (chunk_id,) in local_chunks:
                # 本地chunks
                bitfield[(round_num, self.client_id, chunk_id)] = True
                # 静默添加本地chunk到bitfield
                pass
            
            # 查询BitTorrent交换的chunks（新表）
            cursor.execute('''
                SELECT source_client_id, chunk_id FROM bt_chunks
                WHERE round_num = ? AND holder_client_id = ?
            ''', (round_num, self.client_id))
            
            for source_id, chunk_id in cursor.fetchall():
                bitfield[(round_num, source_id, chunk_id)] = True
                
        except sqlite3.OperationalError:
            # 如果bt_chunks表不存在，初始化它
            logger.warning(f"[ChunkManager] BitTorrent tables not found, initializing...")
            conn.close()
            self._init_bittorrent_tables()
            return self.get_global_bitfield(round_num)
        
        conn.close()
        return bitfield
    
    def save_remote_chunk(self, round_num, source_client_id, chunk_id, chunk_data):
        """
        🔧 修复：保存BitTorrent交换的chunk到新表，避免schema冲突
        """
        import hashlib
        chunk_hash = hashlib.sha256(chunk_data).hexdigest()
        
        # 确保BitTorrent表存在
        try:
            # 直接写入bt_chunks表（避免与现有chunk_metadata表冲突）
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 写入bt_chunks表
            cursor.execute('''
                INSERT OR REPLACE INTO bt_chunks 
                (round_num, source_client_id, chunk_id, chunk_hash, holder_client_id, is_verified)
                VALUES (?, ?, ?, ?, ?, 1)
            ''', (round_num, source_client_id, chunk_id, chunk_hash, self.client_id))
            
            # 写入chunk_data表（共享存储）- 使用REPLACE处理重复哈希
            cursor.execute('''
                INSERT OR REPLACE INTO chunk_data (chunk_hash, data)
                VALUES (?, ?)
            ''', (chunk_hash, pickle.dumps(chunk_data)))
            
            conn.commit()
            conn.close()
            
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                # 初始化BitTorrent表
                self._init_bittorrent_tables()
                # 重试
                return self.save_remote_chunk(round_num, source_client_id, chunk_id, chunk_data)
            else:
                raise e
        
        
        # 触发变化回调
        if self.change_callback:
            # 创建ChunkInfo对象来报告远程chunk保存事件
            chunk_info = ChunkInfo(
                client_id=self.client_id,
                round_num=round_num,
                chunk_id=chunk_id,
                action='remote_chunk_saved',
                chunk_hash=chunk_hash,
                chunk_size=len(chunk_data) if hasattr(chunk_data, '__len__') else 0,
                timestamp=time.time()
            )
            self.change_callback(chunk_info)
    
    def get_chunk_data(self, round_num, source_client_id, chunk_id):
        """
        🆕 新增：获取chunk数据（用于发送给其他peers）
        """
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        try:
            # 先查询本地chunks
            if source_client_id == self.client_id:
                cursor.execute('''
                    SELECT cd.data FROM chunk_metadata cm
                    JOIN chunk_data cd ON cm.chunk_hash = cd.chunk_hash
                    WHERE cm.round_num = ? AND cm.chunk_id = ?
                ''', (round_num, chunk_id))
            else:
                # 查询BitTorrent交换的chunks
                cursor.execute('''
                    SELECT cd.data FROM bt_chunks bc
                    JOIN chunk_data cd ON bc.chunk_hash = cd.chunk_hash
                    WHERE bc.round_num = ? AND bc.source_client_id = ? 
                    AND bc.chunk_id = ? AND bc.holder_client_id = ?
                ''', (round_num, source_client_id, chunk_id, self.client_id))
            
            result = cursor.fetchone()
            logger.debug(f"[ChunkManager] Client {self.client_id}: Query result for chunk ({round_num}, {source_client_id}, {chunk_id}): {result is not None}")
            if result:
                try:
                    chunk_data = pickle.loads(result[0])
                    logger.debug(f"[ChunkManager] Client {self.client_id}: Successfully unpickled chunk data, size: {len(result[0])} bytes")
                    return chunk_data
                except Exception as pickle_error:
                    logger.error(f"[ChunkManager] Client {self.client_id}: Failed to unpickle chunk data: {pickle_error}")
                    return None
            else:
                logger.debug(f"[ChunkManager] Client {self.client_id}: No result found for chunk ({round_num}, {source_client_id}, {chunk_id})")
                return None
            
        except sqlite3.OperationalError as e:
            # 数据库操作错误，记录详细信息
            logger.error(f"[ChunkManager] Client {self.client_id}: SQLite OperationalError in get_chunk_data: {e}")
            logger.error(f"[ChunkManager] Client {self.client_id}: Query params: round_num={round_num}, source_client_id={source_client_id}, chunk_id={chunk_id}")
            return None
        except sqlite3.DatabaseError as e:
            # 数据库错误
            logger.error(f"[ChunkManager] Client {self.client_id}: SQLite DatabaseError in get_chunk_data: {e}")
            return None
        except Exception as e:
            # 其他异常
            logger.error(f"[ChunkManager] Client {self.client_id}: Unexpected error in get_chunk_data: {e}")
            return None
        finally:
            conn.close()
    
    def start_bittorrent_session(self, round_num, expected_chunks):
        """开始BitTorrent交换会话"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO bt_sessions 
                (round_num, start_time, status, total_chunks_expected)
                VALUES (?, ?, 'active', ?)
            ''', (round_num, time.time(), expected_chunks))
            
            conn.commit()
            conn.close()
            
        except sqlite3.OperationalError:
            # 初始化表并重试
            self._init_bittorrent_tables()
            return self.start_bittorrent_session(round_num, expected_chunks)
        
        logger.info(f"[ChunkManager] Started BitTorrent session for round {round_num}")
    
    def finish_bittorrent_session(self, round_num, status='completed'):
        """结束BitTorrent交换会话"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 统计接收到的chunks数量
            cursor.execute('''
                SELECT COUNT(*) FROM bt_chunks
                WHERE round_num = ? AND holder_client_id = ?
            ''', (round_num, self.client_id))
            
            chunks_received = cursor.fetchone()[0]
            
            # 更新会话状态
            cursor.execute('''
                UPDATE bt_sessions 
                SET end_time = ?, status = ?, total_chunks_received = ?
                WHERE round_num = ?
            ''', (time.time(), status, chunks_received, round_num))
            
            conn.commit()
            conn.close()
            
            logger.info(f"[ChunkManager] Finished BitTorrent session for round {round_num}, status: {status}")
            
        except sqlite3.OperationalError:
            # 表不存在，忽略
            pass
    
    def cleanup_bittorrent_data(self, keep_rounds=5):
        """清理旧的BitTorrent数据"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 找到要保留的最小轮次
            cursor.execute("SELECT MAX(round_num) FROM bt_sessions")
            max_round = cursor.fetchone()[0]
            
            if max_round is not None:
                min_keep_round = max_round - keep_rounds + 1
                
                # 删除旧的BitTorrent数据
                cursor.execute("DELETE FROM bt_chunks WHERE round_num < ?", (min_keep_round,))
                cursor.execute("DELETE FROM bt_exchange_status WHERE round_num < ?", (min_keep_round,))
                cursor.execute("DELETE FROM bt_sessions WHERE round_num < ?", (min_keep_round,))
                
                conn.commit()
                logger.info(f"[ChunkManager] Cleaned BitTorrent data before round {min_keep_round}")
                
            conn.close()
            
        except sqlite3.OperationalError:
            # 表不存在，忽略
            pass
    
    def get_available_clients_for_round(self, round_num: int) -> List[int]:
        """
        获取指定轮次可用的所有客户端ID
        
        Args:
            round_num: 目标轮次
            
        Returns:
            List[int]: 可用客户端ID列表
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 查询本地chunk数据的客户端 (local chunks don't have source_client_id, they belong to this client)
            cursor.execute('''
                SELECT DISTINCT ? as client_id
                FROM chunk_metadata 
                WHERE round_num = ?
                LIMIT 1
            ''', (self.client_id, round_num))
            
            local_result = cursor.fetchall()
            local_clients = [row[0] for row in local_result] if local_result else []
            
            # 查询BitTorrent chunks中的客户端
            cursor.execute('''
                SELECT DISTINCT source_client_id 
                FROM bt_chunks 
                WHERE round_num = ?
                ORDER BY source_client_id
            ''', (round_num,))
            
            bt_clients = [row[0] for row in cursor.fetchall()]
            
            # 合并并去重
            all_clients = list(set(local_clients + bt_clients))
            all_clients.sort()
            
            conn.close()
            
            logger.debug(f"[ChunkManager] Round {round_num}: Found {len(all_clients)} clients with chunk data: {all_clients}")
            return all_clients
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to get available clients for round {round_num}: {e}")
            return []
    
    def reconstruct_model_from_chunks(self, client_id: int, round_num: int) -> Optional[Dict]:
        """
        从chunks重构指定客户端的模型参数
        
        Args:
            client_id: 目标客户端ID
            round_num: 目标轮次
            
        Returns:
            Dict: 重构的模型参数字典，失败返回None
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 查询该客户端的所有chunks
            if client_id == self.client_id:
                # Local client - query local chunks
                cursor.execute('''
                    SELECT chunk_id, chunk_hash 
                    FROM chunk_metadata 
                    WHERE round_num = ?
                    ORDER BY chunk_id
                ''', (round_num,))
                local_chunks = cursor.fetchall()
            else:
                # Remote client - query BitTorrent chunks
                cursor.execute('''
                    SELECT chunk_id, chunk_hash 
                    FROM bt_chunks 
                    WHERE source_client_id = ? AND round_num = ?
                    ORDER BY chunk_id
                ''', (client_id, round_num))
                local_chunks = cursor.fetchall()
            
            if not local_chunks:
                logger.warning(f"[ChunkManager] No chunks found for client {client_id}, round {round_num}")
                conn.close()
                return None
            
            # 重构模型参数
            chunk_data_list = []
            missing_chunks = []
            
            for chunk_id, chunk_hash in local_chunks:
                # 获取chunk数据
                cursor.execute('''
                    SELECT data FROM chunk_data WHERE chunk_hash = ?
                ''', (chunk_hash,))
                
                result = cursor.fetchone()
                if result:
                    # 直接获取原始字节数据，不进行反序列化
                    chunk_data = result[0]
                    chunk_data_list.append((chunk_id, chunk_data))
                else:
                    missing_chunks.append(chunk_id)
            
            if missing_chunks:
                logger.warning(f"[ChunkManager] Missing chunk data for client {client_id}, chunks: {missing_chunks}")
            
            if not chunk_data_list:
                logger.error(f"[ChunkManager] No valid chunk data found for client {client_id}, round {round_num}")
                conn.close()
                return None
            
            # 按chunk_id排序
            chunk_data_list.sort(key=lambda x: x[0])
            
            # 反序列化每个chunk并连接
            numpy_chunks = []
            parts_info_list = []
            
            for chunk_id, chunk_data in chunk_data_list:
                # 反序列化chunk数据
                numpy_chunk = pickle.loads(chunk_data)
                numpy_chunks.append(numpy_chunk)
                
                # 获取对应的parts_info
                cursor.execute('''
                    SELECT parts_info FROM chunk_metadata 
                    WHERE chunk_id = ? AND round_num = ?
                ''', (chunk_id, round_num))
                
                parts_result = cursor.fetchone()
                if parts_result:
                    parts_info = json.loads(parts_result[0])
                    parts_info_list.append((chunk_id, parts_info))
            
            conn.close()
            
            # 连接所有chunk数据
            if len(numpy_chunks) == 0:
                logger.error(f"[ChunkManager] No valid chunk data for client {client_id}, round {round_num}")
                return None
                
            combined_numpy = np.concatenate(numpy_chunks) if len(numpy_chunks) > 1 else numpy_chunks[0]
            
            # 使用parts_info重构回参数字典
            model_params = self._reconstruct_params_dict(combined_numpy, parts_info_list)
            
            logger.debug(f"[ChunkManager] Successfully reconstructed model for client {client_id}, round {round_num}")
            return model_params
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to reconstruct model for client {client_id}, round {round_num}: {e}")
            return None
    
    def _reconstruct_params_dict(self, combined_numpy: np.ndarray, parts_info_list: List[Tuple[int, Dict]]) -> Dict:
        """
        使用parts_info将扁平化的numpy数组重构回参数字典
        
        Args:
            combined_numpy: 连接后的扁平化numpy数组
            parts_info_list: [(chunk_id, parts_info), ...]格式的结构信息
            
        Returns:
            重构的模型参数字典
        """
        import torch
        
        params_dict = {}
        current_pos = 0
        
        # 按chunk_id排序parts_info
        parts_info_list.sort(key=lambda x: x[0])
        
        for chunk_id, parts_info in parts_info_list:
            for param_name, parts in parts_info.items():
                if param_name not in params_dict:
                    # 首次遇到这个参数，需要预估总大小
                    total_size = self._estimate_param_size(param_name, parts_info_list)
                    params_dict[param_name] = np.zeros(total_size, dtype=combined_numpy.dtype)
                
                # 填充这个参数的各个部分
                for flat_start, flat_end, shape in parts:
                    chunk_size = flat_end - flat_start
                    chunk_data = combined_numpy[current_pos:current_pos + chunk_size]
                    
                    # 将数据放回原始参数的对应位置
                    params_dict[param_name][flat_start:flat_end] = chunk_data
                    current_pos += chunk_size
        
        # 将numpy数组转换为PyTorch张量并reshape
        final_params = {}
        for param_name, flat_data in params_dict.items():
            # 从parts_info获取原始形状
            original_shape = self._get_original_shape(param_name, parts_info_list)
            if original_shape:
                reshaped_data = flat_data.reshape(original_shape)
                final_params[param_name] = torch.tensor(reshaped_data, dtype=torch.float32)
            else:
                final_params[param_name] = torch.tensor(flat_data, dtype=torch.float32)
        
        return final_params
    
    def _estimate_param_size(self, param_name: str, parts_info_list: List[Tuple[int, Dict]]) -> int:
        """估算参数的总大小"""
        max_end = 0
        for _, parts_info in parts_info_list:
            if param_name in parts_info:
                for flat_start, flat_end, shape in parts_info[param_name]:
                    max_end = max(max_end, flat_end)
        return max_end
    
    def _get_original_shape(self, param_name: str, parts_info_list: List[Tuple[int, Dict]]) -> Optional[tuple]:
        """获取参数的原始形状"""
        for _, parts_info in parts_info_list:
            if param_name in parts_info:
                # 假设同一参数在所有chunks中的形状相同，取第一个
                for flat_start, flat_end, shape in parts_info[param_name]:
                    return tuple(shape)
        return None
    
    def get_client_sample_size(self, client_id: int, round_num: int) -> Optional[int]:
        """
        获取指定客户端在指定轮次的样本数量
        
        Args:
            client_id: 客户端ID
            round_num: 轮次
            
        Returns:
            int: 样本数量，如果未找到返回None
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Check if we have chunks from this client for this round
            # Note: sample_size column doesn't exist in schema, using fallback approach
            
            if client_id == self.client_id:
                # Local client - check local chunks
                cursor.execute('''
                    SELECT COUNT(*) FROM chunk_metadata 
                    WHERE round_num = ?
                ''', (round_num,))
            else:
                # Remote client - check BitTorrent chunks
                cursor.execute('''
                    SELECT COUNT(*) FROM bt_chunks 
                    WHERE round_num = ? AND source_client_id = ?
                ''', (round_num, client_id))
            
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0] > 0:
                # Return fixed sample size for BitTorrent FL (toy dataset default)
                return 128
            else:
                logger.debug(f"[ChunkManager] No chunks found for client {client_id}, round {round_num}")
                return None
                
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to get sample size for client {client_id}, round {round_num}: {e}")
            return None