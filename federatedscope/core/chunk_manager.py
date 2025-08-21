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
        
    def _init_database(self):
        """初始化SQLite数据库表结构"""
        conn = sqlite3.connect(self.db_path)
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(round_num, chunk_id)
            )
        ''')
        
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
    
    def save_model_chunks(self, model: nn.Module, round_num: int, num_chunks: int = 10) -> List[str]:
        """
        将模型分割成chunks并保存到节点特定的数据库
        
        Args:
            model: PyTorch模型
            round_num: 训练轮次
            num_chunks: 分割的chunk数量
            
        Returns:
            保存的chunk哈希列表
        """
        try:
            # 将模型转换为参数字典
            params = self.model_to_params(model)
            
            # 分割模型
            chunks_info = self.split_model(params, num_chunks)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            saved_hashes = []
            
            for chunk_info in chunks_info:
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
                
                # 保存chunk元数据
                parts_json = json.dumps(chunk_info['parts'])
                cursor.execute('''
                    INSERT OR REPLACE INTO chunk_metadata 
                    (round_num, chunk_id, chunk_hash, parts_info, flat_size)
                    VALUES (?, ?, ?, ?, ?)
                ''', (round_num, chunk_info['chunk_id'], chunk_hash, 
                     parts_json, chunk_info['flat_size']))
                
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
            
            logger.info(f"💾 节点 {self.client_id}: 第{round_num}轮保存了 {len(saved_hashes)} 个chunks")
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
    
    def cleanup_old_rounds(self, keep_rounds: int = 5):
        """
        清理旧轮次的chunks，只保留最近的几轮
        
        Args:
            keep_rounds: 保留最近几轮的数据
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 找到要保留的最小轮次
            cursor.execute("SELECT MAX(round_num) FROM chunk_metadata")
            max_round = cursor.fetchone()[0]
            
            if max_round is not None:
                min_keep_round = max_round - keep_rounds + 1
                
                # 删除旧的元数据
                cursor.execute(
                    "DELETE FROM chunk_metadata WHERE round_num < ?",
                    (min_keep_round,)
                )
                
                # 删除不再被引用的chunk数据
                cursor.execute('''
                    DELETE FROM chunk_data 
                    WHERE chunk_hash NOT IN (
                        SELECT DISTINCT chunk_hash FROM chunk_metadata
                    )
                ''')
                
                conn.commit()
                logger.info(f"🧹 节点 {self.client_id}: 清理了第{min_keep_round}轮之前的chunks")
                
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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