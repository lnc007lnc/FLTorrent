"""
ChunkTracker system for distributed chunk management
Implements BitTorrent-like tracker functionality for federated learning chunks
"""

import threading
import time
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
import json
import logging

logger = logging.getLogger(__name__)


class ChunkAction(Enum):
    """Chunk action types"""
    ADD = "add"      # 新增chunk
    DELETE = "delete"  # 删除chunk
    UPDATE = "update"  # 更新chunk


@dataclass
class ChunkInfo:
    """
    Chunk信息数据结构
    """
    client_id: int          # 客户端ID
    round_num: int          # 训练轮次
    chunk_id: int          # chunk索引
    action: str            # 操作类型: add/delete/update
    chunk_hash: str        # chunk的hash值
    chunk_size: int        # chunk大小(字节)
    timestamp: float       # 时间戳
    
    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ChunkInfo':
        """从字典创建ChunkInfo对象"""
        return cls(**data)
    
    def to_json(self) -> str:
        """转换为JSON格式"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ChunkInfo':
        """从JSON创建ChunkInfo对象"""
        return cls.from_dict(json.loads(json_str))


class ChunkTracker:
    """
    服务器端chunk追踪器
    类似BitTorrent tracker，维护chunk分布信息
    """
    
    def __init__(self):
        self.chunk_registry: Dict[Tuple[int, int], Dict[int, Set[int]]] = {}
        # 格式: {(round_num, chunk_id): {client_id: {hash_set}}}
        
        self.client_chunks: Dict[int, Dict[Tuple[int, int], str]] = {}
        # 格式: {client_id: {(round_num, chunk_id): chunk_hash}}
        
        self.chunk_metadata: Dict[str, Dict] = {}
        # 格式: {chunk_hash: {size, first_seen, clients_count}}
        
        self.lock = threading.RLock()  # 线程安全锁
        
        logger.info("🗂️  ChunkTracker: 初始化chunk追踪器")
    
    def update_chunk_info(self, chunk_info: ChunkInfo) -> bool:
        """
        更新chunk信息
        
        Args:
            chunk_info: chunk信息对象
            
        Returns:
            bool: 更新是否成功
        """
        try:
            with self.lock:
                round_num = chunk_info.round_num
                chunk_id = chunk_info.chunk_id
                client_id = chunk_info.client_id
                chunk_hash = chunk_info.chunk_hash
                action = chunk_info.action
                
                chunk_key = (round_num, chunk_id)
                
                if action == ChunkAction.ADD.value:
                    return self._add_chunk(chunk_key, client_id, chunk_hash, chunk_info)
                    
                elif action == ChunkAction.DELETE.value:
                    return self._delete_chunk(chunk_key, client_id, chunk_hash)
                    
                elif action == ChunkAction.UPDATE.value:
                    return self._update_chunk(chunk_key, client_id, chunk_hash, chunk_info)
                
                else:
                    logger.warning(f"⚠️  ChunkTracker: 未知的chunk操作类型: {action}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ ChunkTracker: 更新chunk信息失败: {e}")
            return False
    
    def _add_chunk(self, chunk_key: Tuple[int, int], client_id: int, 
                   chunk_hash: str, chunk_info: ChunkInfo) -> bool:
        """添加chunk记录"""
        round_num, chunk_id = chunk_key
        
        # 更新chunk注册表
        if chunk_key not in self.chunk_registry:
            self.chunk_registry[chunk_key] = {}
        
        if client_id not in self.chunk_registry[chunk_key]:
            self.chunk_registry[chunk_key][client_id] = set()
        
        self.chunk_registry[chunk_key][client_id].add(hash(chunk_hash))
        
        # 更新客户端chunk映射
        if client_id not in self.client_chunks:
            self.client_chunks[client_id] = {}
        
        self.client_chunks[client_id][chunk_key] = chunk_hash
        
        # 更新chunk元数据
        if chunk_hash not in self.chunk_metadata:
            self.chunk_metadata[chunk_hash] = {
                'size': chunk_info.chunk_size,
                'first_seen': chunk_info.timestamp,
                'clients_count': 0
            }
        
        self.chunk_metadata[chunk_hash]['clients_count'] += 1
        
        logger.debug(f"➕ ChunkTracker: 添加chunk - 客户端{client_id}, 轮次{round_num}, chunk{chunk_id}")
        return True
    
    def _delete_chunk(self, chunk_key: Tuple[int, int], client_id: int, chunk_hash: str) -> bool:
        """删除chunk记录"""
        round_num, chunk_id = chunk_key
        
        # 从chunk注册表中删除
        if chunk_key in self.chunk_registry and client_id in self.chunk_registry[chunk_key]:
            self.chunk_registry[chunk_key][client_id].discard(hash(chunk_hash))
            
            if not self.chunk_registry[chunk_key][client_id]:
                del self.chunk_registry[chunk_key][client_id]
                
            if not self.chunk_registry[chunk_key]:
                del self.chunk_registry[chunk_key]
        
        # 从客户端chunk映射中删除
        if client_id in self.client_chunks and chunk_key in self.client_chunks[client_id]:
            del self.client_chunks[client_id][chunk_key]
            
            if not self.client_chunks[client_id]:
                del self.client_chunks[client_id]
        
        # 更新chunk元数据
        if chunk_hash in self.chunk_metadata:
            self.chunk_metadata[chunk_hash]['clients_count'] -= 1
            
            if self.chunk_metadata[chunk_hash]['clients_count'] <= 0:
                del self.chunk_metadata[chunk_hash]
        
        logger.debug(f"➖ ChunkTracker: 删除chunk - 客户端{client_id}, 轮次{round_num}, chunk{chunk_id}")
        return True
    
    def _update_chunk(self, chunk_key: Tuple[int, int], client_id: int, 
                     chunk_hash: str, chunk_info: ChunkInfo) -> bool:
        """更新chunk记录"""
        # 先删除旧记录，再添加新记录
        old_hash = self.client_chunks.get(client_id, {}).get(chunk_key, "")
        if old_hash:
            self._delete_chunk(chunk_key, client_id, old_hash)
        
        return self._add_chunk(chunk_key, client_id, chunk_hash, chunk_info)
    
    def get_chunk_locations(self, round_num: int, chunk_id: int) -> List[int]:
        """
        获取指定chunk的所有持有者
        
        Args:
            round_num: 轮次
            chunk_id: chunk ID
            
        Returns:
            List[int]: 持有该chunk的客户端ID列表
        """
        with self.lock:
            chunk_key = (round_num, chunk_id)
            if chunk_key in self.chunk_registry:
                return list(self.chunk_registry[chunk_key].keys())
            return []
    
    def get_client_chunks(self, client_id: int) -> List[Tuple[int, int]]:
        """
        获取指定客户端持有的所有chunks
        
        Args:
            client_id: 客户端ID
            
        Returns:
            List[Tuple[int, int]]: (round_num, chunk_id)列表
        """
        with self.lock:
            if client_id in self.client_chunks:
                return list(self.client_chunks[client_id].keys())
            return []
    
    def get_chunk_availability(self, round_num: int) -> Dict[int, int]:
        """
        获取指定轮次所有chunks的可用性统计
        
        Args:
            round_num: 轮次
            
        Returns:
            Dict[int, int]: {chunk_id: 持有者数量}
        """
        with self.lock:
            availability = {}
            for (r, chunk_id), clients in self.chunk_registry.items():
                if r == round_num:
                    availability[chunk_id] = len(clients)
            return availability
    
    def get_tracker_stats(self) -> Dict:
        """获取tracker统计信息"""
        with self.lock:
            total_chunks = len(self.chunk_metadata)
            total_clients = len(self.client_chunks)
            total_mappings = sum(len(chunks) for chunks in self.client_chunks.values())
            
            round_stats = {}
            for (round_num, chunk_id) in self.chunk_registry:
                if round_num not in round_stats:
                    round_stats[round_num] = {'chunk_count': 0, 'total_copies': 0}
                
                round_stats[round_num]['chunk_count'] += 1
                round_stats[round_num]['total_copies'] += len(self.chunk_registry[(round_num, chunk_id)])
            
            return {
                'total_unique_chunks': total_chunks,
                'total_active_clients': total_clients,
                'total_chunk_mappings': total_mappings,
                'rounds_tracked': len(round_stats),
                'round_statistics': round_stats
            }
    
    def cleanup_old_rounds(self, keep_rounds: int = 5):
        """
        清理旧轮次的chunk记录
        
        Args:
            keep_rounds: 保留的轮次数量
        """
        with self.lock:
            if not self.chunk_registry:
                return
            
            # 找到最大轮次
            max_round = max(round_num for round_num, _ in self.chunk_registry.keys())
            cutoff_round = max_round - keep_rounds + 1
            
            # 需要删除的chunk keys
            keys_to_delete = [
                (round_num, chunk_id) 
                for round_num, chunk_id in self.chunk_registry.keys() 
                if round_num < cutoff_round
            ]
            
            # 删除旧记录
            for chunk_key in keys_to_delete:
                del self.chunk_registry[chunk_key]
            
            # 清理客户端chunk映射
            for client_id in list(self.client_chunks.keys()):
                old_chunks = [
                    chunk_key for chunk_key in self.client_chunks[client_id].keys()
                    if chunk_key[0] < cutoff_round
                ]
                
                for chunk_key in old_chunks:
                    del self.client_chunks[client_id][chunk_key]
                
                if not self.client_chunks[client_id]:
                    del self.client_chunks[client_id]
            
            logger.info(f"🧹 ChunkTracker: 清理了轮次{cutoff_round}之前的记录")
    
    def export_tracker_data(self) -> Dict:
        """导出tracker数据用于持久化"""
        with self.lock:
            # 转换为可序列化格式
            serializable_registry = {}
            for (round_num, chunk_id), clients in self.chunk_registry.items():
                key = f"{round_num}_{chunk_id}"
                serializable_registry[key] = {
                    str(client_id): list(hash_set) 
                    for client_id, hash_set in clients.items()
                }
            
            serializable_client_chunks = {}
            for client_id, chunks in self.client_chunks.items():
                serializable_client_chunks[str(client_id)] = {
                    f"{round_num}_{chunk_id}": chunk_hash
                    for (round_num, chunk_id), chunk_hash in chunks.items()
                }
            
            return {
                'chunk_registry': serializable_registry,
                'client_chunks': serializable_client_chunks,
                'chunk_metadata': self.chunk_metadata,
                'export_timestamp': time.time()
            }