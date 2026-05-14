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
    ADD = "add"      # Add chunk
    DELETE = "delete"  # Delete chunk
    UPDATE = "update"  # Update chunk


@dataclass
class ChunkInfo:
    """
    Chunk information data structure
    """
    client_id: int          # Client ID
    round_num: int          # Training round
    chunk_id: int          # Chunk index
    action: str            # Action type: add/delete/update
    chunk_hash: str        # Hash value of chunk
    chunk_size: int        # Chunk size (bytes)
    timestamp: float       # Timestamp
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ChunkInfo':
        """Create ChunkInfo object from dictionary"""
        return cls(**data)
    
    def to_json(self) -> str:
        """Convert to JSON format"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ChunkInfo':
        """Create ChunkInfo object from JSON"""
        return cls.from_dict(json.loads(json_str))


class ChunkTracker:
    """
    Server-side chunk tracker
    Similar to BitTorrent tracker, maintains chunk distribution information
    """
    
    def __init__(self):
        self.chunk_registry: Dict[Tuple[int, int], Dict[int, Set[int]]] = {}
        # Format: {(round_num, chunk_id): {client_id: {hash_set}}}
        
        self.client_chunks: Dict[int, Dict[Tuple[int, int], str]] = {}
        # Format: {client_id: {(round_num, chunk_id): chunk_hash}}
        
        self.chunk_metadata: Dict[str, Dict] = {}
        # Format: {chunk_hash: {size, first_seen, clients_count}}
        
        self.lock = threading.RLock()  # Thread-safe lock
        
        logger.info("🗂️  ChunkTracker: Initialize chunk tracker")
    
    def update_chunk_info(self, chunk_info: ChunkInfo) -> bool:
        """
        Update chunk information
        
        Args:
            chunk_info: Chunk information object
            
        Returns:
            bool: Whether update was successful
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
                    logger.debug(f"ChunkTracker: Unknown chunk action type: {action}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ ChunkTracker: Failed to update chunk information: {e}")
            return False
    
    def _add_chunk(self, chunk_key: Tuple[int, int], client_id: int, 
                   chunk_hash: str, chunk_info: ChunkInfo) -> bool:
        """Add chunk record"""
        round_num, chunk_id = chunk_key
        
        # Update chunk registry
        if chunk_key not in self.chunk_registry:
            self.chunk_registry[chunk_key] = {}
        
        if client_id not in self.chunk_registry[chunk_key]:
            self.chunk_registry[chunk_key][client_id] = set()
        
        self.chunk_registry[chunk_key][client_id].add(hash(chunk_hash))
        
        # Update client chunk mapping
        if client_id not in self.client_chunks:
            self.client_chunks[client_id] = {}
        
        self.client_chunks[client_id][chunk_key] = chunk_hash
        
        # Update chunk metadata
        if chunk_hash not in self.chunk_metadata:
            self.chunk_metadata[chunk_hash] = {
                'size': chunk_info.chunk_size,
                'first_seen': chunk_info.timestamp,
                'clients_count': 0
            }
        
        self.chunk_metadata[chunk_hash]['clients_count'] += 1
        
        logger.debug(f"➕ ChunkTracker: Added chunk - client {client_id}, round {round_num}, chunk {chunk_id}")
        return True
    
    def _delete_chunk(self, chunk_key: Tuple[int, int], client_id: int, chunk_hash: str) -> bool:
        """Delete chunk record"""
        round_num, chunk_id = chunk_key
        
        # Remove from chunk registry
        if chunk_key in self.chunk_registry and client_id in self.chunk_registry[chunk_key]:
            self.chunk_registry[chunk_key][client_id].discard(hash(chunk_hash))
            
            if not self.chunk_registry[chunk_key][client_id]:
                del self.chunk_registry[chunk_key][client_id]
                
            if not self.chunk_registry[chunk_key]:
                del self.chunk_registry[chunk_key]
        
        # Remove from client chunk mapping
        if client_id in self.client_chunks and chunk_key in self.client_chunks[client_id]:
            del self.client_chunks[client_id][chunk_key]
            
            if not self.client_chunks[client_id]:
                del self.client_chunks[client_id]
        
        # Update chunk metadata
        if chunk_hash in self.chunk_metadata:
            self.chunk_metadata[chunk_hash]['clients_count'] -= 1
            
            if self.chunk_metadata[chunk_hash]['clients_count'] <= 0:
                del self.chunk_metadata[chunk_hash]
        
        logger.debug(f"➖ ChunkTracker: Deleted chunk - client {client_id}, round {round_num}, chunk {chunk_id}")
        return True
    
    def _update_chunk(self, chunk_key: Tuple[int, int], client_id: int, 
                     chunk_hash: str, chunk_info: ChunkInfo) -> bool:
        """Update chunk record"""
        # First delete old record, then add new record
        old_hash = self.client_chunks.get(client_id, {}).get(chunk_key, "")
        if old_hash:
            self._delete_chunk(chunk_key, client_id, old_hash)
        
        return self._add_chunk(chunk_key, client_id, chunk_hash, chunk_info)
    
    def get_chunk_locations(self, round_num: int, chunk_id: int) -> List[int]:
        """
        Get all holders of the specified chunk
        
        Args:
            round_num: Round number
            chunk_id: Chunk ID
            
        Returns:
            List[int]: List of client IDs that hold the chunk
        """
        with self.lock:
            chunk_key = (round_num, chunk_id)
            if chunk_key in self.chunk_registry:
                return list(self.chunk_registry[chunk_key].keys())
            return []
    
    def get_client_chunks(self, client_id: int) -> List[Tuple[int, int]]:
        """
        Get all chunks held by the specified client
        
        Args:
            client_id: Client ID
            
        Returns:
            List[Tuple[int, int]]: List of (round_num, chunk_id) tuples
        """
        with self.lock:
            if client_id in self.client_chunks:
                return list(self.client_chunks[client_id].keys())
            return []
    
    def get_chunk_availability(self, round_num: int) -> Dict[int, int]:
        """
        Get availability statistics for all chunks in the specified round
        
        Args:
            round_num: Round number
            
        Returns:
            Dict[int, int]: {chunk_id: number of holders}
        """
        with self.lock:
            availability = {}
            for (r, chunk_id), clients in self.chunk_registry.items():
                if r == round_num:
                    availability[chunk_id] = len(clients)
            return availability
    
    def get_tracker_stats(self) -> Dict:
        """Get tracker statistics"""
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
        Clean up chunk records from old rounds
        
        Args:
            keep_rounds: Number of rounds to keep
        """
        with self.lock:
            if not self.chunk_registry:
                return
            
            # Find the maximum round number
            max_round = max(round_num for round_num, _ in self.chunk_registry.keys())
            cutoff_round = max_round - keep_rounds + 1
            
            # Chunk keys to be deleted
            keys_to_delete = [
                (round_num, chunk_id) 
                for round_num, chunk_id in self.chunk_registry.keys() 
                if round_num < cutoff_round
            ]
            
            # Delete old records
            for chunk_key in keys_to_delete:
                del self.chunk_registry[chunk_key]
            
            # Clean up client chunk mappings
            for client_id in list(self.client_chunks.keys()):
                old_chunks = [
                    chunk_key for chunk_key in self.client_chunks[client_id].keys()
                    if chunk_key[0] < cutoff_round
                ]
                
                for chunk_key in old_chunks:
                    del self.client_chunks[client_id][chunk_key]
                
                if not self.client_chunks[client_id]:
                    del self.client_chunks[client_id]
            
            logger.info(f"🧹 ChunkTracker: Cleaned up records before round {cutoff_round}")
    
    def export_tracker_data(self) -> Dict:
        """Export tracker data for persistence"""
        with self.lock:
            # Convert to serializable format
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