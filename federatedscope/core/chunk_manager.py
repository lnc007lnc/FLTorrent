"""
Model chunk management system based on provided algorithm
Uses flat indexing to record parameter chunk information, creates local database per node name to store chunks
Supports real-time change monitoring and chunk information reporting
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
    Unified model chunking logic management, using flat indexing to record parameter chunk information.
    Each chunk is defined in the format:
      {
          'chunk_id': int,
          'parts': { key: [ (flat_start, flat_end, shape), ... ] },
          'flat_size': int
      }
    """
    
    def __init__(self, client_id: int, change_callback: Optional[Callable[[ChunkInfo], None]] = None):
        """
        Initialize ChunkManager, create independent database for specified client
        
        Args:
            client_id: Client ID, used to create node-specific database file
            change_callback: Callback function for database changes, used to report chunk changes to server
        """
        self.client_id = client_id
        self.change_callback = change_callback
        
        # Create database file path by node name: /tmp/client_X/client_X_chunks.db
        client_name = f"client_{client_id}"
        db_dir = os.path.join(os.getcwd(), "tmp", client_name)
        os.makedirs(db_dir, exist_ok=True)
        
        self.db_path = os.path.join(db_dir, f"{client_name}_chunks.db")
        self._init_database()
        
        # Change monitoring related
        self.monitoring_enabled = False
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
        self.last_db_mtime = 0
        
        logger.info(f"📊 Initialize chunk database for node {client_id}: {self.db_path}")
        
        # If callback function is provided, start monitoring
        if change_callback:
            self.start_monitoring()
        
    def _get_optimized_connection(self):
        """Get optimized database connection"""
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        cursor = conn.cursor()
        
        # Enable optimization settings
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL") 
        cursor.execute("PRAGMA cache_size=10000")
        cursor.execute("PRAGMA temp_store=MEMORY")
        cursor.execute("PRAGMA busy_timeout=30000")  # 30 second timeout
        
        return conn
        
    def _init_database(self):
        """Initialize SQLite database table structure"""
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        # Create chunk metadata table
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
        
        # Upgrade existing table structure (if needed)
        try:
            cursor.execute("ALTER TABLE chunk_metadata ADD COLUMN importance_score REAL DEFAULT 0.0")
            logger.info("[ChunkManager] Added importance_score column to chunk_metadata table")
        except sqlite3.OperationalError:
            # Column already exists, ignore
            pass
            
        try:
            cursor.execute("ALTER TABLE chunk_metadata ADD COLUMN pruning_method TEXT DEFAULT 'magnitude'")
            logger.info("[ChunkManager] Added pruning_method column to chunk_metadata table")
        except sqlite3.OperationalError:
            # Column already exists, ignore
            pass
        
        # Create chunk data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunk_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_hash TEXT UNIQUE NOT NULL,
                data BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes to improve query performance
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
    
    def _ensure_round_tables_exist(self, round_num: int):
        """Ensure round-specific tables exist"""
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        try:
            # Create metadata table for this round
            metadata_table = f"chunk_metadata_r{round_num}"
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {metadata_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chunk_id INTEGER NOT NULL,
                    chunk_hash TEXT NOT NULL,
                    parts_info TEXT NOT NULL,
                    flat_size INTEGER NOT NULL,
                    importance_score REAL DEFAULT 0.0,
                    pruning_method TEXT DEFAULT 'magnitude',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chunk_id)
                )
            ''')
            
            # Create BitTorrent chunks table for this round
            bt_table = f"bt_chunks_r{round_num}"
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {bt_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_client_id INTEGER NOT NULL,
                    chunk_id INTEGER NOT NULL,
                    chunk_hash TEXT NOT NULL,
                    holder_client_id INTEGER NOT NULL,
                    received_time REAL DEFAULT (strftime('%s', 'now')),
                    is_verified INTEGER DEFAULT 0,
                    UNIQUE(source_client_id, chunk_id, holder_client_id)
                )
            ''')
            
            # Create chunk data table for this round
            data_table = f"chunk_data_r{round_num}"
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {data_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chunk_hash TEXT UNIQUE NOT NULL,
                    data BLOB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create BitTorrent session table for this round
            session_table = f"bt_sessions_r{round_num}"
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {session_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time REAL NOT NULL,
                    end_time REAL,
                    status TEXT DEFAULT 'active',
                    total_chunks_expected INTEGER,
                    total_chunks_received INTEGER DEFAULT 0,
                    bytes_uploaded INTEGER DEFAULT 0,
                    bytes_downloaded INTEGER DEFAULT 0
                )
            ''')
            
            # Create indexes
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{metadata_table}_hash ON {metadata_table}(chunk_hash)')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{bt_table}_hash ON {bt_table}(chunk_hash)')
            cursor.execute(f'CREATE INDEX IF NOT EXISTS idx_{data_table}_hash ON {data_table}(chunk_hash)')
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"❌ Failed to create round {round_num} tables: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _cleanup_old_rounds_by_tables(self, keep_rounds: int = 2):
        """Ultra-fast cleanup by dropping entire round tables"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Get all existing round tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chunk_metadata_r%'")
            metadata_tables = [row[0] for row in cursor.fetchall()]
            
            # Extract round numbers and sort
            rounds = []
            for table in metadata_tables:
                try:
                    round_num = int(table.replace('chunk_metadata_r', ''))
                    rounds.append(round_num)
                except ValueError:
                    continue
            
            rounds.sort(reverse=True)  # Newest first
            
            if len(rounds) <= keep_rounds:
                logger.debug(f"🧹 Only {len(rounds)} rounds exist, no cleanup needed")
                conn.close()
                return
                
            # Rounds to delete (keep the most recent ones)
            rounds_to_delete = rounds[keep_rounds:]
            logger.info(f"🧹 Deleting {len(rounds_to_delete)} old rounds: {rounds_to_delete}")
            
            start_time = time.time()
            deleted_tables = 0
            
            for round_num in rounds_to_delete:
                # Drop all round-specific tables (chunk + BitTorrent)
                for table_type in ["chunk_metadata", "bt_chunks", "chunk_data", "bt_sessions"]:
                    table_name = f"{table_type}_r{round_num}"
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        cursor.execute(f"DROP INDEX IF EXISTS idx_{table_name}_hash")
                        deleted_tables += 1
                    except Exception as e:
                        logger.warning(f"Failed to drop table {table_name}: {e}")
            
            conn.commit()
            elapsed = time.time() - start_time
            
            logger.info(f"🧹✅ Ultra-fast cleanup completed in {elapsed:.2f}s:")
            logger.info(f"   - Deleted {len(rounds_to_delete)} rounds")
            logger.info(f"   - Dropped {deleted_tables} tables")
            logger.info(f"   - Performance: {deleted_tables/elapsed:.1f} tables/sec")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Failed to cleanup old rounds: {e}")
    
    @staticmethod
    def model_to_params(model: nn.Module) -> Dict[str, np.ndarray]:
        """Convert all parameters and buffers in model to numpy arrays"""
        params = {name: param.data.cpu().numpy() for name, param in model.named_parameters()}
        for name, buffer in model.named_buffers():
            params[name] = buffer.data.cpu().numpy()
        return params

    @staticmethod
    def params_to_model(params: Dict[str, np.ndarray], model: nn.Module):
        """Load parameter dictionary back to model"""
        for name, param in model.named_parameters():
            if name in params:
                param.data = torch.from_numpy(params[name]).to(param.device)
    
    def compute_chunk_importance(self, params: Dict[str, np.ndarray], chunks_info: List[Dict], 
                                method: str = 'magnitude') -> List[float]:
        """
        Calculate importance scores for each chunk
        
        Args:
            params: Model parameter dictionary
            chunks_info: Chunk information list
            method: Importance calculation method ('magnitude', 'l2_norm', 'gradient_norm', 'snip')
            
        Returns:
            List[float]: Importance score for each chunk
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
            
        # Use L1 normalization (sum normalization) to maintain original proportional relationships
        if importance_scores:
            total_score = sum(importance_scores)
            if total_score > 0:
                importance_scores = [s / total_score for s in importance_scores]
            else:
                importance_scores = [1.0 / len(importance_scores)] * len(importance_scores)  # Average allocation
                
        logger.info(f"[ChunkManager] Computed chunk importance scores: {[f'{s:.4f}' for s in importance_scores]}")
        return importance_scores
    
    def _compute_magnitude_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """Importance calculation based on parameter magnitude"""
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
        """Importance calculation based on L2 norm"""
        total_l2_norm = 0.0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    total_l2_norm += np.linalg.norm(chunk_slice) ** 2
        
        return np.sqrt(total_l2_norm)
    
    def _compute_gradient_norm_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """Importance calculation based on gradient norm (requires gradient information)"""
        # Note: This is a simplified implementation, actual applications require gradient information
        # Use magnitude method as fallback
        return self._compute_magnitude_importance(params, chunk_info)
    
    def _compute_snip_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """Importance calculation based on SNIP (Single-shot Network Pruning)"""
        # Improved SNIP implementation: consider parameter layer importance
        total_snip_score = 0.0
        
        for param_name, parts in chunk_info['parts'].items():
            if param_name in params:
                param_array = params[param_name].flatten()
                
                # Set weight factor based on parameter type
                layer_weight = 1.0
                if 'weight' in param_name:
                    layer_weight = 2.0  # Weights are more important than bias
                if 'fc' in param_name or '4.' in param_name:  # Output layer
                    layer_weight *= 1.5  # Output layer is more important
                
                for flat_start, flat_end, _ in parts:
                    chunk_slice = param_array[flat_start:flat_end]
                    if len(chunk_slice) > 0:
                        # Calculate parameter sensitivity metrics
                        abs_values = np.abs(chunk_slice)
                        
                        # 1. Importance of large magnitude parameters
                        magnitude_score = np.sum(abs_values)
                        
                        # 2. Parameter dispersion (variance)
                        variance_score = np.var(abs_values) + 1e-8
                        
                        # 3. Non-zero parameter ratio (sparsity consideration)
                        non_zero_ratio = np.count_nonzero(abs_values) / len(abs_values)
                        
                        # SNIP comprehensive score: combines magnitude, variance and sparsity
                        chunk_score = magnitude_score * (1 + np.sqrt(variance_score)) * (0.5 + non_zero_ratio)
                        total_snip_score += chunk_score * layer_weight
        
        return total_snip_score
    
    def _compute_fisher_importance(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> float:
        """Importance calculation based on Fisher information matrix"""
        # Simplified version of Fisher information matrix: use parameter variance as importance metric
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
        Get importance scores for all chunks in specified round
        
        Args:
            round_num: Target round
            
        Returns:
            Dict[chunk_id, {'importance_score': float, 'pruning_method': str, 'flat_size': int}]
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 🚀 NEW: Query from round-specific table
            metadata_table = f"chunk_metadata_r{round_num}"
            
            # Check if round table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_table,))
            if not cursor.fetchone():
                logger.debug(f"No round {round_num} table found for importance scores")
                conn.close()
                return {}
            
            cursor.execute(f'''
                SELECT chunk_id, importance_score, pruning_method, flat_size, chunk_hash
                FROM {metadata_table} 
                ORDER BY chunk_id
            ''')
            
            results = cursor.fetchall()
            conn.close()
            
            chunk_scores = {}
            for chunk_id, importance_score, pruning_method, flat_size, chunk_hash in results:
                chunk_scores[chunk_id] = {
                    'importance_score': float(importance_score) if importance_score is not None else 0.0,
                    'pruning_method': pruning_method or 'unknown',
                    'flat_size': flat_size,
                    'chunk_hash': chunk_hash[:8] + '...'  # Display short hash
                }
            
            return chunk_scores
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to get chunk importance scores for round {round_num}: {e}")
            return {}

    @staticmethod
    def split_model(params: Dict[str, np.ndarray], num_chunks: int) -> List[Dict]:
        """
        Split model parameters evenly into specified number of chunks, record flat index ranges for each parameter in each chunk.
        Return list, each element formatted as:
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
        
        # For each parameter, split according to flat order
        for key in sorted(params.keys()):
            arr = params[key]
            n = int(np.prod(arr.shape))
            ptr = 0
            while ptr < n:
                # Check if need to start new chunk
                if current_chunk['flat_size'] >= elements_per_chunk and len(chunks) < num_chunks - 1:
                    chunks.append(current_chunk)
                    current_chunk = {'parts': {}, 'flat_size': 0, 'chunk_id': len(chunks)}
                
                # Calculate number of elements that can be put into current chunk
                if len(chunks) < num_chunks - 1:
                    remaining = elements_per_chunk - current_chunk['flat_size']
                    take = min(remaining, n - ptr)
                else:
                    # Last chunk contains all remaining elements
                    take = n - ptr
                    
                # Add this segment information for parameter key in current chunk
                if key not in current_chunk['parts']:
                    current_chunk['parts'][key] = []
                current_chunk['parts'][key].append((int(ptr), int(ptr + take), arr.shape))
                current_chunk['flat_size'] += take
                ptr += take
                
        # Add the last chunk
        if current_chunk['flat_size'] > 0:
            chunks.append(current_chunk)
            
        return chunks
    
    def extract_chunk_data(self, params: Dict[str, np.ndarray], chunk_info: Dict) -> np.ndarray:
        """
        Extract corresponding data from model parameters according to chunk information
        
        Args:
            params: Model parameter dictionary
            chunk_info: Chunk metadata information
            
        Returns:
            Flattened chunk data array
        """
        chunk_data = []
        
        for key, parts in chunk_info['parts'].items():
            if key not in params:
                logger.warning(f"⚠️ Parameter {key} not found in model")
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
        Split model into chunks and save to node-specific database
        
        Args:
            model: PyTorch model
            round_num: Training round number
            num_chunks: Number of chunks to split
            keep_rounds: Keep data from recent rounds, default 2 rounds
            importance_method: Chunk importance calculation method ('magnitude', 'l2_norm', 'snip', 'fisher')
            
        Returns:
            List of saved chunk hashes
        """
        try:
            # Convert model to parameter dictionary
            params = self.model_to_params(model)
            
            # Split model
            chunks_info = self.split_model(params, num_chunks)
            
            # 🧠 Calculate chunk importance scores
            logger.info(f"[ChunkManager] Computing chunk importance using method: {importance_method}")
            importance_scores = self.compute_chunk_importance(params, chunks_info, importance_method)
            
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            saved_hashes = []
            
            for i, chunk_info in enumerate(chunks_info):
                # Extract chunk data
                chunk_data = self.extract_chunk_data(params, chunk_info)
                
                # Calculate chunk hash
                chunk_bytes = pickle.dumps(chunk_data)
                chunk_hash = hashlib.sha256(chunk_bytes).hexdigest()
                
                # 🚀 NEW: Save to round-specific tables
                self._ensure_round_tables_exist(round_num)
                
                # Save chunk data to round-specific table
                data_table = f"chunk_data_r{round_num}"
                cursor.execute(f"""
                    INSERT OR IGNORE INTO {data_table} (chunk_hash, data) VALUES (?, ?)
                """, (chunk_hash, chunk_bytes))
                
                # Save chunk metadata to round-specific table
                parts_json = json.dumps(chunk_info['parts'])
                importance_score = importance_scores[i] if i < len(importance_scores) else 0.0
                
                metadata_table = f"chunk_metadata_r{round_num}"
                cursor.execute(f'''
                    INSERT OR REPLACE INTO {metadata_table} 
                    (chunk_id, chunk_hash, parts_info, flat_size, importance_score, pruning_method)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (chunk_info['chunk_id'], chunk_hash, 
                     parts_json, chunk_info['flat_size'], importance_score, importance_method))
                
                saved_hashes.append(chunk_hash)
                
                # Report chunk change
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
            
            # Automatically clean old round data, keep recent rounds
            self.cleanup_old_rounds(keep_rounds=keep_rounds)
            
            logger.debug(f"💾 Node {self.client_id}: Round {round_num} saved {len(saved_hashes)} chunks")
            return saved_hashes
            
        except Exception as e:
            logger.error(f"❌ Failed to save model chunks: {e}")
            return []
    
    def load_chunks_by_round(self, round_num: int) -> List[Tuple[Dict, np.ndarray]]:
        """
        Load all chunks for specified round
        
        Args:
            round_num: Training round number
            
        Returns:
            List of (chunk_info, chunk_data) tuples
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 🚀 NEW: Query from round-specific tables
            metadata_table = f"chunk_metadata_r{round_num}"
            data_table = f"chunk_data_r{round_num}"
            
            # Check if round tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_table,))
            if not cursor.fetchone():
                logger.debug(f"No round {round_num} tables found")
                conn.close()
                return []
            
            # Query chunk metadata from round-specific table
            cursor.execute(f'''
                SELECT chunk_id, chunk_hash, parts_info, flat_size
                FROM {metadata_table}
                ORDER BY chunk_id
            ''')
            
            metadata_rows = cursor.fetchall()
            
            chunks = []
            for chunk_id, chunk_hash, parts_json, flat_size in metadata_rows:
                # Load chunk data from round-specific table
                cursor.execute(f"""
                    SELECT data FROM {data_table} WHERE chunk_hash = ?
                """, (chunk_hash,))
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
            logger.error(f"❌ Failed to load chunks: {e}")
            return []
    
    def get_chunk_by_id(self, round_num: int, chunk_id: int) -> Optional[Tuple[Dict, np.ndarray]]:
        """
        Get chunk data for specified round and chunk_id
        
        Args:
            round_num: Training round number
            chunk_id: chunk ID
            
        Returns:
            (chunk_info, chunk_data) tuple, returns None if not exists
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 🚀 NEW: Query from round-specific tables
            metadata_table = f"chunk_metadata_r{round_num}"
            data_table = f"chunk_data_r{round_num}"
            
            # Check if round tables exist
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_table,))
            if not cursor.fetchone():
                conn.close()
                return None
            
            cursor.execute(f'''
                SELECT chunk_hash, parts_info, flat_size
                FROM {metadata_table}
                WHERE chunk_id = ?
            ''', (chunk_id,))
            
            row = cursor.fetchone()
            if row:
                chunk_hash, parts_json, flat_size = row
                
                # Load chunk data from round-specific table
                cursor.execute(f"""
                    SELECT data FROM {data_table} WHERE chunk_hash = ?
                """, (chunk_hash,))
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
            logger.error(f"❌ Failed to get chunk: {e}")
            return None
    
    def get_storage_stats(self, round_num: Optional[int] = None) -> Dict:
        """Get database storage statistics
        
        Args:
            round_num: Optional round number. If specified, returns stats for that round only.
                      If None, returns stats for all rounds (default behavior).
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            if round_num is not None:
                # Query specific round tables only
                metadata_tables = [f"chunk_metadata_r{round_num}"]
                data_tables = [f"chunk_data_r{round_num}"]
                
                # Check if round tables exist
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_tables[0],))
                if not cursor.fetchone():
                    metadata_tables = []
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (data_tables[0],))
                if not cursor.fetchone():
                    data_tables = []
            else:
                # 🚀 Aggregate stats from all round tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chunk_metadata_r%'")
                metadata_tables = [row[0] for row in cursor.fetchall()]
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chunk_data_r%'")
                data_tables = [row[0] for row in cursor.fetchall()]
            
            total_metadata = 0
            total_chunks = 0
            total_size = 0
            min_round = None
            max_round = None
            
            # Set min/max round for specific round query
            if round_num is not None and (metadata_tables or data_tables):
                min_round = max_round = round_num
            
            # Count metadata from round tables
            for table in metadata_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    total_metadata += cursor.fetchone()[0]
                    
                    # Extract round number for min/max calculation (only for all-rounds query)
                    if round_num is None:
                        try:
                            table_round_num = int(table.replace('chunk_metadata_r', ''))
                            if min_round is None or table_round_num < min_round:
                                min_round = table_round_num
                            if max_round is None or table_round_num > max_round:
                                max_round = table_round_num
                        except ValueError:
                            continue
                except sqlite3.Error:
                    continue
            
            # Count data from all round tables
            for table in data_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    total_chunks += cursor.fetchone()[0]
                    
                    cursor.execute(f"SELECT SUM(LENGTH(data)) FROM {table}")
                    size = cursor.fetchone()[0] or 0
                    total_size += size
                except sqlite3.Error:
                    continue
                    
            # Fallback to legacy tables if no round tables exist
            if total_metadata == 0 and total_chunks == 0:
                try:
                    if round_num is not None:
                        # Query specific round from legacy tables
                        cursor.execute("SELECT COUNT(*) FROM chunk_metadata WHERE round_num = ?", (round_num,))
                        total_metadata = cursor.fetchone()[0]
                        cursor.execute("SELECT COUNT(DISTINCT chunk_hash) FROM chunk_metadata WHERE round_num = ?", (round_num,))
                        chunk_count = cursor.fetchone()[0]
                        if chunk_count > 0:
                            cursor.execute("""
                                SELECT SUM(LENGTH(cd.data)) FROM chunk_data cd 
                                JOIN chunk_metadata cm ON cd.chunk_hash = cm.chunk_hash 
                                WHERE cm.round_num = ?
                            """, (round_num,))
                            total_size = cursor.fetchone()[0] or 0
                            total_chunks = chunk_count
                            min_round = max_round = round_num
                    else:
                        # Query all rounds from legacy tables
                        cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
                        total_metadata = cursor.fetchone()[0]
                        cursor.execute("SELECT COUNT(DISTINCT chunk_hash) FROM chunk_data")
                        total_chunks = cursor.fetchone()[0]
                        cursor.execute("SELECT SUM(LENGTH(data)) FROM chunk_data")
                        total_size = cursor.fetchone()[0] or 0
                        cursor.execute("SELECT MIN(round_num), MAX(round_num) FROM chunk_metadata")
                        min_round, max_round = cursor.fetchone()
                except sqlite3.Error:
                    pass
            
            conn.close()
            
            return {
                'client_id': self.client_id,
                'db_path': self.db_path,
                'total_metadata_entries': total_metadata,
                'unique_chunks': total_chunks,
                'storage_size_bytes': total_size,
                'storage_size_mb': total_size / (1024 * 1024) if total_size else 0,
                'round_range': (min_round, max_round) if min_round is not None else (None, None),
                'query_scope': f'round_{round_num}' if round_num is not None else 'all_rounds'
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get storage stats: {e}")
            return {}
    
    def cleanup_old_rounds(self, keep_rounds: int = 2):
        """
        Clean up chunks from old rounds, only keep the most recent few rounds
        
        Args:
            keep_rounds: Keep data from recent rounds, default to keep only 2 rounds
        
        Note: This method now redirects to the ultra-fast table-based cleanup for optimal performance
        """
        logger.info(f"🧹 Redirecting to ultra-fast cleanup method for better performance...")
        # Redirect to ultra-fast cleanup method
        return self._cleanup_old_rounds_by_tables(keep_rounds)
    
    def reconstruct_model_from_chunks(self, round_num: int, target_model: nn.Module) -> bool:
        """
        Reconstruct model parameters from chunks
        
        Args:
            round_num: Round to reconstruct
            target_model: Target model whose parameters will be replaced with reconstructed values
            
        Returns:
            Whether reconstruction was successful
        """
        try:
            chunks = self.load_chunks_by_round(round_num)
            if not chunks:
                logger.warning(f"⚠️ No chunks found for round {round_num}")
                return False
            
            # Get target model parameter shape information
            model_params = self.model_to_params(target_model)
            
            # Initialize reconstructed parameter dictionary
            reconstructed_params = {}
            for param_name, param_array in model_params.items():
                reconstructed_params[param_name] = np.zeros_like(param_array)
            
            # Reconstruct parameters from chunks
            for chunk_info, chunk_data in chunks:
                data_ptr = 0
                
                for param_name, parts in chunk_info['parts'].items():
                    if param_name not in reconstructed_params:
                        logger.warning(f"⚠️ Parameter {param_name} does not exist in target model")
                        continue
                        
                    for flat_start, flat_end, shape in parts:
                        chunk_size = flat_end - flat_start
                        part_data = chunk_data[data_ptr:data_ptr + chunk_size]
                        
                        # Directly assign values to flattened parameter array
                        param_flat = reconstructed_params[param_name].reshape(-1)
                        param_flat[flat_start:flat_end] = part_data
                        
                        data_ptr += chunk_size
            
            # Load reconstructed parameters into model
            self.params_to_model(reconstructed_params, target_model)
            
            logger.info(f"📦 Node {self.client_id}: Successfully reconstructed model from chunks for round {round_num}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to reconstruct model: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return False
    
    def start_monitoring(self):
        """Start database change monitoring"""
        if self.monitoring_enabled:
            logger.warning(f"⚠️ Node {self.client_id}: Monitoring already started")
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
        
        logger.info(f"🔍 Node {self.client_id}: Start database change monitoring")
    
    def stop_monitoring_thread(self):
        """Stop database change monitoring"""
        if not self.monitoring_enabled:
            return
            
        self.monitoring_enabled = False
        self.stop_monitoring.set()
        
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=2.0)
            
        logger.info(f"🛑 Node {self.client_id}: Stop database change monitoring")
    
    def _get_db_mtime(self) -> float:
        """Get database file modification time"""
        try:
            return os.path.getmtime(self.db_path) if os.path.exists(self.db_path) else 0
        except OSError:
            return 0
    
    def _monitor_database_changes(self):
        """Background thread for monitoring database changes"""
        logger.debug(f"🔍 Node {self.client_id}: Start monitoring database changes")
        
        while not self.stop_monitoring.is_set():
            try:
                current_mtime = self._get_db_mtime()
                
                if current_mtime > self.last_db_mtime:
                    # Database changed, detecting specific changes
                    self._detect_and_report_changes()
                    self.last_db_mtime = current_mtime
                
                # Check once per second
                self.stop_monitoring.wait(1.0)
                
            except Exception as e:
                logger.error(f"❌ Node {self.client_id}: Failed to monitor database changes: {e}")
                self.stop_monitoring.wait(5.0)  # Wait 5 seconds after error before retry
        
        logger.debug(f"🔍 Node {self.client_id}: Database change monitoring thread exiting")
    
    def _detect_and_report_changes(self):
        """Detect and report database changes"""
        if not self.change_callback:
            return
            
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Get recently added chunk information from round-specific tables
            recent_chunks = []
            
            # Query from all round-specific metadata tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chunk_metadata_r%'")
            metadata_tables = [row[0] for row in cursor.fetchall()]
            
            for table in metadata_tables:
                try:
                    # Extract round number from table name
                    table_round_num = int(table.replace('chunk_metadata_r', ''))
                    cursor.execute(f'''
                        SELECT {table_round_num}, chunk_id, chunk_hash, flat_size, created_at
                        FROM {table}
                        ORDER BY created_at DESC
                        LIMIT 10
                    ''')
                    recent_chunks.extend(cursor.fetchall())
                except (ValueError, sqlite3.Error):
                    continue
            
            # Sort by creation time and limit
            recent_chunks.sort(key=lambda x: x[4] if x[4] else 0, reverse=True)
            recent_chunks = recent_chunks[:10]
            
            # Fallback to legacy table if no round tables exist
            if not recent_chunks:
                cursor.execute('''
                    SELECT round_num, chunk_id, chunk_hash, flat_size, created_at
                    FROM chunk_metadata
                    ORDER BY created_at DESC
                    LIMIT 10
                ''')
                recent_chunks = cursor.fetchall()
            
            conn.close()
            
            # Report recent changes
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
                
                # Call callback function to report changes
                try:
                    self.change_callback(chunk_info)
                    logger.debug(f"📤 Node {self.client_id}: Report chunk change - round {round_num}, chunk {chunk_id}")
                except Exception as e:
                    logger.error(f"❌ Node {self.client_id}: Failed to report chunk change: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Node {self.client_id}: Failed to detect database changes: {e}")
    
    def report_chunk_change(self, round_num: int, chunk_id: int, action: str, chunk_hash: str, chunk_size: int):
        """Manually report chunk change"""
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
            logger.debug(f"📤 Node {self.client_id}: Manually report chunk change - {action} round {round_num}, chunk {chunk_id}")
        except Exception as e:
            logger.error(f"❌ Node {self.client_id}: Failed to manually report chunk change: {e}")
    
    def set_change_callback(self, callback: Callable[[ChunkInfo], None]):
        """Set change callback function"""
        self.change_callback = callback
        
        # If monitoring not started and callback set, start monitoring
        if callback and not self.monitoring_enabled:
            self.start_monitoring()
        # If callback removed, stop monitoring
        elif not callback and self.monitoring_enabled:
            self.stop_monitoring_thread()
            
        logger.info(f"🔄 Node {self.client_id}: Update change callback function")
    
    def get_all_chunks_info(self) -> List[ChunkInfo]:
        """Get all chunk information from round-specific tables"""
        chunk_infos = []
        
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Get all round-specific metadata tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chunk_metadata_r%'")
            metadata_tables = [row[0] for row in cursor.fetchall()]
            
            for table in metadata_tables:
                # Extract round number from table name
                try:
                    round_num = int(table.replace('chunk_metadata_r', ''))
                except ValueError:
                    continue
                    
                cursor.execute(f'''
                    SELECT chunk_id, chunk_hash, flat_size, created_at
                    FROM {table}
                    ORDER BY chunk_id
                ''')
                
                rows = cursor.fetchall()
                
                for chunk_id, chunk_hash, flat_size, created_at in rows:
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
            
            # Fallback: check legacy table if no round tables exist
            if not chunk_infos:
                try:
                    cursor.execute('''
                        SELECT round_num, chunk_id, chunk_hash, flat_size, created_at
                        FROM chunk_metadata
                        ORDER BY round_num, chunk_id
                    ''')
                    
                    rows = cursor.fetchall()
                    
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
                except sqlite3.Error:
                    pass  # Legacy table doesn't exist
            
            conn.close()
                
        except Exception as e:
            logger.error(f"❌ Node {self.client_id}: Failed to get all chunk information: {e}")
            
        return chunk_infos
    
    # =================== BitTorrent Extension Methods ===================
    
    def _init_bittorrent_tables(self, round_num=0):
        """[UPDATED] Initialize BitTorrent tables using round-specific design
        
        Args:
            round_num: Round number for creating round-specific tables (default: 0)
            
        Note: This method now creates round-specific tables instead of legacy tables.
        Removed unused bt_exchange_status table. Now delegates to _ensure_round_tables_exist.
        """
        logger.info(f"[ChunkManager] Initializing round-specific BitTorrent tables for round {round_num}")
        
        # Use the existing round table creation method which now includes bt_sessions
        self._ensure_round_tables_exist(round_num)
        logger.debug(f"[ChunkManager] BitTorrent tables initialized for client {self.client_id}")
    
    def get_global_bitfield(self, round_num=None):
        """
        Fix: Compatible with old code, support optional round_num parameter
        Get global chunk ownership bitfield for specified round
        """
        # If round_num not provided, use current round
        if round_num is None:
            round_num = getattr(self, 'current_round', 0)
            
        bitfield = {}
        
        # Query local chunks (existing table)
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        try:
            # 🚀 NEW: Query from round-specific tables
            metadata_table = f"chunk_metadata_r{round_num}"
            bt_table = f"bt_chunks_r{round_num}"
            
            # Query locally saved chunks from round-specific table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_table,))
            if cursor.fetchone():
                cursor.execute(f'''
                    SELECT chunk_id FROM {metadata_table}
                ''')
                
                local_chunks = cursor.fetchall()
                logger.debug(f"[ChunkManager] Client {self.client_id}: Found {len(local_chunks)} local chunks for round {round_num}")
                
                for (chunk_id,) in local_chunks:
                    # Local chunks
                    bitfield[(round_num, self.client_id, chunk_id)] = True
            
            # Query BitTorrent exchanged chunks from round-specific table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (bt_table,))
            if cursor.fetchone():
                cursor.execute(f'''
                    SELECT source_client_id, chunk_id FROM {bt_table}
                    WHERE holder_client_id = ?
                ''', (self.client_id,))
            
            for source_id, chunk_id in cursor.fetchall():
                bitfield[(round_num, source_id, chunk_id)] = True
                
        except sqlite3.OperationalError:
            # If round-specific tables don't exist, create them
            logger.warning(f"[ChunkManager] Round-specific tables not found for round {round_num}, creating...")
            conn.close()
            self._ensure_round_tables_exist(round_num)
            return self.get_global_bitfield(round_num)
        
        conn.close()
        return bitfield
    
    def save_remote_chunk(self, round_num, source_client_id, chunk_id, chunk_data):
        """
        Fix: Save BitTorrent exchanged chunk to new table, avoid schema conflicts
        """
        import hashlib
        chunk_hash = hashlib.sha256(chunk_data).hexdigest()
        
        # Ensure BitTorrent tables exist
        try:
            # Write directly to bt_chunks table (avoid conflicts with existing chunk_metadata table)
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 🚀 NEW: Save to round-specific tables
            self._ensure_round_tables_exist(round_num)
            
            # Write to round-specific bt_chunks table
            bt_table = f"bt_chunks_r{round_num}"
            cursor.execute(f'''
                INSERT OR REPLACE INTO {bt_table} 
                (source_client_id, chunk_id, chunk_hash, holder_client_id, is_verified)
                VALUES (?, ?, ?, ?, 1)
            ''', (source_client_id, chunk_id, chunk_hash, self.client_id))
            
            # Write to round-specific chunk_data table
            data_table = f"chunk_data_r{round_num}"
            cursor.execute(f'''
                INSERT OR REPLACE INTO {data_table} (chunk_hash, data)
                VALUES (?, ?)
            ''', (chunk_hash, pickle.dumps(chunk_data)))
            
            conn.commit()
            conn.close()
            
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                # Initialize BitTorrent tables for this round
                self._init_bittorrent_tables(round_num)
                # Retry
                return self.save_remote_chunk(round_num, source_client_id, chunk_id, chunk_data)
            else:
                raise e
        
        
        # Trigger change callback
        if self.change_callback:
            # Create ChunkInfo object to report remote chunk save event
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
        New: Get chunk data (for sending to other peers)
        """
        conn = self._get_optimized_connection()
        cursor = conn.cursor()
        
        try:
            # 🚀 NEW: Query from round-specific tables
            metadata_table = f"chunk_metadata_r{round_num}"
            bt_table = f"bt_chunks_r{round_num}"
            data_table = f"chunk_data_r{round_num}"
            
            # First query local chunks
            if source_client_id == self.client_id:
                cursor.execute(f'''
                    SELECT cd.data FROM {metadata_table} cm
                    JOIN {data_table} cd ON cm.chunk_hash = cd.chunk_hash
                    WHERE cm.chunk_id = ?
                ''', (chunk_id,))
            else:
                # Query BitTorrent exchanged chunks
                cursor.execute(f'''
                    SELECT cd.data FROM {bt_table} bc
                    JOIN {data_table} cd ON bc.chunk_hash = cd.chunk_hash
                    WHERE bc.source_client_id = ? 
                    AND bc.chunk_id = ? AND bc.holder_client_id = ?
                ''', (source_client_id, chunk_id, self.client_id))
            
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
            # Database operation error, record detailed information
            logger.error(f"[ChunkManager] Client {self.client_id}: SQLite OperationalError in get_chunk_data: {e}")
            logger.error(f"[ChunkManager] Client {self.client_id}: Query params: round_num={round_num}, source_client_id={source_client_id}, chunk_id={chunk_id}")
            return None
        except sqlite3.DatabaseError as e:
            # Database error
            logger.error(f"[ChunkManager] Client {self.client_id}: SQLite DatabaseError in get_chunk_data: {e}")
            return None
        except Exception as e:
            # Other exception
            logger.error(f"[ChunkManager] Client {self.client_id}: Unexpected error in get_chunk_data: {e}")
            return None
        finally:
            conn.close()
    
    def start_bittorrent_session(self, round_num, expected_chunks):
        """Start BitTorrent exchange session (using round-specific tables)"""
        try:
            # Ensure round-specific tables exist
            self._ensure_round_tables_exist(round_num)
            
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Use round-specific session tracking table
            session_table = f"bt_sessions_r{round_num}"
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {session_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time REAL NOT NULL,
                    status TEXT DEFAULT 'active',
                    total_chunks_expected INTEGER,
                    total_chunks_received INTEGER DEFAULT 0,
                    end_time REAL
                )
            ''')
            
            cursor.execute(f'''
                INSERT OR REPLACE INTO {session_table}
                (start_time, status, total_chunks_expected)
                VALUES (?, 'active', ?)
            ''', (time.time(), expected_chunks))
            
            conn.commit()
            conn.close()
            logger.debug(f"[ChunkManager] Started BitTorrent session for round {round_num} (round-specific table)")
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to start BitTorrent session: {e}")
        
        logger.info(f"[ChunkManager] Started BitTorrent session for round {round_num}")
    
    def finish_bittorrent_session(self, round_num, status='completed'):
        """End BitTorrent exchange session (using round-specific tables)"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Count received chunks from round-specific table
            bt_table = f"bt_chunks_r{round_num}"
            cursor.execute(f'''
                SELECT name FROM sqlite_master WHERE type='table' AND name = ?
            ''', (bt_table,))
            
            chunks_received = 0
            if cursor.fetchone():
                cursor.execute(f'''
                    SELECT COUNT(*) FROM {bt_table}
                    WHERE holder_client_id = ?
                ''', (self.client_id,))
                chunks_received = cursor.fetchone()[0]
            else:
                # Fallback to legacy table
                cursor.execute('''
                    SELECT COUNT(*) FROM bt_chunks
                    WHERE round_num = ? AND holder_client_id = ?
                ''', (round_num, self.client_id))
                chunks_received = cursor.fetchone()[0]
            
            # Update session status in round-specific table
            session_table = f"bt_sessions_r{round_num}"
            cursor.execute(f'''
                UPDATE {session_table}
                SET end_time = ?, status = ?, total_chunks_received = ?
                WHERE id = 1
            ''', (time.time(), status, chunks_received))
            
            conn.commit()
            conn.close()
            
            logger.info(f"[ChunkManager] Finished BitTorrent session for round {round_num}, status: {status}")
            
        except sqlite3.OperationalError:
            # Table doesn't exist, ignore
            pass
    
    def cleanup_bittorrent_data(self, keep_rounds=5):
        """Clean old BitTorrent data (using ultra-fast table drop method)"""
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Find all BitTorrent session tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'bt_sessions_r%'")
            session_tables = [row[0] for row in cursor.fetchall()]
            
            # Find all BitTorrent chunk tables  
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'bt_chunks_r%'")
            bt_tables = [row[0] for row in cursor.fetchall()]
            
            # Extract round numbers and sort
            rounds = set()
            for table in session_tables + bt_tables:
                try:
                    if table.startswith('bt_sessions_r'):
                        round_num = int(table.replace('bt_sessions_r', ''))
                    elif table.startswith('bt_chunks_r'):
                        round_num = int(table.replace('bt_chunks_r', ''))
                    rounds.add(round_num)
                except ValueError:
                    continue
            
            rounds = sorted(list(rounds), reverse=True)  # Newest first
            
            if len(rounds) <= keep_rounds:
                logger.debug(f"🧹 Only {len(rounds)} BitTorrent rounds exist, no cleanup needed")
                conn.close()
                return
                
            # Rounds to delete (keep the most recent ones)
            rounds_to_delete = rounds[keep_rounds:]
            
            start_time = time.time()
            deleted_tables = 0
            
            for round_num in rounds_to_delete:
                # Drop BitTorrent tables for old rounds
                for table_type in ["bt_chunks", "bt_sessions"]:
                    table_name = f"{table_type}_r{round_num}"
                    try:
                        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        deleted_tables += 1
                    except Exception as e:
                        logger.warning(f"Failed to drop BitTorrent table {table_name}: {e}")
            
            conn.commit()
            elapsed = time.time() - start_time
            
            logger.info(f"🧹✅ BitTorrent ultra-fast cleanup completed in {elapsed:.2f}s:")
            logger.info(f"   - Deleted {len(rounds_to_delete)} BitTorrent rounds")
            logger.info(f"   - Dropped {deleted_tables} tables")
            
            conn.close()
            
        except sqlite3.OperationalError:
            # Table doesn't exist, ignore
            pass
    
    def get_available_clients_for_round(self, round_num: int) -> List[int]:
        """
        Get all available client IDs for specified round
        
        Args:
            round_num: Target round
            
        Returns:
            List[int]: Available client ID list
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # 🚀 NEW: Query from round-specific tables
            metadata_table = f"chunk_metadata_r{round_num}"
            bt_table = f"bt_chunks_r{round_num}"
            
            local_clients = []
            bt_clients = []
            
            # Query clients with local chunk data
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_table,))
            if cursor.fetchone():
                cursor.execute(f'''
                    SELECT DISTINCT ? as client_id
                    FROM {metadata_table} 
                    LIMIT 1
                ''', (self.client_id,))
                
                local_result = cursor.fetchall()
                local_clients = [row[0] for row in local_result] if local_result else []
            
            # Query clients in BitTorrent chunks
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (bt_table,))
            if cursor.fetchone():
                cursor.execute(f'''
                    SELECT DISTINCT source_client_id 
                    FROM {bt_table} 
                    ORDER BY source_client_id
                ''')
                
                bt_clients = [row[0] for row in cursor.fetchall()]
            
            # Merge and deduplicate
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
        Reconstruct specified client's model parameters from chunks
        
        Args:
            client_id: Target client ID
            round_num: Target round
            
        Returns:
            Dict: Reconstructed model parameter dictionary, return None if failed
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Query all chunks for this client (round-aware)
            if client_id == self.client_id:
                # Local client - query local chunks from round-specific table
                metadata_table = f"chunk_metadata_r{round_num}"
                cursor.execute(f'''
                    SELECT name FROM sqlite_master WHERE type='table' AND name = ?
                ''', (metadata_table,))
                if cursor.fetchone():
                    cursor.execute(f'''
                        SELECT chunk_id, chunk_hash 
                        FROM {metadata_table}
                        ORDER BY chunk_id
                    ''')
                    local_chunks = cursor.fetchall()
                else:
                    # Fallback to legacy table
                    cursor.execute('''
                        SELECT chunk_id, chunk_hash 
                        FROM chunk_metadata 
                        WHERE round_num = ?
                        ORDER BY chunk_id
                    ''', (round_num,))
                    local_chunks = cursor.fetchall()
            else:
                # Remote client - query BitTorrent chunks from round-specific table
                bt_table = f"bt_chunks_r{round_num}"
                cursor.execute(f'''
                    SELECT name FROM sqlite_master WHERE type='table' AND name = ?
                ''', (bt_table,))
                if cursor.fetchone():
                    cursor.execute(f'''
                        SELECT chunk_id, chunk_hash 
                        FROM {bt_table}
                        WHERE source_client_id = ?
                        ORDER BY chunk_id
                    ''', (client_id,))
                    local_chunks = cursor.fetchall()
                else:
                    # Fallback to legacy table
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
            
            # Reconstruct model parameters
            chunk_data_list = []
            missing_chunks = []
            
            for chunk_id, chunk_hash in local_chunks:
                # Get chunk data (try round-specific table first)
                data_table = f"chunk_data_r{round_num}"
                cursor.execute(f'''
                    SELECT name FROM sqlite_master WHERE type='table' AND name = ?
                ''', (data_table,))
                
                result = None
                if cursor.fetchone():
                    # Try round-specific table first
                    cursor.execute(f'''
                        SELECT data FROM {data_table} WHERE chunk_hash = ?
                    ''', (chunk_hash,))
                    result = cursor.fetchone()
                
                if not result:
                    # Fallback to legacy table (prefer round-specific data)
                    cursor.execute('''
                        SELECT cd.data FROM chunk_data cd 
                        JOIN chunk_metadata cm ON cd.chunk_hash = cm.chunk_hash 
                        WHERE cd.chunk_hash = ? AND cm.round_num = ?
                        LIMIT 1
                    ''', (chunk_hash, round_num))
                    result = cursor.fetchone()
                    
                    # Ultimate fallback: any chunk with this hash
                    if not result:
                        cursor.execute('''
                            SELECT data FROM chunk_data WHERE chunk_hash = ? LIMIT 1
                        ''', (chunk_hash,))
                        result = cursor.fetchone()
                
                if result:
                    # Get raw byte data directly, no deserialization
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
            
            # Sort by chunk_id
            chunk_data_list.sort(key=lambda x: x[0])
            
            # Deserialize each chunk and concatenate
            numpy_chunks = []
            parts_info_list = []
            
            for chunk_id, chunk_data in chunk_data_list:
                # Deserialize chunk data
                numpy_chunk = pickle.loads(chunk_data)
                numpy_chunks.append(numpy_chunk)
                
                # Get corresponding parts_info (round-aware)
                metadata_table = f"chunk_metadata_r{round_num}"
                cursor.execute(f'''
                    SELECT name FROM sqlite_master WHERE type='table' AND name = ?
                ''', (metadata_table,))
                
                parts_result = None
                if cursor.fetchone():
                    # Try round-specific table first
                    cursor.execute(f'''
                        SELECT parts_info FROM {metadata_table} WHERE chunk_id = ?
                    ''', (chunk_id,))
                    parts_result = cursor.fetchone()
                
                if not parts_result:
                    # Fallback to legacy table
                    cursor.execute('''
                        SELECT parts_info FROM chunk_metadata 
                        WHERE chunk_id = ? AND round_num = ?
                    ''', (chunk_id, round_num))
                    parts_result = cursor.fetchone()
                
                if parts_result:
                    parts_info = json.loads(parts_result[0])
                    parts_info_list.append((chunk_id, parts_info))
            
            conn.close()
            
            # Concatenate all chunk data
            if len(numpy_chunks) == 0:
                logger.error(f"[ChunkManager] No valid chunk data for client {client_id}, round {round_num}")
                return None
                
            combined_numpy = np.concatenate(numpy_chunks) if len(numpy_chunks) > 1 else numpy_chunks[0]
            
            # Use parts_info to reconstruct back to parameter dictionary
            model_params = self._reconstruct_params_dict(combined_numpy, parts_info_list)
            
            logger.debug(f"[ChunkManager] Successfully reconstructed model for client {client_id}, round {round_num}")
            return model_params
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to reconstruct model for client {client_id}, round {round_num}: {e}")
            return None
    
    def _reconstruct_params_dict(self, combined_numpy: np.ndarray, parts_info_list: List[Tuple[int, Dict]]) -> Dict:
        """
        Use parts_info to reconstruct flattened numpy array back to parameter dictionary
        
        Args:
            combined_numpy: Concatenated flattened numpy array
            parts_info_list: Format structure information [(chunk_id, parts_info), ...]
            
        Returns:
            Reconstructed model parameter dictionary
        """
        import torch
        
        params_dict = {}
        current_pos = 0
        
        # Sort parts_info by chunk_id
        parts_info_list.sort(key=lambda x: x[0])
        
        for chunk_id, parts_info in parts_info_list:
            for param_name, parts in parts_info.items():
                if param_name not in params_dict:
                    # First time encountering this parameter, need to estimate total size
                    total_size = self._estimate_param_size(param_name, parts_info_list)
                    params_dict[param_name] = np.zeros(total_size, dtype=combined_numpy.dtype)
                
                # Fill various parts of this parameter
                for flat_start, flat_end, shape in parts:
                    chunk_size = flat_end - flat_start
                    chunk_data = combined_numpy[current_pos:current_pos + chunk_size]
                    
                    # Put data back to corresponding position of original parameter
                    params_dict[param_name][flat_start:flat_end] = chunk_data
                    current_pos += chunk_size
        
        # Convert numpy array to PyTorch tensor and reshape
        final_params = {}
        for param_name, flat_data in params_dict.items():
            # Get original shape from parts_info
            original_shape = self._get_original_shape(param_name, parts_info_list)
            if original_shape:
                reshaped_data = flat_data.reshape(original_shape)
                final_params[param_name] = torch.tensor(reshaped_data, dtype=torch.float32)
            else:
                final_params[param_name] = torch.tensor(flat_data, dtype=torch.float32)
        
        return final_params
    
    def _estimate_param_size(self, param_name: str, parts_info_list: List[Tuple[int, Dict]]) -> int:
        """Estimate parameter total size based on original shape"""
        import numpy as np
        
        # Get original shape from parts_info - this gives us the complete parameter dimensions
        original_shape = self._get_original_shape(param_name, parts_info_list)
        if original_shape:
            # Calculate full parameter size from shape: np.prod([256,128,3,3]) = 786432
            full_size = int(np.prod(original_shape))
            return full_size
        else:
            # Fallback to old logic if shape not available (shouldn't happen in normal cases)
            max_end = 0
            for _, parts_info in parts_info_list:
                if param_name in parts_info:
                    for flat_start, flat_end, shape in parts_info[param_name]:
                        max_end = max(max_end, flat_end)
            return max_end
    
    def _get_original_shape(self, param_name: str, parts_info_list: List[Tuple[int, Dict]]) -> Optional[tuple]:
        """Get parameter original shape"""
        for _, parts_info in parts_info_list:
            if param_name in parts_info:
                # Assume same parameter has same shape in all chunks, take the first one
                for flat_start, flat_end, shape in parts_info[param_name]:
                    return tuple(shape)
        return None
    
    def get_client_sample_size(self, client_id: int, round_num: int) -> Optional[int]:
        """
        Get sample count for specified client in specified round
        
        Args:
            client_id: Client ID
            round_num: Round
            
        Returns:
            int: Sample count, return None if not found
        """
        try:
            conn = self._get_optimized_connection()
            cursor = conn.cursor()
            
            # Check if we have chunks from this client for this round
            # Note: sample_size column doesn't exist in schema, using fallback approach
            
            # 🚀 NEW: Query from round-specific tables
            if client_id == self.client_id:
                # Local client - check round-specific metadata table
                metadata_table = f"chunk_metadata_r{round_num}"
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (metadata_table,))
                if cursor.fetchone():
                    cursor.execute(f'''
                        SELECT COUNT(*) FROM {metadata_table}
                    ''')
                else:
                    # Fallback to legacy table
                    cursor.execute('''
                        SELECT COUNT(*) FROM chunk_metadata 
                        WHERE round_num = ?
                    ''', (round_num,))
            else:
                # Remote client - check round-specific bt_chunks table
                bt_table = f"bt_chunks_r{round_num}"
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (bt_table,))
                if cursor.fetchone():
                    cursor.execute(f'''
                        SELECT COUNT(*) FROM {bt_table} 
                        WHERE source_client_id = ?
                    ''', (client_id,))
                else:
                    # Fallback to legacy table
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