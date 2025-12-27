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
import random
import functools
from typing import Dict, List, Optional, Tuple, Any, Callable, NamedTuple
import numpy as np
import torch
import torch.nn as nn
from datetime import datetime
import logging
from contextlib import closing

# Import ChunkInfo for change monitoring
from federatedscope.core.chunk_tracker import ChunkInfo, ChunkAction
from federatedscope.core.chunk_cache import ChunkDataCache
from federatedscope.core.trainers.utils import filter_by_specified_keywords

logger = logging.getLogger(__name__)


class Reconstruction(NamedTuple):
    """
    Model parameter reconstruction result with coverage information
    
    Attributes:
        params: Dictionary of reconstructed model parameters
        mask: Boolean mask indicating which parts were actually received
        coverage: Per-parameter coverage ratio (0.0 to 1.0)
        missing_chunks: Number of chunks that were missing
    """
    params: Dict[str, torch.Tensor]
    mask: Dict[str, torch.Tensor]
    coverage: Dict[str, float]
    missing_chunks: int

# 🚀 DATABASE RETRY DECORATOR WITH EXPONENTIAL BACKOFF
def db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0):
    """
    Decorator for database operations with exponential backoff retry on lock/busy errors.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    error_msg = str(e).lower()
                    if ("database is locked" in error_msg or "database is busy" in error_msg) and attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        jitter = random.uniform(0, delay * 0.1)  # Add 10% jitter
                        total_delay = delay + jitter
                        
                        logger.warning(f"[DB-RETRY] {func.__name__} attempt {attempt + 1}/{max_retries} failed: {e}, retrying in {total_delay:.3f}s")
                        time.sleep(total_delay)
                        continue
                    else:
                        logger.error(f"[DB-RETRY] {func.__name__} failed after {attempt + 1} attempts: {e}")
                        raise
                except Exception as e:
                    # Non-lock errors should not be retried
                    logger.error(f"[DB-RETRY] {func.__name__} failed with non-retryable error: {e}")
                    raise
            
            # This should never be reached, but just in case
            raise sqlite3.OperationalError(f"Database operation failed after {max_retries} attempts")
        return wrapper
    return decorator

# 🚀 UNIFIED DATABASE CONNECTION WITH STANDARD PRAGMA SETTINGS  
def create_optimized_connection(db_path: str, timeout: float = 30.0) -> sqlite3.Connection:
    """
    Create a SQLite connection with optimized and unified PRAGMA settings.
    
    Args:
        db_path: Path to the database file
        timeout: Connection timeout in seconds
        
    Returns:
        Optimized SQLite connection
    
    Notes:
        - page_size only affects new databases or after VACUUM
        - mmap_size reduced to 64MB to limit memory usage with many connections
        - Settings balanced for federated learning workload patterns
    """
    conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)
    cursor = conn.cursor()
    
    # 🚀 UNIFIED PRAGMA SETTINGS FOR ALL CONNECTIONS
    pragma_settings = [
        ("journal_mode", "WAL"),           # Enable WAL mode for better concurrency
        ("synchronous", "NORMAL"),         # Balance safety and performance
        ("cache_size", "10000"),           # 10000 pages cache
        ("temp_store", "MEMORY"),          # Store temp tables in memory
        ("busy_timeout", "30000"),         # 30 second busy timeout
        ("wal_autocheckpoint", "1000"),    # Auto-checkpoint every 1000 pages
        ("mmap_size", "67108864"),         # 🚀 REDUCED: 64MB (was 256MB) to reduce memory footprint
        ("page_size", "4096"),             # 4KB page size (only effective for new DBs)
        ("foreign_keys", "ON"),            # Enable foreign key constraints
    ]
    
    for pragma, value in pragma_settings:
        try:
            cursor.execute(f"PRAGMA {pragma}={value}")
            logger.debug(f"[DB-CONFIG] Set {pragma}={value} for {db_path}")
        except sqlite3.OperationalError as e:
            logger.warning(f"[DB-CONFIG] Failed to set {pragma}={value}: {e}")
    
    return conn


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
    
    def __init__(self, client_id: int, change_callback: Optional[Callable[[ChunkInfo], None]] = None, cfg=None):
        """
        Initialize ChunkManager, create independent database files for each round

        Args:
            client_id: Client ID, used to create node-specific database directory
            change_callback: Callback function for database changes, used to report chunk changes to server
            cfg: Configuration object (optional, for accessing bittorrent settings)
        """
        self.client_id = client_id
        self.change_callback = change_callback
        self._cfg = cfg  # 🚀 NEW: Store config for fast path optimization
        self.chunk_write_queue = None  # 🚀 NEW: Reference to ChunkWriteQueue
        self._bt_session_ids = {}  # 🚀 Track session IDs by round
        
        # Create database directory by node name: /tmp/fl_chunks/client_X/
        # Use /tmp (tmpfs, 2.8GB/s) instead of NFS (441MB/s) for 6x faster I/O
        client_name = f"client_{client_id}"
        self.db_dir = os.path.join("/tmp", "fl_chunks", client_name)
        os.makedirs(self.db_dir, exist_ok=True)
        
        # 🚀 NEW: Initialize high-performance chunk data cache (replaces database for chunk data)
        # Features: Two-tier caching (RAM + Disk), streaming I/O support, bounded memory usage
        self.chunk_cache = ChunkDataCache(
            client_id=client_id, 
            cache_dir=os.path.join(self.db_dir, "chunks"),
            max_ram_chunks=200,  # Allow more chunks in RAM for better performance
            max_ram_size_mb=512  # 512MB RAM cache with LRU eviction policy
        )
        
        # Store round-specific database paths
        self.round_db_paths = {}  # {round_num: db_path} or {(round_num, table_type): db_path}
        
        # Legacy database path for fallback compatibility
        self.legacy_db_path = os.path.join(self.db_dir, f"{client_name}_chunks.db")
        
        # Change monitoring related
        self.monitoring_enabled = False
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
        self.last_db_mtime = 0
        
        logger.info(f"📊 Initialize round-based chunk database system for node {client_id}: {self.db_dir}")
        
        # If callback function is provided, start monitoring
        if change_callback:
            self.start_monitoring()
        
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def _get_optimized_connection(self, round_num: Optional[int] = None, table_type: str = None, _ensure_exists: bool = True):
        """Get optimized database connection for specific round and table type
        
        Args:
            round_num: Round number. If specified, connects to round-specific database.
                      If None, connects to legacy database for backward compatibility.
            table_type: Table type ('metadata', 'data', 'bt', 'sessions'). If None, uses legacy unified database
            _ensure_exists: Internal flag to prevent recursion, set to False in _ensure_table_database_exists
        """
        if round_num is not None and table_type is not None and _ensure_exists:
            # 🔧 COMPATIBILITY FIX: Ensure table-specific database exists before connecting
            self._ensure_table_database_exists(round_num, table_type)
            db_path = self._get_round_db_path(round_num, table_type)
        elif round_num is not None:
            # Legacy compatibility: use unified database path
            if table_type is None:
                # 🔧 COMPATIBILITY WARNING: Legacy unified database mode
                logger.debug(f"[ChunkManager] Using legacy unified database for round {round_num}")
            db_path = self._get_round_db_path(round_num, table_type)
        else:
            db_path = self.legacy_db_path
            
        # 🚀 Use unified connection function with standard PRAGMA settings
        return create_optimized_connection(db_path, timeout=30.0)
    
    def set_chunk_write_queue(self, chunk_write_queue):
        """
        🚀 NEW: Set ChunkWriteQueue reference, used to check write queue when database query fails
        Args:
            chunk_write_queue: ChunkWriteQueue instance
        """
        self.chunk_write_queue = chunk_write_queue
        logger.debug(f"[ChunkManager] Set chunk_write_queue reference for client {self.client_id}")
    
    def _get_round_db_path(self, round_num: int, table_type: str = None) -> str:
        """Get database file path for specific round and table type
        
        Args:
            round_num: Round number
            table_type: Table type ('metadata', 'data', 'bt', 'sessions'). If None, returns legacy path
        Returns:
            str: Path to table-specific database file
        """
        if table_type is None:
            # Legacy compatibility: return unified database path
            if round_num not in self.round_db_paths:
                client_name = f"client_{self.client_id}"
                db_filename = f"{client_name}_chunks_r{round_num}.db"
                self.round_db_paths[round_num] = os.path.join(self.db_dir, db_filename)
            return self.round_db_paths[round_num]
        
        # New: table-specific database paths
        cache_key = (round_num, table_type)
        if cache_key not in self.round_db_paths:
            client_name = f"client_{self.client_id}"
            db_filename = f"{client_name}_{table_type}_r{round_num}.db"
            self.round_db_paths[cache_key] = os.path.join(self.db_dir, db_filename)
        
        return self.round_db_paths[cache_key]
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def _init_round_database(self, round_num: int):
        """Initialize table-specific databases for specific round (deprecated - use _ensure_table_database_exists)
        
        Args:
            round_num: Round number for which to create database
        """
        # Redirect to new table-specific database creation
        self._ensure_table_database_exists(round_num, 'metadata')
        self._ensure_table_database_exists(round_num, 'data')
        self._ensure_table_database_exists(round_num, 'bt')
        self._ensure_table_database_exists(round_num, 'sessions')
        
        logger.debug(f"[ChunkManager] Initialized split databases for client {self.client_id} round {round_num}")
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def _ensure_round_database_exists(self, round_num: int):
        """Ensure round-specific database file exists (DEPRECATED - use _ensure_table_database_exists)
        
        Args:
            round_num: Round number for which to ensure database exists
        """
        # Redirect to new table-specific database creation
        self._ensure_table_database_exists(round_num, 'metadata')
        self._ensure_table_database_exists(round_num, 'data')
        self._ensure_table_database_exists(round_num, 'bt')
        self._ensure_table_database_exists(round_num, 'sessions')
        
        logger.debug(f"[ChunkManager] Ensured all split databases exist for client {self.client_id} round {round_num}")
        
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
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
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def _ensure_round_tables_exist(self, round_num: int, table_type: str = None):
        """Ensure round-specific database and tables exist for specific table type
        
        Args:
            round_num: Round number
            table_type: Specific table type ('metadata', 'data', 'bt', 'sessions'). 
                       If None, ensures all tables (legacy compatibility)
        """
        if table_type is None:
            # Legacy: ensure all table databases exist
            self._ensure_table_database_exists(round_num, 'metadata')
            self._ensure_table_database_exists(round_num, 'data') 
            self._ensure_table_database_exists(round_num, 'bt')
            self._ensure_table_database_exists(round_num, 'sessions')
        else:
            # New: ensure only the specific table database exists
            self._ensure_table_database_exists(round_num, table_type)
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def _ensure_table_database_exists(self, round_num: int, table_type: str):
        """Ensure table-specific database exists and create its table
        
        Args:
            round_num: Round number
            table_type: Table type ('metadata', 'data', 'bt', 'sessions')
        """
        db_path = self._get_round_db_path(round_num, table_type)
        
        # Check if database file already exists
        if not os.path.exists(db_path):
            logger.debug(f"[ChunkManager] Creating new {table_type} database for round {round_num}")
            
            # Connect to the table-specific database (prevent recursion)
            conn = self._get_optimized_connection(round_num=round_num, table_type=table_type, _ensure_exists=False)
            cursor = conn.cursor()
            
            try:
                if table_type == 'metadata':
                    # Create metadata table
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS chunk_metadata (
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
                elif table_type == 'data':
                    # Create chunk data table
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS chunk_data (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            chunk_hash TEXT UNIQUE NOT NULL,
                            data BLOB NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                elif table_type == 'bt':
                    # Create BitTorrent chunks table
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS bt_chunks (
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
                elif table_type == 'sessions':
                    # Create BitTorrent session table
                    cursor.execute('''
                        CREATE TABLE IF NOT EXISTS bt_sessions (
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
                
                # Create table-specific indexes
                if table_type == 'metadata':
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chunk_metadata_hash ON chunk_metadata(chunk_hash)')
                elif table_type == 'bt':
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_bt_chunks_hash ON bt_chunks(chunk_hash)')
                elif table_type == 'data':
                    cursor.execute('CREATE INDEX IF NOT EXISTS idx_chunk_data_hash ON chunk_data(chunk_hash)')
                
                conn.commit()
                logger.debug(f"✅ Created {table_type} database for round {round_num}")
                
            except Exception as e:
                logger.error(f"❌ Failed to create {table_type} database for round {round_num}: {e}")
                conn.rollback()
            finally:
                conn.close()
        else:
            logger.debug(f"[ChunkManager] {table_type} database for round {round_num} already exists")
    
    def _cleanup_old_rounds_by_files(self, keep_rounds: int = 2, current_round: int = None):
        """Ultra-fast cleanup by deleting entire round database files"""
        try:
            # Get all existing round database files
            import glob
            # 🔧 COMPATIBILITY FIX: Support both legacy and split database patterns
            legacy_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_chunks_r*.db")
            split_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_*_r*.db")
            legacy_files = glob.glob(legacy_pattern)
            split_files = glob.glob(split_pattern)
            db_files = legacy_files + split_files
            
            # Extract round numbers and sort
            rounds = []
            for db_file in db_files:
                try:
                    # Extract round number from filename like "client_1_chunks_r5.db"
                    basename = os.path.basename(db_file)
                    round_str = basename.split('_r')[1].replace('.db', '')
                    round_num = int(round_str)
                    rounds.append((round_num, db_file))
                except (ValueError, IndexError):
                    continue
            
            rounds.sort(reverse=True)  # Newest first
            
            # Use current_round-based cleanup if provided (preferred)
            if current_round is not None:
                cutoff_round = max(current_round - keep_rounds, 0)
                files_to_delete = [(round_num, db_file) for round_num, db_file in rounds if round_num < cutoff_round]
                logger.debug(f"🧹 Round-based cleanup: current={current_round}, keep={keep_rounds}, cutoff={cutoff_round}")
            else:
                # Fallback: count-based cleanup (legacy behavior)
                if len(rounds) <= keep_rounds:
                    logger.debug(f"🧹 Only {len(rounds)} round databases exist, no cleanup needed")
                    return
                files_to_delete = rounds[keep_rounds:]
            logger.info(f"🧹 Deleting {len(files_to_delete)} old round database files")
            
            start_time = time.time()
            deleted_files = 0
            
            for round_num, db_file in files_to_delete:
                try:
                    if os.path.exists(db_file):
                        os.remove(db_file)
                        deleted_files += 1
                        logger.debug(f"🗑️ Deleted round {round_num} database: {db_file}")
                        
                        # Also remove from cache
                        if round_num in self.round_db_paths:
                            del self.round_db_paths[round_num]
                            
                except Exception as e:
                    logger.warning(f"Failed to delete database file {db_file}: {e}")
            
            elapsed = time.time() - start_time
            
            logger.info(f"🧹✅ Ultra-fast file cleanup completed in {elapsed:.2f}s:")
            logger.info(f"   - Deleted {deleted_files} round database files")
            logger.info(f"   - Performance: {deleted_files/elapsed:.1f} files/sec" if elapsed > 0 else "   - Performance: instant")
            
        except Exception as e:
            logger.error(f"❌ Failed to cleanup old round database files: {e}")
    
    @staticmethod
    def model_to_params(model: nn.Module) -> Dict[str, np.ndarray]:
        """
        Convert model parameters to numpy arrays, excluding ALL BatchNorm parameters
        
        Filters out ALL BN-related parameters:
        - Trainable: weight (γ), bias (β) 
        - Statistics: running_mean, running_var, num_batches_tracked
        
        Only Conv/Linear layers and other non-BN parameters participate in federated learning
        """
        params = {}
        
        # Include trainable parameters but exclude ALL BN parameters
        for name, param in model.named_parameters():
            # Skip ALL BatchNorm parameters (weight/bias from BN layers)
            if any(bn_key in name for bn_key in [
                'bn', 'batch_norm', 'batchnorm', '_bn', '.bn.', 
                'norm', 'layernorm', 'layer_norm'  # Also exclude other normalization layers
            ]):
                continue
            params[name] = param.data.cpu().numpy()
        
        # Skip ALL buffers (running_mean, running_var, num_batches_tracked, etc.)
        # All normalization statistics remain local to each client
        
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
        
        # Log parameter names in the highest importance chunk
        if importance_scores and chunks_info:
            max_idx = importance_scores.index(max(importance_scores))
            highest_chunk_params = list(chunks_info[max_idx]['parts'].keys())
            logger.info(f"[ChunkManager] Highest importance chunk (score: {importance_scores[max_idx]:.4f}) contains parameters: {highest_chunk_params}")
        
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
            # 🚀 NEW: Check if metadata database exists for this round
            metadata_db_path = self._get_round_db_path(round_num, 'metadata')
            if not os.path.exists(metadata_db_path):
                logger.debug(f"No metadata database found for round {round_num}")
                return {}
            
            conn = self._get_optimized_connection(round_num, 'metadata')
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT chunk_id, importance_score, pruning_method, flat_size, chunk_hash
                FROM chunk_metadata 
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
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def save_model_chunks(self, model: nn.Module, round_num: int, num_chunks: int = 10, keep_rounds: int = 2, 
                         importance_method: str = 'magnitude', persist_to_db: bool = False) -> List[str]:
        """
        Split model into chunks and save to cache system (and optionally database)
        
        Args:
            model: PyTorch model
            round_num: Training round number
            num_chunks: Number of chunks to split
            keep_rounds: Keep data from recent rounds, default 2 rounds
            importance_method: Chunk importance calculation method ('magnitude', 'l2_norm', 'snip', 'fisher')
            persist_to_db: Whether to persist chunk data to database (default False - cache only)
            
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
            
            # 🚀 NEW: Ensure metadata database exists (data stored in cache system)
            self._ensure_table_database_exists(round_num, 'metadata')
            
            saved_hashes = []
            chunk_metadata_batch = []  # Batch metadata operations
            data_batch = []  # Batch data operations if persisting to DB
            
            # Prepare all chunks and collect batch data
            for i, chunk_info in enumerate(chunks_info):
                # Extract chunk data
                chunk_data = self.extract_chunk_data(params, chunk_info)
                
                # Calculate chunk hash
                chunk_bytes = pickle.dumps(chunk_data)
                chunk_hash = hashlib.sha256(chunk_bytes).hexdigest()
                
                # 🚀 Save to high-performance cache system first
                if hasattr(self, 'chunk_cache') and self.chunk_cache is not None:
                    self.chunk_cache.save_chunk_data(
                        round_num=round_num,
                        source_client_id=self.client_id,  # Local chunks from this client
                        chunk_id=chunk_info['chunk_id'],
                        chunk_data=chunk_bytes  # Already serialized bytes
                    )
                
                # Prepare metadata batch
                parts_json = json.dumps(chunk_info['parts'])
                importance_score = importance_scores[i] if i < len(importance_scores) else 0.0
                chunk_metadata_batch.append((
                    chunk_info['chunk_id'], chunk_hash, parts_json, 
                    chunk_info['flat_size'], importance_score, importance_method
                ))
                
                # Prepare data batch if persistence is enabled
                if persist_to_db:
                    data_batch.append((chunk_hash, chunk_bytes))
                
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
            
            # 🚀 BATCH OPERATION: Save all metadata in one transaction
            if chunk_metadata_batch:
                metadata_conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                try:
                    metadata_cursor = metadata_conn.cursor()
                    metadata_cursor.executemany('''
                        INSERT OR REPLACE INTO chunk_metadata 
                        (chunk_id, chunk_hash, parts_info, flat_size, importance_score, pruning_method)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', chunk_metadata_batch)
                    metadata_conn.commit()
                    logger.debug(f"✅ Batch saved {len(chunk_metadata_batch)} chunk metadata entries for round {round_num}")
                finally:
                    metadata_conn.close()
            
            # 🚀 BATCH OPERATION: Save all data in one transaction (if persistence enabled)
            if persist_to_db and data_batch:
                self._ensure_table_database_exists(round_num, 'data')
                data_conn = self._get_optimized_connection(round_num=round_num, table_type='data')
                try:
                    data_cursor = data_conn.cursor()
                    data_cursor.executemany('''
                        INSERT OR IGNORE INTO chunk_data (chunk_hash, data)
                        VALUES (?, ?)
                    ''', data_batch)
                    data_conn.commit()
                    logger.debug(f"✅ Batch persisted {len(data_batch)} chunks to database for round {round_num}")
                except Exception as db_error:
                    logger.warning(f"⚠️ Failed to batch persist chunks to database: {db_error}")
                finally:
                    data_conn.close()
                

            
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
            # 🚀 NEW: Check if round-specific database exists
            db_path = self._get_round_db_path(round_num, 'metadata')
            if not os.path.exists(db_path):
                logger.debug(f"No metadata database found for round {round_num}")
                return []
            
            # 🚀 EXPLICIT: Use clearly named metadata connection
            metadata_conn = self._get_optimized_connection(round_num, 'metadata')
            try:
                metadata_cursor = metadata_conn.cursor()
                
                # Query chunk metadata from metadata-specific database
                metadata_cursor.execute('''
                    SELECT chunk_id, chunk_hash, parts_info, flat_size
                    FROM chunk_metadata
                    ORDER BY chunk_id
                ''')
                
                metadata_rows = metadata_cursor.fetchall()
            finally:
                metadata_conn.close()  # Close metadata connection explicitly
            
            chunks = []
            for chunk_id, chunk_hash, parts_json, flat_size in metadata_rows:
                # 🚀 NEW: Cache-first approach with write queue fallback
                chunk_bytes = self.chunk_cache.get_chunk_data(round_num, self.client_id, chunk_id)
                
                if chunk_bytes is None and self.chunk_write_queue:
                    # Cache miss - check write queue fallback
                    pending_chunk = self.chunk_write_queue.search_pending_chunk(round_num, self.client_id, chunk_id)
                    if pending_chunk is not None:
                        logger.debug(f"🎯 Cache miss - found chunk {chunk_id} in write queue")
                        # search_pending_chunk now always returns bytes
                        chunk_bytes = pending_chunk
                
                if chunk_bytes:
                    try:
                        chunk_data = pickle.loads(chunk_bytes)
                        chunk_info = {
                            'chunk_id': chunk_id,
                            'parts': json.loads(parts_json),
                            'flat_size': flat_size
                        }
                        chunks.append((chunk_info, chunk_data))
                    except Exception as e:
                        logger.error(f"Failed to deserialize chunk {chunk_id}: {e}")
                        continue
                else:
                    logger.warning(f"⚠️ Chunk {chunk_id} not found in cache or write queue for round {round_num}")
                    
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
            # 🚀 FIX: Check if metadata-specific database exists
            db_path = self._get_round_db_path(round_num, 'metadata')
            if not os.path.exists(db_path):
                logger.debug(f"No metadata database found for round {round_num}")
                return None
            
            # 🚀 EXPLICIT: Use clearly named metadata connection
            metadata_conn = self._get_optimized_connection(round_num, 'metadata')
            try:
                metadata_cursor = metadata_conn.cursor()
                
                metadata_cursor.execute('''
                    SELECT chunk_hash, parts_info, flat_size
                    FROM chunk_metadata
                    WHERE chunk_id = ?
                ''', (chunk_id,))
                
                row = metadata_cursor.fetchone()
            finally:
                metadata_conn.close()  # Close metadata connection explicitly
            
            if row:
                chunk_hash, parts_json, flat_size = row
                
                # 🚀 NEW: Cache-first approach with write queue fallback
                chunk_bytes = self.chunk_cache.get_chunk_data(round_num, self.client_id, chunk_id)
                source = "cache"
                
                if chunk_bytes is None and self.chunk_write_queue:
                    # Cache miss - check write queue fallback
                    pending_chunk = self.chunk_write_queue.search_pending_chunk(round_num, self.client_id, chunk_id)
                    if pending_chunk is not None:
                        source = "write_queue"
                        logger.debug(f"🎯 Cache miss - found chunk {chunk_id} in write queue for round {round_num}")
                        # search_pending_chunk now always returns bytes
                        chunk_bytes = pending_chunk
                
                if chunk_bytes:
                    try:
                        chunk_data = pickle.loads(chunk_bytes)
                        chunk_info = {
                            'chunk_id': chunk_id,
                            'parts': json.loads(parts_json),
                            'flat_size': flat_size
                        }
                        logger.debug(f"✅ Retrieved chunk {chunk_id} for round {round_num} (source: {source})")
                        return (chunk_info, chunk_data)
                    except Exception as e:
                        logger.error(f"Failed to deserialize chunk {chunk_id}: {e}")
                        return None
                    
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
            if round_num is not None:
                # Query specific round database only
                if not os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                    return {
                        'total_metadata_entries': 0,
                        'unique_chunks': 0,
                        'storage_size_bytes': 0,
                        'storage_size_mb': 0,
                        'round_range': (round_num, round_num),
                        'query_scope': f'round_{round_num}'
                    }
                
                # Query from table-specific databases
                total_metadata = 0
                total_chunks = 0 
                total_size = 0
                
                # Query metadata database
                try:
                    metadata_conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                    metadata_cursor = metadata_conn.cursor()
                    metadata_cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
                    total_metadata = metadata_cursor.fetchone()[0]
                    metadata_conn.close()
                except (sqlite3.Error, FileNotFoundError):
                    total_metadata = 0
                
                # 🚀 NEW: Get data stats from cache system instead of database
                total_chunks = total_metadata  # Use metadata count as proxy for chunk count
                total_size = 0
                
                # Estimate size from cache system if available
                if hasattr(self, 'chunk_cache') and self.chunk_cache is not None:
                    try:
                        cache_stats = self.chunk_cache.get_stats()
                        # Use disk files count and RAM size as approximation
                        if 'disk_files' in cache_stats:
                            # Each disk file represents a chunk, estimate average size
                            estimated_avg_size = 1024 * 100  # 100KB average per chunk (rough estimate)
                            total_size = cache_stats['disk_files'] * estimated_avg_size
                    except Exception:
                        total_size = 0
                
                min_round = max_round = round_num
                    
            else:
                # Aggregate stats from all round database files (support both unified and split databases)
                import glob
                
                # Find all rounds by looking for any database files
                pattern = os.path.join(self.db_dir, f"client_{self.client_id}_*_r*.db")
                all_files = glob.glob(pattern)
                
                # Extract unique round numbers
                rounds = set()
                for file_path in all_files:
                    basename = os.path.basename(file_path)
                    try:
                        round_str = basename.split('_r')[1].replace('.db', '')
                        round_num = int(round_str)
                        rounds.add(round_num)
                    except (ValueError, IndexError):
                        continue
                
                total_metadata = 0
                total_chunks = 0
                total_size = 0
                min_round = None
                max_round = None
                
                for round_num in rounds:
                    # Update min/max round numbers
                    if min_round is None or round_num < min_round:
                        min_round = round_num
                    if max_round is None or round_num > max_round:
                        max_round = round_num
                        
                    # Query metadata from metadata database
                    try:
                        metadata_conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                        metadata_cursor = metadata_conn.cursor()
                        metadata_cursor.execute("SELECT COUNT(*) FROM chunk_metadata")
                        round_metadata = metadata_cursor.fetchone()[0]
                        total_metadata += round_metadata
                        total_chunks += round_metadata  # 🚀 NEW: Use metadata count as chunk count proxy
                        metadata_conn.close()
                    except (sqlite3.Error, FileNotFoundError):
                        pass
            
            # 🚀 Add cache system statistics
            cache_stats = {}
            if hasattr(self, 'chunk_cache') and self.chunk_cache is not None:
                try:
                    cache_stats = self.chunk_cache.get_stats()
                except Exception as cache_error:
                    logger.warning(f"Failed to get cache stats: {cache_error}")
                    cache_stats = {'cache_error': str(cache_error)}
            
            return {
                'client_id': self.client_id,
                'db_dir': self.db_dir,
                'total_metadata_entries': total_metadata,
                'unique_chunks': total_chunks,
                'storage_size_bytes': total_size,
                'storage_size_mb': total_size / (1024 * 1024) if total_size else 0,
                'round_range': (min_round, max_round) if min_round is not None else (None, None),
                'query_scope': f'round_{round_num}' if round_num is not None else 'all_rounds',
                'cache_stats': cache_stats  # 🚀 Include cache performance metrics
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to get storage stats: {e}")
            return {}
    
    def cleanup_old_rounds(self, keep_rounds: int = 2, current_round: int = None):
        """
        Clean up chunks from old rounds, only keep the most recent few rounds
        
        Args:
            keep_rounds: Keep data from recent rounds, default to keep only 2 rounds
            current_round: Current round number (required for accurate cleanup)
        
        Note: This method now cleans both cache system and legacy database files
        """
        if current_round is None:
            logger.warning("🚨 cleanup_old_rounds called without current_round parameter - skipping cleanup")
            return []
            
        logger.info(f"🧹 Starting cleanup of old rounds (current: {current_round}, keep {keep_rounds} recent rounds)...")
        
        # 🚀 Clean cache system first - high-performance cache cleanup
        if hasattr(self, 'chunk_cache') and self.chunk_cache is not None:
            try:
                # Clean cache system with provided current round
                self.chunk_cache.cleanup_round(current_round, keep_rounds)
                logger.info(f"🚀 Cleaned cache system for old rounds (current round: {current_round})")
            except Exception as e:
                logger.error(f"❌ Failed to clean cache system: {e}")
        
        # Clean legacy database files (for compatibility)
        return self._cleanup_old_rounds_by_files(keep_rounds, current_round)
    
    def _cleanup_old_database_files(self, keep_rounds: int = 2):
        """Ultra-fast cleanup by deleting entire round database files using glob patterns
        
        Args:
            keep_rounds: Number of most recent rounds to keep
        """
        try:
            import glob
            client_name = f"client_{self.client_id}"
            
            # Find all rounds that exist
            pattern = os.path.join(self.db_dir, f"{client_name}_*_r*.db")
            all_files = glob.glob(pattern)
            
            # Extract unique round numbers
            rounds = set()
            for file_path in all_files:
                filename = os.path.basename(file_path)
                try:
                    # Extract round number: client_X_TYPE_rN.db -> N
                    round_part = filename.split('_r')[-1].replace('.db', '')
                    round_num = int(round_part)
                    rounds.add(round_num)
                except (ValueError, IndexError):
                    continue
            
            # Sort rounds (newest first)
            sorted_rounds = sorted(rounds, reverse=True)
            
            if len(sorted_rounds) <= keep_rounds:
                logger.debug(f"🧹 Only {len(sorted_rounds)} rounds exist, no cleanup needed")
                return
            
            # Rounds to delete (keep the most recent ones)
            rounds_to_delete = sorted_rounds[keep_rounds:]
            logger.info(f"🧹 Deleting {len(rounds_to_delete)} old rounds")
            
            start_time = time.time()
            deleted_files = 0
            total_size_freed = 0
            
            for round_num in rounds_to_delete:
                # Delete all database files for this round using glob
                round_pattern = os.path.join(self.db_dir, f"{client_name}_*_r{round_num}.db")
                round_files = glob.glob(round_pattern)
                
                for file_path in round_files:
                    try:
                        if os.path.exists(file_path):
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            total_size_freed += file_size
                            deleted_files += 1
                            filename = os.path.basename(file_path)
                            logger.debug(f"🗑️ Deleted: {filename}")
                    except Exception as e:
                        logger.warning(f"Failed to delete {file_path}: {e}")
                
                # Clean cache for this round (both old and new formats)
                if round_num in self.round_db_paths:
                    del self.round_db_paths[round_num]
                
                # Clean new cache format
                keys_to_remove = [key for key in self.round_db_paths.keys() if isinstance(key, tuple) and key[0] == round_num]
                for key in keys_to_remove:
                    del self.round_db_paths[key]
            
            elapsed = time.time() - start_time
            
            logger.info(f"🧹✅ Ultra-fast database cleanup completed in {elapsed:.2f}s:")
            logger.info(f"   - Deleted {deleted_files} database files")
            logger.info(f"   - Freed {total_size_freed / (1024*1024):.2f} MB disk space")
            logger.info(f"   - Performance: {deleted_files/elapsed:.1f} files/sec")
            
        except Exception as e:
            logger.error(f"❌ Failed to cleanup old database files: {e}")
    
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
        """Get database file modification time for split databases"""
        try:
            # 🚀 FIX: Scan all split database files for most recent modification time
            max_mtime = 0
            
            # Scan directory for split database files
            import glob
            patterns = [
                os.path.join(self.db_dir, f"client_{self.client_id}_metadata_r*.db"),
                os.path.join(self.db_dir, f"client_{self.client_id}_data_r*.db"),
                os.path.join(self.db_dir, f"client_{self.client_id}_bt_r*.db"),
                os.path.join(self.db_dir, f"client_{self.client_id}_sessions_r*.db")
            ]
            
            for pattern in patterns:
                for db_file in glob.glob(pattern):
                    if os.path.exists(db_file):
                        mtime = os.path.getmtime(db_file)
                        if mtime > max_mtime:
                            max_mtime = mtime
            
            # Fallback: check legacy unified database files if no split files found
            if max_mtime == 0:
                try:
                    import glob
                    # 🔧 COMPATIBILITY FIX: Support both legacy and split database patterns
                    legacy_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_chunks_r*.db")
                    split_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_*_r*.db")
                    legacy_files = glob.glob(legacy_pattern)
                    split_files = glob.glob(split_pattern)
                    db_files = legacy_files + split_files
                    
                    for db_file in db_files:
                        if os.path.exists(db_file):
                            mtime = os.path.getmtime(db_file)
                            if mtime > max_mtime:
                                max_mtime = mtime
                except Exception:
                    pass
            
            return max_mtime
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
        """Detect and report database changes from round-specific databases"""
        if not self.change_callback:
            return
            
        try:
            # Get recently added chunk information from all round databases
            recent_chunks = []
            
            # Get all existing round database files
            import glob
            # 🔧 COMPATIBILITY FIX: Support both legacy and split database patterns
            legacy_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_chunks_r*.db")
            split_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_*_r*.db")
            legacy_files = glob.glob(legacy_pattern)
            split_files = glob.glob(split_pattern)
            db_files = legacy_files + split_files
            
            for db_file in db_files:
                try:
                    # Extract round number from filename
                    basename = os.path.basename(db_file)
                    round_str = basename.split('_r')[1].replace('.db', '')
                    round_num = int(round_str)
                    
                    # Connect to this round's metadata database
                    conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                    cursor = conn.cursor()
                    
                    # Query recent chunks from this round metadata database
                    cursor.execute('''
                        SELECT chunk_id, chunk_hash, flat_size, created_at
                        FROM chunk_metadata
                        ORDER BY created_at DESC
                        LIMIT 5
                    ''')
                    
                    # Add round number to each result
                    for chunk_id, chunk_hash, flat_size, created_at in cursor.fetchall():
                        recent_chunks.append((round_num, chunk_id, chunk_hash, flat_size, created_at))
                    
                    conn.close()
                    
                except (ValueError, sqlite3.Error, IndexError) as e:
                    logger.debug(f"Failed to process round database {db_file}: {e}")
                    continue
            
            # Sort by creation time and limit
            recent_chunks.sort(key=lambda x: x[4] if x[4] else 0, reverse=True)
            recent_chunks = recent_chunks[:10]
            
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
        """Get all chunk information from round-specific databases"""
        chunk_infos = []
        
        try:
            # Get all existing round database files
            import glob
            # 🔧 COMPATIBILITY FIX: Support both legacy and split database patterns
            legacy_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_chunks_r*.db")
            split_pattern = os.path.join(self.db_dir, f"client_{self.client_id}_*_r*.db")
            legacy_files = glob.glob(legacy_pattern)
            split_files = glob.glob(split_pattern)
            db_files = legacy_files + split_files
            
            for db_file in db_files:
                try:
                    # Extract round number from filename
                    basename = os.path.basename(db_file)
                    round_str = basename.split('_r')[1].replace('.db', '')
                    round_num = int(round_str)
                    
                    # Connect to this round's metadata database
                    conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                    cursor = conn.cursor()
                    
                    # Query all chunks from this round metadata database
                    cursor.execute('''
                        SELECT chunk_id, chunk_hash, flat_size, created_at
                        FROM chunk_metadata
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
                    
                    conn.close()
                    
                except (ValueError, sqlite3.Error, IndexError) as e:
                    logger.debug(f"Failed to process round database {db_file}: {e}")
                    continue
                
        except Exception as e:
            logger.error(f"❌ Node {self.client_id}: Failed to get all chunk information: {e}")
            
        return chunk_infos
    
    # =================== BitTorrent Extension Methods ===================
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def _init_bittorrent_tables(self, round_num=0):
        """[UPDATED] Initialize BitTorrent tables using round-specific database files
        
        Args:
            round_num: Round number for creating round-specific database (default: 0)
            
        Note: This method now creates round-specific database instead of legacy tables.
        """
        logger.info(f"[ChunkManager] Initializing round-specific BitTorrent database for round {round_num}")
        
        # Use the existing round database creation method which includes all necessary tables
        self._ensure_round_database_exists(round_num)
        logger.debug(f"[ChunkManager] BitTorrent database initialized for client {self.client_id} round {round_num}")
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def get_global_bitfield(self, round_num=None):
        """
        Fix: Compatible with old code, support optional round_num parameter
        Get global chunk ownership bitfield for specified round
        """
        import time as _time
        func_start = _time.time()

        # If round_num not provided, use current round
        if round_num is None:
            round_num = getattr(self, 'current_round', 0)

        bitfield = {}

        try:
            # Query locally saved chunks from metadata database (if exists)
            if os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                # 🔍 DEBUG: Timing for metadata DB connection
                conn_start = _time.time()
                metadata_conn = self._get_optimized_connection(round_num, 'metadata')
                conn_time = _time.time() - conn_start
                if conn_time > 1.0:
                    logger.warning(f"[🐌 DB-SLOW] Client {self.client_id}: metadata connection took {conn_time:.2f}s")

                try:
                    metadata_cursor = metadata_conn.cursor()
                    # 🔍 DEBUG: Timing for metadata query
                    query_start = _time.time()
                    metadata_cursor.execute('''
                        SELECT chunk_id FROM chunk_metadata
                    ''')
                    local_chunks = metadata_cursor.fetchall()
                    query_time = _time.time() - query_start
                    if query_time > 1.0:
                        logger.warning(f"[🐌 DB-SLOW] Client {self.client_id}: metadata query took {query_time:.2f}s")

                    logger.debug(f"[ChunkManager] Client {self.client_id}: Found {len(local_chunks)} local chunks for round {round_num}")

                    for (chunk_id,) in local_chunks:
                        # Local chunks
                        bitfield[(round_num, self.client_id, chunk_id)] = True
                finally:
                    metadata_conn.close()
            else:
                logger.debug(f"No metadata database found for round {round_num}")

            # Query BitTorrent exchanged chunks from bt database
            # 🔍 DEBUG: Timing for bt DB connection
            bt_conn_start = _time.time()
            bt_conn = self._get_optimized_connection(round_num, 'bt')
            bt_conn_time = _time.time() - bt_conn_start
            if bt_conn_time > 1.0:
                logger.warning(f"[🐌 DB-SLOW] Client {self.client_id}: bt connection took {bt_conn_time:.2f}s")

            bt_cursor = bt_conn.cursor()
            try:
                # 🔍 DEBUG: Timing for bt query
                bt_query_start = _time.time()
                bt_cursor.execute('''
                    SELECT source_client_id, chunk_id FROM bt_chunks
                    WHERE holder_client_id = ?
                ''', (self.client_id,))

                for source_id, chunk_id in bt_cursor.fetchall():
                    bitfield[(round_num, source_id, chunk_id)] = True
                bt_query_time = _time.time() - bt_query_start
                if bt_query_time > 1.0:
                    logger.warning(f"[🐌 DB-SLOW] Client {self.client_id}: bt query took {bt_query_time:.2f}s")
            finally:
                bt_conn.close()

        except sqlite3.OperationalError:
            # If table-specific databases don't exist, create them
            logger.warning(f"[ChunkManager] Table-specific databases not found for round {round_num}, creating...")
            self._ensure_table_database_exists(round_num, 'metadata')
            self._ensure_table_database_exists(round_num, 'bt')
            return self.get_global_bitfield(round_num)

        # Log total function time
        func_time = _time.time() - func_start
        if func_time > 2.0:
            logger.warning(f"[🐌 DB-SLOW] Client {self.client_id}: get_global_bitfield TOTAL took {func_time:.2f}s")

        return bitfield
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def save_remote_chunk(self, round_num, source_client_id, chunk_id, chunk_data, on_cache_saved_callback=None):
        """
        🚀 HIGH-PERFORMANCE: Save BitTorrent exchanged chunk using cache system
        Note: chunk_data should be bytes for optimal performance
        Args:
            on_cache_saved_callback: Callback called immediately after cache save (before DB write)
        """
        import hashlib
        import pickle
        
        # Convert to bytes if needed
        if isinstance(chunk_data, bytes):
            chunk_bytes = chunk_data
        else:
            # Serialize only if not already bytes
            chunk_bytes = pickle.dumps(chunk_data)
        
        chunk_hash = hashlib.sha256(chunk_bytes).hexdigest()
        
        try:
            # 🚀 Save to high-performance cache instead of database
            self.chunk_cache.save_chunk_data(round_num, source_client_id, chunk_id, chunk_bytes)
            
            # 🚀 IMMEDIATE CALLBACK: Data is now available in cache - trigger HAVE broadcast
            if on_cache_saved_callback:
                try:
                    on_cache_saved_callback(round_num, source_client_id, chunk_id)
                    logger.debug(f"[ChunkManager] 🚀 Called cache-saved callback for chunk {source_client_id}:{chunk_id}")
                except Exception as e:
                    logger.error(f"[ChunkManager] Error in cache-saved callback: {e}")
            
            # Still maintain BitTorrent tracking in database for metadata (async)
            self._ensure_table_database_exists(round_num, 'bt')
            bt_conn = self._get_optimized_connection(round_num, 'bt')
            bt_cursor = bt_conn.cursor()
            try:
                bt_cursor.execute('''
                    INSERT OR REPLACE INTO bt_chunks 
                    (source_client_id, chunk_id, chunk_hash, holder_client_id, is_verified)
                    VALUES (?, ?, ?, ?, 1)
                ''', (source_client_id, chunk_id, chunk_hash, self.client_id))
                bt_conn.commit()
            finally:
                bt_conn.close()
            
            logger.debug(f"[ChunkManager] 🚀 Saved chunk {source_client_id}:{chunk_id} to cache system for round {round_num}")
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to save remote chunk {source_client_id}:{chunk_id}: {e}")
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
                chunk_size=len(chunk_bytes),
                timestamp=time.time()
            )
            self.change_callback(chunk_info)
    
    def get_chunk_data(self, round_num, source_client_id, chunk_id):
        """
        🚀 HIGH-PERFORMANCE: Get chunk data from cache system (no database overhead)
        
        Returns raw bytes for optimal transmission performance - caller responsible for deserialization if needed
        """
        # 🚀 Direct cache lookup - no database complexity
        chunk_bytes = self.chunk_cache.get_chunk_data(round_num, source_client_id, chunk_id)
        
        if chunk_bytes is not None:
            # Cache hit - return bytes directly for zero-copy performance
            logger.debug(f"[ChunkManager] Client {self.client_id}: Cache hit for chunk ({round_num}, {source_client_id}, {chunk_id}), size: {len(chunk_bytes)} bytes")
            return chunk_bytes
        
        # Cache miss - check write queue fallback
        if self.chunk_write_queue:
            pending_chunk = self.chunk_write_queue.search_pending_chunk(round_num, source_client_id, chunk_id)
            if pending_chunk is not None:
                logger.debug(f"[ChunkManager] 🎯 Client {self.client_id}: Found chunk ({round_num}, {source_client_id}, {chunk_id}) in write queue!")
                return pending_chunk
        
        logger.debug(f"[ChunkManager] Client {self.client_id}: Chunk ({round_num}, {source_client_id}, {chunk_id}) not found in cache or write queue")
        return None
    
    def get_chunk_data_deserialized(self, round_num, source_client_id, chunk_id):
        """
        Get chunk data and deserialize it for aggregation/computation use
        
        Returns deserialized numpy array/object - only use when computation is needed
        """
        chunk_bytes = self.get_chunk_data(round_num, source_client_id, chunk_id)
        if chunk_bytes is not None:
            try:
                chunk_data = pickle.loads(chunk_bytes)
                return chunk_data
            except Exception as pickle_error:
                logger.error(f"[ChunkManager] Client {self.client_id}: Failed to deserialize chunk data: {pickle_error}")
                return None
        return None
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def start_bittorrent_session(self, round_num, expected_chunks):
        """Start BitTorrent exchange session using round-specific database"""
        try:
            # Ensure round-specific database and tables exist
            self._ensure_round_tables_exist(round_num)
            
            # Ensure sessions database exists
            self._ensure_table_database_exists(round_num, 'sessions')
            
            # Connect to the sessions-specific database
            conn = self._get_optimized_connection(round_num=round_num, table_type='sessions')
            try:
                cursor = conn.cursor()
                
                # Insert session into sessions database (using INSERT to ensure clean lastrowid)
                cursor.execute('''
                    INSERT INTO bt_sessions
                    (start_time, status, total_chunks_expected)
                    VALUES (?, 'active', ?)
                ''', (time.time(), expected_chunks))
                
                # 🚀 FIX: Save session ID for later use
                session_id = cursor.lastrowid
                self._bt_session_ids[round_num] = session_id
                
                conn.commit()
                logger.debug(f"[ChunkManager] Started BitTorrent session for round {round_num} (round-specific table)")
            finally:
                conn.close()
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to start BitTorrent session: {e}")
        
        logger.info(f"[ChunkManager] Started BitTorrent session for round {round_num}")
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def finish_bittorrent_session(self, round_num, status='completed'):
        """End BitTorrent exchange session using round-specific database"""
        try:
            # Check if bt database exists
            if not os.path.exists(self._get_round_db_path(round_num, 'bt')):
                logger.debug(f"No bt database found for round {round_num}")
                return
                
            # Count received chunks from bt-specific database
            bt_conn = self._get_optimized_connection(round_num=round_num, table_type='bt')
            bt_cursor = bt_conn.cursor()
            try:
                bt_cursor.execute('''
                    SELECT COUNT(*) FROM bt_chunks
                    WHERE holder_client_id = ?
                ''', (self.client_id,))
                chunks_received = bt_cursor.fetchone()[0]
            finally:
                bt_conn.close()
            
            # Update session status in sessions-specific database
            sessions_conn = self._get_optimized_connection(round_num=round_num, table_type='sessions')
            sessions_cursor = sessions_conn.cursor()
            try:
                # 🚀 FIX: Use saved session ID instead of hardcoded 1
                session_id = self._bt_session_ids.get(round_num)
                if session_id:
                    sessions_cursor.execute('''
                        UPDATE bt_sessions
                        SET end_time = ?, status = ?, total_chunks_received = ?
                        WHERE id = ?
                    ''', (time.time(), status, chunks_received, session_id))
                else:
                    # Fallback to latest session if ID not found
                    sessions_cursor.execute('''
                        UPDATE bt_sessions
                        SET end_time = ?, status = ?, total_chunks_received = ?
                        WHERE id = (SELECT id FROM bt_sessions ORDER BY id DESC LIMIT 1)
                    ''', (time.time(), status, chunks_received))
                sessions_conn.commit()
            finally:
                sessions_conn.close()
            
            logger.info(f"[ChunkManager] Finished BitTorrent session for round {round_num}, status: {status}")
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to finish BitTorrent session: {e}")
    
    def cleanup_bittorrent_data(self, keep_rounds=5):
        """Clean old BitTorrent data by deleting round database files"""
        logger.info(f"🧹 BitTorrent cleanup redirected to general round database cleanup")
        # Since BitTorrent data is stored in the same round databases as chunk data,
        # we can use the same file cleanup method
        return self._cleanup_old_rounds_by_files(keep_rounds)
    
    def get_available_clients_for_round(self, round_num: int) -> List[int]:
        """
        Get all available client IDs for specified round using round-specific database
        
        Args:
            round_num: Target round
            
        Returns:
            List[int]: Available client ID list
        """
        try:
            local_clients = []
            bt_clients = []
            
            # Check local client data from metadata database (if exists)
            if os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                # Connect to round-specific metadata database
                metadata_conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                try:
                    metadata_cursor = metadata_conn.cursor()
                    # 🚀 SEMANTIC FIX: Check if local client has chunk data with clearer SQL
                    metadata_cursor.execute('''
                        SELECT CASE WHEN EXISTS(SELECT 1 FROM chunk_metadata) THEN ? END as client_id
                    ''', (self.client_id,))
                    
                    local_result = metadata_cursor.fetchone()
                    local_clients = [local_result[0]] if local_result and local_result[0] is not None else []
                except sqlite3.Error:
                    pass  # Table doesn't exist
                finally:
                    metadata_conn.close()
            else:
                logger.debug(f"No metadata database found for round {round_num}")
            
            # Query clients in BitTorrent chunks from round-specific bt database
            try:
                bt_conn = self._get_optimized_connection(round_num=round_num, table_type='bt')
                bt_cursor = bt_conn.cursor()
                bt_cursor.execute('''
                    SELECT DISTINCT source_client_id 
                    FROM bt_chunks 
                    ORDER BY source_client_id
                ''')
                
                bt_clients = [row[0] for row in bt_cursor.fetchall()]
            except sqlite3.Error:
                pass  # Table doesn't exist
            finally:
                bt_conn.close()
            
            # Merge and deduplicate
            all_clients = list(set(local_clients + bt_clients))
            all_clients.sort()
            
            logger.debug(f"[ChunkManager] Round {round_num}: Found {len(all_clients)} clients with chunk data: {all_clients}")
            return all_clients
            
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to get available clients for round {round_num}: {e}")
            return []
    
    def reconstruct_model_from_client_chunks(self, client_id: int, round_num: int, fill_missing_with_baseline: Optional[Dict] = None) -> Optional[Reconstruction]:
        """
        重建：把该 client 在本轮收到/产生的 chunks 还原成参数 + 掩码/coverage。
        
        Args:
            client_id: Target client ID
            round_num: Target round
            fill_missing_with_baseline: Optional baseline parameters for filling missing data
                                      If None, missing data filled with zeros
                                      If provided, missing data filled with baseline values
        """
        try:
            # 1) 取 chunk 列表（一次打开一次关闭）
            if not os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                logger.debug(f"[ChunkManager] No metadata DB for round {round_num}")
                return None

            local_chunks: List[Tuple[int, str]] = []
            if client_id == self.client_id:
                with closing(self._get_optimized_connection(round_num, 'metadata')) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT chunk_id, chunk_hash FROM chunk_metadata ORDER BY chunk_id")
                    local_chunks = cur.fetchall()
            else:
                with closing(self._get_optimized_connection(round_num, 'bt')) as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT chunk_id, chunk_hash
                        FROM bt_chunks
                        WHERE source_client_id = ?
                        ORDER BY chunk_id
                    """, (client_id,))
                    local_chunks = cur.fetchall()

            if not local_chunks:
                logger.info(f"[ChunkManager] client {client_id}, round {round_num}: no chunks")
                return None

            # 2) 读出 chunk 原始 bytes（cache→queue→缺失）
            chunk_bytes_list: List[Tuple[int, bytes]] = []
            missing = 0
            for chunk_id, _ in local_chunks:
                data = self.chunk_cache.get_chunk_data(round_num, client_id, chunk_id)
                if data is None and self.chunk_write_queue:
                    data = self.chunk_write_queue.search_pending_chunk(round_num, client_id, chunk_id)
                if data is None:
                    missing += 1
                    continue
                chunk_bytes_list.append((chunk_id, data))

            if not chunk_bytes_list:
                logger.info(f"[ChunkManager] client {client_id}, round {round_num}: all chunks missing")
                return None

            chunk_bytes_list.sort(key=lambda x: x[0])

            # 3) 一次性取所有 parts_info（减少 per-chunk 查询）
            chunk_ids = tuple(cid for cid, _ in chunk_bytes_list)
            parts_map: Dict[int, Dict] = {}
            if chunk_ids:  # Only query if we have chunk IDs
                with closing(self._get_optimized_connection(round_num, 'metadata')) as conn:
                    cur = conn.cursor()
                    q = f"SELECT chunk_id, parts_info FROM chunk_metadata WHERE chunk_id IN ({','.join(['?']*len(chunk_ids))})"
                    cur.execute(q, chunk_ids)
                    for cid, parts_json in cur.fetchall():
                        parts_map[cid] = json.loads(parts_json)

            # 4) 先扫一遍，确定各参数总长度 & 原始形状（由 parts_info 提供的 shape 汇总）
            total_len: Dict[str, int] = {}
            shapes: Dict[str, Tuple[int, ...]] = {}
            for cid, parts_info in parts_map.items():
                for pname, parts in parts_info.items():
                    # parts: List[(flat_start, flat_end, shape)]
                    for flat_start, flat_end, shape in parts:
                        total_len[pname] = max(total_len.get(pname, 0), flat_end)
                        if pname not in shapes and shape:
                            shapes[pname] = tuple(shape)

            # Check if we have any parameters to reconstruct
            if not total_len:
                logger.warning(f"[ChunkManager] client {client_id}, round {round_num}: no parameters found in parts_info")
                return None
                
            # 5) 为每个参数分配扁平数组 & 掩码
            params_flat: Dict[str, np.ndarray] = {p: np.zeros(L, dtype=np.float32) for p, L in total_len.items()}
            mask_flat:   Dict[str, np.ndarray] = {p: np.zeros(L, dtype=bool)  for p, L in total_len.items()}

            # 6) 逐 chunk 反序列化后，按 parts_info 切片填充（同时标记 mask）
            #    注意：反序列化→numpy：避免多余 copy
            for cid, raw in chunk_bytes_list:
                np_chunk = pickle.loads(raw)        # 假设你存的就是 np.ndarray；否则这里可换成内存视图
                pos = 0
                parts_info = parts_map.get(cid, {})
                # 确保同一 chunk 内部的 parts 与序列化顺序一致（你已有 ORDER BY chunk_id）
                for pname, parts in parts_info.items():
                    for flat_start, flat_end, _shape in parts:
                        size = flat_end - flat_start
                        slice_ = np_chunk[pos:pos+size]
                        params_flat[pname][flat_start:flat_end] = slice_
                        mask_flat[pname][flat_start:flat_end]   = True
                        pos += size

            # 7) reshape & 转 torch；计算 coverage
            params: Dict[str, torch.Tensor] = {}
            mask:   Dict[str, torch.Tensor] = {}
            coverage: Dict[str, float] = {}

            for pname, flat in params_flat.items():
                shp = shapes.get(pname, (flat.size,))
                mflat = mask_flat[pname]
                
                # Verify size consistency before reshape
                expected_size = np.prod(shp)
                actual_size = flat.size
                
                if expected_size != actual_size:
                    logger.warning(f"[ChunkManager] Size mismatch for {pname}: expected {expected_size}, got {actual_size}")

                    # 🔧 SIMPLE LOGIC (user's original requirement):
                    # "If length is enough, directly restore shape; if not enough, fill with zeros"
                    # No baseline filling to avoid shape mismatch issues

                    if actual_size == 0:
                        # No data received - fill with zeros
                        params[pname] = torch.zeros(shp, dtype=torch.float32)
                        mask[pname] = torch.zeros(shp, dtype=torch.bool)
                        coverage[pname] = 0.0
                        logger.info(f"[ChunkManager] {pname}: No data received, filled with zeros")

                    elif actual_size < expected_size:
                        # Partial data received - pad with zeros
                        padded_flat = np.zeros(expected_size, dtype=flat.dtype)
                        padded_flat[:actual_size] = flat
                        padded_mask = np.zeros(expected_size, dtype=bool)
                        padded_mask[:actual_size] = mflat

                        params[pname] = torch.from_numpy(padded_flat.reshape(shp))
                        mask[pname] = torch.from_numpy(padded_mask.reshape(shp))
                        coverage[pname] = float(actual_size) / float(expected_size)
                        logger.info(f"[ChunkManager] {pname}: Partial data ({actual_size}/{expected_size}), "
                                   f"filled missing with zeros, coverage={coverage[pname]:.3f}")
                    else:
                        # More data than expected - truncate
                        truncated_flat = flat[:expected_size]
                        truncated_mask = mflat[:expected_size]

                        params[pname] = torch.from_numpy(truncated_flat.reshape(shp))
                        mask[pname] = torch.from_numpy(truncated_mask.reshape(shp))
                        coverage[pname] = float(truncated_mask.sum()) / float(expected_size)
                        logger.warning(f"[ChunkManager] {pname}: Excess data ({actual_size}/{expected_size}), "
                                      f"truncated, coverage={coverage[pname]:.3f}")
                else:
                    # Size matches, normal reshape
                    params[pname] = torch.from_numpy(flat.reshape(shp))
                    mask[pname] = torch.from_numpy(mflat.reshape(shp))
                    coverage[pname] = float(mflat.sum()) / float(mflat.size)

            # Calculate data completeness statistics
            total_expected_elements = sum(np.prod(shapes.get(pname, (0,))) for pname in params.keys())
            total_received_elements = sum(mask[pname].sum().item() for pname in params.keys())
            overall_data_coverage = total_received_elements / total_expected_elements if total_expected_elements > 0 else 1.0
            
            logger.info(f"[ChunkManager] client {client_id}, round {round_num}: "
                        f"reconstructed {len(params)} params")
            logger.info(f"  Chunk-level: {len(chunk_bytes_list)} chunks available, {missing} chunks missing from cache")
            logger.info(f"  Data-level: {total_received_elements:,}/{total_expected_elements:,} elements "
                       f"({overall_data_coverage:.1%} coverage)")
            
            if missing == 0 and overall_data_coverage < 0.95:
                logger.warning(f"  ⚠️  Low data coverage despite all chunks present - indicates partial/corrupted chunks")

            return Reconstruction(params=params, mask=mask, coverage=coverage, missing_chunks=missing)

        except Exception as e:
            logger.error(f"[ChunkManager] reconstruct failed: {e}")
            import traceback; logger.debug(traceback.format_exc())
            return None
    
    def reconstruct_with_coverage_info(self, client_id: int, round_num: int, baseline_params: Optional[Dict] = None) -> Optional[tuple]:
        """
        🔧 NEW: Reconstruction that preserves coverage information for post-aggregation compensation

        Args:
            client_id: Target client ID
            round_num: Target round
            baseline_params: Baseline parameters for completion

        Returns:
            Tuple[Dict, Dict[str, float]]: (model_params, coverage_ratios) or None if failed
        """
        if baseline_params is None:
            reconstruction = self.reconstruct_model_from_client_chunks(client_id, round_num)
            if reconstruction:
                params = {name: param.clone() for name, param in reconstruction.params.items()}
                # For full reconstruction without baseline, assume coverage from mask
                coverage = {name: float(mask.sum().item()) / float(mask.numel())
                           for name, mask in reconstruction.mask.items()}
                return params, coverage
            return None

        # 🚀 FAST PATH: When compensation disabled, use standard reconstruction without baseline filling
        # Check config with detailed logging
        has_cfg = hasattr(self, '_cfg') and self._cfg is not None
        has_bittorrent = has_cfg and hasattr(self._cfg, 'bittorrent')
        enable_compensation = True  # Default

        if has_bittorrent:
            enable_compensation = getattr(self._cfg.bittorrent, 'enable_compensation', True)
            logger.debug(f"[ChunkManager] Client {client_id}: enable_compensation from config = {enable_compensation}")
        else:
            logger.warning(f"[ChunkManager] Client {client_id}: No bittorrent config found (has_cfg={has_cfg}, has_bittorrent={has_bittorrent}), defaulting to compensation enabled")

        if not enable_compensation:
            # Use the standard reconstruction method without baseline (no compensation needed)
            # Pass None for baseline to avoid shape mismatch - function will fill missing data with zeros
            logger.info(f"[ChunkManager] Client {client_id}: Using FAST PATH (compensation disabled)")
            reconstruction = self.reconstruct_model_from_client_chunks(client_id, round_num, fill_missing_with_baseline=None)
            if reconstruction:
                logger.info(f"[ChunkManager] Client {client_id}: Reconstruction completed without compensation")
                return reconstruction.params, reconstruction.coverage
            return None

        # Standard path with compensation
        logger.debug(f"[ChunkManager] Client {client_id}: Using STANDARD PATH (compensation enabled)")
        result = self._compensate_from_chunks_fast(client_id, round_num, baseline_params)
        if result is not None:
            return result.params, result.coverage
        return None
    
    def _compensate_from_chunks_fast(self, client_id: int, round_num: int, baseline_params: Dict) -> Optional[Dict]:
        """
        🔧 FIXED: Pure reconstruction + completion, NO compensation to avoid double compensation

        Only fills received chunks into baseline without any scaling/compensation.
        Post-aggregation compensation will handle bias correction at parameter level.

        🚀 NEW: Fast path for complete chunk collection - directly deserialize without per-chunk processing
        """
        try:
            # 1) 取 chunk 列表
            if not os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                return None

            if client_id == self.client_id:
                with closing(self._get_optimized_connection(round_num, 'metadata')) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT chunk_id, chunk_hash FROM chunk_metadata ORDER BY chunk_id")
                    local_chunks = cur.fetchall()
            else:
                with closing(self._get_optimized_connection(round_num, 'bt')) as conn:
                    cur = conn.cursor()
                    cur.execute("""
                        SELECT chunk_id, chunk_hash
                        FROM bt_chunks
                        WHERE source_client_id = ?
                        ORDER BY chunk_id
                    """, (client_id,))
                    local_chunks = cur.fetchall()
            if not local_chunks:
                return self._filter_personalized_params(baseline_params)

            # 2) 读 bytes（cache→queue）
            chunk_bytes_list = []
            for cid, _ in local_chunks:
                data = None
                if hasattr(self, 'chunk_cache') and self.chunk_cache:
                    data = self.chunk_cache.get_chunk_data(round_num, client_id, cid)
                if data is None and hasattr(self, 'chunk_write_queue') and self.chunk_write_queue:
                    data = self.chunk_write_queue.search_pending_chunk(round_num, client_id, cid)
                if data is not None:
                    chunk_bytes_list.append((cid, data))
            if not chunk_bytes_list:
                return self._filter_personalized_params(baseline_params)
            chunk_bytes_list.sort(key=lambda x: x[0])

            # 3) 取 parts_info
            with closing(self._get_optimized_connection(round_num, 'metadata')) as conn:
                cur = conn.cursor()
                ids = tuple(cid for cid, _ in chunk_bytes_list)
                q = f"SELECT chunk_id, parts_info FROM chunk_metadata WHERE chunk_id IN ({','.join(['?']*len(ids))})"
                cur.execute(q, ids)
                parts_map = {cid: json.loads(js) for cid, js in cur.fetchall()}

            # 4) 第一遍：统计 total_len / covered_len，并构建 "cid -> 片段列表"
            total_len, covered_len, shapes = {}, {}, {}
            cid_segs = {}   # cid -> list[(pname, pos0, s, e)]
            for cid, raw in chunk_bytes_list:
                parts_info = parts_map.get(cid, {})
                pos = 0
                for pname, parts in parts_info.items():
                    # 若 parts 顺序不保证，保险：parts = sorted(parts, key=lambda x: x[0])
                    for s, e, shape in parts:
                        if pname not in shapes and shape:
                            shapes[pname] = tuple(shape)
                        total_len[pname] = max(total_len.get(pname, 0), e)
                        covered_len[pname] = covered_len.get(pname, 0) + (e - s)
                        cid_segs.setdefault(cid, []).append((pname, pos, s, e))
                        pos += (e - s)

            if not total_len:
                return self._filter_personalized_params(baseline_params)

            # 5) 🔧 FIXED: 准备 out 扁平向量，不计算补偿alpha（纯拼接模式）
            out = {}
            for pname, L in total_len.items():
                base = baseline_params.get(pname)
                if base is not None:
                    base_t = (base.detach().cpu().to(torch.float32) 
                              if hasattr(base, 'detach') else torch.tensor(base, dtype=torch.float32))
                    out[pname] = base_t.view(-1).clone()   # 扁平
                else:
                    # 如果baseline中没有这个参数，创建零向量
                    out[pname] = torch.zeros(L, dtype=torch.float32)

            # 6) 🔧 FIXED: 第二遍：按片段纯拼接，无补偿 (alpha=1)
            raw_map = dict(chunk_bytes_list)
            for cid, _ in chunk_bytes_list:
                np_chunk = pickle.loads(raw_map[cid])          # 反序列化为 np.ndarray (1-D)
                pos_max = np_chunk.size
                for pname, pos0, s, e in cid_segs.get(cid, []):
                    size = e - s
                    # 安全断言：片段长度不要越界
                    assert 0 <= pos0 and pos0 + size <= pos_max, f"Chunk {cid} slice OOB"
                    recv_slice = torch.from_numpy(np_chunk[pos0:pos0+size]).to(torch.float32)
                    flat = out[pname]
                    # 纯拼接：直接覆盖，不做补偿
                    flat[s:e] = recv_slice

            # 7) 还原形状；计算覆盖率；补齐 baseline 中缺的键
            final = {}
            coverage = {}  # 🔧 NEW: Calculate real coverage ratios
            
            for pname, flat in out.items():
                shp = shapes.get(pname, (flat.numel(),))
                final[pname] = flat.view(*shp)
                
                # Calculate real coverage ratio for this parameter
                total_elements = total_len.get(pname, 0)
                covered_elements = covered_len.get(pname, 0)
                coverage[pname] = float(covered_elements) / float(total_elements) if total_elements > 0 else 0.0

            # Apply personalization filtering and add missing baseline parameters
            filtered_baseline = self._filter_personalized_params(baseline_params)
            for pname, base in filtered_baseline.items():
                if pname not in final:
                    final[pname] = (base.detach().cpu().to(torch.float32) 
                                    if hasattr(base, 'detach') else torch.tensor(base, dtype=torch.float32))
                    coverage[pname] = 0.0  # No coverage for missing parameters

            # 🔧 FIXED: Log pure completion info with coverage stats
            avg_coverage = sum(coverage.values()) / len(coverage) if coverage else 0.0
            logger.info(f"[ChunkManager] Client {client_id}: Pure reconstruction completed for {len(final)} parameters, avg coverage: {avg_coverage:.3f}")

            # 🔧 CRITICAL FIX: Return Reconstruction object with coverage info
            from collections import namedtuple
            Reconstruction = namedtuple('Reconstruction', ['params', 'coverage', 'mask', 'missing_chunks'])
            
            # Create dummy mask for compatibility (not used in post-aggregation compensation)
            dummy_mask = {pname: torch.ones_like(param, dtype=torch.bool) for pname, param in final.items()}
            
            return Reconstruction(params=final, coverage=coverage, mask=dummy_mask, missing_chunks=0)

        except Exception as e:
            logger.error(f"[ChunkManager] fast compensation failed: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return self._filter_personalized_params(baseline_params) if baseline_params else None

    def _get_local_sample_size(self) -> Optional[int]:
        """
        Calculate local sample size using the same logic as client worker
        """
        try:
            # Try to get from change_callback context if available
            if hasattr(self, 'change_callback') and self.change_callback is not None:
                # The change_callback is typically bound to client, try to access client config and trainer
                callback_self = getattr(self.change_callback, '__self__', None)
                if callback_self and hasattr(callback_self, '_cfg'):
                    cfg = callback_self._cfg
                    if hasattr(cfg.train, 'batch_or_epoch'):
                        if cfg.train.batch_or_epoch == 'batch':
                            return cfg.train.local_update_steps * cfg.dataloader.batch_size
                        else:
                            # For epoch mode, try to get actual training data size
                            if hasattr(callback_self, 'trainer') and hasattr(callback_self.trainer, 'data') and hasattr(callback_self.trainer.data, 'train_data'):
                                return cfg.train.local_update_steps * len(callback_self.trainer.data.train_data)
                            else:
                                # If trainer data not available, epoch mode typically means one epoch over all data
                                # For CIFAR-10 with 50 clients, each client typically has ~1000 samples
                                # This is an estimation based on typical federated learning setups
                                return cfg.train.local_update_steps * 1000
            
            # If we can't access client config, return None to use fallback
            return None
            
        except Exception as e:
            logger.debug(f"[ChunkManager] Failed to calculate local sample size: {e}")
            return None
    
    
    def _filter_personalized_params(self, state_dict: Dict) -> Dict:
        """
        Filter out personalized parameters (e.g., BN) based on cfg.personalization.local_param
        This ensures consistency with FederatedScope's native personalization mechanism.

        Args:
            state_dict: Original state dictionary

        Returns:
            Dict: Filtered state dictionary without personalized parameters
        """
        if not state_dict:
            return state_dict

        # Get filter keywords from config (e.g., ['bn'] for FedBN)
        filter_keywords = []
        if self._cfg and hasattr(self._cfg, 'personalization'):
            filter_keywords = self._cfg.personalization.local_param

        # If no personalization config, return all parameters
        if not filter_keywords:
            return state_dict

        # Use FederatedScope's native filter function
        filtered_dict = dict(
            filter(
                lambda elem: filter_by_specified_keywords(elem[0], filter_keywords),
                state_dict.items()
            )
        )

        return filtered_dict

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
            # First, try to get sample size from stored metadata if available
            if hasattr(self, '_sample_sizes') and round_num in self._sample_sizes and client_id in self._sample_sizes[round_num]:
                return self._sample_sizes[round_num][client_id]
            
            # Check if metadata database exists
            if not os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                logger.debug(f"No metadata database found for round {round_num}")
                return None
                
            # Database connections now handled per table type below
            
            # Check if we have chunks from this client for this round
            # Note: sample_size column doesn't exist in schema, using fallback approach
            
            result = None
            
            if client_id == self.client_id:
                # Local client - check metadata database
                try:
                    metadata_conn = self._get_optimized_connection(round_num, 'metadata')
                    metadata_cursor = metadata_conn.cursor()
                    try:
                        metadata_cursor.execute('''
                            SELECT COUNT(*) FROM chunk_metadata
                        ''')
                        result = metadata_cursor.fetchone()
                    finally:
                        metadata_conn.close()
                except sqlite3.Error:
                    result = (0,)  # Return 0 if table doesn't exist
            else:
                # Remote client - check bt database
                try:
                    bt_conn = self._get_optimized_connection(round_num, 'bt')
                    bt_cursor = bt_conn.cursor()
                    try:
                        bt_cursor.execute('''
                            SELECT COUNT(*) FROM bt_chunks 
                            WHERE source_client_id = ?
                        ''', (client_id,))
                        result = bt_cursor.fetchone()
                    finally:
                        bt_conn.close()
                except sqlite3.Error:
                    result = (0,)  # Return 0 if table doesn't exist
            
            if result and result[0] > 0:
                # Use local calculated sample size instead of fixed 128
                local_sample_size = self._get_local_sample_size()
                if local_sample_size is not None:
                    logger.info(f"[ChunkManager] Using calculated local sample size {local_sample_size} for client {client_id}")
                    return local_sample_size
                else:
                    # Fallback to fixed sample size for BitTorrent FL (toy dataset default)
                    logger.info(f"[ChunkManager] Using fallback sample size 128 for client {client_id}")
                    return 128
            else:
                logger.debug(f"[ChunkManager] No chunks found for client {client_id}, round {round_num}")
                return None
                
        except Exception as e:
            logger.error(f"[ChunkManager] Failed to get sample size for client {client_id}, round {round_num}: {e}")
            return None
    

    def close(self):
        """
        🚀 Shutdown chunk manager and release resources
        """
        logger.info(f"[ChunkManager] Client {self.client_id}: Shutting down...")
        
        # Close cache system
        if hasattr(self, 'chunk_cache') and self.chunk_cache is not None:
            try:
                self.chunk_cache.close()
                logger.info(f"[ChunkManager] Client {self.client_id}: Cache system shutdown completed")
            except Exception as e:
                logger.error(f"[ChunkManager] Client {self.client_id}: Error during cache shutdown: {e}")
        
        # Close write queue if exists
        if hasattr(self, 'chunk_write_queue') and self.chunk_write_queue is not None:
            try:
                # Shutdown write queue if it has a shutdown method
                if hasattr(self.chunk_write_queue, 'shutdown'):
                    self.chunk_write_queue.shutdown()
            except Exception as e:
                logger.error(f"[ChunkManager] Client {self.client_id}: Error during write queue shutdown: {e}")
        
        logger.info(f"[ChunkManager] Client {self.client_id}: Shutdown completed")
    
    def __del__(self):
        """Destructor to ensure proper cleanup"""
        try:
            self.close()
        except:
            pass  # Ignore errors during cleanup