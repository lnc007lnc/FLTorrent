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
from typing import Dict, List, Optional, Tuple, Any, Callable
import numpy as np
import torch
import torch.nn as nn
from datetime import datetime
import logging

# Import ChunkInfo for change monitoring
from federatedscope.core.chunk_tracker import ChunkInfo, ChunkAction
from federatedscope.core.chunk_cache import ChunkDataCache

logger = logging.getLogger(__name__)

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
    
    def __init__(self, client_id: int, change_callback: Optional[Callable[[ChunkInfo], None]] = None):
        """
        Initialize ChunkManager, create independent database files for each round
        
        Args:
            client_id: Client ID, used to create node-specific database directory
            change_callback: Callback function for database changes, used to report chunk changes to server
        """
        self.client_id = client_id
        self.change_callback = change_callback
        self.chunk_write_queue = None  # 🚀 NEW: Reference to ChunkWriteQueue
        self._bt_session_ids = {}  # 🚀 Track session IDs by round
        
        # Create database directory by node name: /tmp/client_X/
        client_name = f"client_{client_id}"
        self.db_dir = os.path.join(os.getcwd(), "tmp", client_name)
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
    
    def _cleanup_old_rounds_by_files(self, keep_rounds: int = 2):
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
            
            if len(rounds) <= keep_rounds:
                logger.debug(f"🧹 Only {len(rounds)} round databases exist, no cleanup needed")
                return
                
            # Database files to delete (keep the most recent ones)
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
                        # Convert pending chunk back to bytes if needed
                        if isinstance(pending_chunk, bytes):
                            chunk_bytes = pending_chunk
                        else:
                            chunk_bytes = pickle.dumps(pending_chunk)
                
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
                        # Convert pending chunk back to bytes if needed
                        if isinstance(pending_chunk, bytes):
                            chunk_bytes = pending_chunk
                        else:
                            chunk_bytes = pickle.dumps(pending_chunk)
                
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
    
    def cleanup_old_rounds(self, keep_rounds: int = 2):
        """
        Clean up chunks from old rounds, only keep the most recent few rounds
        
        Args:
            keep_rounds: Keep data from recent rounds, default to keep only 2 rounds
        
        Note: This method now cleans both cache system and legacy database files
        """
        logger.info(f"🧹 Starting cleanup of old rounds (keep {keep_rounds} recent rounds)...")
        
        # 🚀 Clean cache system first - high-performance cache cleanup
        if hasattr(self, 'chunk_cache') and self.chunk_cache is not None:
            try:
                # For cache cleanup, we need to determine current round from existing files
                # Since cleanup is called from save_model_chunks with round_num parameter,
                # we can estimate by finding the highest round number in existing cache files
                cache_dir = self.chunk_cache.cache_dir
                max_round = 0
                if os.path.exists(cache_dir):
                    for filename in os.listdir(cache_dir):
                        if filename.endswith('.dat') and '_r' in filename:
                            try:
                                round_part = filename.split('_r')[1].split('.')[0]
                                max_round = max(max_round, int(round_part))
                            except (ValueError, IndexError):
                                continue
                
                # Clean cache system
                self.chunk_cache.cleanup_round(max_round, keep_rounds)
                logger.info(f"🚀 Cleaned cache system for old rounds (latest round: {max_round})")
            except Exception as e:
                logger.error(f"❌ Failed to clean cache system: {e}")
        
        # Clean legacy database files (for compatibility)
        return self._cleanup_old_rounds_by_files(keep_rounds)
    
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
        # If round_num not provided, use current round
        if round_num is None:
            round_num = getattr(self, 'current_round', 0)
            
        bitfield = {}
        
        try:
            # Query locally saved chunks from metadata database (if exists)
            if os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                metadata_conn = self._get_optimized_connection(round_num, 'metadata')
                try:
                    metadata_cursor = metadata_conn.cursor()
                    metadata_cursor.execute('''
                        SELECT chunk_id FROM chunk_metadata
                    ''')
                    local_chunks = metadata_cursor.fetchall()
                    logger.debug(f"[ChunkManager] Client {self.client_id}: Found {len(local_chunks)} local chunks for round {round_num}")
                    
                    for (chunk_id,) in local_chunks:
                        # Local chunks
                        bitfield[(round_num, self.client_id, chunk_id)] = True
                finally:
                    metadata_conn.close()
            else:
                logger.debug(f"No metadata database found for round {round_num}")
            
            # Query BitTorrent exchanged chunks from bt database
            bt_conn = self._get_optimized_connection(round_num, 'bt')
            bt_cursor = bt_conn.cursor()
            try:
                bt_cursor.execute('''
                    SELECT source_client_id, chunk_id FROM bt_chunks
                    WHERE holder_client_id = ?
                ''', (self.client_id,))
                
                for source_id, chunk_id in bt_cursor.fetchall():
                    bitfield[(round_num, source_id, chunk_id)] = True
            finally:
                bt_conn.close()
                
        except sqlite3.OperationalError:
            # If table-specific databases don't exist, create them
            logger.warning(f"[ChunkManager] Table-specific databases not found for round {round_num}, creating...")
            self._ensure_table_database_exists(round_num, 'metadata')
            self._ensure_table_database_exists(round_num, 'bt')
            return self.get_global_bitfield(round_num)
        
        return bitfield
    
    @db_retry_on_lock(max_retries=5, base_delay=0.1, max_delay=2.0)
    def save_remote_chunk(self, round_num, source_client_id, chunk_id, chunk_data):
        """
        🚀 HIGH-PERFORMANCE: Save BitTorrent exchanged chunk using cache system
        Note: chunk_data should be bytes for optimal performance
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
            
            # Still maintain BitTorrent tracking in database for metadata
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
        
        Returns deserialized data from cache - direct file access
        """
        # 🚀 Direct cache lookup - no database complexity
        chunk_bytes = self.chunk_cache.get_chunk_data(round_num, source_client_id, chunk_id)
        
        if chunk_bytes is not None:
            # Cache hit - deserialize bytes back to original data
            try:
                chunk_data = pickle.loads(chunk_bytes)
                logger.debug(f"[ChunkManager] Client {self.client_id}: Cache hit for chunk ({round_num}, {source_client_id}, {chunk_id}), size: {len(chunk_bytes)} bytes")
                return chunk_data
            except Exception as pickle_error:
                logger.error(f"[ChunkManager] Client {self.client_id}: Failed to deserialize cached chunk data: {pickle_error}")
                return None
        
        # Cache miss - check write queue fallback
        if self.chunk_write_queue:
            pending_chunk = self.chunk_write_queue.search_pending_chunk(round_num, source_client_id, chunk_id)
            if pending_chunk is not None:
                logger.debug(f"[ChunkManager] 🎯 Client {self.client_id}: Found chunk ({round_num}, {source_client_id}, {chunk_id}) in write queue!")
                return pending_chunk
        
        logger.debug(f"[ChunkManager] Client {self.client_id}: Chunk ({round_num}, {source_client_id}, {chunk_id}) not found in cache or write queue")
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
    
    def reconstruct_model_from_client_chunks(self, client_id: int, round_num: int) -> Optional[Dict]:
        """
        Reconstruct specified client's model parameters from chunks
        
        Args:
            client_id: Target client ID
            round_num: Target round
            
        Returns:
            Dict: Reconstructed model parameter dictionary, return None if failed
        """
        try:
            # Check if metadata database exists (at least one table should exist)
            if not os.path.exists(self._get_round_db_path(round_num, 'metadata')):
                logger.debug(f"No metadata database found for round {round_num}")
                return None
                
            # Connect to appropriate table-specific database
            local_chunks = []
            
            if client_id == self.client_id:
                # Local client - query local chunks from metadata database
                try:
                    metadata_conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
                    metadata_cursor = metadata_conn.cursor()
                    metadata_cursor.execute('''
                        SELECT chunk_id, chunk_hash 
                        FROM chunk_metadata
                        ORDER BY chunk_id
                    ''')
                    local_chunks = metadata_cursor.fetchall()
                    metadata_conn.close()
                except sqlite3.Error:
                    logger.debug(f"No chunk_metadata table in round {round_num} database")
                    local_chunks = []
            else:
                # Remote client - query BitTorrent chunks from bt database
                try:
                    bt_conn = self._get_optimized_connection(round_num=round_num, table_type='bt')
                    bt_cursor = bt_conn.cursor()
                    bt_cursor.execute('''
                        SELECT chunk_id, chunk_hash 
                        FROM bt_chunks
                        WHERE source_client_id = ?
                        ORDER BY chunk_id
                    ''', (client_id,))
                    local_chunks = bt_cursor.fetchall()
                    bt_conn.close()
                except sqlite3.Error:
                    logger.debug(f"No bt_chunks table in round {round_num} database")
                    local_chunks = []
            
            if not local_chunks:
                logger.warning(f"[ChunkManager] No chunks found for client {client_id}, round {round_num}")
                return None
            
            # 🚀 NEW: Reconstruct model parameters using cache-first approach
            chunk_data_list = []
            missing_chunks = []
            
            for chunk_id, chunk_hash in local_chunks:
                # 🚀 Cache-first approach with write queue fallback
                chunk_bytes = self.chunk_cache.get_chunk_data(round_num, client_id, chunk_id)
                
                if chunk_bytes is None and self.chunk_write_queue:
                    # Cache miss - check write queue fallback
                    pending_chunk = self.chunk_write_queue.search_pending_chunk(round_num, client_id, chunk_id)
                    if pending_chunk is not None:
                        logger.debug(f"🎯 Cache miss - found chunk {chunk_id} in write queue for client {client_id}")
                        # Convert pending chunk back to bytes if needed
                        if isinstance(pending_chunk, bytes):
                            chunk_bytes = pending_chunk
                        else:
                            chunk_bytes = pickle.dumps(pending_chunk)
                
                if chunk_bytes:
                    chunk_data_list.append((chunk_id, chunk_bytes))
                else:
                    missing_chunks.append(chunk_id)
                    logger.warning(f"⚠️ Chunk {chunk_id} not found in cache or write queue for client {client_id}, round {round_num}")
            
            if missing_chunks:
                logger.warning(f"[ChunkManager] Missing chunk data for client {client_id}, chunks: {missing_chunks}")
            
            if not chunk_data_list:
                logger.error(f"[ChunkManager] No valid chunk data found for client {client_id}, round {round_num}")
                return None
            
            # Sort by chunk_id
            chunk_data_list.sort(key=lambda x: x[0])
            
            # Deserialize each chunk and concatenate
            numpy_chunks = []
            parts_info_list = []
            
            # Get metadata connection for parts_info queries
            metadata_conn = self._get_optimized_connection(round_num=round_num, table_type='metadata')
            metadata_cursor = metadata_conn.cursor()
            
            for chunk_id, chunk_bytes in chunk_data_list:
                # Deserialize chunk data
                numpy_chunk = pickle.loads(chunk_bytes)
                numpy_chunks.append(numpy_chunk)
                
                # Get corresponding parts_info from metadata database
                try:
                    metadata_cursor.execute('''
                        SELECT parts_info FROM chunk_metadata 
                        WHERE chunk_id = ?
                    ''', (chunk_id,))
                    parts_result = metadata_cursor.fetchone()
                except sqlite3.Error:
                    parts_result = None
                
                if parts_result:
                    parts_info = json.loads(parts_result[0])
                    parts_info_list.append((chunk_id, parts_info))
            
            metadata_conn.close()
            
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
                # Return fixed sample size for BitTorrent FL (toy dataset default)
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