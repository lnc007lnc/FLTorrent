#!/usr/bin/env python3
"""
🚀 Regression Test: ChunkManager Cache Integration
Tests the cache-first modifications to ensure all functions work correctly
"""

import os
import sys
import tempfile
import shutil
import numpy as np
import torch
import torch.nn as nn
from typing import List, Dict

# Add project root to path
sys.path.insert(0, os.path.abspath('.'))

from federatedscope.core.chunk_manager import ChunkManager


class SimpleModel(nn.Module):
    """Simple test model"""
    def __init__(self):
        super(SimpleModel, self).__init__()
        self.fc1 = nn.Linear(10, 5)
        self.fc2 = nn.Linear(5, 2)
    
    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = self.fc2(x)
        return x


def test_cache_first_modifications():
    """Test all cache-first modifications"""
    print("🧪 Testing ChunkManager cache integration modifications...")
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    old_cwd = os.getcwd()
    
    try:
        os.chdir(temp_dir)
        
        # Initialize ChunkManager
        client_id = 1
        chunk_manager = ChunkManager(client_id=client_id)
        
        # Create test model
        model = SimpleModel()
        round_num = 5
        num_chunks = 3
        
        print(f"📊 Test 1: save_model_chunks with cache-first approach...")
        
        # Test 1: Save model chunks (cache-first, no DB persistence by default)
        chunk_hashes = chunk_manager.save_model_chunks(
            model=model,
            round_num=round_num,
            num_chunks=num_chunks,
            persist_to_db=False  # Test default behavior
        )
        
        assert len(chunk_hashes) == num_chunks, f"Expected {num_chunks} chunks, got {len(chunk_hashes)}"
        print(f"✅ Saved {len(chunk_hashes)} chunks to cache system")
        
        print(f"📊 Test 2: load_chunks_by_round with cache-first approach...")
        
        # Test 2: Load chunks (cache-first with write queue fallback)
        loaded_chunks = chunk_manager.load_chunks_by_round(round_num)
        assert len(loaded_chunks) == num_chunks, f"Expected {num_chunks} chunks, got {len(loaded_chunks)}"
        print(f"✅ Loaded {len(loaded_chunks)} chunks from cache")
        
        print(f"📊 Test 3: get_chunk_by_id with cache-first approach...")
        
        # Test 3: Get individual chunk (cache-first with write queue fallback)
        for chunk_id in range(num_chunks):
            chunk_result = chunk_manager.get_chunk_by_id(round_num, chunk_id)
            assert chunk_result is not None, f"Expected chunk {chunk_id}, got None"
            chunk_info, chunk_data = chunk_result
            assert chunk_info['chunk_id'] == chunk_id, f"Expected chunk_id {chunk_id}, got {chunk_info['chunk_id']}"
            print(f"✅ Retrieved chunk {chunk_id} from cache")
        
        print(f"📊 Test 4: reconstruct_model_from_chunks...")
        
        # Test 4: Reconstruct model from chunks (original function)
        target_model = SimpleModel()
        success = chunk_manager.reconstruct_model_from_chunks(round_num, target_model)
        assert success, "Failed to reconstruct model from chunks"
        print(f"✅ Reconstructed model from chunks successfully")
        
        print(f"📊 Test 5: reconstruct_model_from_client_chunks (cache-first)...")
        
        # Test 5: Reconstruct client model from chunks (cache-first)
        client_params = chunk_manager.reconstruct_model_from_client_chunks(client_id, round_num)
        assert client_params is not None, "Failed to reconstruct client model from chunks"
        assert isinstance(client_params, dict), "Expected parameter dictionary"
        print(f"✅ Reconstructed client model parameters from cache: {len(client_params)} params")
        
        print(f"📊 Test 6: save_model_chunks with database persistence...")
        
        # Test 6: Save with database persistence enabled
        round_num_db = 6
        chunk_hashes_db = chunk_manager.save_model_chunks(
            model=model,
            round_num=round_num_db,
            num_chunks=num_chunks,
            persist_to_db=True  # Enable database persistence
        )
        
        assert len(chunk_hashes_db) == num_chunks, f"Expected {num_chunks} chunks, got {len(chunk_hashes_db)}"
        print(f"✅ Saved {len(chunk_hashes_db)} chunks to cache + database")
        
        print(f"📊 Test 7: Cache statistics and memory usage...")
        
        # Test 7: Check cache statistics
        cache_stats = chunk_manager.chunk_cache.get_stats()
        print(f"✅ Cache stats: {cache_stats}")
        
        # Test storage stats
        storage_stats = chunk_manager.get_storage_stats()
        print(f"✅ Storage stats: {storage_stats}")
        
        print(f"📊 Test 8: Cleanup operations...")
        
        # Test 8: Cleanup operations
        initial_stats = chunk_manager.get_storage_stats()
        chunk_manager.cleanup_old_rounds(keep_rounds=1, current_round=3)  # Should keep only 1 recent round
        final_stats = chunk_manager.get_storage_stats()
        print(f"✅ Cleanup completed - before: {initial_stats.get('unique_chunks', 0)}, after: {final_stats.get('unique_chunks', 0)}")
        
        # Close chunk manager
        chunk_manager.close()
        
        print("🎉 All cache integration tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        os.chdir(old_cwd)
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    success = test_cache_first_modifications()
    exit(0 if success else 1)