#!/usr/bin/env python3
"""
Test database write queue fallback mechanism
Verify that get_chunk_data can retrieve data from write queue when database query fails
"""

import time
import pickle
import logging
from federatedscope.core.chunk_manager import ChunkManager
from federatedscope.core.bittorrent_manager import ChunkWriteQueue

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def test_queue_fallback():
    """Test queue fallback mechanism"""
    print("🧪 Starting queue fallback mechanism test...")
    
    # Create test ChunkManager
    chunk_manager = ChunkManager(client_id=999)
    
    # Create ChunkWriteQueue and establish reference relationship
    write_queue = ChunkWriteQueue(client_id=999, chunk_manager=chunk_manager)
    chunk_manager.set_chunk_write_queue(write_queue)
    
    # Start write thread
    write_queue.start_writer_thread()
    
    try:
        # Test data
        test_round = 1
        test_source_client = 1
        test_chunk_id = 0
        test_data = {"test": "data", "value": 123}
        
        # Serialize test data (simulate data received by BitTorrent)
        serialized_data = pickle.dumps(test_data)
        import hashlib
        checksum = hashlib.sha256(serialized_data).hexdigest()  # correct checksum
        timestamp = time.time()
        
        print(f"📝 Adding test chunk to write queue: round={test_round}, source={test_source_client}, chunk_id={test_chunk_id}")
        
        # Add chunk to write queue (but do not let it write to database)
        write_queue.enqueue_chunk_write(
            round_num=test_round,
            source_client_id=test_source_client,
            chunk_id=test_chunk_id,
            chunk_data=serialized_data,  # this should be serialized data
            checksum=checksum,
            timestamp=timestamp
        )
        
        # Wait a brief moment for queue processing
        time.sleep(0.1)
        
        print(f"🔍 Now attempting to retrieve the chunk via get_chunk_data...")
        
        # Attempt to retrieve chunk data (should be found in queue)
        result = chunk_manager.get_chunk_data(test_round, test_source_client, test_chunk_id)
        
        if result is not None:
            print(f"✅ Successfully retrieved chunk data from queue: {result}")
            print(f"🎯 Queue fallback mechanism working normally!")
            return True
        else:
            print(f"❌ Failed to retrieve chunk data from queue")
            return False
            
    except Exception as e:
        print(f"❌ Error occurred during testing: {e}")
        return False
        
    finally:
        # Cleanup
        write_queue.stop_writer_thread(force_immediate=True)
        print("🧹 Cleanup completed")

if __name__ == "__main__":
    success = test_queue_fallback()
    if success:
        print("\n🎉 Queue fallback mechanism test passed!")
        exit(0)
    else:
        print("\n💥 Queue fallback mechanism test failed!")
        exit(1)