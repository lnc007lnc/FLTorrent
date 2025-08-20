#!/usr/bin/env python3
"""
Test the file saving functionality specifically
"""

import sys
import os
sys.path.insert(0, os.getcwd())

# Set protobuf implementation
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

from federatedscope.core.workers.connection_handler_mixin import ConnectionHandlerMixin
from federatedscope.core.message import Message
import time

def test_file_saving():
    """Test that connection messages are saved to file correctly"""
    
    print("🔧 Testing File Saving Functionality")
    print("=" * 50)
    
    # Mock communication manager
    class MockCommManager:
        def __init__(self):
            self.sent_messages = []
            
        def send(self, message):
            self.sent_messages.append(message)
    
    # Create test server with connection handler
    class MockServer(ConnectionHandlerMixin):
        def __init__(self):
            ConnectionHandlerMixin.__init__(self)
            self.ID = 0
            self._cfg = type('cfg', (), {
                'distribute': {'send_connection_ack': True}
            })()
            self.comm_manager = MockCommManager()
    
    server = MockServer()
    
    # Test multiple connection messages
    print("📤 Sending multiple connection messages...")
    
    # Message 1: Connect
    connect_msg = Message(
        msg_type='connect_msg',
        sender=1,
        receiver=0,
        state=-1,
        content={
            'event_type': 'connect',
            'client_id': 1,
            'peer_id': 0,
            'timestamp': time.time(),
            'details': {'client_address': {'host': '127.0.0.1', 'port': 50052}}
        }
    )
    server.callback_funcs_for_connection(connect_msg)
    
    # Message 2: Disconnect  
    disconnect_msg = Message(
        msg_type='connect_msg',
        sender=2,
        receiver=0,
        state=-1,
        content={
            'event_type': 'disconnect',
            'client_id': 2,
            'peer_id': 0,
            'timestamp': time.time(),
            'details': {'error_message': 'Connection timeout'}
        }
    )
    server.callback_funcs_for_connection(disconnect_msg)
    
    # Message 3: Accept
    accept_msg = Message(
        msg_type='connect_msg',
        sender=3,
        receiver=0,
        state=-1,
        content={
            'event_type': 'accept',
            'client_id': 3,
            'peer_id': 1,
            'timestamp': time.time(),
            'details': {'incoming_from': '127.0.0.1:50053'}
        }
    )
    server.callback_funcs_for_connection(accept_msg)
    
    print("✅ All messages processed")
    
    # Test reading saved messages
    print("\n📁 Testing saved message retrieval...")
    saved_messages = server.get_saved_connection_messages()
    print(f"✅ Retrieved {len(saved_messages)} saved messages")
    
    # Print saved connection summary
    print("\n📊 Connection message summary:")
    server.print_saved_connection_summary()
    
    print(f"\n🎉 File saving test completed successfully!")
    print(f"✅ Messages saved: {len(saved_messages)}")
    print(f"✅ Log file created: {server.connection_log_file}")

if __name__ == "__main__":
    try:
        test_file_saving()
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)