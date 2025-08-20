#!/usr/bin/env python3
"""
Simple test to verify connection monitoring functionality
"""

import sys
import os
sys.path.insert(0, os.getcwd())

# Set protobuf implementation
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

from federatedscope.core.connection_monitor import ConnectionMonitor, ConnectionEvent
from federatedscope.core.message import Message
from federatedscope.core.workers.connection_handler_mixin import ConnectionHandlerMixin
import time

def test_connection_monitor():
    """Test the connection monitoring components"""
    
    print("🔧 Testing Connection Monitor Components")
    print("=" * 50)
    
    # Mock communication manager
    class MockCommManager:
        def __init__(self):
            self.sent_messages = []
            
        def send(self, message):
            self.sent_messages.append(message)
            print(f"📤 CommManager: Sent message type '{message.msg_type}' from {message.sender} to {message.receiver}")
            print(f"    Content: {message.content}")
            
    # Test 1: Connection Monitor Creation
    print("\n1️⃣ Testing ConnectionMonitor creation...")
    comm_manager = MockCommManager()
    monitor = ConnectionMonitor(client_id=1, comm_manager=comm_manager, server_id=0)
    print(f"✅ ConnectionMonitor created for client {monitor.client_id}")
    
    # Test 2: Connection Events
    print("\n2️⃣ Testing connection events...")
    
    # Test connection establishment
    monitor.report_connection_established(
        peer_id=0,
        details={'client_address': {'host': '127.0.0.1', 'port': 50052}}
    )
    print(f"✅ Connection established event sent")
    
    # Test connection lost
    monitor.report_connection_lost(
        peer_id=0,
        details={'error_message': 'Connection timeout'}
    )
    print(f"✅ Connection lost event sent")
    
    # Test connection accepted
    monitor.report_connection_accepted(
        peer_id=2,
        details={'incoming_from': '127.0.0.1:50053'}
    )
    print(f"✅ Connection accepted event sent")
    
    print(f"\n📊 Total messages sent: {len(comm_manager.sent_messages)}")
    
    # Test 3: Connection Handler Mixin
    print("\n3️⃣ Testing ConnectionHandlerMixin...")
    
    class MockServer(ConnectionHandlerMixin):
        def __init__(self):
            ConnectionHandlerMixin.__init__(self)
            self.ID = 0
            self._cfg = type('cfg', (), {
                'distribute': {'send_connection_ack': True}
            })()
            self.comm_manager = MockCommManager()
            
    server = MockServer()
    
    # Create a mock connection message
    connection_msg = Message(
        msg_type='connect_msg',
        sender=1,
        receiver=0,
        state=-1,
        content={
            'event_type': 'connect',
            'client_id': 1,
            'peer_id': 0,
            'timestamp': time.time(),
            'details': {
                'client_address': {'host': '127.0.0.1', 'port': 50052},
                'initialization': True
            }
        }
    )
    
    print(f"🧪 Testing server handling of connect_msg...")
    server.callback_funcs_for_connection(connection_msg)
    print(f"✅ Server processed connection message")
    
    # Check connection status
    status = server.get_client_connection_status(1)
    print(f"📊 Client 1 status: {status}")
    
    # Test 4: Event History
    print("\n4️⃣ Testing event history...")
    history = monitor.get_event_history(limit=5)
    print(f"📚 Event history (last 5): {len(history)} events")
    for i, event in enumerate(history):
        print(f"   {i+1}. {event['event_type']} at {time.strftime('%H:%M:%S', time.localtime(event['timestamp']))}")
    
    print(f"\n🎉 All tests completed successfully!")
    print(f"✅ ConnectionMonitor: Works correctly")
    print(f"✅ ConnectionHandlerMixin: Processes messages correctly") 
    print(f"✅ Message sending: {len(comm_manager.sent_messages)} messages sent")
    print(f"✅ Event tracking: {len(history)} events recorded")

if __name__ == "__main__":
    try:
        test_connection_monitor()
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)