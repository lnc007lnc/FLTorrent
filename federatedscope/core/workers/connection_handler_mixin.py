"""
Connection handler mixin for server to handle connection monitoring messages
"""

import logging
import time
import json
import os
from datetime import datetime
from federatedscope.core.message import Message

logger = logging.getLogger(__name__)


class ConnectionHandlerMixin:
    """
    Mixin class to add connection monitoring capabilities to server
    """
    
    def __init__(self):
        # Connection tracking
        self.connected_clients = {}
        self.client_connection_history = {}
        
        # File saving setup
        self.connection_log_dir = "connection_logs"
        self.connection_log_file = os.path.join(self.connection_log_dir, "connection_messages.jsonl")
        self._setup_connection_logging()
        
    def _setup_connection_logging(self):
        """Setup directory and file for connection logging"""
        try:
            os.makedirs(self.connection_log_dir, exist_ok=True)
            # Write header if file doesn't exist
            if not os.path.exists(self.connection_log_file):
                with open(self.connection_log_file, 'w') as f:
                    header = {
                        "log_started": datetime.now().isoformat(),
                        "description": "FederatedScope Connection Messages Log",
                        "format": "Each line is a JSON object containing connection event data"
                    }
                    f.write(json.dumps(header) + "\n")
            logger.info(f"Connection logging initialized: {self.connection_log_file}")
        except Exception as e:
            logger.error(f"Failed to setup connection logging: {e}")
            
    def _save_connection_message_to_file(self, message, connection_data, processing_timestamp):
        """Save connection message to local file"""
        try:
            log_entry = {
                "timestamp": processing_timestamp,
                "datetime": datetime.fromtimestamp(processing_timestamp).isoformat(),
                "message_info": {
                    "msg_type": message.msg_type,
                    "sender": message.sender,
                    "receiver": message.receiver,
                    "state": message.state,
                    "message_timestamp": getattr(message, 'timestamp', None)
                },
                "connection_data": connection_data,
                "server_id": getattr(self, 'ID', 0)
            }
            
            with open(self.connection_log_file, 'a') as f:
                f.write(json.dumps(log_entry) + "\n")
                
            logger.debug(f"Connection message saved to {self.connection_log_file}")
            
        except Exception as e:
            logger.error(f"Failed to save connection message to file: {e}")
        
    def callback_funcs_for_connection(self, message):
        """
        Handle connection monitoring messages from clients
        
        Args:
            message: Message object containing connection event data
        """
        try:
            sender_id = message.sender
            connection_data = message.content
            
            # Process connection message
            logger.debug(f"Received connect_msg from client {sender_id}")
            
            if not isinstance(connection_data, dict):
                logger.warning(f"Invalid connection message format from client {sender_id}")
                return
                
            event_type = connection_data.get('event_type', 'unknown')
            peer_id = connection_data.get('peer_id')
            timestamp = connection_data.get('timestamp', time.time())
            details = connection_data.get('details', {})
            
            logger.info(f"🔌 Connection event from Client {sender_id}: "
                       f"{event_type} (peer: {peer_id})")
            
            # Save message to local file
            processing_timestamp = time.time()
            self._save_connection_message_to_file(message, connection_data, processing_timestamp)
            
            # Update connection tracking
            self._update_connection_status(sender_id, event_type, timestamp, details)
            
            # Update topology manager if available and this is a topology-related connection
            if hasattr(self, 'topology_manager') and self.topology_manager is not None:
                if event_type == 'connect' and peer_id is not None:
                    # Record the connection in topology manager
                    is_expected = self.topology_manager.record_connection_established(sender_id, peer_id)
                    # Connection already logged by topology manager
            
            # Log detailed connection information
            self._log_connection_event(sender_id, event_type, peer_id, details, timestamp)
            
            # Send acknowledgment back to client (optional)
            if self._cfg.distribute.get('send_connection_ack', True):
                ack_message = Message(
                    msg_type='connection_ack',
                    sender=self.ID,
                    receiver=sender_id,
                    state=-1,
                    content={
                        'ack_timestamp': time.time(),
                        'original_event': event_type,
                        'status': 'received'
                    }
                )
                self.comm_manager.send(ack_message)
                
        except Exception as e:
            logger.error(f"Error processing connection message from client {message.sender}: {e}")
            
    def _update_connection_status(self, client_id, event_type, timestamp, details):
        """Update internal connection status tracking"""
        
        # Initialize client tracking if not exists
        if client_id not in self.connected_clients:
            self.connected_clients[client_id] = {
                'status': 'unknown',
                'last_seen': timestamp,
                'first_seen': timestamp,
                'event_count': 0
            }
            
        if client_id not in self.client_connection_history:
            self.client_connection_history[client_id] = []
            
        # Update status based on event type
        client_info = self.connected_clients[client_id]
        client_info['last_seen'] = timestamp
        client_info['event_count'] += 1
        
        if event_type in ['connect', 'accept']:
            client_info['status'] = 'connected'
            client_info['connection_details'] = details
        elif event_type == 'disconnect':
            client_info['status'] = 'disconnected'
            client_info['disconnect_reason'] = details.get('error_message', 'normal_disconnect')
        elif event_type == 'reject':
            client_info['status'] = 'rejected'
            
        # Add to history
        self.client_connection_history[client_id].append({
            'event_type': event_type,
            'timestamp': timestamp,
            'details': details
        })
        
        # Keep history limited
        if len(self.client_connection_history[client_id]) > 50:
            self.client_connection_history[client_id] = \
                self.client_connection_history[client_id][-25:]
                
    def _log_connection_event(self, client_id, event_type, peer_id, details, timestamp):
        """Log connection event with appropriate detail level"""
        
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp))
        
        if event_type == 'connect':
            if details.get('type') == 'heartbeat':
                logger.debug(f"[{time_str}] 💓 Heartbeat from Client {client_id} "
                           f"(uptime: {details.get('uptime', 0):.1f}s)")
            else:
                logger.info(f"[{time_str}] ✅ Client {client_id} connected to peer {peer_id}")
                if details.get('client_address'):
                    logger.info(f"    Client address: {details['client_address']}")
                    
        elif event_type == 'disconnect':
            error_msg = details.get('error_message', 'No error message')
            logger.warning(f"[{time_str}] ❌ Client {client_id} disconnected from peer {peer_id}: {error_msg}")
            
        elif event_type == 'accept':
            logger.info(f"[{time_str}] 🤝 Client {client_id} accepted connection from peer {peer_id}")
            
        elif event_type == 'reject':
            logger.warning(f"[{time_str}] 🚫 Client {client_id} rejected connection from peer {peer_id}")
            
    def get_client_connection_status(self, client_id=None):
        """
        Get connection status for specific client or all clients
        
        Args:
            client_id: Specific client ID, or None for all clients
            
        Returns:
            dict: Connection status information
        """
        if client_id is not None:
            return self.connected_clients.get(client_id, {'status': 'unknown'})
        return self.connected_clients.copy()
        
    def get_client_connection_history(self, client_id, limit=10):
        """
        Get connection event history for a specific client
        
        Args:
            client_id: Client ID
            limit: Maximum number of events to return
            
        Returns:
            list: Recent connection events
        """
        history = self.client_connection_history.get(client_id, [])
        return history[-limit:] if limit else history
        
    def get_connected_clients_count(self):
        """Get number of currently connected clients"""
        return len([client for client, info in self.connected_clients.items() 
                   if info['status'] == 'connected'])
                   
    def print_connection_summary(self):
        """Print a summary of all client connections"""
        logger.info("📊 Client Connection Summary:")
        logger.info(f"   Total clients seen: {len(self.connected_clients)}")
        logger.info(f"   Currently connected: {self.get_connected_clients_count()}")
        
        for client_id, info in self.connected_clients.items():
            status_emoji = {
                'connected': '✅',
                'disconnected': '❌',
                'rejected': '🚫',
                'unknown': '❓'
            }.get(info['status'], '❓')
            
            last_seen = time.strftime('%H:%M:%S', time.localtime(info['last_seen']))
            logger.info(f"   {status_emoji} Client {client_id}: {info['status']} "
                       f"(last seen: {last_seen}, events: {info['event_count']})")
                       
    def get_saved_connection_messages(self, limit=None):
        """Read and return saved connection messages from file"""
        messages = []
        try:
            if os.path.exists(self.connection_log_file):
                with open(self.connection_log_file, 'r') as f:
                    for line_num, line in enumerate(f):
                        if line_num == 0:  # Skip header
                            continue
                        try:
                            message = json.loads(line.strip())
                            messages.append(message)
                        except json.JSONDecodeError:
                            continue
                            
                if limit:
                    messages = messages[-limit:]
                    
        except Exception as e:
            logger.error(f"Error reading saved connection messages: {e}")
            
        return messages
        
    def print_saved_connection_summary(self):
        """Print summary of saved connection messages"""
        messages = self.get_saved_connection_messages()
        print(f"\n📁 SAVED CONNECTION MESSAGES SUMMARY:")
        print(f"   Total messages saved: {len(messages)}")
        print(f"   Log file: {self.connection_log_file}")
        
        if messages:
            print(f"   Recent messages:")
            for i, msg in enumerate(messages[-5:], 1):
                event_type = msg.get('connection_data', {}).get('event_type', 'unknown')
                client_id = msg.get('message_info', {}).get('sender', 'unknown')
                timestamp = msg.get('datetime', 'unknown')
                print(f"     {i}. [{timestamp}] Client {client_id}: {event_type}")
        else:
            print(f"   No messages found.")