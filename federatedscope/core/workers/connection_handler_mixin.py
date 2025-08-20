"""
Connection handler mixin for server to handle connection monitoring messages
"""

import logging
import time
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
        
    def callback_funcs_for_connection(self, message):
        """
        Handle connection monitoring messages from clients
        
        Args:
            message: Message object containing connection event data
        """
        try:
            sender_id = message.sender
            connection_data = message.content
            
            if not isinstance(connection_data, dict):
                logger.warning(f"Invalid connection message format from client {sender_id}")
                return
                
            event_type = connection_data.get('event_type', 'unknown')
            peer_id = connection_data.get('peer_id')
            timestamp = connection_data.get('timestamp', time.time())
            details = connection_data.get('details', {})
            
            logger.info(f"🔌 Connection event from Client {sender_id}: "
                       f"{event_type} (peer: {peer_id})")
            
            # Update connection tracking
            self._update_connection_status(sender_id, event_type, timestamp, details)
            
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