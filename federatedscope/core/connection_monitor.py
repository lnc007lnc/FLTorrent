"""
Connection monitoring module for FederatedScope
Monitors connection events and reports them to the server
"""

import logging
import time
import threading
from enum import Enum
from federatedscope.core.message import Message

logger = logging.getLogger(__name__)


class ConnectionEvent(Enum):
    """Types of connection events"""
    CONNECT = "connect"        # New connection established
    DISCONNECT = "disconnect"  # Connection lost
    ACCEPT = "accept"         # Accepted incoming connection
    REJECT = "reject"         # Rejected incoming connection


class ConnectionStatus(Enum):
    """Connection status states"""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


class ConnectionMonitor:
    """
    Monitors connection events and sends notifications to server
    """
    
    def __init__(self, client_id, comm_manager, server_id=0):
        """
        Initialize connection monitor
        
        Args:
            client_id: ID of the client
            comm_manager: Communication manager instance
            server_id: ID of the server (default: 0)
        """
        self.client_id = client_id
        self.comm_manager = comm_manager
        self.server_id = server_id
        self.connection_status = ConnectionStatus.DISCONNECTED
        self.last_heartbeat = None
        self.monitor_active = True
        
        # Connection event history
        self.event_history = []
        self.max_history_size = 100
        
        # Thread for periodic connection checks
        self.monitor_thread = None
        self.heartbeat_interval = 30  # seconds
        
    def start_monitoring(self):
        """Start the connection monitoring thread"""
        if self.monitor_thread is None or not self.monitor_thread.is_alive():
            self.monitor_active = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitor_thread.start()
            logger.info(f"Connection monitor started for client {self.client_id}")
            
    def stop_monitoring(self):
        """Stop the connection monitoring thread"""
        self.monitor_active = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
        logger.info(f"Connection monitor stopped for client {self.client_id}")
        
    def report_connection_event(self, event_type: ConnectionEvent, peer_id=None, 
                               details=None, auto_timestamp=True):
        """
        Report a connection event to the server
        
        Args:
            event_type: Type of connection event
            peer_id: ID of the peer involved (optional)
            details: Additional details about the event
            auto_timestamp: Whether to automatically add timestamp
        """
        try:
            # Filter out server connections from reporting
            # Only report peer-to-peer connections, not client-server connections
            if peer_id is not None and peer_id == self.server_id:
                logger.debug(f"Client {self.client_id}: Skipping report for server connection "
                           f"(peer {peer_id} is server {self.server_id})")
                return
                
            # Create event data
            event_data = {
                'event_type': event_type.value,
                'client_id': self.client_id,
                'peer_id': peer_id,
                'timestamp': time.time() if auto_timestamp else None,
                'details': details or {}
            }
            
            # Add to event history
            self._add_to_history(event_data)
            
            # Update connection status based on event
            self._update_status_from_event(event_type)
            
            # Create and send CONNECT_MSG to server
            message = Message(
                msg_type='connect_msg',
                sender=self.client_id,
                receiver=self.server_id,
                state=-1,  # Connection messages are not tied to training rounds
                content=event_data,
                timestamp=event_data['timestamp']
            )
            
            # Send message to server
            self.comm_manager.send(message)
            
            logger.info(f"Client {self.client_id}: Reported {event_type.value} event "
                       f"for peer {peer_id} to server {self.server_id}")
                       
        except Exception as e:
            logger.error(f"Failed to report connection event: {e}")
            
    def report_connection_established(self, peer_id=None, details=None):
        """Report that a connection has been established"""
        self.report_connection_event(
            ConnectionEvent.CONNECT, 
            peer_id=peer_id, 
            details=details
        )
        
    def report_connection_lost(self, peer_id=None, details=None):
        """Report that a connection has been lost"""
        self.report_connection_event(
            ConnectionEvent.DISCONNECT, 
            peer_id=peer_id, 
            details=details
        )
        
    def report_connection_accepted(self, peer_id=None, details=None):
        """Report that an incoming connection was accepted"""
        self.report_connection_event(
            ConnectionEvent.ACCEPT, 
            peer_id=peer_id, 
            details=details
        )
        
    def report_connection_rejected(self, peer_id=None, details=None):
        """Report that an incoming connection was rejected"""
        self.report_connection_event(
            ConnectionEvent.REJECT, 
            peer_id=peer_id, 
            details=details
        )
        
    def get_connection_status(self):
        """Get current connection status"""
        return self.connection_status
        
    def get_event_history(self, limit=None):
        """Get connection event history"""
        if limit is None:
            return self.event_history[:]
        return self.event_history[-limit:]
        
    def _monitor_loop(self):
        """Main monitoring loop running in separate thread"""
        while self.monitor_active:
            try:
                self._check_connection_health()
                time.sleep(self.heartbeat_interval)
            except Exception as e:
                logger.error(f"Error in connection monitor loop: {e}")
                time.sleep(5)  # Short delay before retrying
                
    def _check_connection_health(self):
        """Check connection health and report issues"""
        current_time = time.time()
        
        # Check if we haven't sent a heartbeat recently
        if (self.last_heartbeat is None or 
            current_time - self.last_heartbeat > self.heartbeat_interval * 2):
            
            # Send heartbeat connection status (this will be filtered out by report_connection_event)
            self.report_connection_event(
                ConnectionEvent.CONNECT,
                peer_id=self.server_id,
                details={
                    'type': 'heartbeat',
                    'status': self.connection_status.value,
                    'uptime': current_time - (self.last_heartbeat or current_time)
                }
            )
            self.last_heartbeat = current_time
            
    def _update_status_from_event(self, event_type: ConnectionEvent):
        """Update connection status based on event type"""
        if event_type in [ConnectionEvent.CONNECT, ConnectionEvent.ACCEPT]:
            self.connection_status = ConnectionStatus.CONNECTED
        elif event_type == ConnectionEvent.DISCONNECT:
            self.connection_status = ConnectionStatus.DISCONNECTED
        elif event_type == ConnectionEvent.REJECT:
            self.connection_status = ConnectionStatus.ERROR
            
    def _add_to_history(self, event_data):
        """Add event to history with size limit"""
        self.event_history.append(event_data)
        
        # Trim history if it gets too large
        if len(self.event_history) > self.max_history_size:
            self.event_history = self.event_history[-self.max_history_size//2:]