#!/usr/bin/env python3
"""
Threaded Network Server with Socket Communication
Runs on a specified localhost port and accepts node connections
"""

import socket
import threading
import json
import time
import hashlib
from typing import Dict, Any
from collections import defaultdict
import argparse


class ThreadedNetworkServer:
    """Network coordinator that listens on a socket port"""
    
    def __init__(self, host='localhost', port=5500):
        self.host = host
        self.port = port
        self.nodes: Dict[str, str] = {}  # node_id -> "host:port"
        self.connections: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.active_transfers: Dict[str, dict] = {}
        self.running = False
        self.server_socket = None
        self.lock = threading.Lock()
        
    def start(self):
        """Start the network coordinator server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"[Network] Coordinator started on {self.host}:{self.port}")
        
        # Start accepting connections in a separate thread
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
        
    def stop(self):
        """Stop the network coordinator server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        print("[Network] Coordinator stopped")
        
    def _accept_connections(self):
        """Accept incoming client connections"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                # Handle each client in a separate thread
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
            except:
                break
                
    def _handle_client(self, client_socket, address):
        """Handle individual client requests"""
        try:
            while True:
                # Receive data
                data = client_socket.recv(4096)
                if not data:
                    break
                    
                # Parse request
                request = json.loads(data.decode('utf-8'))
                command = request.get('command')
                args = request.get('args', {})
                
                # Process command
                response = self._process_command(command, args)
                
                # Send response
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                
        except Exception as e:
            print(f"[Network] Error handling client: {e}")
        finally:
            client_socket.close()
            
    def _process_command(self, command: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Process commands from clients"""
        with self.lock:
            try:
                if command == "register_node":
                    node_id = args.get('node_id')
                    node_address = args.get('node_address')
                    
                    if not node_id or not node_address:
                        return {"error": "Missing node_id or node_address"}
                    
                    self.nodes[node_id] = node_address
                    print(f"[Network] Registered {node_id} at {node_address}")
                    return {"success": True, "registered": node_id}
                
                elif command == "list_nodes":
                    return {"nodes": self.nodes}
                
                elif command == "create_connection":
                    node1_id = args.get('node1_id')
                    node2_id = args.get('node2_id')
                    bandwidth = args.get('bandwidth')
                    
                    if not all([node1_id, node2_id, bandwidth]):
                        return {"error": "Missing required fields"}
                    
                    if node1_id not in self.nodes or node2_id not in self.nodes:
                        return {"error": "One or both nodes not registered"}
                    
                    # Tell both nodes about the connection
                    self._send_to_node(node1_id, {
                        "command": "add_connection",
                        "args": {"node_id": node2_id, "bandwidth": bandwidth}
                    })
                    
                    self._send_to_node(node2_id, {
                        "command": "add_connection",
                        "args": {"node_id": node1_id, "bandwidth": bandwidth}
                    })
                    
                    self.connections[node1_id][node2_id] = bandwidth
                    self.connections[node2_id][node1_id] = bandwidth
                    
                    return {
                        "success": True,
                        "connection": f"{node1_id} <-> {node2_id}",
                        "bandwidth": bandwidth
                    }
                
                elif command == "initiate_transfer":
                    source_node_id = args.get('source_node_id')
                    target_node_id = args.get('target_node_id')
                    file_name = args.get('file_name')
                    file_size = args.get('file_size')
                    
                    if not all([source_node_id, target_node_id, file_name, file_size]):
                        return {"error": "Missing required fields"}
                    
                    if source_node_id not in self.nodes or target_node_id not in self.nodes:
                        return {"error": "One or both nodes not registered"}
                    
                    # Generate unique file ID
                    file_id = hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()
                    
                    # Tell target node to prepare for transfer
                    result = self._send_to_node(target_node_id, {
                        "command": "initiate_transfer",
                        "args": {
                            "file_id": file_id,
                            "file_name": file_name,
                            "file_size": file_size,
                            "source_node": source_node_id
                        }
                    })
                    
                    if 'error' in result:
                        return result
                    
                    # Track transfer
                    self.active_transfers[file_id] = {
                        "file_id": file_id,
                        "source_node_id": source_node_id,
                        "target_node_id": target_node_id,
                        "file_name": file_name,
                        "file_size": file_size,
                        "total_chunks": result.get('total_chunks', 0),
                        "completed_chunks": 0,
                        "status": "in_progress",
                        "started_at": time.time()
                    }
                    
                    return {
                        "success": True,
                        "file_id": file_id,
                        "total_chunks": result.get('total_chunks', 0)
                    }
                
                elif command == "process_transfer":
                    file_id = args.get('file_id')
                    chunks_to_process = args.get('chunks_to_process', 1)
                    
                    if not file_id or file_id not in self.active_transfers:
                        return {"error": "Transfer not found"}
                    
                    transfer = self.active_transfers[file_id]
                    chunks_processed = 0
                    
                    # Process chunks
                    for chunk_id in range(transfer['completed_chunks'],
                                          min(transfer['completed_chunks'] + chunks_to_process,
                                              transfer['total_chunks'])):
                        result = self._send_to_node(transfer['target_node_id'], {
                            "command": "process_chunk",
                            "args": {
                                "file_id": file_id,
                                "chunk_id": chunk_id,
                                "source_node": transfer['source_node_id']
                            }
                        })
                        
                        if result.get('success'):
                            chunks_processed += 1
                            transfer['completed_chunks'] += 1
                            
                            if result.get('completed'):
                                transfer['status'] = 'completed'
                                transfer['completed_at'] = time.time()
                                break
                        else:
                            transfer['status'] = 'failed'
                            break
                    
                    return {
                        "success": True,
                        "chunks_processed": chunks_processed,
                        "completed_chunks": transfer['completed_chunks'],
                        "total_chunks": transfer['total_chunks'],
                        "status": transfer['status'],
                        "completed": transfer['status'] == 'completed'
                    }
                
                elif command == "network_stats":
                    total_storage = 0
                    used_storage = 0
                    
                    for node_id in self.nodes:
                        result = self._send_to_node(node_id, {
                            "command": "storage_stats",
                            "args": {}
                        })
                        if 'error' not in result:
                            total_storage += result.get('total_bytes', 0)
                            used_storage += result.get('used_bytes', 0)
                    
                    return {
                        "total_nodes": len(self.nodes),
                        "total_storage_bytes": total_storage,
                        "used_storage_bytes": used_storage,
                        "storage_utilization_percent": (used_storage / total_storage * 100) if total_storage > 0 else 0,
                        "active_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'in_progress']),
                        "completed_transfers": len([t for t in self.active_transfers.values() if t['status'] == 'completed'])
                    }
                
                elif command == "tick":
                    for node_id in self.nodes:
                        self._send_to_node(node_id, {"command": "tick", "args": {}})
                    return {"success": True}
                
                else:
                    return {"error": f"Unknown command: {command}"}
                    
            except Exception as e:
                return {"error": str(e)}
    
    def _send_to_node(self, node_id: str, request: dict) -> dict:
        """Send a request to a specific node"""
        if node_id not in self.nodes:
            return {"error": "Node not found"}
        
        try:
            node_address = self.nodes[node_id]
            host, port = node_address.split(':')
            port = int(port)
            
            # Connect to node
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((host, port))
            
            # Send request
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            # Receive response
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {"error": f"Failed to communicate with node: {str(e)}"}


def main():
    parser = argparse.ArgumentParser(description='Threaded Network Coordinator Server')
    parser.add_argument('--host', default='localhost', help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=5500, help='Port to bind to (default: 5500)')
    
    args = parser.parse_args()
    
    server = ThreadedNetworkServer(host=args.host, port=args.port)
    server.start()
    
    print(f"\nNetwork Coordinator running. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nStopping server...")
        server.stop()


if __name__ == '__main__':
    main()