#!/usr/bin/env python3
"""
Enhanced Storage Node with File Operations and Network Integration
"""

import socket
import threading
import json
import time
import argparse
import os
import shutil
from typing import Dict, Any
import hashlib

class EnhancedStorageNode:
    def __init__(self, node_id: str, cpu: int, memory: int, storage: int, bandwidth: int):
        self.node_id = node_id
        self.cpu = cpu
        self.memory = memory
        self.storage = storage  # GB
        self.bandwidth = bandwidth  # Mbps
        
        self.storage_path = f"storage_{node_id}"
        self.files: Dict[str, dict] = {}
        self.running = False
        
        # Create storage directory
        os.makedirs(self.storage_path, exist_ok=True)
        
    def start_server(self, host='localhost', port=0):
        """Start node server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((host, port))
        self.actual_port = self.server_socket.getsockname()[1]
        self.server_socket.listen(5)
        self.running = True
        
        print(f"🖥️  Node {self.node_id} started on {host}:{self.actual_port}")
        
        # Start accepting connections
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
        
        return self.actual_port
        
    def stop_server(self):
        """Stop node server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        print(f"🛑 Node {self.node_id} stopped")
        
    def _accept_connections(self):
        """Accept incoming connections"""
        while self.running:
            try:
                client_socket, address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, address),
                    daemon=True
                )
                client_thread.start()
            except:
                break
                
    def _handle_client(self, client_socket, address):
        """Handle client requests"""
        try:
            data = client_socket.recv(8192)
            if not data:
                return
                
            request = json.loads(data.decode('utf-8'))
            command = request.get('command')
            args = request.get('args', {})
            
            response = self._process_command(command, args)
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
        except Exception as e:
            print(f"❌ Node {self.node_id} client error: {e}")
        finally:
            client_socket.close()
            
    def _process_command(self, command: str, args: dict) -> dict:
        """Process node commands"""
        try:
            if command == "node_info":
                return self._get_node_info()
            elif command == "create_file":
                return self._create_file(args)
            elif command == "delete_file":
                return self._delete_file(args)
            elif command == "list_files":
                return self._list_files()
            elif command == "file_info":
                return self._file_info(args)
            elif command == "storage_stats":
                return self._storage_stats()
            elif command == "health":
                return {"status": "healthy", "node_id": self.node_id}
            else:
                return {"success": False, "error": f"Unknown command: {command}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _get_node_info(self) -> dict:
        """Get node information"""
        return {
            "success": True,
            "node_id": self.node_id,
            "cpu": self.cpu,
            "memory": self.memory,
            "storage": self.storage,
            "bandwidth": self.bandwidth,
            "address": f"localhost:{self.actual_port}",
            "files_count": len(self.files)
        }
    
    def _create_file(self, args: dict) -> dict:
        """Create a file on this node"""
        file_name = args['file_name']
        file_size = args['file_size']
        file_id = args.get('file_id', hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()[:8])
        
        # Check storage availability
        available = self._get_available_storage()
        if file_size > available:
            return {"success": False, "error": "Insufficient storage"}
        
        # Create file
        file_path = os.path.join(self.storage_path, file_name)
        
        # Create dummy file content
        with open(file_path, 'wb') as f:
            f.write(b'0' * file_size)  # Create file with zeros
        
        # Register file
        self.files[file_id] = {
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "file_path": file_path,
            "created_at": time.time()
        }
        
        print(f"📁 Created {file_name} ({file_size/1024/1024:.2f} MB) on {self.node_id}")
        
        return {"success": True, "file_id": file_id}
    
    def _delete_file(self, args: dict) -> dict:
        """Delete a file from this node"""
        file_id = args['file_id']
        
        if file_id not in self.files:
            return {"success": False, "error": "File not found"}
        
        file_info = self.files[file_id]
        
        # Delete physical file
        if os.path.exists(file_info['file_path']):
            os.remove(file_info['file_path'])
        
        # Remove from registry
        del self.files[file_id]
        
        print(f"🗑️  Deleted {file_info['file_name']} from {self.node_id}")
        return {"success": True}
    
    def _list_files(self) -> dict:
        """List all files on this node"""
        files_list = []
        for file_id, file_info in self.files.items():
            files_list.append({
                "file_id": file_id,
                "file_name": file_info['file_name'],
                "file_size": file_info['file_size'],
                "created_at": file_info['created_at']
            })
        
        return {"success": True, "files": files_list}
    
    def _file_info(self, args: dict) -> dict:
        """Get information about a specific file"""
        file_id = args.get('file_id')
        file_name = args.get('file_name')
        
        for fid, file_info in self.files.items():
            if fid == file_id or file_info['file_name'] == file_name:
                return {"success": True, "file": file_info}
        
        return {"success": False, "error": "File not found"}
    
    def _storage_stats(self) -> dict:
        """Get storage statistics"""
        total_storage = self.storage * (1024**3)  # Convert to bytes
        used_storage = sum(file_info['file_size'] for file_info in self.files.values())
        available_storage = total_storage - used_storage
        
        return {
            "success": True,
            "total_bytes": total_storage,
            "used_bytes": used_storage,
            "available_bytes": available_storage,
            "utilization_percent": (used_storage / total_storage) * 100,
            "files_count": len(self.files)
        }
    
    def _get_available_storage(self) -> float:
        """Calculate available storage in bytes"""
        total_storage = self.storage * (1024**3)
        used_storage = sum(file_info['file_size'] for file_info in self.files.values())
        return total_storage - used_storage

class EnhancedNodeServer:
    """Enhanced node server with network registration"""
    
    def __init__(self, node: EnhancedStorageNode, network_host='localhost', network_port=5000):
        self.node = node
        self.network_host = network_host
        self.network_port = network_port
        self.registered = False
        
    def start(self, host='localhost', port=0):
        """Start node and register with network"""
        # Start node server
        actual_port = self.node.start_server(host, port)
        
        # Register with network
        self._register_with_network(actual_port)
        
        return actual_port
        
    def stop(self):
        """Stop node and unregister from network"""
        self.node.stop_server()
        if self.registered:
            self._unregister_from_network()
    
    def _register_with_network(self, node_port: int):
        """Register node with network controller"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.network_host, self.network_port))
            
            node_info = {
                "node_id": self.node.node_id,
                "cpu": self.node.cpu,
                "memory": self.node.memory,
                "storage": self.node.storage,
                "bandwidth": self.node.bandwidth
            }
            
            request = {
                "command": "register_node",
                "args": {
                    "node_id": self.node.node_id,
                    "node_info": node_info
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                self.registered = True
                return True
            else:
                print(f"❌ Failed to register: {response}")
                return False
                
        except Exception as e:
            print(f"❌ Network registration error: {e}")
            return False
    
    def _unregister_from_network(self):
        """Unregister node from network"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "unregister_node",
                "args": {
                    "node_id": self.node.node_id
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            sock.recv(4096)  # Wait for response
            
            sock.close()
            self.registered = False
            print(f"🔴 Node {self.node.node_id} unregistered from network")
            
        except Exception as e:
            print(f"❌ Network unregistration error: {e}")

def main():
    parser = argparse.ArgumentParser(description='Enhanced Storage Node Server')
    parser.add_argument('--node-id', required=True, help='Node identifier')
    parser.add_argument('--network-host', default='localhost', help='Network controller host')
    parser.add_argument('--network-port', type=int, default=5000, help='Network controller port')
    parser.add_argument('--host', default='localhost', help='Node host')
    parser.add_argument('--cpu', type=int, default=4, help='CPU capacity')
    parser.add_argument('--memory', type=int, default=16, help='Memory capacity (GB)')
    parser.add_argument('--storage', type=int, default=1000, help='Storage capacity (GB)')
    parser.add_argument('--bandwidth', type=int, default=1000, help='Bandwidth (Mbps)')
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Enhanced Storage Node: {args.node_id}")
    print("=" * 50)
    
    # Create node
    node = EnhancedStorageNode(
        node_id=args.node_id,
        cpu=args.cpu,
        memory=args.memory,
        storage=args.storage,
        bandwidth=args.bandwidth
    )
    
    # Create server
    server = EnhancedNodeServer(node, args.network_host, args.network_port)
    
    try:
        # Start server
        server.start(args.host, 0)  # Auto-assign port
        
        print(f"✅ Node {args.node_id} ready!")
        print("Press Ctrl+C to stop")
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping node {args.node_id}...")
        server.stop()

if __name__ == '__main__':
    main()