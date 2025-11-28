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
        
        # Create storage directory with absolute path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.storage_path = os.path.join(current_dir, f"storage_{node_id}")
        
        # Ensure storage directory exists and is empty
        if os.path.exists(self.storage_path):
            shutil.rmtree(self.storage_path)
        os.makedirs(self.storage_path, exist_ok=True)
        
        self.files: Dict[str, dict] = {}
        self.running = False
        self.status = "online"  # Track node status
        
        print(f"📁 Storage directory created: {self.storage_path}")

    def start_server(self, host='localhost', port=0):
        """Start node server"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((host, port))
        self.actual_port = self.server_socket.getsockname()[1]
        self.server_socket.listen(5)
        self.running = True
        
        print(f"🖥️  Node {self.node_id} started on {host}:{self.actual_port}")
        print(f"📁 Storage path: {self.storage_path}")
        print(f"🟢 Node {self.node_id} is ONLINE")
        
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
        
    def set_online(self):
        """Set node online"""
        if self.status != "online":
            self.status = "online"
            print(f"🎯 NODE {self.node_id} STATUS: 🟢 ONLINE")
        return {"success": True, "message": f"Node {self.node_id} is online"}
    
    def set_offline(self):
        """Set node offline"""
        if self.status != "offline":
            self.status = "offline"
            print(f"🎯 NODE {self.node_id} STATUS: 🔴 OFFLINE")
        return {"success": True, "message": f"Node {self.node_id} is offline"}
        
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
            
            print(f"🔧 {self.node_id} received command: {command}")
            
            response = self._process_command(command, args)
            client_socket.sendall(json.dumps(response).encode('utf-8'))
            
            # Log the action on the node
            if command == "create_file":
                print(f"✅ {self.node_id} successfully created file: {args.get('file_name')}")
            elif command == "delete_file":
                print(f"✅ {self.node_id} successfully deleted file: {args.get('file_id')}")
            elif command == "download_file":
                print(f"✅ {self.node_id} successfully downloaded file: {args.get('file_name')}")
            elif command == "set_online":
                print(f"🎯 {self.node_id} status changed: 🟢 ONLINE")
            elif command == "set_offline":
                print(f"🎯 {self.node_id} status changed: 🔴 OFFLINE")
            
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
            elif command == "transfer_chunk":
                return self._transfer_chunk(args)
            elif command == "download_file":
                return self._download_file(args)
            elif command == "transfer_file":
                return self._transfer_file(args)
            elif command == "set_online":
                return self.set_online()
            elif command == "set_offline":
                return self.set_offline()
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
            "files_count": len(self.files),
            "storage_path": self.storage_path,
            "status": self.status
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
        
        # Create file with actual content
        file_path = os.path.join(self.storage_path, file_name)
        
        try:
            # Create actual file with readable content
            with open(file_path, 'w') as f:
                # Write file metadata and some content
                f.write(f"File: {file_name}\n")
                f.write(f"Created: {time.ctime()}\n")
                f.write(f"Size: {file_size} bytes\n")
                f.write(f"Node: {self.node_id}\n")
                f.write(f"ID: {file_id}\n")
                f.write("-" * 40 + "\n")
                
                # Add some dummy content to reach the specified size
                content_size = file_size - f.tell()
                if content_size > 0:
                    # Write pattern that shows this is test data
                    pattern = f"This is test data for {file_name} stored on node {self.node_id}. "
                    repetitions = max(1, content_size // len(pattern))
                    f.write((pattern * repetitions)[:content_size])
            
            # Verify file was created and has correct size
            actual_size = os.path.getsize(file_path)
            if actual_size != file_size:
                # Adjust file size if needed
                with open(file_path, 'a') as f:
                    f.write(' ' * (file_size - actual_size))
        
        except Exception as e:
            return {"success": False, "error": f"File creation failed: {str(e)}"}
        
        # Register file
        self.files[file_id] = {
            "file_id": file_id,
            "file_name": file_name,
            "file_size": file_size,
            "file_path": file_path,
            "created_at": time.time(),
            "actual_size": os.path.getsize(file_path)
        }
        
        print(f"📁 {self.node_id} created {file_name} ({file_size/1024/1024:.2f} MB)")
        print(f"   📍 Location: {file_path}")
        print(f"   📊 Actual size: {os.path.getsize(file_path)} bytes")
        
        return {"success": True, "file_id": file_id, "file_path": file_path}
    
    def _delete_file(self, args: dict) -> dict:
        """Delete a file from this node"""
        file_id = args['file_id']
        
        if file_id not in self.files:
            return {"success": False, "error": "File not found on this node"}
        
        file_info = self.files[file_id]
        file_path = file_info['file_path']
        file_name = file_info['file_name']
        
        # Delete physical file
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"🗑️  {self.node_id} deleted physical file: {file_path}")
            else:
                print(f"⚠️  {self.node_id}: File not found at path: {file_path}")
        except Exception as e:
            print(f"❌ {self.node_id} error deleting file: {e}")
            return {"success": False, "error": f"File deletion failed: {str(e)}"}
        
        # Remove from registry
        del self.files[file_id]
        
        print(f"🗑️  {self.node_id} removed {file_name} from registry")
        return {"success": True, "message": f"File {file_name} deleted from {self.node_id}"}
    
    def _download_file(self, args: dict) -> dict:
        """Download a file from another node"""
        file_name = args['file_name']
        file_size = args['file_size']
        source_node = args['source_node']
        source_host = args['source_host']
        source_port = args['source_port']
        
        print(f"📥 {self.node_id} downloading {file_name} from {source_node}...")
        
        # Check storage availability
        available = self._get_available_storage()
        if file_size > available:
            return {"success": False, "error": "Insufficient storage for download"}
        
        try:
            # Connect to source node to get file content
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((source_host, source_port))
            
            # Request file transfer from source node
            transfer_request = {
                "command": "transfer_file",
                "args": {
                    "file_name": file_name,
                    "target_node": self.node_id
                }
            }
            
            sock.sendall(json.dumps(transfer_request).encode('utf-8'))
            data = sock.recv(4096)
            transfer_response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if transfer_response.get('success'):
                # Create the file locally with the same content
                file_path = os.path.join(self.storage_path, file_name)
                file_id = hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()[:8]
                
                # Create file with the same structure as source
                with open(file_path, 'w') as f:
                    f.write(f"File: {file_name}\n")
                    f.write(f"Created: {time.ctime()}\n")
                    f.write(f"Size: {file_size} bytes\n")
                    f.write(f"Node: {self.node_id}\n")
                    f.write(f"ID: {file_id}\n")
                    f.write(f"Source: {source_node}\n")
                    f.write("-" * 40 + "\n")
                    
                    # Add content to reach specified size
                    content_size = file_size - f.tell()
                    if content_size > 0:
                        pattern = f"This is downloaded data for {file_name} from {source_node} to {self.node_id}. "
                        repetitions = max(1, content_size // len(pattern))
                        f.write((pattern * repetitions)[:content_size])
                
                # Register the downloaded file
                self.files[file_id] = {
                    "file_id": file_id,
                    "file_name": file_name,
                    "file_size": file_size,
                    "file_path": file_path,
                    "created_at": time.time(),
                    "actual_size": os.path.getsize(file_path),
                    "source_node": source_node
                }
                
                print(f"✅ {self.node_id} successfully downloaded {file_name} from {source_node}")
                print(f"   📍 Location: {file_path}")
                return {"success": True, "file_id": file_id, "file_path": file_path}
            else:
                return {"success": False, "error": transfer_response.get('error', 'Download failed')}
                
        except Exception as e:
            return {"success": False, "error": f"Download failed: {str(e)}"}
    
    def _transfer_file(self, args: dict) -> dict:
        """Transfer file to another node (source node perspective)"""
        file_name = args['file_name']
        target_node = args.get('target_node', 'unknown')
        
        # Find the file on this node
        file_info = None
        file_id = None
        for fid, info in self.files.items():
            if info['file_name'] == file_name:
                file_info = info
                file_id = fid
                break
        
        if not file_info:
            return {"success": False, "error": "File not found on this node"}
        
        print(f"📤 {self.node_id} transferring {file_name} to {target_node}...")
        
        # Check if file exists physically
        if not os.path.exists(file_info['file_path']):
            return {"success": False, "error": "File not found on disk"}
        
        try:
            # Read file content to simulate transfer
            with open(file_info['file_path'], 'r') as f:
                file_content = f.read()
            
            # Simulate transfer time based on file size and bandwidth
            transfer_time = file_info['file_size'] / (self.bandwidth * 1000000)  # Convert Mbps to bytes/sec
            if transfer_time > 0.1:  # Cap simulation time
                transfer_time = 0.1
            
            print(f"   ⏳ Transfer simulation: {file_info['file_size']/1024/1024:.2f} MB, ~{transfer_time:.2f}s")
            time.sleep(transfer_time)
            
            print(f"✅ {self.node_id} successfully transferred {file_name} to {target_node}")
            return {
                "success": True, 
                "message": f"File {file_name} transferred successfully",
                "file_size": file_info['file_size'],
                "file_content_preview": file_content[:100] + "..." if len(file_content) > 100 else file_content
            }
            
        except Exception as e:
            return {"success": False, "error": f"Transfer failed: {str(e)}"}
    
    def _list_files(self) -> dict:
        """List all files on this node"""
        # Also check physical files in storage directory
        physical_files = []
        if os.path.exists(self.storage_path):
            physical_files = [f for f in os.listdir(self.storage_path) 
                            if os.path.isfile(os.path.join(self.storage_path, f))]
        
        files_list = []
        for file_id, file_info in self.files.items():
            file_exists = os.path.exists(file_info['file_path'])
            files_list.append({
                "file_id": file_id,
                "file_name": file_info['file_name'],
                "file_size": file_info['file_size'],
                "actual_size": file_info.get('actual_size', 0),
                "created_at": file_info['created_at'],
                "file_path": file_info['file_path'],
                "physical_file_exists": file_exists
            })
        
        return {
            "success": True, 
            "files": files_list,
            "storage_path": self.storage_path,
            "physical_files_count": len(physical_files)
        }
    
    def _file_info(self, args: dict) -> dict:
        """Get information about a specific file"""
        file_id = args.get('file_id')
        file_name = args.get('file_name')
        
        for fid, file_info in self.files.items():
            if fid == file_id or file_info['file_name'] == file_name:
                file_exists = os.path.exists(file_info['file_path'])
                return {
                    "success": True, 
                    "file": file_info,
                    "physical_file_exists": file_exists
                }
        
        return {"success": False, "error": "File not found"}
    
    def _storage_stats(self) -> dict:
        """Get storage statistics"""
        total_storage = self.storage * (1024**3)  # Convert to bytes
        used_storage = sum(file_info['file_size'] for file_info in self.files.values())
        available_storage = total_storage - used_storage
        
        # Count physical files
        physical_files = []
        if os.path.exists(self.storage_path):
            physical_files = [f for f in os.listdir(self.storage_path) 
                            if os.path.isfile(os.path.join(self.storage_path, f))]
        
        return {
            "success": True,
            "total_bytes": total_storage,
            "used_bytes": used_storage,
            "available_bytes": available_storage,
            "utilization_percent": (used_storage / total_storage) * 100,
            "files_count": len(self.files),
            "physical_files_count": len(physical_files),
            "storage_path": self.storage_path
        }
    
    def _transfer_chunk(self, args: dict) -> dict:
        """Handle file chunk transfer (for replication)"""
        # This would handle actual file transfers between nodes
        # For now, we'll just create the file locally
        return self._create_file(args)
    
    def _get_available_storage(self) -> float:
        """Calculate available storage in bytes"""
        total_storage = self.storage * (1024**3)
        used_storage = sum(file_info['file_size'] for file_info in self.files.values())
        return total_storage - used_storage

class EnhancedNodeServer:
    """Enhanced node server with network registration"""
    
    def __init__(self, node: EnhancedStorageNode, network_host='localhost', network_port=5500):
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
        
        # Start interactive menu in a separate thread
        menu_thread = threading.Thread(target=self._interactive_menu, daemon=True)
        menu_thread.start()
        
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
                "bandwidth": self.node.bandwidth,
                "address": f"localhost:{node_port}"
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
                print(f"✅ Node {self.node.node_id} registered with network controller")
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

    def _interactive_menu(self):
        """Interactive menu for node operations"""
        time.sleep(2)  # Wait for initialization
        
        while True:
            print(f"\n🎯 NODE {self.node.node_id} - COMMAND MENU")
            print("=" * 50)
            print("1. 📊 Node Status")
            print("2. 📁 Create File")
            print("3. 🗑️  Delete File")
            print("4. 📋 List Files")
            print("5. 💾 Storage Stats")
            print("6. 🟢 Set Online")
            print("7. 🔴 Set Offline")
            print("8. 🌐 Network Status")
            print("9. 📥 Download File")
            print("0. 🚪 Exit Node")
            print("-" * 50)
            
            try:
                choice = input("Choose option (0-9): ").strip()
                
                if choice == '1':
                    self._display_node_status()
                elif choice == '2':
                    self._create_file_interactive()
                elif choice == '3':
                    self._delete_file_interactive()
                elif choice == '4':
                    self._list_files_interactive()
                elif choice == '5':
                    self._storage_stats_interactive()
                elif choice == '6':
                    self._set_online_interactive()
                elif choice == '7':
                    self._set_offline_interactive()
                elif choice == '8':
                    self._network_status_interactive()
                elif choice == '9':
                    self._download_file_interactive()
                elif choice == '0':
                    print(f"👋 Shutting down node {self.node.node_id}...")
                    self.stop()
                    break
                else:
                    print("❌ Invalid choice")
                    
            except KeyboardInterrupt:
                print(f"\n👋 Shutting down node {self.node.node_id}...")
                self.stop()
                break
            except Exception as e:
                print(f"❌ Error: {e}")
    
    def _display_node_status(self):
        """Display node status"""
        info = self.node._get_node_info()
        if info['success']:
            print(f"\n🖥️  NODE {self.node.node_id} STATUS")
            print("=" * 40)
            print(f"Status: {'🟢 ONLINE' if self.node.status == 'online' else '🔴 OFFLINE'}")
            print(f"CPU: {info['cpu']} vCPUs")
            print(f"Memory: {info['memory']} GB")
            print(f"Storage: {info['storage']} GB")
            print(f"Bandwidth: {info['bandwidth']} Mbps")
            print(f"Files: {info['files_count']}")
            print(f"Port: {self.node.actual_port}")
            print(f"Storage Path: {info['storage_path']}")
    
    def _create_file_interactive(self):
        """Create file interactively"""
        try:
            file_name = input("File name: ").strip()
            size_mb = float(input("Size (MB): ").strip())
            
            result = self.node._create_file({
                "file_name": file_name,
                "file_size": int(size_mb * 1024 * 1024)
            })
            
            if result['success']:
                print(f"✅ File {file_name} created successfully")
                print(f"📍 Location: {result.get('file_path')}")
                
                # Notify network controller about the new file
                self._notify_network_file_created(
                    result['file_id'],
                    file_name,
                    int(size_mb * 1024 * 1024)
                )
            else:
                print(f"❌ Failed: {result.get('error')}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def _delete_file_interactive(self):
        """Delete file interactively"""
        files_result = self.node._list_files()
        if not files_result['success'] or not files_result['files']:
            print("❌ No files available to delete")
            return
            
        print("\n📂 Available files:")
        for i, file_info in enumerate(files_result['files']):
            print(f"{i+1}. {file_info['file_name']} (ID: {file_info['file_id']})")
        
        try:
            file_idx = int(input("File number to delete: ")) - 1
            if 0 <= file_idx < len(files_result['files']):
                file_id = files_result['files'][file_idx]['file_id']
                file_name = files_result['files'][file_idx]['file_name']
                result = self.node._delete_file({"file_id": file_id})
                if result['success']:
                    print("✅ File deleted successfully")
                    # Notify network controller about the deletion
                    self._notify_network_file_deleted(file_id, file_name)
                else:
                    print(f"❌ Failed: {result.get('error')}")
            else:
                print("❌ Invalid file number")
        except Exception as e:
            print(f"❌ Error: {e}")
    
    def _list_files_interactive(self):
        """List files interactively"""
        result = self.node._list_files()
        if result['success']:
            files = result['files']
            if files:
                print(f"\n📂 FILES ON {self.node.node_id} ({len(files)} total)")
                print("=" * 60)
                for file_info in files:
                    size_mb = file_info['file_size'] / (1024 * 1024)
                    print(f"📄 {file_info['file_name']} ({size_mb:.2f}MB)")
                    print(f"   ID: {file_info['file_id']}")
                    print(f"   Path: {file_info['file_path']}")
                    print(f"   Created: {time.ctime(file_info['created_at'])}")
                    print()
            else:
                print("📂 No files found")
        else:
            print("❌ Failed to list files")
    
    def _storage_stats_interactive(self):
        """Display storage stats interactively"""
        result = self.node._storage_stats()
        if result['success']:
            print(f"\n💾 STORAGE STATS - {self.node.node_id}")
            print("=" * 40)
            used_gb = result['used_bytes'] / (1024**3)
            total_gb = result['total_bytes'] / (1024**3)
            available_gb = result['available_bytes'] / (1024**3)
            
            print(f"Total: {total_gb:.2f} GB")
            print(f"Used: {used_gb:.2f} GB ({result['utilization_percent']:.1f}%)")
            print(f"Available: {available_gb:.2f} GB")
            print(f"Files: {result['files_count']}")
            print(f"Physical Files: {result['physical_files_count']}")
        else:
            print("❌ Failed to get storage stats")
    
    def _set_online_interactive(self):
        """Set node online interactively"""
        result = self.node.set_online()
        if result['success']:
            print(f"✅ {result['message']}")
            # Notify network controller about status change
            self._notify_network_status_change("online")
        else:
            print(f"❌ Failed: {result.get('error')}")
    
    def _set_offline_interactive(self):
        """Set node offline interactively"""
        result = self.node.set_offline()
        if result['success']:
            print(f"✅ {result['message']}")
            # Notify network controller about status change
            self._notify_network_status_change("offline")
        else:
            print(f"❌ Failed: {result.get('error')}")
    
    def _notify_network_status_change(self, status: str):
        """Notify network controller about node status change"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "node_status",
                "args": {
                    "node_id": self.node.node_id,
                    "status": status
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(1024)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                print(f"✅ Network controller notified: node is {status}")
            else:
                print(f"⚠️  Warning: Network not notified: {response.get('error')}")
                
        except Exception as e:
            print(f"⚠️  Warning: Could not notify network controller: {e}")
    
    def _network_status_interactive(self):
        """Get network status from controller"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "network_stats",
                "args": {}
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                stats = response
                print(f"\n🌐 NETWORK STATUS")
                print("=" * 40)
                print(f"Total Nodes: {stats['total_nodes']}")
                print(f"Online Nodes: {stats['online_nodes']}")
                print(f"Total Files: {stats['total_files']}")
                print(f"Storage Used: {stats['used_storage_gb']:.2f} GB")
                print(f"Total Storage: {stats['total_storage_gb']:.2f} GB")
            else:
                print(f"❌ Failed to get network status: {response.get('error')}")
                
        except Exception as e:
            print(f"❌ Network error: {e}")
    
    def _notify_network_file_created(self, file_id: str, file_name: str, file_size: int):
        """Notify network controller that a file was created locally"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "register_file",
                "args": {
                    "file_id": file_id,
                    "file_name": file_name,
                    "file_size": file_size,
                    "owner_node": self.node.node_id
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(1024)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                print(f"✅ File registered with network controller")
            else:
                print(f"⚠️  Warning: File not registered with network: {response.get('error')}")
                
        except Exception as e:
            print(f"⚠️  Warning: Could not notify network controller: {e}")
    
    def _notify_network_file_deleted(self, file_id: str, file_name: str):
        """Notify network controller that a file was deleted locally"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "unregister_file",
                "args": {
                    "file_id": file_id,
                    "node_id": self.node.node_id
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(1024)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                print(f"✅ File unregistered from network controller")
            else:
                print(f"⚠️  Warning: File not unregistered from network: {response.get('error')}")
                
        except Exception as e:
            print(f"⚠️  Warning: Could not notify network controller: {e}")
    
    def _download_file_interactive(self):
        """Download file from another node"""
        try:
            # First get network files
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((self.network_host, self.network_port))
            
            request = {
                "command": "list_files",
                "args": {}
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(65536)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if not response.get('success') or not response.get('files'):
                print("❌ No files available in network")
                return
            
            files = response['files']
            print("\n📂 Available files in network:")
            for i, file_info in enumerate(files):
                size_mb = file_info['file_size'] / (1024 * 1024)
                print(f"{i+1}. {file_info['file_name']} ({size_mb:.2f}MB) - Available on: {', '.join(file_info['available_on'])}")
            
            file_idx = int(input("File number to download: ")) - 1
            if 0 <= file_idx < len(files):
                file_info = files[file_idx]
                source_node = input(f"Source node ({', '.join(file_info['available_on'])}): ").strip()
                
                if source_node not in file_info['available_on']:
                    print("❌ File not available on that node")
                    return
                
                # Use controller to handle the download
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(30)
                sock.connect((self.network_host, self.network_port))
                
                download_request = {
                    "command": "download_file",
                    "args": {
                        "target_node": self.node.node_id,
                        "file_name": file_info['file_name'],
                        "source_node": source_node
                    }
                }
                
                sock.sendall(json.dumps(download_request).encode('utf-8'))
                data = sock.recv(4096)
                download_response = json.loads(data.decode('utf-8'))
                
                sock.close()
                
                if download_response.get('success'):
                    print(f"✅ {download_response.get('message')}")
                else:
                    print(f"❌ Download failed: {download_response.get('error')}")
            else:
                print("❌ Invalid file number")
                
        except Exception as e:
            print(f"❌ Error: {e}")

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
        
        print(f"✅ Node {args.node_id} ready with interactive menu!")
        
        # Keep running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print(f"\n🛑 Stopping node {args.node_id}...")
        server.stop()

if __name__ == '__main__':
    main()