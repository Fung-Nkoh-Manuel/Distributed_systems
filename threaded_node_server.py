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
    def __init__(self, node_id: str, cpu: int, memory: int, storage: int, bandwidth: int, use_existing_folder=False):
        self.node_id = node_id
        self.cpu = cpu
        self.memory = memory
        self.storage = storage  # GB
        self.bandwidth = bandwidth  # Mbps
        
        # Create storage directory with absolute path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.storage_path = os.path.join(current_dir, f"storage_{node_id}")
        
        # Handle storage directory based on use_existing_folder flag
        if use_existing_folder and os.path.exists(self.storage_path):
            # Use existing folder, don't clear it
            print(f"📁 Using existing storage directory: {self.storage_path}")
            # Scan existing files in the directory
            self._scan_existing_files()
        else:
            # Create fresh storage directory
            if os.path.exists(self.storage_path):
                shutil.rmtree(self.storage_path)
            os.makedirs(self.storage_path, exist_ok=True)
            print(f"📁 Created new storage directory: {self.storage_path}")
        
        self.files: Dict[str, dict] = {}
        self.running = False
        self.status = "online"
        
    def _scan_existing_files(self):
        """Scan existing files in storage directory and register them"""
        if not os.path.exists(self.storage_path):
            return
            
        for filename in os.listdir(self.storage_path):
            file_path = os.path.join(self.storage_path, filename)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                file_id = hashlib.md5(f"{filename}-{os.path.getctime(file_path)}".encode()).hexdigest()[:8]
                
                self.files[file_id] = {
                    "file_id": file_id,
                    "file_name": filename,
                    "file_size": file_size,
                    "file_path": file_path,
                    "created_at": os.path.getctime(file_path),
                    "actual_size": file_size
                }
                
                print(f"📁 Registered existing file: {filename} ({file_size/1024/1024:.2f} MB)")

    # ... rest of the EnhancedStorageNode class remains the same ...

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
        
        # Register existing files with network
        self._register_existing_files()
        
        # Start interactive menu in a separate thread
        menu_thread = threading.Thread(target=self._interactive_menu, daemon=True)
        menu_thread.start()
        
        return actual_port
        
    def _register_existing_files(self):
        """Register existing files with network controller"""
        files_result = self.node._list_files()
        if files_result['success'] and files_result['files']:
            print(f"📁 Registering {len(files_result['files'])} existing files with network...")
            for file_info in files_result['files']:
                self._notify_network_file_created(
                    file_info['file_id'],
                    file_info['file_name'],
                    file_info['file_size']
                )

    # ... rest of the EnhancedNodeServer class remains the same ...

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
    parser.add_argument('--use-existing-folder', action='store_true', help='Use existing storage folder if available')
    
    args = parser.parse_args()
    
    print(f"🚀 Starting Enhanced Storage Node: {args.node_id}")
    print("=" * 50)
    
    # Create node with use_existing_folder option
    node = EnhancedStorageNode(
        node_id=args.node_id,
        cpu=args.cpu,
        memory=args.memory,
        storage=args.storage,
        bandwidth=args.bandwidth,
        use_existing_folder=args.use_existing_folder
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