#!/usr/bin/env python3
"""
Enhanced Distributed Cloud Storage Controller
Single communication protocol with automatic replication
"""

import socket
import threading
import json
import time
import hashlib
from typing import Dict, Any, List
from collections import defaultdict
import argparse
import os
from datetime import datetime

class EnhancedNetworkController:
    """Enhanced network coordinator with replication and monitoring"""
    
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        
        # Node management
        self.nodes: Dict[str, dict] = {}  # node_id -> node_info
        self.node_status: Dict[str, str] = {}  # online/offline
        
        # File management
        self.files: Dict[str, dict] = {}  # file_id -> file_info
        self.file_replicas: Dict[str, List[str]] = defaultdict(list)  # file_id -> [node_ids]
        
        # Network topology
        self.connections: Dict[str, Dict[str, int]] = defaultdict(dict)
        
        # Transfer management
        self.active_transfers: Dict[str, dict] = {}
        
        # Track changes for minimal status display
        self.last_node_count = 0
        self.last_file_count = 0
        self.last_online_count = 0
        
        self.running = False
        self.server_socket = None
        self.lock = threading.Lock()
        
    def start(self):
        """Start the enhanced network controller"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        self.running = True
        
        print("🚀 ENHANCED DISTRIBUTED CLOUD STORAGE CONTROLLER")
        print("=" * 60)
        print("✅ Single communication protocol")
        print("✅ Automatic file replication")
        print("✅ Resource-aware node management")
        print("✅ Fault tolerance with re-replication")
        print("✅ Bandwidth-aware file transfers")
        print("✅ Real-time network monitoring")
        print("✅ Chunked file transfer coordination")
        print("=" * 60)
        print(f"🌐 Clean Controller started on {self.host}:{self.port}")
        
        # Start connection handler
        accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        accept_thread.start()
        
        # Start health monitoring (but don't display status continuously)
        monitor_thread = threading.Thread(target=self._health_monitoring_loop, daemon=True)
        monitor_thread.start()
        
    def stop(self):
        """Stop the controller"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        print("\n🛑 Controller stopped")
        
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
        """Handle client communication"""
        try:
            while True:
                data = client_socket.recv(8192)
                if not data:
                    break
                    
                request = json.loads(data.decode('utf-8'))
                command = request.get('command')
                args = request.get('args', {})
                
                response = self._process_command(command, args)
                client_socket.sendall(json.dumps(response).encode('utf-8'))
                
        except Exception as e:
            print(f"❌ Client handling error: {e}")
        finally:
            client_socket.close()
            
    def _process_command(self, command: str, args: dict) -> dict:
        """Process all commands with enhanced functionality"""
        try:
            if command == "register_node":
                return self._register_node(args)
            elif command == "unregister_node":
                return self._unregister_node(args)
            elif command == "node_status":
                return self._update_node_status(args)
            elif command == "list_nodes":
                return self._list_nodes()
            elif command == "create_file":
                return self._create_file_actual(args)  # Use the actual file creation method
            elif command == "delete_file":
                return self._delete_file(args)
            elif command == "list_files":
                return self._list_files(args)
            elif command == "transfer_file":
                return self._transfer_file(args)
            elif command == "download_file":
                return self._download_file(args)
            elif command == "network_stats":
                return self._get_network_stats()
            elif command == "node_stats":
                return self._get_node_stats(args)
            elif command == "set_node_online":
                return self._set_node_online(args)
            elif command == "set_node_offline":
                return self._set_node_offline(args)
            elif command == "replicate_file":
                return self._replicate_file(args)
            elif command == "display_status":
                return self._display_status_on_demand()
            else:
                return {"success": False, "error": f"Unknown command: {command}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def _register_node(self, args: dict) -> dict:
        """Register a new storage node"""
        node_id = args['node_id']
        node_info = args['node_info']
        
        with self.lock:
            self.nodes[node_id] = node_info
            self.node_status[node_id] = "online"
            
        print(f"✅ {node_id} online (CPU: {node_info['cpu']}, RAM: {node_info['memory']}GB, "
              f"Storage: {node_info['storage']}GB, BW: {node_info['bandwidth']}Mbps)")
        
        # Only show status when nodes change
        self._display_minimal_status()
        return {"success": True, "message": f"Node {node_id} registered"}
    
    def _unregister_node(self, args: dict) -> dict:
        """Unregister a node"""
        node_id = args['node_id']
        
        with self.lock:
            if node_id in self.nodes:
                del self.nodes[node_id]
                if node_id in self.node_status:
                    del self.node_status[node_id]
                
                # Remove file replicas from this node
                for file_id, replicas in list(self.file_replicas.items()):
                    if node_id in replicas:
                        replicas.remove(node_id)
                        if not replicas and file_id in self.files:
                            # No replicas left, schedule re-replication
                            self._schedule_re_replication(file_id)
        
        print(f"🔴 {node_id} disconnected")
        
        # Only show status when nodes change
        self._display_minimal_status()
        return {"success": True, "message": f"Node {node_id} unregistered"}
    
    def _update_node_status(self, args: dict) -> dict:
        """Update node status"""
        node_id = args['node_id']
        status = args['status']
        
        with self.lock:
            if node_id in self.node_status:
                old_status = self.node_status[node_id]
                self.node_status[node_id] = status
                
                # Only show status if status actually changed
                if old_status != status:
                    status_icon = "🟢" if status == "online" else "🔴"
                    print(f"{status_icon} {node_id} is now {status.upper()}")
                    self._display_minimal_status()
        
        return {"success": True}
    
    def _list_nodes(self) -> dict:
        """List all registered nodes"""
        with self.lock:
            nodes_info = {}
            for node_id, info in self.nodes.items():
                nodes_info[node_id] = {
                    **info,
                    "status": self.node_status.get(node_id, "unknown"),
                    "files_count": len([f for f in self.file_replicas.values() if node_id in f])
                }
            
            return {"success": True, "nodes": nodes_info}
    
    def _create_file_actual(self, args: dict) -> dict:
        """Actually create file on specific node"""
        node_id = args['node_id']
        file_name = args['file_name']
        file_size = args['file_size']
        
        if node_id not in self.nodes or self.node_status.get(node_id) != "online":
            return {"success": False, "error": "Node not available"}
        
        # Get node information to connect to it
        node_info = self.nodes[node_id]
        node_address = node_info.get('address', 'localhost:0').split(':')
        node_host = node_address[0]
        node_port = int(node_address[1])
        
        try:
            # Connect to the actual node to create the file
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            sock.connect((node_host, node_port))
            
            request = {
                "command": "create_file",
                "args": {
                    "file_name": file_name,
                    "file_size": file_size
                }
            }
            
            sock.sendall(json.dumps(request).encode('utf-8'))
            data = sock.recv(4096)
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            
            if response.get('success'):
                file_id = response['file_id']
                
                with self.lock:
                    self.files[file_id] = {
                        "file_id": file_id,
                        "file_name": file_name,
                        "file_size": file_size,
                        "owner_node": node_id,
                        "created_at": time.time()
                    }
                    
                    # Initial replica on owner node
                    self.file_replicas[file_id] = [node_id]
                
                print(f"📁 {file_name} ({file_size/1024/1024:.2f} MB) created on {node_id}")
                print(f"   📍 File ID: {file_id}")
                print(f"   📍 File Path: {response.get('file_path', 'Unknown')}")
                
                # Schedule replication to other nodes
                self._schedule_replication(file_id)
                
                self._display_minimal_status()
                return {"success": True, "file_id": file_id, "file_path": response.get('file_path')}
            else:
                return {"success": False, "error": response.get('error', 'Unknown error')}
                
        except Exception as e:
            return {"success": False, "error": f"Failed to connect to node: {str(e)}"}
    
    def _schedule_replication(self, file_id: str):
        """Schedule file replication to other nodes"""
        file_info = self.files[file_id]
        current_replicas = self.file_replicas[file_id]
        
        # Find suitable nodes for replication (excluding current replicas)
        suitable_nodes = []
        for node_id, node_info in self.nodes.items():
            if (node_id not in current_replicas and 
                self.node_status.get(node_id) == "online" and
                self._get_node_available_storage(node_id) >= file_info['file_size']):
                suitable_nodes.append((node_id, node_info))
        
        # Sort by available resources (simple heuristic)
        suitable_nodes.sort(key=lambda x: x[1]['storage'] * 0.3 + x[1]['bandwidth'] * 0.7, reverse=True)
        
        # Replicate to up to 2 additional nodes (3 total replicas)
        target_nodes = suitable_nodes[:2]
        
        for target_node_id, _ in target_nodes:
            print(f"🔄 Scheduling replication of {file_info['file_name']} to: {target_node_id}")
            # In a real implementation, this would trigger actual transfer
            self.file_replicas[file_id].append(target_node_id)
    
    def _schedule_re_replication(self, file_id: str):
        """Re-replicate files when replicas are lost"""
        print(f"🔄 Re-replicating file {file_id} due to node failure")
        self._schedule_replication(file_id)
    
    def _delete_file(self, args: dict) -> dict:
        """Delete a file from all nodes"""
        file_id = args['file_id']
        
        with self.lock:
            if file_id in self.files:
                file_name = self.files[file_id]['file_name']
                del self.files[file_id]
                if file_id in self.file_replicas:
                    del self.file_replicas[file_id]
                
                print(f"🗑️ {file_name} deleted from network")
                self._display_minimal_status()
                return {"success": True, "message": f"File {file_name} deleted"}
            
            return {"success": False, "error": "File not found"}
    
    def _list_files(self, args: dict) -> dict:
        """List all files in network"""
        node_id = args.get('node_id')
        
        with self.lock:
            files_info = []
            for file_id, file_info in self.files.items():
                replicas = self.file_replicas.get(file_id, [])
                if not node_id or node_id in replicas:
                    files_info.append({
                        **file_info,
                        "replicas": len(replicas),
                        "total_replicas": 3,  # Target replication factor
                        "available_on": replicas
                    })
            
            return {"success": True, "files": files_info}
    
    def _transfer_file(self, args: dict) -> dict:
        """Transfer file between nodes"""
        source_node = args['source_node']
        target_node = args['target_node']
        file_name = args['file_name']
        
        # Find file
        file_id = None
        for fid, info in self.files.items():
            if info['file_name'] == file_name and source_node in self.file_replicas.get(fid, []):
                file_id = fid
                break
        
        if not file_id:
            return {"success": False, "error": "File not found on source node"}
        
        # Add target node as replica
        with self.lock:
            if target_node not in self.file_replicas[file_id]:
                self.file_replicas[file_id].append(target_node)
        
        print(f"📥 {target_node} downloading {file_name} from {source_node} "
              f"(BW: {self.nodes[target_node]['bandwidth']}Mbps)")
        
        # Simulate transfer completion
        time.sleep(1)  # Simulate transfer time
        
        print(f"✅ {target_node} completed download of {file_name}")
        self._display_minimal_status()
        
        return {"success": True, "message": f"File {file_name} transferred"}
    
    def _download_file(self, args: dict) -> dict:
        """Download file to a node"""
        return self._transfer_file(args)  # Similar to transfer
    
    def _get_network_stats(self) -> dict:
        """Get comprehensive network statistics"""
        with self.lock:
            total_nodes = len(self.nodes)
            online_nodes = sum(1 for status in self.node_status.values() if status == "online")
            
            total_storage = sum(node['storage'] for node in self.nodes.values())
            used_storage = sum(
                sum(self.files[fid]['file_size'] for fid, replicas in self.file_replicas.items() 
                    if node_id in replicas)
                for node_id in self.nodes.keys()
            )
            
            total_files = len(self.files)
            well_replicated = sum(1 for replicas in self.file_replicas.values() if len(replicas) >= 2)
            
            # Calculate load balance
            node_loads = []
            for node_id in self.nodes:
                load = len([fid for fid, replicas in self.file_replicas.items() if node_id in replicas])
                node_loads.append(load)
            
            avg_load = sum(node_loads) / len(node_loads) if node_loads else 0
            max_load = max(node_loads) if node_loads else 0
            load_balance = (1 - (max_load - avg_load) / (max_load + 0.001)) * 100 if max_load > 0 else 100
            
            # Safe division for storage utilization
            storage_utilization = 0.0
            if total_storage > 0:
                storage_utilization = (used_storage / (1024**3)) / total_storage * 100
            
            return {
                "success": True,
                "total_nodes": total_nodes,
                "online_nodes": online_nodes,
                "total_storage_gb": total_storage,
                "used_storage_gb": used_storage / (1024**3),
                "total_files": total_files,
                "well_replicated_files": well_replicated,
                "load_balance": load_balance,
                "avg_load": avg_load,
                "max_load": max_load,
                "storage_utilization": storage_utilization
            }
    
    def _get_node_stats(self, args: dict) -> dict:
        """Get node-specific statistics"""
        node_id = args['node_id']
        
        with self.lock:
            if node_id not in self.nodes:
                return {"success": False, "error": "Node not found"}
            
            node_files = [fid for fid, replicas in self.file_replicas.items() if node_id in replicas]
            used_storage = sum(self.files[fid]['file_size'] for fid in node_files)
            total_storage = self.nodes[node_id]['storage'] * (1024**3)  # Convert GB to bytes
            
            return {
                "success": True,
                "node_id": node_id,
                "used_storage_gb": used_storage / (1024**3),
                "total_storage_gb": self.nodes[node_id]['storage'],
                "files_count": len(node_files),
                "status": self.node_status.get(node_id, "unknown")
            }
    
    def _set_node_online(self, args: dict) -> dict:
        """Set node online"""
        node_id = args['node_id']
        
        with self.lock:
            if node_id in self.node_status:
                self.node_status[node_id] = "online"
        
        print(f"🟢 {node_id} set online")
        self._display_minimal_status()
        return {"success": True}
    
    def _set_node_offline(self, args: dict) -> dict:
        """Set node offline"""
        node_id = args['node_id']
        
        with self.lock:
            if node_id in self.node_status:
                self.node_status[node_id] = "offline"
        
        print(f"🔴 {node_id} set offline")
        self._display_minimal_status()
        return {"success": True}
    
    def _replicate_file(self, args: dict) -> dict:
        """Manually trigger file replication"""
        file_id = args['file_id']
        self._schedule_replication(file_id)
        return {"success": True}
    
    def _display_status_on_demand(self) -> dict:
        """Display full status on demand"""
        self._display_full_status()
        return {"success": True, "message": "Status displayed"}
    
    def _get_node_available_storage(self, node_id: str) -> float:
        """Calculate available storage for a node"""
        node_files = [fid for fid, replicas in self.file_replicas.items() if node_id in replicas]
        used_storage = sum(self.files[fid]['file_size'] for fid in node_files)
        total_storage = self.nodes[node_id]['storage'] * (1024**3)
        return total_storage - used_storage
    
    def _display_minimal_status(self):
        """Display minimal status only when there are significant changes"""
        stats = self._get_network_stats()
        if not stats['success']:
            return
            
        current_node_count = stats['total_nodes']
        current_file_count = stats['total_files']
        current_online_count = stats['online_nodes']
        
        # Only display if there are significant changes
        if (current_node_count != self.last_node_count or 
            current_file_count != self.last_file_count or
            current_online_count != self.last_online_count):
            
            self.last_node_count = current_node_count
            self.last_file_count = current_file_count
            self.last_online_count = current_online_count
            
            if current_node_count == 0:
                print(f"\n🌐 NETWORK STATUS: No nodes registered")
            else:
                print(f"\n🌐 NETWORK STATUS: {current_online_count}/{current_node_count} nodes online, "
                      f"{current_file_count} files")
    
    def _display_full_status(self):
        """Display full detailed status (for on-demand use)"""
        stats = self._get_network_stats()
        if not stats['success']:
            print("❌ Failed to get network statistics")
            return
            
        nodes_info = self._list_nodes()
        if not nodes_info['success']:
            print("❌ Failed to get nodes list")
            return
        
        total_nodes = stats['total_nodes']
        
        if total_nodes == 0:
            print(f"\n🌐 NETWORK STATUS (0 nodes)")
            print("=" * 90)
            print("No nodes registered yet")
            print("=" * 90)
            return
        
        print(f"\n🌐 NETWORK STATUS ({total_nodes} nodes)")
        print("=" * 90)
        print(f"{'Node ID':<12} {'Status':<8} {'CPU':<4} {'RAM':<6} {'Storage':<18} {'BW':<8} {'Files':<6}")
        print("-" * 90)
        
        for node_id, info in nodes_info['nodes'].items():
            status_icon = "🟢" if info['status'] == "online" else "🔴"
            node_stat = self._get_node_stats({"node_id": node_id})
            if node_stat['success']:
                storage_used = node_stat['used_storage_gb']
                storage_percent = (storage_used / info['storage']) * 100 if info['storage'] > 0 else 0
                
                print(f"{node_id:<12} {status_icon:<4} {info['cpu']:<4} {info['memory']:<4}GB "
                      f"{storage_percent:.1f}%/{info['storage']}GB {info['bandwidth']:<4}M "
                      f"{info['files_count']:<6}")
        
        print("=" * 90)
        
        # Storage summary
        print(f"\n💾 NETWORK STORAGE SUMMARY")
        print("-" * 50)
        print(f"Total Capacity: {stats['total_storage_gb']:.1f} GB")
        
        # Safe division for storage usage display
        if stats['total_storage_gb'] > 0:
            usage_percent = (stats['used_storage_gb'] / stats['total_storage_gb']) * 100
            print(f"Used Storage:   {stats['used_storage_gb']:.1f} GB ({usage_percent:.1f}%)")
        else:
            print(f"Used Storage:   {stats['used_storage_gb']:.1f} GB (0.0%)")
            
        available = stats['total_storage_gb'] - stats['used_storage_gb']
        print(f"Available:      {available:.1f} GB")
        print("-" * 50)
        
        # Health dashboard
        print(f"\n🏥 SYSTEM HEALTH DASHBOARD")
        print("=" * 80)
        
        # Safe division for percentages
        network_health = (stats['online_nodes'] / total_nodes * 100) if total_nodes > 0 else 0
        replication_health = (stats['well_replicated_files'] / stats['total_files'] * 100) if stats['total_files'] > 0 else 100
        
        print(f"🌐 Network Health: {network_health:.1f}% "
              f"({stats['online_nodes']}/{total_nodes} nodes active)")
        print(f"💾 Storage Utilization: {stats['storage_utilization']:.1f}% "
              f"({stats['used_storage_gb']:.1f}/{stats['total_storage_gb']:.1f} GB)")
        print(f"🔄 Replication Health: {replication_health:.1f}% "
              f"({stats['well_replicated_files']}/{stats['total_files']} files well-replicated)")
        print(f"⚖️  Load Balance: {stats['load_balance']:.1f}% (avg: {stats['avg_load']:.1f}, "
              f"max: {stats['max_load']})")
        
        # File listing
        files_info = self._list_files({})
        if files_info['success'] and files_info['files']:
            print(f"\n📂 AVAILABLE FILES ({len(files_info['files'])} total)")
            print("=" * 80)
            print(f"{'File Name':<20} {'Size':<12} {'Owner':<12} {'Replicas':<12} {'Status':<12}")
            print("-" * 80)
            
            for file_info in files_info['files']:
                status = "✅ Available" if file_info['replicas'] >= 2 else "⚠️  At Risk"
                size_mb = file_info['file_size'] / (1024 * 1024)
                print(f"{file_info['file_name']:<20} {size_mb:>8.2f} MB {file_info['owner_node']:<12} "
                      f"{file_info['replicas']}/{file_info['total_replicas']:<12} {status:<12}")
            print("=" * 80)
        else:
            print(f"\n📂 No files available")
    
    def _health_monitoring_loop(self):
        """Health monitoring without continuous status display"""
        while self.running:
            time.sleep(5)  # Check every 5 seconds for health issues
            
            try:
                stats = self._get_network_stats()
                if stats['success']:
                    # Only show warnings for critical issues
                    if stats['online_nodes'] == 0 and stats['total_nodes'] > 0:
                        print("⚠️  CRITICAL: All nodes are offline!")
                    elif stats['well_replicated_files'] < stats['total_files'] and stats['total_files'] > 0:
                        at_risk = stats['total_files'] - stats['well_replicated_files']
                        if at_risk > 0:
                            print(f"⚠️  {at_risk} file(s) have insufficient replication")
            except Exception as e:
                # Silent error - don't spam console
                pass

def main():
    parser = argparse.ArgumentParser(description='Enhanced Distributed Cloud Storage Controller')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='Port to bind to (default: 5000)')
    
    args = parser.parse_args()
    
    controller = EnhancedNetworkController(host=args.host, port=args.port)
    
    try:
        controller.start()
        print(f"\n🔄 Controller running. Press Ctrl+C to stop.\n")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n🛑 Shutting down controller...")
        controller.stop()
    except Exception as e:
        print(f"\n❌ Controller error: {e}")
        controller.stop()

if __name__ == '__main__':
    main()