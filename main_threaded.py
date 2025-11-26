#!/usr/bin/env python3
"""
Enhanced Distributed Storage Network Client
"""

import socket
import json
import time
import argparse
import os
import sys
from typing import Dict, List

class EnhancedNetworkClient:
    """Enhanced client for distributed storage network"""
    
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        
    def _send_request(self, command: str, args: dict = None) -> dict:
        """Send request to network controller"""
        if args is None:
            args = {}
            
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            sock.connect((self.host, self.port))
            
            request = {"command": command, "args": args}
            sock.sendall(json.dumps(request).encode('utf-8'))
            
            data = sock.recv(65536)  # Larger buffer for detailed responses
            response = json.loads(data.decode('utf-8'))
            
            sock.close()
            return response
            
        except Exception as e:
            return {"success": False, "error": f"Network error: {str(e)}"}
    
    def list_nodes(self) -> dict:
        """List all nodes"""
        return self._send_request("list_nodes")
    
    def create_file(self, node_id: str, file_name: str, size_mb: float) -> dict:
        """Create a file on a node"""
        return self._send_request("create_file", {
            "node_id": node_id,
            "file_name": file_name,
            "file_size": int(size_mb * 1024 * 1024)  # Convert to bytes
        })
    
    def delete_file(self, file_id: str) -> dict:
        """Delete a file"""
        return self._send_request("delete_file", {
            "file_id": file_id
        })
    
    def list_files(self, node_id: str = None) -> dict:
        """List files (optionally for a specific node)"""
        args = {}
        if node_id:
            args["node_id"] = node_id
        return self._send_request("list_files", args)
    
    def transfer_file(self, source_node: str, target_node: str, file_name: str) -> dict:
        """Transfer file between nodes"""
        return self._send_request("transfer_file", {
            "source_node": source_node,
            "target_node": target_node,
            "file_name": file_name
        })
    
    def download_file(self, target_node: str, file_name: str, source_node: str) -> dict:
        """Download file to a node"""
        return self._send_request("download_file", {
            "target_node": target_node,
            "file_name": file_name,
            "source_node": source_node
        })
    
    def network_stats(self) -> dict:
        """Get network statistics"""
        return self._send_request("network_stats")
    
    def node_stats(self, node_id: str) -> dict:
        """Get node statistics"""
        return self._send_request("node_stats", {
            "node_id": node_id
        })
    
    def set_node_online(self, node_id: str) -> dict:
        """Set node online"""
        return self._send_request("set_node_online", {
            "node_id": node_id
        })
    
    def set_node_offline(self, node_id: str) -> dict:
        """Set node offline"""
        return self._send_request("set_node_offline", {
            "node_id": node_id
        })

def display_network_status(client: EnhancedNetworkClient):
    """Display beautiful network status"""
    stats = client.network_stats()
    if not stats.get('success'):
        print("❌ Failed to get network status")
        return
    
    nodes = client.list_nodes()
    if not nodes.get('success'):
        print("❌ Failed to get nodes list")
        return
    
    print(f"\n🌐 NETWORK STATUS ({stats['total_nodes']} nodes)")
    print("=" * 90)
    print(f"{'Node ID':<12} {'Status':<8} {'CPU':<4} {'RAM':<6} {'Storage':<18} {'BW':<8} {'Files':<6}")
    print("-" * 90)
    
    for node_id, info in nodes['nodes'].items():
        status_icon = "🟢" if info['status'] == "online" else "🔴"
        node_stat = client.node_stats(node_id)
        if node_stat['success']:
            storage_used = node_stat['used_storage_gb']
            storage_percent = (storage_used / info['storage']) * 100
            
            print(f"{node_id:<12} {status_icon:<4} {info['cpu']:<4} {info['memory']:<4}GB "
                  f"{storage_percent:.1f}%/{info['storage']}GB {info['bandwidth']:<4}M "
                  f"{info['files_count']:<6}")
    
    print("=" * 90)
    
    # Storage summary
    print(f"\n💾 NETWORK STORAGE SUMMARY")
    print("-" * 50)
    print(f"Total Capacity: {stats['total_storage_gb']:.1f} GB")
    print(f"Used Storage:   {stats['used_storage_gb']:.1f} GB ({stats['used_storage_gb']/stats['total_storage_gb']*100:.1f}%)")
    print(f"Available:      {stats['total_storage_gb'] - stats['used_storage_gb']:.1f} GB")
    print("-" * 50)
    
    # Health dashboard
    print(f"\n🏥 SYSTEM HEALTH DASHBOARD")
    print("=" * 80)
    print(f"🌐 Network Health: {stats['online_nodes']/stats['total_nodes']*100:.1f}% "
          f"({stats['online_nodes']}/{stats['total_nodes']} nodes active)")
    print(f"💾 Storage Utilization: {stats['used_storage_gb']/stats['total_storage_gb']*100:.1f}% "
          f"({stats['used_storage_gb']:.1f}/{stats['total_storage_gb']:.1f} GB)")
    print(f"🔄 Replication Health: {stats['well_replicated_files']/stats['total_files']*100:.1f}% "
          f"({stats['well_replicated_files']}/{stats['total_files']} files well-replicated)")
    print(f"⚖️  Load Balance: {stats['load_balance']:.1f}% (avg: {stats['avg_load']:.1f}, "
          f"max: {stats['max_load']})")
    
    # File listing
    files = client.list_files()
    if files.get('success') and files['files']:
        print(f"\n📂 AVAILABLE FILES ({len(files['files'])} total)")
        print("=" * 80)
        print(f"{'File Name':<20} {'Size':<12} {'Owner':<12} {'Replicas':<12} {'Status':<12}")
        print("-" * 80)
        
        for file_info in files['files']:
            status = "✅ Available" if file_info['replicas'] >= 2 else "⚠️  At Risk"
            size_mb = file_info['file_size'] / (1024 * 1024)
            print(f"{file_info['file_name']:<20} {size_mb:>8.2f} MB {file_info['owner_node']:<12} "
                  f"{file_info['replicas']}/3{'':<9} {status:<12}")
        print("=" * 80)

def interactive_menu():
    """Interactive command menu"""
    client = EnhancedNetworkClient()
    
    while True:
        print(f"\n🎯 DISTRIBUTED STORAGE NETWORK - COMMAND MENU")
        print("=" * 50)
        print("1. 📊 Network Status")
        print("2. 🖥️  List Nodes")
        print("3. 📁 Create File")
        print("4. 🗑️  Delete File")
        print("5. 📋 List Files")
        print("6. 📤 Transfer File")
        print("7. 📥 Download File")
        print("8. 🟢 Set Node Online")
        print("9. 🔴 Set Node Offline")
        print("0. 🚪 Exit")
        print("-" * 50)
        
        choice = input("Choose option (0-9): ").strip()
        
        try:
            if choice == '1':
                display_network_status(client)
                
            elif choice == '2':
                result = client.list_nodes()
                if result['success']:
                    print(f"\n🖥️  REGISTERED NODES ({len(result['nodes'])} total)")
                    print("=" * 60)
                    for node_id, info in result['nodes'].items():
                        status_icon = "🟢" if info['status'] == "online" else "🔴"
                        print(f"{status_icon} {node_id}: CPU{info['cpu']} RAM{info['memory']}GB "
                              f"Storage{info['storage']}GB BW{info['bandwidth']}Mbps "
                              f"Files:{info['files_count']}")
                else:
                    print("❌ Failed to list nodes")
                    
            elif choice == '3':
                node_id = input("Node ID: ").strip()
                file_name = input("File name: ").strip()
                size_mb = float(input("Size (MB): ").strip())
                
                result = client.create_file(node_id, file_name, size_mb)
                if result['success']:
                    print(f"✅ File {file_name} created on {node_id}")
                else:
                    print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                    
            elif choice == '4':
                files = client.list_files()
                if files['success'] and files['files']:
                    print("\n📂 Available files:")
                    for i, file_info in enumerate(files['files']):
                        print(f"{i+1}. {file_info['file_name']} (ID: {file_info['file_id']})")
                    
                    file_idx = int(input("File number to delete: ")) - 1
                    if 0 <= file_idx < len(files['files']):
                        file_id = files['files'][file_idx]['file_id']
                        result = client.delete_file(file_id)
                        if result['success']:
                            print("✅ File deleted")
                        else:
                            print(f"❌ Failed: {result.get('error')}")
                else:
                    print("❌ No files available")
                    
            elif choice == '5':
                node_id = input("Node ID (optional, press enter for all): ").strip()
                node_id = node_id if node_id else None
                
                result = client.list_files(node_id)
                if result['success']:
                    files = result['files']
                    if files:
                        scope = f"on {node_id}" if node_id else "in network"
                        print(f"\n📂 FILES {scope} ({len(files)} total)")
                        print("=" * 60)
                        for file_info in files:
                            size_mb = file_info['file_size'] / (1024 * 1024)
                            print(f"📄 {file_info['file_name']} ({size_mb:.2f}MB) "
                                  f"Owner: {file_info['owner_node']} "
                                  f"Replicas: {file_info['replicas']}/3")
                    else:
                        print("📂 No files found")
                else:
                    print("❌ Failed to list files")
                    
            elif choice == '6':
                source = input("Source node: ").strip()
                target = input("Target node: ").strip()
                file_name = input("File name: ").strip()
                
                result = client.transfer_file(source, target, file_name)
                if result['success']:
                    print(f"✅ Transfer initiated: {source} → {target}")
                else:
                    print(f"❌ Transfer failed: {result.get('error')}")
                    
            elif choice == '7':
                target = input("Target node: ").strip()
                file_name = input("File name: ").strip()
                source = input("Source node (optional): ").strip()
                
                result = client.download_file(target, file_name, source)
                if result['success']:
                    print(f"✅ Download initiated to {target}")
                else:
                    print(f"❌ Download failed: {result.get('error')}")
                    
            elif choice == '8':
                node_id = input("Node ID to set online: ").strip()
                result = client.set_node_online(node_id)
                if result['success']:
                    print(f"🟢 {node_id} set online")
                else:
                    print(f"❌ Failed: {result.get('error')}")
                    
            elif choice == '9':
                node_id = input("Node ID to set offline: ").strip()
                result = client.set_node_offline(node_id)
                if result['success']:
                    print(f"🔴 {node_id} set offline")
                else:
                    print(f"❌ Failed: {result.get('error')}")
                    
            elif choice == '0':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice")
                
        except Exception as e:
            print(f"❌ Error: {e}")
        
        input("\nPress Enter to continue...")

def main():
    parser = argparse.ArgumentParser(description='Enhanced Distributed Storage Network Client')
    parser.add_argument('--host', default='localhost', help='Network controller host')
    parser.add_argument('--port', type=int, default=5000, help='Network controller port')
    parser.add_argument('--interactive', action='store_true', help='Start interactive mode')
    
    args = parser.parse_args()
    
    if args.interactive:
        interactive_menu()
    else:
        # Quick status check
        client = EnhancedNetworkClient(args.host, args.port)
        display_network_status(client)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")