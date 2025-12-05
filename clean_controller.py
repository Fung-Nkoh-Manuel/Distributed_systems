#!/usr/bin/env python3
"""
Enhanced Clean Controller with data export for Flask UI
Now with persistent file and statistics storage
"""

import socket
import threading
import time
import pickle
import json
from typing import Dict, Any, List
from dataclasses import dataclass, asdict, field
import subprocess
import sys
import os
import signal

@dataclass
class NodeInfo:
    """Node information with resource tracking"""
    node_id: str
    host: str
    port: int
    cpu_cores: int
    memory_gb: int
    storage_gb: int
    bandwidth_mbps: int
    used_storage: int = 0
    active_transfers: int = 0
    last_seen: float = 0.0
    status: str = 'active'

    def get_available_storage(self) -> int:
        return (self.storage_gb * 1024**3) - self.used_storage

    def get_storage_usage_percent(self) -> float:
        total = self.storage_gb * 1024**3
        return (self.used_storage / total) * 100 if total > 0 else 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'node_id': self.node_id,
            'host': self.host,
            'port': self.port,
            'cpu_cores': self.cpu_cores,
            'memory_gb': self.memory_gb,
            'storage_gb': self.storage_gb,
            'bandwidth_mbps': self.bandwidth_mbps,
            'used_storage': self.used_storage,
            'active_transfers': self.active_transfers,
            'last_seen': self.last_seen,
            'status': self.status
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'NodeInfo':
        """Create from dictionary"""
        return NodeInfo(**data)


@dataclass
class FileInfo:
    """File information with replication tracking"""
    file_id: str
    file_name: str
    file_size: int
    owner_node: str
    replica_nodes: List[str] = field(default_factory=list)
    created_at: float = 0.0
    chunk_size: int = 1024 * 1024
    total_chunks: int = 0
    is_uploaded: bool = False

    def __post_init__(self):
        if self.total_chunks == 0:
            self.total_chunks = (self.file_size + self.chunk_size - 1) // self.chunk_size
        if not self.replica_nodes:
            self.replica_nodes = [self.owner_node]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'file_id': self.file_id,
            'file_name': self.file_name,
            'file_size': self.file_size,
            'owner_node': self.owner_node,
            'replica_nodes': self.replica_nodes,
            'created_at': self.created_at,
            'chunk_size': self.chunk_size,
            'total_chunks': self.total_chunks,
            'is_uploaded': self.is_uploaded
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'FileInfo':
        """Create from dictionary"""
        return FileInfo(**data)


class CleanController:
    """Enhanced distributed cloud storage controller with persistent storage"""

    def __init__(self, host: str = '0.0.0.0', port: int = 5000):
        self.host = host
        self.port = port

        self.nodes: Dict[str, NodeInfo] = {}
        self.files: Dict[str, FileInfo] = {}
        self.file_chunks: Dict[str, Dict[int, List[str]]] = {}

        self.running = False
        self.socket = None
        self.lock = threading.RLock()

        self.total_connections = 0
        self.active_connections = 0
        self.max_connections = 15
        self.total_transfers = 0
        self.successful_transfers = 0
        self.failed_transfers = 0

        self.transfer_history = []
        self.node_performance = {}
        self.bandwidth_utilization = {}

        self.default_replication_factor = 2
        self.min_nodes_for_replication = 2
        self.max_replicas_per_node = 5

        self.load_balancing_enabled = True
        self.prefer_local_replicas = True
        self.max_concurrent_transfers_per_node = 3

        # Track nodes that are manually forced offline and those deleted/blocked
        self.forced_offline = set()
        self.blocked_nodes = set()

        # Persistent storage paths
        self.storage_dir = "controller_storage"
        self.files_db_path = os.path.join(self.storage_dir, "files.json")
        self.nodes_db_path = os.path.join(self.storage_dir, "nodes.json")
        self.stats_db_path = os.path.join(self.storage_dir, "stats.json")
        
        # Create storage directory
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        
        # Load persistent data on startup
        self._load_persistent_files()
        self._load_persistent_stats()

    def _load_persistent_files(self):
        """Load files from persistent storage"""
        try:
            if os.path.exists(self.files_db_path):
                with open(self.files_db_path, 'r') as f:
                    files_data = json.load(f)
                    for file_id, file_dict in files_data.items():
                        self.files[file_id] = FileInfo.from_dict(file_dict)
                    print(f"📂 Loaded {len(self.files)} files from persistent storage")
            else:
                print("📂 No persistent files found, starting fresh")
        except Exception as e:
            print(f"⚠️  Error loading persistent files: {e}")

    def _load_persistent_stats(self):
        """Load statistics from persistent storage"""
        try:
            if os.path.exists(self.stats_db_path):
                with open(self.stats_db_path, 'r') as f:
                    stats_data = json.load(f)
                    self.total_transfers = stats_data.get('total_transfers', 0)
                    self.successful_transfers = stats_data.get('successful_transfers', 0)
                    self.failed_transfers = stats_data.get('failed_transfers', 0)
                    print(f"📊 Loaded statistics from persistent storage")
            else:
                print("📊 No persistent stats found, starting fresh")
        except Exception as e:
            print(f"⚠️  Error loading persistent stats: {e}")

    def _save_persistent_files(self):
        """Save files to persistent storage"""
        try:
            files_data = {}
            for file_id, file_info in self.files.items():
                files_data[file_id] = file_info.to_dict()
            
            with open(self.files_db_path, 'w') as f:
                json.dump(files_data, f, indent=2)
        except Exception as e:
            print(f"⚠️  Error saving persistent files: {e}")

    def _save_persistent_stats(self):
        """Save statistics to persistent storage"""
        try:
            stats_data = {
                'total_transfers': self.total_transfers,
                'successful_transfers': self.successful_transfers,
                'failed_transfers': self.failed_transfers,
                'last_saved': time.time()
            }
            
            with open(self.stats_db_path, 'w') as f:
                json.dump(stats_data, f, indent=2)
        except Exception as e:
            print(f"⚠️  Error saving persistent stats: {e}")

    def _spawn_initial_nodes(self, count: int):
        """Spawns CleanNode instances based on existing storage folders"""

        print(f"\n🔍 Looking for existing node storage folders...")

        existing_nodes = []

        # Look for folders matching node_storage_* pattern
        for item in os.listdir('.'):
            if os.path.isdir(item) and item.startswith('node_storage_'):
                node_id = item.replace('node_storage_', '')
                # Check if it's a valid node ID
                if node_id and len(node_id) > 0:
                    existing_nodes.append(node_id)

        if existing_nodes:
            print(f"📂 Found {len(existing_nodes)} existing node storage folders: {', '.join(existing_nodes)}")
            controller_connect_host = self.host if self.host != '0.0.0.0' else 'localhost'

            for node_id in existing_nodes:
                # Check if we already have a nodes.json or config file in the folder
                storage_dir = f"node_storage_{node_id}"
                config_file = os.path.join(storage_dir, 'node_config.json')

                if os.path.exists(config_file):
                    # Load config from file
                    try:
                        with open(config_file, 'r') as f:
                            config = json.load(f)
                            cpu = config.get('cpu', 4)
                            memory = config.get('memory', 16)
                            storage = config.get('storage', 1000)
                            bandwidth = config.get('bandwidth', 1000)
                    except Exception as e:
                        print(f"⚠️  Error loading config for {node_id}: {e}, using defaults")
                        cpu, memory, storage, bandwidth = 4, 16, 1000, 1000
                else:
                    # Use default values based on node ID
                    if node_id.startswith('N'):
                        try:
                            num = int(node_id[1:])
                            cpu = max(2, min(8, 2 + num))
                            memory = max(4, min(32, 4 + num * 4))
                            storage = max(500, min(2000, 500 + num * 300))
                            bandwidth = max(500, min(2000, 500 + num * 300))
                        except:
                            cpu, memory, storage, bandwidth = 4, 16, 1000, 1000
                    else:
                        cpu, memory, storage, bandwidth = 4, 16, 1000, 1000

                command = [
                    sys.executable, 'clean_node.py',
                    '--node-id', node_id,
                    '--cpu', str(cpu),
                    '--memory', str(memory),
                    '--storage', str(storage),
                    '--bandwidth', str(bandwidth),
                    '--controller-host', controller_connect_host,
                    '--controller-port', str(self.port)
                ]

                try:
                    subprocess.Popen(command)
                    print(f"   ➕ Node {node_id} spawned (CPU: {cpu}, RAM: {memory}GB, Storage: {storage}GB, BW: {bandwidth}Mbps)")
                    time.sleep(0.5)
                except Exception as e:
                    print(f"❌ Error spawning node {node_id}: {e}")
        else:
            print("📂 No existing node storage folders found.")
            print(f"🚀 Spawning {count} new CleanNode instances...")

            # Fallback to original default nodes if none exist
            node_configs = [
                {'node_id': 'N1', 'cpu': 4, 'memory': 16, 'storage': 1000, 'bandwidth': 1000},
                {'node_id': 'N2', 'cpu': 2, 'memory': 8, 'storage': 500, 'bandwidth': 500},
                {'node_id': 'N3', 'cpu': 6, 'memory': 32, 'storage': 2000, 'bandwidth': 2000},
                {'node_id': 'N4', 'cpu': 4, 'memory': 16, 'storage': 1500, 'bandwidth': 1500},
                {'node_id': 'N5', 'cpu': 2, 'memory': 4, 'storage': 250, 'bandwidth': 250},
            ]

            controller_connect_host = self.host if self.host != '0.0.0.0' else 'localhost'

            for config in node_configs[:count]:
                node_id = config['node_id']
                command = [
                    sys.executable, 'clean_node.py',
                    '--node-id', node_id,
                    '--cpu', str(config['cpu']),
                    '--memory', str(config['memory']),
                    '--storage', str(config['storage']),
                    '--bandwidth', str(config['bandwidth']),
                    '--controller-host', controller_connect_host,
                    '--controller-port', str(self.port)
                ]

                try:
                    subprocess.Popen(command)
                    print(f"   ➕ Node {node_id} spawned")
                    time.sleep(1.0)
                except Exception as e:
                    print(f"❌ Error spawning node {node_id}: {e}")

    def start(self):
        """Start the controller"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(20)
            self.socket.settimeout(1.0)
            
            self.running = True
            print(f"🌐 Clean Controller started on {self.host}:{self.port}")
            
            heartbeat_thread = threading.Thread(target=self._heartbeat_checker, daemon=True)
            heartbeat_thread.start()
            
            # Auto-save thread
            save_thread = threading.Thread(target=self._auto_save, daemon=True)
            save_thread.start()
            
            self._spawn_initial_nodes(5)
            
            while self.running:
                try:
                    conn, addr = self.socket.accept()
                    
                    if self.active_connections < self.max_connections:
                        self.active_connections += 1
                        thread = threading.Thread(
                            target=self._handle_connection,
                            args=(conn, addr),
                            daemon=True
                        )
                        thread.start()
                    else:
                        conn.close()
                        
                except socket.timeout:
                    continue
                except Exception as e:
                    if self.running:
                        print(f"⚠️  Accept error: {e}")
                    
        except Exception as e:
            print(f"❌ Controller start failed: {e}")
        finally:
            if self.socket:
                self.socket.close()
    
    def _auto_save(self):
        """Automatically save data every 10 seconds"""
        while self.running:
            time.sleep(10)
            with self.lock:
                self._save_persistent_files()
                self._save_persistent_stats()
    
    def _handle_connection(self, conn, addr):
        """Handle client connection"""
        try:
            conn.settimeout(10)
            data = conn.recv(8192)
            if not data:
                return
            
            message = pickle.loads(data)
            response = self._process_message(message)
            conn.sendall(pickle.dumps(response))
            
        except Exception as e:
            print(f"⚠️  Connection error from {addr}: {e}")
        finally:
            try:
                conn.close()
            except:
                pass
            self.active_connections -= 1
    
    def _process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming message"""
        action = message.get('action', '')
        
        with self.lock:
            if action == 'REGISTER':
                return self._handle_register(message)
            elif action == 'HEARTBEAT':
                return self._handle_heartbeat(message)
            elif action == 'FILE_CREATED':
                return self._handle_file_created(message)
            elif action == 'STATUS_REQUEST':
                self._display_network_status()
                return {'status': 'OK', 'message': 'Status refresh triggered.'}
            elif action == 'FORCE_OFFLINE':
                return self._handle_force_offline(message)
            elif action == 'LIST_FILES':
                return self._handle_list_files(message)
            elif action == 'DOWNLOAD_REQUEST':
                return self._handle_download_request(message)
            elif action == 'UPLOAD_REQUEST':
                return self._handle_upload_request(message)
            elif action == 'TRANSFER_COMPLETE':
                return self._handle_transfer_complete(message)
            elif action == 'GET_NODES':
                return self._handle_get_nodes(message)
            elif action == 'GET_STATS':
                return self._handle_get_stats(message)
            elif action == 'DELETE_NODE':
                return self._handle_delete_node(message)
            elif action == 'UNBLOCK_NODE':
                return self._handle_unblock_node(message)
            elif action == 'DELETE_FILE':
                return self._handle_delete_file(message)
            else:
                return {'status': 'ERROR', 'error': f'Unknown action: {action}'}

    def _handle_get_nodes(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Return all nodes data"""
        nodes_data = []
        for node_id, node in self.nodes.items():
            nodes_data.append({
                'id': node.node_id,
                'cpu': node.cpu_cores,
                'memory': node.memory_gb,
                'storage': node.storage_gb,
                'bandwidth': node.bandwidth_mbps,
                'status': node.status,
                'used_storage': node.used_storage,
                'active_transfers': node.active_transfers,
                'storage_pct': round(node.get_storage_usage_percent(), 2)  # Changed to 2 decimal places
            })
        return {'status': 'OK', 'nodes': nodes_data}

    def _handle_get_stats(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Return network statistics"""
        active_nodes = sum(1 for n in self.nodes.values() if n.status == 'active')
        total_storage = sum(n.storage_gb for n in self.nodes.values())
        used_storage = sum(n.used_storage for n in self.nodes.values())
        storage_util = (used_storage / (total_storage * 1024**3) * 100) if total_storage > 0 else 0
        total_transfers = sum(n.active_transfers for n in self.nodes.values())

        return {
            'status': 'OK',
            'active_nodes': active_nodes,
            'total_nodes': len(self.nodes),
            'storage_utilization': round(storage_util, 2),  # Changed to 2 decimal places
            'used_storage': used_storage,
            'total_storage': total_storage * 1024**3,
            'total_transfers': total_transfers,
            'total_files': len(self.files),
            'successful_transfers': self.successful_transfers,
            'failed_transfers': self.failed_transfers
        }
                
    def _handle_force_offline(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle force offline request (toggle)"""
        node_id = message.get('node_id')
        if node_id in self.nodes:
            if node_id in self.forced_offline or self.nodes[node_id].status != 'active':
                self.forced_offline.discard(node_id)
                self.nodes[node_id].status = 'active'
                self.nodes[node_id].last_seen = time.time()
                print(f"✅ {node_id} manually set to online status.")
                self._display_network_status()
                return {'status': 'OK', 'message': f'Node {node_id} is now online.'}
            else:
                self.forced_offline.add(node_id)
                self.nodes[node_id].status = 'inactive'
                self.nodes[node_id].last_seen = 0
                print(f"⚠️  {node_id} manually set to offline status (forced).")
                self._handle_node_failures([node_id])
                self._display_network_status()
                return {'status': 'OK', 'message': f'Node {node_id} is now offline.'}
        return {'status': 'ERROR', 'error': f'Node {node_id} not found.'}
    
    def _handle_register(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle node registration"""
        try:
            node_id = message['node_id']
            host = message.get('host', 'localhost')
            port = message.get('port', 0)
            resources = message.get('resources', {})

            required_fields = ['cpu_cores', 'memory_gb', 'storage_gb', 'bandwidth_mbps']
            for field in required_fields:
                if field not in resources:
                    return {'status': 'ERROR', 'error': f'Missing required resource: {field}'}

            # Prevent re-registration of blocked/deleted nodes
            if node_id in self.blocked_nodes:
                return {'status': 'ERROR', 'error': f'Node {node_id} is blocked from registering'}

            # Update used_storage from node's actual files
            used_storage = 0
            for file_info in self.files.values():
                if node_id in file_info.replica_nodes:
                    used_storage += file_info.file_size

            self.nodes[node_id] = NodeInfo(
                node_id=node_id,
                host=host,
                port=port,
                cpu_cores=resources['cpu_cores'],
                memory_gb=resources['memory_gb'],
                storage_gb=resources['storage_gb'],
                bandwidth_mbps=resources['bandwidth_mbps'],
                used_storage=used_storage,
                last_seen=time.time(),
                status='active'
            )

            print(f"📡 {node_id} connected")
            print(f"✅ {node_id} online (CPU: {resources['cpu_cores']}, RAM: {resources['memory_gb']}GB, Storage: {resources['storage_gb']}GB, BW: {resources['bandwidth_mbps']}Mbps)")
            self._display_network_status()

            return {'status': 'OK', 'message': 'Registration successful'}

        except Exception as e:
            return {'status': 'ERROR', 'error': f'Registration failed: {e}'}
    
    def _handle_heartbeat(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle heartbeat and update node metrics"""
        try:
            node_id = message['node_id']

            if node_id in self.nodes:
                if node_id in self.forced_offline:
                    return {'status': 'ACK'}

                node = self.nodes[node_id]
                node.last_seen = time.time()

                hb_used = message.get('used_storage')
                if isinstance(hb_used, (int, float)):
                    node.used_storage = max(0, int(hb_used))

                hb_transfers = message.get('active_transfers')
                if isinstance(hb_transfers, (int, float)):
                    node.active_transfers = max(0, int(hb_transfers))

                if node.status == 'inactive':
                    node.status = 'active'
                    print(f"🟢 {node_id} came back online.")
                    self._display_network_status()

                return {'status': 'ACK'}
            else:
                return {'status': 'ERROR', 'error': 'Node not registered'}

        except Exception as e:
            return {'status': 'ERROR', 'error': f'Heartbeat failed: {e}'}
    
    def _handle_file_created(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file creation"""
        try:
            node_id = message['node_id']
            file_info = message['file_info']

            file_record = FileInfo(
                file_id=file_info['file_id'],
                file_name=file_info['file_name'],
                file_size=file_info['file_size'],
                owner_node=file_info['owner_node'],
                replica_nodes=[file_info['owner_node']],
                created_at=time.time()
            )

            self.files[file_record.file_id] = file_record

            if node_id in self.nodes:
                self.nodes[node_id].used_storage += file_record.file_size

            size_mb = file_record.file_size / (1024 * 1024)
            print(f"📄 {file_record.file_name} ({size_mb:.2f} MB) created on {file_record.owner_node}")

            self._schedule_file_upload(file_record)
            self._display_network_status()
            
            # Save immediately after file creation
            self._save_persistent_files()

            actual_replicas = [n for n in file_record.replica_nodes if n in self.nodes]
            return {
                'status': 'ACK',
                'message': 'File registered and upload scheduled',
                'replica_nodes': actual_replicas,
                'chunk_size': file_record.chunk_size,
                'total_chunks': file_record.total_chunks
            }

        except Exception as e:
            return {'status': 'ERROR', 'error': f'File creation failed: {e}'}
    
    def _handle_list_files(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file list request"""
        try:
            files_data = []
            for file_info in self.files.values():
                online_replicas = [node for node in file_info.replica_nodes
                                 if node in self.nodes and self.nodes[node].status == 'active']

                if online_replicas and file_info.is_uploaded:
                    file_data = {
                        'file_id': file_info.file_id,
                        'file_name': file_info.file_name,
                        'file_size': file_info.file_size,
                        'owner_node': file_info.owner_node,
                        'replica_count': len(online_replicas),
                        'total_chunks': file_info.total_chunks,
                        'chunk_size': file_info.chunk_size,
                        'created_at': file_info.created_at
                    }
                    files_data.append(file_data)

            return {'status': 'OK', 'files': files_data, 'total_files': len(files_data)}

        except Exception as e:
            return {'status': 'ERROR', 'error': f'List files failed: {e}'}

    def _handle_download_request(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file download request"""
        try:
            requesting_node = message['node_id']
            file_id = message['file_id']

            if file_id not in self.files:
                return {'status': 'ERROR', 'error': 'File not found'}

            file_info = self.files[file_id]
            online_replicas = [(node, self.nodes[node]) for node in file_info.replica_nodes
                             if node in self.nodes and self.nodes[node].status == 'active']

            if not online_replicas:
                return {'status': 'ERROR', 'error': 'No online replicas available'}

            source_node_id, source_node = self._select_best_source_node(online_replicas, requesting_node)
            source_bw = source_node.bandwidth_mbps
            dest_bw = self.nodes[requesting_node].bandwidth_mbps if requesting_node in self.nodes else 100
            effective_bw = min(source_bw, dest_bw)

            source_node.active_transfers += 1
            if requesting_node in self.nodes:
                self.nodes[requesting_node].active_transfers += 1

            print(f"📥 {requesting_node} downloading {file_info.file_name} from {source_node_id}")

            return {
                'status': 'OK',
                'source_node': source_node_id,
                'source_host': source_node.host,
                'file_info': {
                    'file_id': file_info.file_id,
                    'file_name': file_info.file_name,
                    'file_size': file_info.file_size,
                    'chunk_size': file_info.chunk_size,
                    'total_chunks': file_info.total_chunks
                },
                'transfer_params': {
                    'bandwidth_mbps': effective_bw,
                    'estimated_time': file_info.file_size / (effective_bw * 1024 * 1024 / 8)
                }
            }

        except Exception as e:
            return {'status': 'ERROR', 'error': f'Download request failed: {e}'}

    def _handle_upload_request(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle file upload request"""
        try:
            return {'status': 'OK', 'message': 'Upload coordinated'}
        except Exception as e:
            return {'status': 'ERROR', 'error': f'Upload request failed: {e}'}

    def _handle_transfer_complete(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Handle transfer completion"""
        try:
            node_id = message['node_id']
            file_id = message.get('file_id')
            transfer_type = message.get('transfer_type', 'unknown')

            if node_id in self.nodes:
                self.nodes[node_id].active_transfers = max(0, self.nodes[node_id].active_transfers - 1)

            if transfer_type == 'download' and file_id in self.files:
                file_info = self.files[file_id]
                if node_id in self.nodes:
                    self.nodes[node_id].used_storage += file_info.file_size
                    if node_id not in file_info.replica_nodes:
                        file_info.replica_nodes.append(node_id)

                print(f"✅ {node_id} completed download of {file_info.file_name}")
                self._display_network_status()
                
                # Save after transfer completion
                self._save_persistent_files()

            return {'status': 'OK', 'message': 'Transfer completion recorded'}

        except Exception as e:
            return {'status': 'ERROR', 'error': f'Transfer completion failed: {e}'}
    
    def _display_files(self):
        """Display current file list"""
        if not self.files:
            print("📂 No files available")
            return

        print(f"\n📂 AVAILABLE FILES ({len(self.files)} total)")
        print("=" * 80)

        for file_info in self.files.values():
            size_mb = file_info.file_size / (1024 * 1024)
            replica_count = len(file_info.replica_nodes)
            status = "✅ Available" if file_info.is_uploaded else "⏳ Uploading"

            online_replicas = [node for node in file_info.replica_nodes
                             if node in self.nodes and self.nodes[node].status == 'active']
            replica_str = f"{len(online_replicas)}/{replica_count}"

            print(f"{file_info.file_name:<25} {size_mb:>8.2f} MB {file_info.owner_node:<12} {replica_str:<15} {status:<10}")

        print("=" * 80)

    def _display_network_status(self):
        """Display network and node status"""
        if not self.nodes:
            return

        print(f"\n🌐 NETWORK STATUS ({len(self.nodes)} nodes)")
        print("=" * 100)
        print(f"{'Node':<12} {'Status':<8} {'CPU':<5} {'RAM':<5} {'Storage':<18} {'Bandwidth':<8} {'Files':<6}")
        print("=" * 100)

        for node in self.nodes.values():
            status_icon = "🟢" if node.status == 'active' else "🔴"
            storage_used = node.get_storage_usage_percent()
            storage_str = f"{storage_used:>6.2f}%/{node.storage_gb}GB"
            file_count = sum(1 for f in self.files.values() if node.node_id in f.replica_nodes)

            print(f"{node.node_id:<12} {status_icon:<8} {node.cpu_cores:<5} {node.memory_gb:<4}GB {storage_str:<18} {node.bandwidth_mbps:<8}M {file_count:<6}")

        print("=" * 100)
        self._display_files()

    def _schedule_file_upload(self, file_info: FileInfo):
        """Schedule file upload"""
        try:
            file_info.is_uploaded = True

            if len(self.nodes) >= self.min_nodes_for_replication:
                replica_nodes = self._select_replica_nodes(file_info.owner_node, self.default_replication_factor)
                file_info.replica_nodes.extend(replica_nodes)
                file_info.replica_nodes = list(set(file_info.replica_nodes))

                if replica_nodes:
                    print(f"📄 Scheduling replication of {file_info.file_name} to: {', '.join(replica_nodes)}")
                    for n in replica_nodes:
                        if n in self.nodes:
                            self.nodes[n].used_storage += file_info.file_size

        except Exception as e:
            print(f"⚠️  Upload scheduling failed: {e}")

    def _select_replica_nodes(self, owner_node: str, replication_factor: int) -> List[str]:
        """Select replica nodes"""
        available_nodes = []

        for node_id, node in self.nodes.items():
            if (node_id != owner_node and
                node.status == 'active' and
                node.get_available_storage() > 0):

                available_nodes.append((node_id, node.get_available_storage()))

        available_nodes.sort(key=lambda x: -x[1])
        selected = [node[0] for node in available_nodes[:replication_factor-1]]
        return selected

    def _get_node_performance_score(self, node_id: str) -> float:
        """Get performance score"""
        if node_id not in self.node_performance:
            return 0.5

        perf_data = self.node_performance[node_id]
        success_rate = perf_data.get('success_rate', 0.5)
        avg_speed = perf_data.get('avg_speed_mbps', 100) / 1000

        return (success_rate * 0.7) + (min(avg_speed, 1.0) * 0.3)

    def _select_best_source_node(self, online_replicas: List, requesting_node: str):
        """Select best source node"""
        if not online_replicas:
            return online_replicas[0]
        return min(online_replicas, key=lambda x: x[1].active_transfers)

    def _heartbeat_checker(self):
        """Check node heartbeats"""
        while self.running:
            try:
                current_time = time.time()
                timeout = 30

                with self.lock:
                    nodes_went_offline = []

                    for node_id, node_info in list(self.nodes.items()):
                        if current_time - node_info.last_seen > timeout:
                            if node_info.status == 'active':
                                node_info.status = 'inactive'
                                nodes_went_offline.append(node_id)
                                print(f"⚠️  {node_id} went offline")

                    if nodes_went_offline:
                        self._handle_node_failures(nodes_went_offline)
                        self._display_network_status()

                time.sleep(10)

            except Exception as e:
                print(f"⚠️  Heartbeat checker error: {e}")

    def _handle_node_failures(self, failed_nodes: List[str]):
        """Handle node failures"""
        for node_id in failed_nodes:
            print(f"📄 Handling failure of node {node_id}")

    def _schedule_re_replication(self, file_info: FileInfo):
        """Schedule re-replication"""
        try:
            online_replicas = [n for n in file_info.replica_nodes
                             if n in self.nodes and self.nodes[n].status == 'active']

            if not online_replicas:
                print(f"❌ No online replicas for {file_info.file_name}")
                return

        except Exception as e:
            print(f"⚠️  Re-replication failed: {e}")

    def _handle_delete_node(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Delete a node from controller and block future registrations"""
        node_id = message.get('node_id')
        if not node_id:
            return {'status': 'ERROR', 'error': 'node_id required'}

        if node_id in self.nodes:
            self.forced_offline.add(node_id)
            del self.nodes[node_id]
            self.blocked_nodes.add(node_id)
            print(f"🗑️  Node {node_id} deleted and blocked from re-registering.")
            self._display_network_status()
            return {'status': 'OK', 'message': f'Node {node_id} deleted'}
        else:
            self.blocked_nodes.add(node_id)
            print(f"🗑️  Node {node_id} not found; added to block list.")
            return {'status': 'OK', 'message': f'Node {node_id} deleted'}

    def _handle_unblock_node(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Allow a previously deleted/blocked node to register again"""
        node_id = message.get('node_id')
        if not node_id:
            return {'status': 'ERROR', 'error': 'node_id required'}
        removed_block = node_id in self.blocked_nodes
        self.blocked_nodes.discard(node_id)
        self.forced_offline.discard(node_id)
        return {'status': 'OK', 'message': f'Unblocked {node_id}', 'was_blocked': removed_block}

    def _handle_delete_file(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Delete a file from controller's persistent storage"""
        file_id = message.get('file_id')
        if not file_id:
            return {'status': 'ERROR', 'error': 'file_id required'}
        
        try:
            if file_id in self.files:
                file_info = self.files[file_id]
                owner_node = file_info.owner_node
                file_size = file_info.file_size
                
                # Remove file from nodes' used storage
                for node_id in file_info.replica_nodes:
                    if node_id in self.nodes:
                        self.nodes[node_id].used_storage = max(0, self.nodes[node_id].used_storage - file_size)
                
                # Delete from controller's in-memory storage
                del self.files[file_id]
                
                # Save to persistent storage immediately
                self._save_persistent_files()
                
                print(f"🗑️  File {file_id} deleted from controller")
                self._display_network_status()
                
                return {'status': 'OK', 'message': f'File {file_id} deleted from controller'}
            else:
                return {'status': 'ERROR', 'error': f'File {file_id} not found in controller'}
        except Exception as e:
            return {'status': 'ERROR', 'error': f'Delete file failed: {str(e)}'}

    def stop(self):
        """Stop the controller"""
        self.running = False
        self._save_persistent_files()
        self._save_persistent_stats()
        if self.socket:
            self.socket.close()
        print("🛑 Controller stopped")


def main():
    """Main function"""
    print("🚀 ENHANCED DISTRIBUTED CLOUD STORAGE CONTROLLER")
    print("=" * 60)
    print("✅ Single communication protocol")
    print("✅ Automatic file replication")
    print("✅ Resource-aware node management")
    print("✅ Real-time network monitoring")
    print("✅ Persistent storage with auto-save")
    print("=" * 60)
    
    controller = CleanController()
    
    try:
        controller.start()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        controller.stop()


if __name__ == "__main__":
    main()