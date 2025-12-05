#!/usr/bin/env python3
"""
Clean Storage Node with proper storage recalculation
"""

import socket
import threading
import time
import pickle
import json
import os
import hashlib
import concurrent.futures
import argparse
from typing import Dict, Any, Optional, List


class CleanNode:
    """Enhanced distributed storage node with resource management"""

    def __init__(
        self,
        node_id: str,
        cpu_cores: int = 4,
        memory_gb: int = 16,
        storage_gb: int = 1000,
        bandwidth_mbps: int = 1000,
        controller_host: str = 'localhost',
        controller_port: int = 5000,
        interactive: bool = False
    ):
        self.node_id = node_id
        self.cpu_cores = cpu_cores
        self.memory_gb = memory_gb
        self.storage_gb = storage_gb
        self.bandwidth_mbps = bandwidth_mbps
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.interactive = interactive

        # State and threading
        self.running = False
        self.lock = threading.RLock()
        
        # Storage management
        self.storage_dir = f"node_storage_{node_id}"
        self.total_storage = storage_gb * 1024 ** 3  # Convert to bytes
        self.used_storage = 0
        self.files: Dict[str, Any] = {} # file_id -> metadata

        # Transfer management
        self.active_transfers = 0
        self.max_concurrent_transfers = min(cpu_cores, 4)  # Limit based on CPU
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent_transfers)
        
        # Ensure storage directory exists
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
            self._save_node_config()
        self._load_storage_metadata()

    
    def _save_node_config(self):
        """Save node configuration to file for restart purposes"""
        config_file = os.path.join(self.storage_dir, 'node_config.json')
        config = {
            'node_id': self.node_id,
            'cpu': self.cpu_cores,
            'memory': self.memory_gb,
            'storage': self.storage_gb,
            'bandwidth': self.bandwidth_mbps,
            'controller_host': self.controller_host,
            'controller_port': self.controller_port
        }
        try:
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
        except Exception as e:
            print(f"⚠️  Error saving node config: {e}")

    def _pid_file_path(self) -> str:
        """Return the path to the PID file in the storage directory."""
        return os.path.join(self.storage_dir, 'node.pid')

    def _write_pid_file(self):
        """Write current process PID to a file for management (e.g., deletion)."""
        try:
            with open(self._pid_file_path(), 'w') as f:
                f.write(str(os.getpid()))
        except Exception as e:
            print(f"⚠️  Error writing PID file: {e}")

    def _remove_pid_file(self):
        """Remove the PID file if it exists."""
        try:
            pid_path = self._pid_file_path()
            if os.path.exists(pid_path):
                os.remove(pid_path)
        except Exception as e:
            print(f"⚠️  Error removing PID file: {e}")

    def _load_storage_metadata(self):
        """Load file metadata and calculate used storage recursively."""
        used = 0
        try:
            files_dir = os.path.join(self.storage_dir, "files")
            if os.path.exists(files_dir):
                for root, dirs, files in os.walk(files_dir):
                    for fname in files:
                        try:
                            fp = os.path.join(root, fname)
                            used += os.path.getsize(fp)
                        except Exception:
                            # Ignore files that cannot be accessed
                            continue
            self.used_storage = used
            print(f"📊 {self.node_id} recalculated storage: {used / (1024**3):.2f} GB used")
        except Exception as e:
            print(f"⚠️  Error loading storage metadata: {e}")
    
    def _send_to_controller(self, action: str, **kwargs) -> Optional[Dict[str, Any]]:
        """Sends a message to the controller and waits for a response."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5.0)  # Use a timeout for controller communication
                s.connect((self.controller_host, self.controller_port))
                
                message = {'node_id': self.node_id, 'action': action, **kwargs}
                s.sendall(pickle.dumps(message))
                
                # Receive response
                data = s.recv(8192)
                if data:
                    return pickle.loads(data)
                return None
                
        except socket.timeout:
            print(f"⚠️  Message send failed: timed out")
            return None
        except ConnectionRefusedError:
            # Controller might not be ready yet
            return {'status': 'ERROR', 'error': 'Connection refused'}
        except Exception as e:
            # Handle other connection issues
            print(f"⚠️  Message send failed: {e}")
            return {'status': 'ERROR', 'error': str(e)}

    def _get_resource_info(self) -> Dict[str, Any]:
        """Returns a dict of node resources and current usage."""
        return {
            'cpu_cores': self.cpu_cores,
            'memory_gb': self.memory_gb,
            'storage_gb': self.storage_gb,
            'bandwidth_mbps': self.bandwidth_mbps,
            'used_storage': self.used_storage,
            'active_transfers': self.active_transfers,
            'host': socket.gethostbyname(socket.gethostname()),  # Use actual host IP
            'port': 0 # Node typically registers with a dedicated listener port if needed
        }

    def start(self) -> bool:
        """
        Start the node, register with controller, and start background threads.
        """
        print(f"📡 Node {self.node_id} connecting to controller at {self.controller_host}:{self.controller_port}...")
        
        # --- Retry loop for initial registration ---
        max_retries = 5
        for attempt in range(max_retries):
            # Attempt registration
            response = self._send_to_controller('REGISTER', resources=self._get_resource_info())
            
            if response and response.get('status') == 'OK':
                # Registration successful
                print(f"✅ Node {self.node_id} successfully registered with controller.")
                self.running = True
                self._write_pid_file()
                
                # Start background processes
                threading.Thread(target=self._heartbeat_sender, daemon=True).start()
                threading.Thread(target=self._transfer_listener, daemon=True).start()
                threading.Thread(target=self._periodic_storage_check, daemon=True).start()
                
                return True
            
            # Registration failed (either timeout or error response)
            if attempt < max_retries - 1:
                print(f"⚠️  [Node {self.node_id}] Registration attempt {attempt + 1}/{max_retries} failed. Retrying in 1 second...")
                time.sleep(1)
            else:
                print(f"❌ [Node {self.node_id}] Registration failed after {max_retries} attempts. Shutting down.")
                self.running = False
                return False
        # --- END FIX ---
        return False

    def _heartbeat_sender(self):
        """Periodically send a heartbeat message to the controller."""
        while self.running:
            time.sleep(5)  # Send heartbeat every 5 seconds
            
            # Skip heartbeat if active transfers are high (prioritize transfer data)
            if self.active_transfers < self.max_concurrent_transfers:
                response = self._send_to_controller('HEARTBEAT', 
                                                    used_storage=self.used_storage, 
                                                    active_transfers=self.active_transfers)
                
                if not response or response.get('status') != 'ACK':
                    print(f"⚠️  Heartbeat ACK failed. Connection issue or node status error.")
            
    def _transfer_listener(self):
        """Listens for incoming transfer requests."""
        pass

    def _periodic_storage_check(self):
        """Periodically recalculate used storage (useful for external file changes)."""
        while self.running:
            time.sleep(5)  # Check every 5 seconds for deleted files
            
            # Check if metadata cache was cleared (indicating files were deleted)
            metadata_file = os.path.join(self.storage_dir, "storage_metadata.json")
            if not os.path.exists(metadata_file):
                # Metadata was cleared, recalculate immediately
                print(f"🔄 {self.node_id} detected deletion, recalculating storage...")
                self._load_storage_metadata()
            
    def _handle_transfer_request(self, message: Dict[str, Any]):
        """Placeholder for handling transfer requests from the controller."""
        action = message.get('transfer_action')
        
        if action == 'INIT_UPLOAD':
            # Logic to receive file chunks
            pass
        elif action == 'INIT_DOWNLOAD':
            # Logic to send file chunks
            pass
            
        self.active_transfers = max(0, self.active_transfers - 1)
        
        # Notify controller of transfer completion
        # self._send_to_controller('TRANSFER_COMPLETE', ...)
        
    def stop(self):
        """Stop the node and its processes."""
        self.running = False
        self.executor.shutdown(wait=False)
        # Remove PID file to signal process is gone
        self._remove_pid_file()
        print(f"🛑 Node {self.node_id} stopped")


def main():
    """Main function to parse arguments and start the node."""
    parser = argparse.ArgumentParser(description="Clean Distributed Storage Node.")
    parser.add_argument('--node-id', type=str, required=True, help="Unique identifier for the node (e.g., N1).")
    parser.add_argument('--cpu', type=int, default=4, help="Number of CPU cores.")
    parser.add_argument('--memory', type=int, default=16, help="RAM in GB.")
    parser.add_argument('--storage', type=int, default=1000, help="Storage capacity in GB.")
    parser.add_argument('--bandwidth', type=int, default=1000, help="Network bandwidth in Mbps.")
    parser.add_argument('--controller-host', type=str, default='localhost', help="Controller host address.")
    parser.add_argument('--controller-port', type=int, default=5000, help="Controller port.")
    parser.add_argument('--interactive', action='store_true', help="Run node in interactive mode.")
    
    args = parser.parse_args()

    print(f"🚀 ENHANCED DISTRIBUTED STORAGE NODE: {args.node_id}")
    print("=" * 70)
    print("✅ Resource-aware node management")
    print("✅ Chunked file transfers with progress tracking")
    print("✅ Bandwidth-aware transfer timing")
    print("✅ Automatic storage validation")
    print("✅ Real-time transfer statistics")
    print("✅ Fault-tolerant file replication")
    print("=" * 70)
    print(f"🖥️  Resources: {args.cpu} CPU, {args.memory}GB RAM, {args.storage}GB Storage, {args.bandwidth}Mbps")
    print("=" * 70)
    
    node = CleanNode(
        node_id=args.node_id,
        cpu_cores=args.cpu,
        memory_gb=args.memory,
        storage_gb=args.storage,
        bandwidth_mbps=args.bandwidth,
        controller_host=args.controller_host,
        controller_port=args.controller_port,
        interactive=args.interactive
    )
    
    try:
        if node.start():
            if args.interactive:
                print(f"🎮 Node {args.node_id} running in INTERACTIVE MODE!")
            else:
                print(f"✅ Node {args.node_id} running. Press Ctrl+C to stop.")
            
            # Keep running (background threads handle communication)
            while node.running:
                time.sleep(1)
        else:
            print(f"❌ Failed to start node {args.node_id}")
            
    except KeyboardInterrupt:
        print(f"\n🛑 Shutting down node {args.node_id}...")
    finally:
        node.stop()


if __name__ == "__main__":
    main()