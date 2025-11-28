import requests
import time
import argparse
from tqdm import tqdm

class NetworkClient:
    def __init__(self, network_url: str):
        self.network_url = network_url
    
    def register_node(self, node_id: str, node_url: str):
        """Register a node with the network"""
        response = requests.post(
            f"{self.network_url}/node/register",
            json={"node_id": node_id, "url": node_url}
        )
        return response.json()
    
    def connect_nodes(self, node1_id: str, node2_id: str, bandwidth: int):
        """Create connection between two nodes"""
        response = requests.post(
            f"{self.network_url}/connection/create",
            json={
                "node1_id": node1_id,
                "node2_id": node2_id,
                "bandwidth": bandwidth
            }
        )
        return response.json()
    
    def initiate_transfer(self, source_node_id: str, target_node_id: str, 
                         file_name: str, file_size: int):
        """Initiate a file transfer"""
        response = requests.post(
            f"{self.network_url}/transfer/initiate",
            json={
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "file_name": file_name,
                "file_size": file_size
            }
        )
        return response.json()
    
    def process_transfer(self, file_id: str, chunks_to_process: int = 1):
        """Process chunks of a transfer"""
        response = requests.post(
            f"{self.network_url}/transfer/process",
            json={
                "file_id": file_id,
                "chunks_to_process": chunks_to_process
            }
        )
        return response.json()
    
    def get_transfer_status(self, file_id: str):
        """Get transfer status"""
        response = requests.get(f"{self.network_url}/transfer/status/{file_id}")
        return response.json()
    
    def get_network_stats(self):
        """Get network statistics"""
        response = requests.get(f"{self.network_url}/stats")
        return response.json()
    
    def list_nodes(self):
        """List all nodes"""
        response = requests.get(f"{self.network_url}/node/list")
        return response.json()
    
    def tick(self):
        """Reset network utilization"""
        response = requests.post(f"{self.network_url}/tick")
        return response.json()


def main():
    parser = argparse.ArgumentParser(description='Distributed Storage Network Client')
    parser.add_argument('--network-url', default='http://localhost:5500', 
                       help='Network coordinator URL')
    parser.add_argument('--node1-url', default='http://localhost:5001',
                       help='Node 1 URL')
    parser.add_argument('--node2-url', default='http://localhost:5002',
                       help='Node 2 URL')
    parser.add_argument('--file-size-mb', type=int, default=100,
                       help='File size in MB to transfer')
    parser.add_argument('--chunks-per-step', type=int, default=3,
                       help='Number of chunks to process per step')
    
    args = parser.parse_args()
    
    client = NetworkClient(args.network_url)
    
    print("=" * 60)
    print("Distributed Storage Network - File Transfer Simulation")
    print("=" * 60)
    
    # Register nodes
    print("\n[1/5] Registering nodes...")
    try:
        result1 = client.register_node("node1", args.node1_url)
        print(f"  ✓ Registered node1: {result1}")
        
        result2 = client.register_node("node2", args.node2_url)
        print(f"  ✓ Registered node2: {result2}")
    except Exception as e:
        print(f"  ✗ Failed to register nodes: {e}")
        print("  Make sure all node servers are running!")
        return
    
    # List nodes
    print("\n[2/5] Network topology...")
    nodes_info = client.list_nodes()
    for node_id, info in nodes_info['nodes'].items():
        if info.get('status') != 'unreachable':
            print(f"  • {node_id}: {info.get('cpu_capacity')}vCPU, "
                  f"{info.get('memory_capacity')}GB RAM, "
                  f"{info.get('total_storage', 0) / (1024**3):.0f}GB storage")
    
    # Connect nodes
    print("\n[3/5] Creating network connections...")
    result = client.connect_nodes("node1", "node2", bandwidth=1000)
    print(f"  ✓ Connected node1 <-> node2 @ 1Gbps")
    
    # Initiate transfer
    file_size_bytes = args.file_size_mb * 1024 * 1024
    print(f"\n[4/5] Initiating file transfer ({args.file_size_mb}MB)...")
    transfer_result = client.initiate_transfer(
        source_node_id="node1",
        target_node_id="node2",
        file_name="large_dataset.zip",
        file_size=file_size_bytes
    )
    
    if not transfer_result.get('success'):
        print(f"  ✗ Failed to initiate transfer: {transfer_result}")
        return
    
    file_id = transfer_result['file_id']
    total_chunks = transfer_result['total_chunks']
    print(f"  ✓ Transfer initiated (ID: {file_id[:8]}...)")
    print(f"  ✓ Total chunks: {total_chunks}")
    
    # Process transfer with progress bar
    print(f"\n[5/5] Processing transfer...")
    with tqdm(total=total_chunks, desc="  Transferring", unit="chunk") as pbar:
        completed = False
        while not completed:
            # Tick the network
            client.tick()
            
            # Process chunks
            try:
                result = client.process_transfer(file_id, args.chunks_per_step)
                
                if result.get('success'):
                    chunks_processed = result['chunks_processed']
                    pbar.update(chunks_processed)
                    completed = result.get('completed', False)
                    
                    if completed:
                        print("\n  ✓ Transfer completed successfully!")
                        break
                else:
                    print(f"\n  ✗ Transfer failed: {result}")
                    break
                    
            except Exception as e:
                print(f"\n  ✗ Error during transfer: {e}")
                break
            
            # Small delay between steps
            time.sleep(0.1)
    
    # Get final statistics
    print("\n" + "=" * 60)
    print("Final Statistics")
    print("=" * 60)
    
    try:
        stats = client.get_network_stats()
        print(f"Total Nodes:        {stats['total_nodes']}")
        print(f"Storage Used:       {stats['used_storage_bytes'] / (1024**3):.2f}GB / "
              f"{stats['total_storage_bytes'] / (1024**3):.2f}GB "
              f"({stats['storage_utilization_percent']:.1f}%)")
        print(f"Active Transfers:   {stats['active_transfers']}")
        print(f"Completed:          {stats['completed_transfers']}")
        
        # Get node-specific stats
        print("\nNode Details:")
        for node_id, node_url in [("node1", args.node1_url), ("node2", args.node2_url)]:
            try:
                storage = requests.get(f"{node_url}/stats/storage").json()
                perf = requests.get(f"{node_url}/stats/performance").json()
                print(f"\n  {node_id}:")
                print(f"    Storage:  {storage['used_bytes'] / (1024**3):.2f}GB / "
                      f"{storage['total_bytes'] / (1024**3):.2f}GB "
                      f"({storage['utilization_percent']:.1f}%)")
                print(f"    Files:    {storage['files_stored']}")
                print(f"    Data Tx:  {perf['total_data_transferred_bytes'] / (1024**2):.2f}MB")
            except:
                pass
                
    except Exception as e:
        print(f"Could not retrieve stats: {e}")
    
    print("\n" + "=" * 60)


if __name__ == '__main__':
    main()