from flask import Flask, request, jsonify
from storage_virtual_node import StorageVirtualNode, TransferStatus
import argparse

app = Flask(__name__)
node = None

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "node_id": node.node_id})

@app.route('/info', methods=['GET'])
def get_info():
    """Get node information"""
    return jsonify({
        "node_id": node.node_id,
        "cpu_capacity": node.cpu_capacity,
        "memory_capacity": node.memory_capacity,
        "total_storage": node.total_storage,
        "bandwidth": node.bandwidth,
        "connections": list(node.connections.keys())
    })

@app.route('/connection', methods=['POST'])
def add_connection():
    """Add connection to another node"""
    data = request.json
    node_id = data.get('node_id')
    bandwidth = data.get('bandwidth')
    
    if not node_id or not bandwidth:
        return jsonify({"error": "Missing node_id or bandwidth"}), 400
    
    node.add_connection(node_id, bandwidth)
    return jsonify({"success": True, "connected_to": node_id})

@app.route('/transfer/initiate', methods=['POST'])
def initiate_transfer():
    """Initiate a file transfer to this node"""
    data = request.json
    file_id = data.get('file_id')
    file_name = data.get('file_name')
    file_size = data.get('file_size')
    source_node = data.get('source_node')
    
    if not all([file_id, file_name, file_size]):
        return jsonify({"error": "Missing required fields"}), 400
    
    transfer = node.initiate_file_transfer(file_id, file_name, file_size, source_node)
    
    if not transfer:
        return jsonify({"error": "Insufficient storage space"}), 507
    
    return jsonify({
        "success": True,
        "file_id": transfer.file_id,
        "total_chunks": len(transfer.chunks),
        "chunk_size": transfer.chunks[0].size if transfer.chunks else 0
    })

@app.route('/transfer/chunk', methods=['POST'])
def process_chunk():
    """Process a single chunk transfer"""
    data = request.json
    file_id = data.get('file_id')
    chunk_id = data.get('chunk_id')
    source_node = data.get('source_node')
    
    if not all([file_id, chunk_id is not None, source_node]):
        return jsonify({"error": "Missing required fields"}), 400
    
    success = node.process_chunk_transfer(file_id, chunk_id, source_node)
    
    if not success:
        return jsonify({"error": "Failed to process chunk"}), 500
    
    # Check if transfer is complete
    transfer = node.active_transfers.get(file_id) or node.stored_files.get(file_id)
    is_complete = False
    
    if transfer:
        is_complete = transfer.status == TransferStatus.COMPLETED
    
    return jsonify({
        "success": True,
        "chunk_id": chunk_id,
        "completed": is_complete
    })

@app.route('/transfer/status/<file_id>', methods=['GET'])
def get_transfer_status(file_id):
    """Get status of a file transfer"""
    transfer = node.active_transfers.get(file_id) or node.stored_files.get(file_id)
    
    if not transfer:
        return jsonify({"error": "Transfer not found"}), 404
    
    completed_chunks = sum(1 for c in transfer.chunks if c.status == TransferStatus.COMPLETED)
    
    return jsonify({
        "file_id": transfer.file_id,
        "file_name": transfer.file_name,
        "status": transfer.status.name,
        "total_chunks": len(transfer.chunks),
        "completed_chunks": completed_chunks,
        "progress_percent": (completed_chunks / len(transfer.chunks)) * 100
    })

@app.route('/stats/storage', methods=['GET'])
def get_storage_stats():
    """Get storage utilization statistics"""
    return jsonify(node.get_storage_utilization())

@app.route('/stats/network', methods=['GET'])
def get_network_stats():
    """Get network utilization statistics"""
    return jsonify(node.get_network_utilization())

@app.route('/stats/performance', methods=['GET'])
def get_performance_stats():
    """Get performance metrics"""
    return jsonify(node.get_performance_metrics())

@app.route('/tick', methods=['POST'])
def tick():
    """Reset network utilization for new simulation step"""
    node.network_utilization = 0
    return jsonify({"success": True})

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Storage Virtual Node Server')
    parser.add_argument('--node-id', required=True, help='Node identifier')
    parser.add_argument('--cpu', type=int, default=4, help='CPU capacity (vCPUs)')
    parser.add_argument('--memory', type=int, default=16, help='Memory capacity (GB)')
    parser.add_argument('--storage', type=int, default=500, help='Storage capacity (GB)')
    parser.add_argument('--bandwidth', type=int, default=1000, help='Bandwidth (Mbps)')
    parser.add_argument('--port', type=int, default=5000, help='Server port')
    parser.add_argument('--host', default='0.0.0.0', help='Server host')
    
    args = parser.parse_args()
    
    # Initialize the node
    node = StorageVirtualNode(
        node_id=args.node_id,
        cpu_capacity=args.cpu,
        memory_capacity=args.memory,
        storage_capacity=args.storage,
        bandwidth=args.bandwidth
    )
    
    print(f"Starting node server: {args.node_id}")
    print(f"CPU: {args.cpu} vCPUs, Memory: {args.memory}GB")
    print(f"Storage: {args.storage}GB, Bandwidth: {args.bandwidth}Mbps")
    print(f"Listening on {args.host}:{args.port}")
    
    app.run(host=args.host, port=args.port, debug=False)