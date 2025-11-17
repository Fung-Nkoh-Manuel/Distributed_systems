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

