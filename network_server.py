from flask import Flask, request, jsonify
import requests
import hashlib
import time
from collections import defaultdict
from typing import Dict, Optional
import argparse

app = Flask(__name__)

# Network state
nodes: Dict[str, str] = {}  # node_id -> url
connections: Dict[str, Dict[str, int]] = defaultdict(dict)  # node1_id -> {node2_id: bandwidth}
active_transfers: Dict[str, dict] = {}  # file_id -> transfer_info

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "total_nodes": len(nodes)})

@app.route('/node/register', methods=['POST'])
def register_node():
    """Register a node with the network"""
    data = request.json
    node_id = data.get('node_id')
    url = data.get('url')
    
    if not node_id or not url:
        return jsonify({"error": "Missing node_id or url"}), 400
    
    # Verify node is reachable
    try:
        response = requests.get(f"{url}/health", timeout=5)
        if response.status_code != 200:
            return jsonify({"error": "Node not reachable"}), 503
    except Exception as e:
        return jsonify({"error": f"Cannot reach node: {str(e)}"}), 503
    
    nodes[node_id] = url
    return jsonify({"success": True, "registered": node_id})

@app.route('/node/list', methods=['GET'])
def list_nodes():
    """List all registered nodes"""
    node_info = {}
    for node_id, url in nodes.items():
        try:
            response = requests.get(f"{url}/info", timeout=5)
            if response.status_code == 200:
                node_info[node_id] = response.json()
        except:
            node_info[node_id] = {"status": "unreachable", "url": url}
    
    return jsonify({"nodes": node_info})

@app.route('/connection/create', methods=['POST'])
def create_connection():
    """Create a bidirectional connection between two nodes"""
    data = request.json
    node1_id = data.get('node1_id')
    node2_id = data.get('node2_id')
    bandwidth = data.get('bandwidth')
    
    if not all([node1_id, node2_id, bandwidth]):
        return jsonify({"error": "Missing required fields"}), 400
    
    if node1_id not in nodes or node2_id not in nodes:
        return jsonify({"error": "One or both nodes not registered"}), 404
    
    # Add connection to both nodes
    try:
        # Connect node1 to node2
        requests.post(
            f"{nodes[node1_id]}/connection",
            json={"node_id": node2_id, "bandwidth": bandwidth},
            timeout=5
        )
        
        # Connect node2 to node1
        requests.post(
            f"{nodes[node2_id]}/connection",
            json={"node_id": node1_id, "bandwidth": bandwidth},
            timeout=5
        )
        
        # Track in network coordinator
        connections[node1_id][node2_id] = bandwidth
        connections[node2_id][node1_id] = bandwidth
        
        return jsonify({
            "success": True,
            "connection": f"{node1_id} <-> {node2_id}",
            "bandwidth": bandwidth
        })
    except Exception as e:
        return jsonify({"error": f"Failed to create connection: {str(e)}"}), 500

@app.route('/transfer/initiate', methods=['POST'])
def initiate_transfer():
    """Initiate a file transfer between nodes"""
    data = request.json
    source_node_id = data.get('source_node_id')
    target_node_id = data.get('target_node_id')
    file_name = data.get('file_name')
    file_size = data.get('file_size')
    
    if not all([source_node_id, target_node_id, file_name, file_size]):
        return jsonify({"error": "Missing required fields"}), 400
    
    if source_node_id not in nodes or target_node_id not in nodes:
        return jsonify({"error": "One or both nodes not registered"}), 404
    
    # Generate unique file ID
    file_id = hashlib.md5(f"{file_name}-{time.time()}".encode()).hexdigest()
    
    # Initiate transfer on target node
    try:
        response = requests.post(
            f"{nodes[target_node_id]}/transfer/initiate",
            json={
                "file_id": file_id,
                "file_name": file_name,
                "file_size": file_size,
                "source_node": source_node_id
            },
            timeout=10
        )
        
        if response.status_code != 200:
            return jsonify({"error": "Failed to initiate transfer on target node"}), 500
        
        transfer_info = response.json()
        
        # Track transfer
        active_transfers[file_id] = {
            "file_id": file_id,
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "file_name": file_name,
            "file_size": file_size,
            "total_chunks": transfer_info['total_chunks'],
            "completed_chunks": 0,
            "status": "in_progress",
            "started_at": time.time()
        }
        
        return jsonify({
            "success": True,
            "file_id": file_id,
            "total_chunks": transfer_info['total_chunks']
        })
        
    except Exception as e:
        return jsonify({"error": f"Failed to initiate transfer: {str(e)}"}), 500

@app.route('/transfer/process', methods=['POST'])
def process_transfer():
    """Process chunks of an active transfer"""
    data = request.json
    file_id = data.get('file_id')
    chunks_to_process = data.get('chunks_to_process', 1)
    
    if not file_id or file_id not in active_transfers:
        return jsonify({"error": "Transfer not found"}), 404
    
    transfer = active_transfers[file_id]
    target_url = nodes[transfer['target_node_id']]
    chunks_processed = 0
    
    # Process chunks
    for chunk_id in range(transfer['completed_chunks'], 
                          min(transfer['completed_chunks'] + chunks_to_process, 
                              transfer['total_chunks'])):
        try:
            response = requests.post(
                f"{target_url}/transfer/chunk",
                json={
                    "file_id": file_id,
                    "chunk_id": chunk_id,
                    "source_node": transfer['source_node_id']
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                chunks_processed += 1
                transfer['completed_chunks'] += 1
                
                if result.get('completed'):
                    transfer['status'] = 'completed'
                    transfer['completed_at'] = time.time()
                    break
            else:
                transfer['status'] = 'failed'
                break
                
        except Exception as e:
            transfer['status'] = 'failed'
            return jsonify({
                "error": f"Failed to process chunk {chunk_id}: {str(e)}"
            }), 500
    
    return jsonify({
        "success": True,
        "chunks_processed": chunks_processed,
        "completed_chunks": transfer['completed_chunks'],
        "total_chunks": transfer['total_chunks'],
        "status": transfer['status'],
        "completed": transfer['status'] == 'completed'
    })
