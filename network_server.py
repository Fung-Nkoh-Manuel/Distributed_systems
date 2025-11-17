from flask import Flask, request, jsonify
import requests
import hashlib
import time
from collections import defaultdict
from typing import Dict, Optional
import argparse

app = Flask(__name__)

# Network state(node_id -> url, connection and filetransfer)
nodes: Dict[str, str] = {}
connections: Dict[str, Dict[str, int]] = defaultdict(dict) # node1_id -> {node2_id:bandwidth} 
active_transfer: Dict[str, dict] = {} #file_id -> transfer_info

@app.route('\health', method=['GET'])
def health(): 
    return jsonify({"status": "health", "total_nodes": len(nodes)})

@app.route('/node/register', mathod=['POST'])
def register_node():
    data = request.json
    node_id = data.get('node_id')
    url = data.get('url')

    if not node_id or not url:
        return jsonify({"error": "Missing node_id or url"}), 400
    
    # verify if node is reachable
    try:
        response = requests.get(f"{url}/health", timeout=5)
        if response.tatus_code != 200:
            return jsonify({"error": "Node not reachable"}), 503
    except Exception as e:
        return jsonify({"error": f"cannot reach node: {str(e)}"}), 503
    nodes[node_id] = url
    return jsonify({"success": True, "registered": node_id})

@app.route('/node/list', method = ['GET'])
def list_nodes():
    node_info = {}
    for node_id , url in nodes.items():
        try:
            response = request.get(f"{url}/info", timeout=5)
            if response.status_code == 200:
                node_info[node_id] = response.json()
        except:
            node_info[node_id] =  {"status": "unreachable", "url": url}
    
    return jsonify({"nodes": node_info})

