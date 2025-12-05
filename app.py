#!/usr/bin/env python3
"""
Integrated Cloud Storage Web UI with Authentication, File Management, and Dual Dashboards
FIXED: Files persist across user sessions and server restarts
"""

from flask import Flask, render_template, request, jsonify, session, redirect, url_for, send_file
import pickle
import socket
import json
import os
import hashlib
import uuid
from threading import Thread
import time
from datetime import datetime, timedelta
from functools import wraps
from io import BytesIO
import grpc
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc

app = Flask(__name__)
app.secret_key = 'your-secret-key-change-this-in-production'

# Configuration
CONTROLLER_HOST = 'localhost'
CONTROLLER_PORT = 5000
AUTH_SERVER_HOST = 'localhost'
AUTH_SERVER_PORT = 51234
CHUNK_SIZE = 1024 * 1024  # 1 MB chunks
USER_STORAGE_QUOTA = 100 * 1024 * 1024 * 1024  # 100 GB per user
ADMIN_EMAIL = 'admin@gmail.com'
ADMIN_PASSWORD = 'admin123'

# Persistent user storage database
USER_DB_DIR = "user_storage"
if not os.path.exists(USER_DB_DIR):
    os.makedirs(USER_DB_DIR)

# In-memory cache for session (loaded from disk on login)
user_storage = {}

# Temporary file tracking during uploads/downloads
temp_files = {}


def get_user_db_path(user_id: str) -> str:
    """Get the path to a user's database file"""
    return os.path.join(USER_DB_DIR, f"{user_id}.json")


def load_user_from_disk(user_id: str) -> dict:
    """Load user data from persistent storage"""
    db_path = get_user_db_path(user_id)
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Error loading user {user_id}: {e}")
    return None


def save_user_to_disk(user_id: str, user_data: dict):
    """Save user data to persistent storage"""
    db_path = get_user_db_path(user_id)
    try:
        with open(db_path, 'w') as f:
            json.dump(user_data, f, indent=2)
    except Exception as e:
        print(f"⚠️  Error saving user {user_id}: {e}")


def send_to_controller(action, **kwargs):
    """Send message to controller via socket"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5.0)
            s.connect((CONTROLLER_HOST, CONTROLLER_PORT))
            
            message = {'action': action, **kwargs}
            s.sendall(pickle.dumps(message))
            
            data = s.recv(16384)
            if data:
                return pickle.loads(data)
            return None
    except Exception as e:
        print(f"Error communicating with controller: {e}")
        return {'status': 'ERROR', 'error': str(e)}


def create_auth_stub():
    """Create gRPC stub for authentication"""
    try:
        channel = grpc.insecure_channel(f'{AUTH_SERVER_HOST}:{AUTH_SERVER_PORT}')
        return cloudsecurity_pb2_grpc.UserServiceStub(channel)
    except Exception as e:
        return None


def login_required(f):
    """Decorator to require login"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Decorator to require admin role"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session or session.get('role') != 'admin':
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def initialize_user_storage(user_id, email, role='user'):
    """Initialize storage quota for a new user from disk or create new"""
    # Try to load from disk first
    user_data = load_user_from_disk(user_id)
    
    if user_data is None:
        # Create new user
        user_data = {
            'user_id': user_id,
            'email': email,
            'used': 0,
            'quota': USER_STORAGE_QUOTA,
            'files': {},  # file_id -> {name, size, chunks_info, created_at}
            'created_at': datetime.now().isoformat(),
            'role': role
        }
        save_user_to_disk(user_id, user_data)
    else:
        # Recalculate used storage from actual files
        user_data['used'] = 0
        for file_id, file_info in user_data.get('files', {}).items():
            user_data['used'] += file_info.get('size', 0)
        save_user_to_disk(user_id, user_data)
    
    # Load into memory
    user_storage[user_id] = user_data
    return user_data


def sync_user_files_with_controller(user_id: str):
    """Sync user's files with controller's persistent storage"""
    try:
        # Get all files from controller
        response = send_to_controller('LIST_FILES')
        
        if response and response.get('status') == 'OK':
            user_files = user_storage.get(user_id, {}).get('files', {})
            
            for file_data in response.get('files', []):
                # Check if this file belongs to this user
                # The owner_node in controller contains the user_id
                owner_node = file_data.get('owner_node', '')
                
                if owner_node == user_id:
                    file_id = file_data['file_id']
                    
                    # Only add if not already in user's files AND not marked as deleted
                    if file_id not in user_files and not _is_file_deleted_locally(user_id, file_id):
                        user_files[file_id] = {
                            'name': file_data['file_name'],
                            'size': file_data['file_size'],
                            'chunks_info': {
                                f"chunk_{i}": {
                                    'size': file_data['chunk_size'],
                                    'hash': '',
                                    'nodes': file_data.get('replica_nodes', [])
                                }
                                for i in range(file_data.get('total_chunks', 0))
                            },
                            'created_at': datetime.fromtimestamp(
                                file_data.get('created_at', time.time())
                            ).isoformat(),
                            'chunk_count': file_data.get('total_chunks', 0)
                        }
                        
                        # Update user's used storage
                        user_storage[user_id]['used'] += file_data['file_size']
            
            # Save synced data back to disk
            save_user_to_disk(user_id, user_storage[user_id])
    except Exception as e:
        print(f"⚠️  Error syncing files for {user_id}: {e}")


def _is_file_deleted_locally(user_id: str, file_id: str) -> bool:
    """Check if a file was deleted locally by checking if it's in deleted_files tracker"""
    deleted_tracker_path = os.path.join(USER_DB_DIR, f"{user_id}_deleted.json")
    try:
        if os.path.exists(deleted_tracker_path):
            with open(deleted_tracker_path, 'r') as f:
                deleted_files = json.load(f)
                return file_id in deleted_files
    except Exception:
        pass
    return False


def _mark_file_as_deleted(user_id: str, file_id: str):
    """Mark a file as deleted to prevent re-sync from controller"""
    deleted_tracker_path = os.path.join(USER_DB_DIR, f"{user_id}_deleted.json")
    try:
        deleted_files = {}
        if os.path.exists(deleted_tracker_path):
            with open(deleted_tracker_path, 'r') as f:
                deleted_files = json.load(f)
        
        deleted_files[file_id] = datetime.now().isoformat()
        
        with open(deleted_tracker_path, 'w') as f:
            json.dump(deleted_files, f, indent=2)
    except Exception as e:
        print(f"⚠️  Error marking file as deleted: {e}")


# ==================== AUTHENTICATION ROUTES ====================

@app.route('/')
def index():
    """Redirect to appropriate dashboard or login"""
    if 'user_id' in session:
        if session.get('role') == 'admin':
            return redirect(url_for('admin_dashboard'))
        else:
            return redirect(url_for('user_dashboard'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page and handler"""
    if request.method == 'POST':
        data = request.json
        email = data.get('email')
        password = data.get('password')

        # Admin auto-detection
        if email == ADMIN_EMAIL and password == ADMIN_PASSWORD:
            admin_id = f"admin_{hashlib.md5(email.encode()).hexdigest()[:16]}"
            session['user_id'] = admin_id
            session['email'] = email
            session['role'] = 'admin'
            initialize_user_storage(admin_id, email, 'admin')
            return jsonify({'success': True, 'redirect': '/admin-dashboard'})

        # Regular user login via auth service
        try:
            stub = create_auth_stub()
            if not stub:
                return jsonify({'success': False, 'message': 'Auth server unavailable'}), 500

            response = stub.login(
                cloudsecurity_pb2.LoginRequest(email=email, password=password)
            )

            if 'OTP sent' in response.result:
                session['temp_email'] = email
                return jsonify({'success': True, 'otp_required': True})
            else:
                return jsonify({'success': False, 'message': response.result}), 400

        except Exception as e:
            return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500

    return render_template('login.html')


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    """Signup page and handler"""
    if request.method == 'POST':
        data = request.json
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')

        try:
            stub = create_auth_stub()
            if not stub:
                return jsonify({'success': False, 'message': 'Auth server unavailable'}), 500

            response = stub.signup(
                cloudsecurity_pb2.SignupRequest(
                    login=username,
                    password=password,
                    email=email
                )
            )

            if 'successful' in response.result.lower():
                return jsonify({'success': True, 'message': response.result})
            else:
                return jsonify({'success': False, 'message': response.result}), 400

        except Exception as e:
            return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500

    return render_template('signup.html')


@app.route('/api/verify-otp', methods=['POST'])
def verify_otp():
    """Verify OTP and create session"""
    data = request.json
    otp = data.get('otp')
    email = session.get('temp_email')

    if not email:
        return jsonify({'success': False, 'message': 'Session expired'}), 401

    try:
        stub = create_auth_stub()
        if not stub:
            return jsonify({'success': False, 'message': 'Auth server unavailable'}), 500

        response = stub.verifyOtp(
            cloudsecurity_pb2.OtpRequest(email=email, otp=otp)
        )

        if 'successful' in response.result.lower():
            user_id = hashlib.md5(email.encode()).hexdigest()[:16]
            session['user_id'] = user_id
            session['email'] = email
            session['role'] = 'user'
            session.pop('temp_email', None)
            
            # Initialize user storage from disk
            initialize_user_storage(user_id, email, 'user')
            
            # Sync with controller to get any files uploaded
            sync_user_files_with_controller(user_id)
            
            return jsonify({'success': True, 'message': 'Login successful'})
        else:
            return jsonify({'success': False, 'message': response.result}), 400

    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/logout')
def logout():
    """Logout user"""
    session.clear()
    return redirect(url_for('login'))


# ==================== USER DASHBOARD ====================

@app.route('/dashboard')
@login_required
def user_dashboard():
    """User dashboard for file management"""
    if session.get('role') == 'admin':
        return redirect(url_for('admin_dashboard'))
    return render_template('dashboard.html')


@app.route('/api/storage/info')
@login_required
def get_storage_info():
    """Get user storage information"""
    user_id = session['user_id']
    info = user_storage.get(user_id, {})
    
    used = info.get('used', 0)
    quota = info.get('quota', USER_STORAGE_QUOTA)
    
    return jsonify({
        'used': used,
        'quota': quota,
        'available': quota - used,
        'used_percent': (used / quota * 100) if quota > 0 else 0,
        'used_gb': used / (1024**3),
        'quota_gb': quota / (1024**3)
    })


@app.route('/api/files/list')
@login_required
def list_files():
    """List all files for the user"""
    user_id = session['user_id']
    
    # Sync with controller first to get latest files
    sync_user_files_with_controller(user_id)
    
    user_info = user_storage.get(user_id, {})
    files = user_info.get('files', {})
    
    files_list = []
    for file_id, file_data in files.items():
        files_list.append({
            'id': file_id,
            'name': file_data['name'],
            'size': file_data['size'],
            'size_mb': file_data['size'] / (1024**2),
            'created_at': file_data['created_at'],
            'status': 'available'
        })
    
    return jsonify(files_list)


@app.route('/api/files/upload', methods=['POST'])
@login_required
def upload_file():
    """Handle file upload with chunking"""
    user_id = session['user_id']
    
    if 'file' not in request.files:
        return jsonify({'success': False, 'message': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'No file selected'}), 400
    
    try:
        # Read file into memory
        file_data = file.read()
        file_size = len(file_data)
        
        # Ensure storage record exists
        if user_id not in user_storage:
            initialize_user_storage(user_id, session.get('email'), 'user')
        
        # Check quota
        user_info = user_storage[user_id]
        if user_info['used'] + file_size > user_info['quota']:
            return jsonify({
                'success': False,
                'message': f'Insufficient storage. Need {file_size / (1024**2):.2f} MB, have {(user_info["quota"] - user_info["used"]) / (1024**2):.2f} MB'
            }), 400
        
        # Create file record
        file_id = str(uuid.uuid4())
        chunks_info = {}
        
        # Split into chunks
        for i in range(0, file_size, CHUNK_SIZE):
            chunk_data = file_data[i:i + CHUNK_SIZE]
            chunk_id = f"{file_id}_chunk_{i // CHUNK_SIZE}"
            chunk_hash = hashlib.md5(chunk_data).hexdigest()
            
            chunks_info[chunk_id] = {
                'size': len(chunk_data),
                'hash': chunk_hash,
                'nodes': []
            }
        
        # Register file with controller
        file_info = {
            'file_id': file_id,
            'file_name': file.filename,
            'file_size': file_size,
            'owner_node': user_id,  # Use user_id as owner
            'chunk_count': len(chunks_info),
            'user_id': user_id
        }
        
        response = send_to_controller('FILE_CREATED', 
                                    node_id=user_id,
                                    file_info=file_info)
        
        if response and response.get('status') in ('OK', 'ACK'):
            # Get replica nodes from controller
            replica_nodes = response.get('replica_nodes') or []
            if not replica_nodes:
                try:
                    nodes_resp = send_to_controller('GET_NODES')
                    if nodes_resp and nodes_resp.get('status') == 'OK':
                        active_nodes = [n['id'] for n in nodes_resp.get('nodes', []) if n.get('status') == 'active']
                        if active_nodes:
                            replica_nodes = [active_nodes[0]]
                except Exception:
                    replica_nodes = []

            # Persist chunks across replica nodes
            import random
            candidate_nodes = list(replica_nodes) if replica_nodes else []
            try:
                nodes_resp = send_to_controller('GET_NODES')
                if nodes_resp and nodes_resp.get('status') == 'OK':
                    active_nodes = [n['id'] for n in nodes_resp.get('nodes', []) if n.get('status') == 'active']
                    if active_nodes:
                        candidate_nodes = active_nodes
            except Exception:
                pass

            CHUNK_REPLICATION = 2

            if candidate_nodes:
                rng = random.Random(hash(file_id))
                shuffled = list(candidate_nodes)
                rng.shuffle(shuffled)

                total_nodes = len(shuffled)
                chunk_index = 0
                for i in range(0, file_size, CHUNK_SIZE):
                    chunk_data = file_data[i:i + CHUNK_SIZE]

                    max_repl = min(CHUNK_REPLICATION, total_nodes)
                    start = (chunk_index * max_repl) % total_nodes
                    replicas_for_chunk = []
                    for r in range(max_repl):
                        replicas_for_chunk.append(shuffled[(start + r) % total_nodes])

                    chunk_id = f"{file_id}_chunk_{chunk_index}"
                    if chunk_id in chunks_info:
                        chunks_info[chunk_id]['nodes'] = replicas_for_chunk

                    for node_id_rep in replicas_for_chunk:
                        base_dir = os.path.join(f"node_storage_{node_id_rep}", "files", file_id, "chunks")
                        try:
                            os.makedirs(base_dir, exist_ok=True)
                            chunk_path = os.path.join(base_dir, f"chunk_{chunk_index}")
                            with open(chunk_path, 'wb') as f:
                                f.write(chunk_data)
                        except Exception:
                            continue

                    chunk_index += 1

            # Store file metadata in user storage
            user_info['files'][file_id] = {
                'name': file.filename,
                'size': file_size,
                'chunks_info': chunks_info,
                'created_at': datetime.now().isoformat(),
                'chunk_count': len(chunks_info)
            }
            
            # Update used storage
            user_info['used'] += file_size
            
            # Save to disk immediately
            save_user_to_disk(user_id, user_info)
            
            # Keep in memory for quick access
            temp_files[file_id] = file_data
            
            return jsonify({
                'success': True,
                'message': 'File uploaded successfully',
                'file_id': file_id,
                'chunks': len(chunks_info)
            })
        else:
            return jsonify({'success': False, 'message': 'Failed to register with storage'}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'Upload failed: {str(e)}'}), 500


@app.route('/api/files/<file_id>/download')
@login_required
def download_file(file_id):
    """Download file"""
    user_id = session['user_id']
    user_info = user_storage.get(user_id, {})
    
    if file_id not in user_info.get('files', {}):
        return jsonify({'success': False, 'message': 'File not found'}), 404
    
    try:
        file_info = user_info['files'][file_id]
        
        # Try to get from temp_files first (in-memory cache)
        file_data = temp_files.get(file_id)
        
        # If not in cache, reconstruct from chunks
        if not file_data:
            print(f"📥 Reconstructing file {file_id} from chunks...")
            file_data = bytearray()
            chunks_info = file_info.get('chunks_info', {})
            
            # Sort chunks by index to reconstruct in order
            sorted_chunks = sorted(chunks_info.items(), key=lambda x: int(x[0].split('_')[-1]))
            
            for chunk_id, chunk_data in sorted_chunks:
                # Try to get chunk from replica nodes
                nodes = chunk_data.get('nodes', [])
                chunk_reconstructed = False
                
                for node_id in nodes:
                    try:
                        chunk_index = int(chunk_id.split('_')[-1])
                        chunk_path = os.path.join(
                            f"node_storage_{node_id}", 
                            "files", 
                            file_id, 
                            "chunks", 
                            f"chunk_{chunk_index}"
                        )
                        
                        if os.path.exists(chunk_path):
                            with open(chunk_path, 'rb') as f:
                                file_data.extend(f.read())
                            chunk_reconstructed = True
                            print(f"  ✅ Chunk {chunk_index} loaded from {node_id}")
                            break
                    except Exception as e:
                        print(f"  ⚠️  Could not load chunk from {node_id}: {e}")
                        continue
                
                if not chunk_reconstructed:
                    raise Exception(f"Could not reconstruct chunk {chunk_id}")
            
            file_data = bytes(file_data)
        
        return send_file(
            BytesIO(file_data),
            as_attachment=True,
            download_name=file_info['name']
        )
        
    except Exception as e:
        print(f"Error downloading file {file_id}: {e}")
        return jsonify({'success': False, 'message': f'Download failed: {str(e)}'}), 500


@app.route('/api/files/<file_id>', methods=['DELETE'])
@login_required
def delete_file(file_id):
    """Delete file from user account and clean up storage"""
    user_id = session['user_id']
    
    # Reload user data from disk to ensure we have latest
    user_info = load_user_from_disk(user_id)
    if not user_info:
        user_info = user_storage.get(user_id, {})
    
    if file_id not in user_info.get('files', {}):
        return jsonify({'success': False, 'message': 'File not found'}), 404
    
    try:
        file_info = user_info['files'][file_id]
        file_size = file_info['size']
        
        # Get replica nodes before deleting from chunks_info
        replica_nodes = set()
        chunks_info = file_info.get('chunks_info', {})
        for chunk_id, chunk_data in chunks_info.items():
            nodes_list = chunk_data.get('nodes', [])
            if isinstance(nodes_list, list):
                replica_nodes.update(nodes_list)
        
        replica_nodes = list(replica_nodes)  # Convert back to list
        print(f"🗑️  File {file_id} chunks stored on nodes: {replica_nodes}")
        
        # Update user storage
        user_info['used'] = max(0, user_info['used'] - file_size)
        del user_info['files'][file_id]
        
        # Save to disk
        save_user_to_disk(user_id, user_info)
        
        # Mark file as deleted in controller tracker (prevents re-sync)
        _mark_file_as_deleted(user_id, file_id)
        
        # Update in-memory cache
        if user_id in user_storage:
            user_storage[user_id] = user_info
        
        # Clean up temp file from memory
        temp_files.pop(file_id, None)
        
        # Delete chunks from all replica nodes and trigger node metadata reload
        import shutil
        deleted_from_nodes = []
        
        for node_id in replica_nodes:
            if not node_id:  # Skip empty node IDs
                continue
            try:
                # Delete the file directory
                chunk_dir = os.path.join(f"node_storage_{node_id}", "files", file_id)
                if os.path.exists(chunk_dir):
                    shutil.rmtree(chunk_dir)
                    print(f"✅ Deleted chunks from node {node_id}")
                    deleted_from_nodes.append(node_id)
                else:
                    print(f"ℹ️  No chunks directory found for {node_id}")
            except Exception as e:
                print(f"⚠️  Could not delete chunks from {node_id}: {e}")
        
        # Force nodes to recalculate their used storage on next heartbeat
        # by removing their metadata files
        for node_id in deleted_from_nodes:
            try:
                metadata_file = os.path.join(f"node_storage_{node_id}", "storage_metadata.json")
                if os.path.exists(metadata_file):
                    os.remove(metadata_file)
                    print(f"✅ Cleared metadata cache for {node_id}")
            except Exception as e:
                print(f"⚠️  Could not clear metadata for {node_id}: {e}")
        
        # Notify controller to delete the file from its persistent storage
        try:
            controller_response = send_to_controller('DELETE_FILE', file_id=file_id, node_id=user_id)
            if controller_response and controller_response.get('status') == 'OK':
                print(f"✅ Notified controller to delete file {file_id}")
            else:
                print(f"⚠️  Controller did not delete file {file_id}: {controller_response}")
        except Exception as e:
            print(f"⚠️  Could not notify controller: {e}")
        
        return jsonify({
            'success': True, 
            'message': f'File deleted successfully (removed from {len(deleted_from_nodes)} nodes)',
            'freed_space': file_size / (1024**3),
            'nodes_cleaned': deleted_from_nodes
        })
        
    except Exception as e:
        print(f"Error deleting file {file_id}: {e}")
        return jsonify({'success': False, 'message': f'Delete failed: {str(e)}'}), 500


@app.route('/api/profile')
@login_required
def get_profile():
    """Get user profile"""
    user_id = session['user_id']
    user_data = user_storage.get(user_id, {})
    return jsonify({
        'email': session.get('email'),
        'user_id': user_id,
        'role': session.get('role'),
        'joined': user_data.get('created_at', '')
    })


# ==================== ADMIN DASHBOARD ====================

@app.route('/admin-dashboard')
@admin_required
def admin_dashboard():
    """Admin dashboard for system monitoring"""
    return render_template('index.html')


@app.route('/api/stats')
@admin_required
def get_stats():
    """Get network statistics from controller"""
    try:
        response = send_to_controller('GET_STATS')
        
        if response and response.get('status') == 'OK':
            return jsonify({
                'active_nodes': response.get('active_nodes', 0),
                'total_nodes': response.get('total_nodes', 0),
                'storage_utilization': response.get('storage_utilization', 0),
                'used_storage': response.get('used_storage', 0),
                'total_storage': response.get('total_storage', 0),
                'total_transfers': response.get('total_transfers', 0),
                'total_files': response.get('total_files', 0)
            })
        else:
            return jsonify({'error': 'Controller error'}), 500
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/nodes')
@admin_required
def get_nodes():
    """Get all nodes from controller"""
    try:
        response = send_to_controller('GET_NODES')
        
        if response and response.get('status') == 'OK':
            return jsonify(response.get('nodes', []))
        else:
            return jsonify([])
    except Exception as e:
        print(f"Error: {e}")
        return jsonify([])


@app.route('/api/files')
@admin_required
def get_files():
    """Get all files from controller"""
    try:
        response = send_to_controller('LIST_FILES')
        
        if response and response.get('status') == 'OK':
            files_list = []
            for file_data in response.get('files', []):
                files_list.append({
                    'id': file_data['file_id'],
                    'name': file_data['file_name'],
                    'size': file_data['file_size'] // (1024 * 1024),
                    'owner': file_data['owner_node'],
                    'replicas': file_data['replica_count'],
                    'status': 'available'
                })
            return jsonify(files_list)
        else:
            return jsonify([])
    except Exception as e:
        print(f"Error fetching files: {e}")
        return jsonify([])


@app.route('/api/nodes', methods=['POST'])
@admin_required
def create_node():
    """Create a new node process"""
    data = request.json
    node_id = data.get('nodeId')
    
    if not node_id:
        return jsonify({'status': 'error', 'message': 'Node ID required'}), 400

    try:
        send_to_controller('UNBLOCK_NODE', node_id=node_id)
    except Exception:
        pass

    storage_dir = f"node_storage_{node_id}"
    if not os.path.exists(storage_dir):
        try:
            os.makedirs(storage_dir)
        except Exception as e:
            return jsonify({'status': 'error', 'message': f'Failed to create storage folder: {e}'}), 500
    
    try:
        import subprocess
        command = [
            'python', 'clean_node.py',
            '--node-id', node_id,
            '--cpu', str(data.get('cpu', 4)),
            '--memory', str(data.get('memory', 16)),
            '--storage', str(data.get('storage', 1000)),
            '--bandwidth', str(data.get('bandwidth', 1000)),
            '--controller-host', CONTROLLER_HOST,
            '--controller-port', str(CONTROLLER_PORT)
        ]
        subprocess.Popen(command)
        time.sleep(2.5)
        
        verify = send_to_controller('GET_NODES')
        found = False
        if verify and verify.get('status') == 'OK':
            found = any(n.get('id') == node_id for n in verify.get('nodes', []))
        
        return jsonify({'status': 'success', 'message': f'Node {node_id} spawned', 'registered': found}), 201
    except Exception as e:
        print(f"Error creating node: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/nodes/<node_id>/toggle', methods=['PUT'])
@admin_required
def toggle_node_status(node_id):
    """Toggle node status"""
    try:
        response = send_to_controller('FORCE_OFFLINE', node_id=node_id)
        if response and response.get('status') == 'OK':
            return jsonify({'status': 'success', 'message': response.get('message', 'Node toggled')})
        else:
            msg = response.get('error', 'Failed to toggle node') if response else 'No response from controller'
            return jsonify({'status': 'error', 'message': msg}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/api/nodes/<node_id>', methods=['DELETE'])
@admin_required
def delete_node(node_id):
    """Delete a node"""
    try:
        import shutil
        response = send_to_controller('DELETE_NODE', node_id=node_id)
        if response and response.get('status') == 'OK':
            storage_dir = f"node_storage_{node_id}"
            storage_deleted = False
            storage_msg = None

            if os.path.isdir(storage_dir):
                pid_file = os.path.join(storage_dir, 'node.pid')
                if os.path.isfile(pid_file):
                    try:
                        with open(pid_file, 'r') as f:
                            pid_str = f.read().strip()
                        if pid_str.isdigit():
                            pid = int(pid_str)
                            try:
                                import subprocess
                                subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True)
                            except Exception:
                                pass
                            time.sleep(0.5)
                    except Exception:
                        pass
                try:
                    shutil.rmtree(storage_dir)
                    storage_deleted = True
                    storage_msg = f"Removed {storage_dir}"
                except Exception as e:
                    storage_msg = f"Failed to remove {storage_dir}: {e}"
            else:
                storage_msg = f"{storage_dir} not found"

            return jsonify({
                'status': 'success',
                'message': response.get('message', f'Node {node_id} deleted'),
                'storage_deleted': storage_deleted,
                'storage_message': storage_msg
            })
        else:
            msg = response.get('error', 'Failed to delete node') if response else 'No response from controller'
            return jsonify({'status': 'error', 'message': msg}), 400
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


if __name__ == '__main__':
    print("🚀 Cloud Storage UI starting...")
    print(f"📡 Controller at {CONTROLLER_HOST}:{CONTROLLER_PORT}")
    print(f"🔐 Auth Server at {AUTH_SERVER_HOST}:{AUTH_SERVER_PORT}")
    print("👤 User Dashboard at http://localhost:5001/dashboard")
    print("👨‍💼 Admin Dashboard at http://localhost:5001/admin-dashboard")
    print("   Admin: admin@gmail.com / admin123")
    app.run(debug=True, host='0.0.0.0', port=5001, use_reloader=False)