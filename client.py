import sys
import grpc
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
import subprocess
import time
import os

def run_signup(login, password, email):
    """Signup a new user via Firebase (server handles creation)."""
    with grpc.insecure_channel('localhost:51234') as channel:
        stub = cloudsecurity_pb2_grpc.UserServiceStub(channel)
        response = stub.signup(
            cloudsecurity_pb2.SignupRequest(login=login, password=password, email=email)
        )
        print(f"Result: {response.result}")
        return response

def start_storage_node(username, network_port=5500, foreground=False):
    """Spawn a storage node subprocess for the given username.
    
    This will use existing user folder if it exists.
    """
    node_id = username
    cmd = [
        sys.executable, 'threaded_node_server.py',
        '--node-id', node_id,
        '--network-port', str(network_port),
        '--use-existing-folder'  # New flag to use existing folder
    ]
    try:
        print(f"\n🖥️  Starting storage node '{node_id}' connecting to port {network_port}...")
        
        # Check if user folder exists
        current_dir = os.path.dirname(os.path.abspath(__file__))
        user_folder = os.path.join(current_dir, f"storage_{node_id}")
        if os.path.exists(user_folder):
            print(f"📁 Using existing storage folder: {user_folder}")
        else:
            print(f"📁 Creating new storage folder: {user_folder}")

        if foreground:
            # Run in the same terminal and wait (interactive)
            print("Running node in foreground. Press Ctrl+C to stop the node and return.")
            completed = subprocess.run(cmd)
            print(f"Storage node process exited with return code {completed.returncode}")
            return None

        # Background mode
        creationflags = 0
        if sys.platform == 'win32' and hasattr(subprocess, 'CREATE_NEW_CONSOLE'):
            creationflags = subprocess.CREATE_NEW_CONSOLE

        proc = subprocess.Popen(cmd, creationflags=creationflags)
        time.sleep(2)
        print(f"✅ Storage node '{node_id}' started (pid={proc.pid}).")
        return proc
    except Exception as e:
        print(f"❌ Failed to start storage node: {e}")
        return None

def run_login(email, password, foreground=False):
    """Login with email/password and OTP verification"""
    with grpc.insecure_channel('localhost:51234') as channel:
        stub = cloudsecurity_pb2_grpc.UserServiceStub(channel)

        # Step 1: login with email/password to get OTP
        response = stub.login(cloudsecurity_pb2.LoginRequest(email=email, password=password))
        print(f"Result: {response.result}")

        if "OTP sent" in response.result:
            # Step 2: Verify OTP
            entered_otp = input("Enter the OTP sent to your email: ")
            response = stub.verifyOtp(cloudsecurity_pb2.OtpRequest(email=email, otp=entered_otp))
            print(f"Result: {response.result}")

            # Step 3: If OTP is correct, start the storage node
            if "successful" in response.result.lower():
                # Extract username from email (or use email as username)
                username = email.split('@')[0]  # Use email prefix as username
                print(f"✅ Login successful! Starting storage node for user: {username}")
                start_storage_node(username, network_port=5500, foreground=foreground)
                return True
            else:
                print("❌ Login failed: Invalid OTP")
                return False
        else:
            print(f"❌ Login failed: {response.result}")
            return False

if __name__ == '__main__':
    # Handle --foreground flag
    foreground = False
    if '--foreground' in sys.argv:
        foreground = True
        sys.argv = [a for a in sys.argv if a != '--foreground']

    if len(sys.argv) < 2:
        print("Usage: python client.py <signup|login> <username> <password> <email> [--foreground]")
        print("       python client.py login <password> <email> [--foreground]")
        sys.exit(1)

    action = sys.argv[1].lower()

    if action == "signup":
        if len(sys.argv) < 5:
            print("Usage: python client.py signup <username> <password> <email> [--foreground]")
            sys.exit(1)
        login = sys.argv[2]
        password = sys.argv[3]
        email = sys.argv[4]
        resp = run_signup(login, password, email)
        if resp and ("success" in resp.result.lower()):
            start_storage_node(login, network_port=5500, foreground=foreground)
        else:
            print("Signup did not succeed; node not started.")

    elif action == "login":
        if len(sys.argv) < 4:
            print("Usage: python client.py login <password> <email> [--foreground]")
            sys.exit(1)
        password = sys.argv[2]
        email = sys.argv[3]
        run_login(email, password, foreground)

    else:
        print("Invalid action. Use 'signup' or 'login'.")
        sys.exit(1)