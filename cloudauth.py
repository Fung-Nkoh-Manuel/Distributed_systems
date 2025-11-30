import grpc
from concurrent import futures
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
from utils import send_otp
import threading
import subprocess
import time
import sys
import socket
import signal

# Firebase imports
import firebase_admin
from firebase_admin import credentials, auth

# Initialize Firebase Admin SDK
cred = credentials.Certificate("grcpauth-firebase-adminsdk-fbsvc-687effe5bf.json")
firebase_admin.initialize_app(cred)

# Temporary store for OTPs
otp_store = {}

# Global variable to track network controller process
network_controller_proc = None

class UserServiceSkeleton(cloudsecurity_pb2_grpc.UserServiceServicer):
    def signup(self, request, context):
        try:
            # Create user in Firebase
            user = auth.create_user(
                email=request.email,
                password=request.password,
                display_name=request.login
            )
            return cloudsecurity_pb2.Response(result="Signup successful")
        except Exception as e:
            return cloudsecurity_pb2.Response(result=f"Signup failed: {str(e)}")

    def login(self, request, context):
        try:
            # Get user by email to check if they exist
            user = auth.get_user_by_email(request.email)
            
            otp = send_otp(request.email)
            if otp:
                otp_store[request.email] = otp
                return cloudsecurity_pb2.Response(result="OTP sent to your email")
            else:
                return cloudsecurity_pb2.Response(result="Failed to send OTP")
        except Exception as e:
            return cloudsecurity_pb2.Response(result=f"Login failed: {str(e)}")

    def verifyOtp(self, request, context):
        if otp_store.get(request.email) == request.otp:
            # Clear OTP after successful verification
            del otp_store[request.email]
            return cloudsecurity_pb2.Response(result="Login successful")
        else:
            return cloudsecurity_pb2.Response(result="Invalid OTP")

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully"""
    print(f"\n🛑 Received interrupt signal. Shutting down gracefully...")
    if network_controller_proc:
        print("🛑 Stopping network controller...")
        network_controller_proc.terminate()
        network_controller_proc.wait()
    sys.exit(0)

def run():
    global network_controller_proc
    
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start network controller as a subprocess
    print('\n🌐 Starting Network Controller on port 5500...')
    try:
        network_controller_proc = subprocess.Popen(
            [sys.executable, 'threaded_network_server.py', '--host', '0.0.0.0', '--port', '5500'],
            stdout=None,
            stderr=None
        )

        # Wait until the network controller is accepting connections on port 5500
        start_ts = time.time()
        timeout = 15.0
        while True:
            try:
                with socket.create_connection(('127.0.0.1', 5500), timeout=1):
                    print('✅ Network Controller is listening on 127.0.0.1:5500\n')
                    break
            except Exception:
                if time.time() - start_ts > timeout:
                    print(f'❌ Timed out waiting for network controller to listen on port 5500')
                    network_controller_proc.terminate()
                    network_controller_proc.wait()
                    raise RuntimeError('Network controller failed to start')
                time.sleep(0.5)

    except Exception as e:
        print(f'❌ Failed to start network controller: {e}\n')
        sys.exit(1)

    # Start gRPC auth service
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cloudsecurity_pb2_grpc.add_UserServiceServicer_to_server(UserServiceSkeleton(), server)
    server.add_insecure_port('[::]:51234')
    print('🔐 Starting Auth Service on port 51234 ............', end='')
    server.start()
    print('[OK]')
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n🛑 Auth service interrupted. Shutting down...")
    finally:
        # Clean up network controller
        if network_controller_proc:
            print("🛑 Stopping network controller...")
            network_controller_proc.terminate()
            network_controller_proc.wait()
        print("✅ All services stopped.")

if __name__ == '__main__':
    run()