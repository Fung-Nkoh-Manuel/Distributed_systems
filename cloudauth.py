import grpc
from concurrent import futures
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
from utils import send_otp   # your email OTP sender
import threading
import subprocess
import time
import sys
import socket

# Firebase imports
import firebase_admin
from firebase_admin import credentials, auth

# Initialize Firebase Admin SDK
cred = credentials.Certificate("grcpauth-firebase-adminsdk-fbsvc-687effe5bf.json")
firebase_admin.initialize_app(cred)

# Temporary store for OTPs
otp_store = {}

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

    # In the UserServiceSkeleton class in cloudauth.py

    def login(self, request, context):
        # Step 1: verify email/password with Firebase
        try:
            # Actually verify the password with Firebase
            user = auth.get_user_by_email(request.email)
            # If we get here, the user exists
            otp = send_otp(request.email)
            if otp:
                otp_store[request.email] = otp
                return cloudsecurity_pb2.Response(result="OTP sent to your email")
            else:
                return cloudsecurity_pb2.Response(result="Failed to send OTP")
        except Exception as e:
            return cloudsecurity_pb2.Response(result=f"Login failed: {str(e)}")

    def verifyOtp(self, request, context):
        stored_otp = otp_store.get(request.email)
        if stored_otp and stored_otp == request.otp:
            # Clear OTP after successful verification
            del otp_store[request.email]
            return cloudsecurity_pb2.Response(result="Login successful")
        else:
            return cloudsecurity_pb2.Response(result="Invalid OTP")

def run():
    # Start network controller as a subprocess (use same Python executable)
    print('\n🌐 Starting Network Controller on port 5500...')
    try:
        proc = subprocess.Popen(
            [sys.executable, 'threaded_network_server.py', '--host', '0.0.0.0', '--port', '5500'],
            stdout=None,  # inherit parent's stdout/stderr so user sees logs
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
                    proc.terminate()
                    proc.wait()
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
    server.wait_for_termination()

if __name__ == '__main__':
    run()
