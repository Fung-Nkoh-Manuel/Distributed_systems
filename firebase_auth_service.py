import firebase_admin
from firebase_admin import credentials, auth
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
from utils import send_otp
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('env')

firebase_creds_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
cred = credentials.Certificate(firebase_creds_path)
firebase_admin.initialize_app(cred)

otp_store = {}

class FirebaseUserService(cloudsecurity_pb2_grpc.UserServiceServicer):
    def signup(self, request, context):
        try:
            user = auth.create_user(
                email=request.email,
                password=request.password,
                display_name=request.login
            )
            return cloudsecurity_pb2.Response(result="Signup successful")
        except Exception as e:
            return cloudsecurity_pb2.Response(result=f"Signup failed: {str(e)}")

    def login(self, request, context):
        # Step 1: verify email/password with Firebase REST API (client side usually)
        # Step 2: send OTP to email
        if not request.otp:
            otp = send_otp(request.email)
            otp_store[request.email] = otp
            return cloudsecurity_pb2.Response(result="OTP sent to your email")

        # Step 3: verify OTP
        if otp_store.get(request.email) == request.otp:
            return cloudsecurity_pb2.Response(result="Login successful")
        else:
            return cloudsecurity_pb2.Response(result="Invalid OTP")
