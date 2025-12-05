import grpc
from concurrent import futures
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
from utils import send_otp   # your email OTP sender

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

    def login(self, request, context):
        # Step 1: verify email/password with Firebase
        # (client normally does this via Firebase REST, but here we assume password is valid)
        otp = send_otp(request.email)
        otp_store[request.email] = otp
        return cloudsecurity_pb2.Response(result="OTP sent to your email")

    def verifyOtp(self, request, context):
        if otp_store.get(request.email) == request.otp:
            return cloudsecurity_pb2.Response(result="Login successful")
        else:
            return cloudsecurity_pb2.Response(result="Invalid OTP")

def run():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cloudsecurity_pb2_grpc.add_UserServiceServicer_to_server(UserServiceSkeleton(), server)
    server.add_insecure_port('[::]:51234')
    print('Starting Server on port 51234 ............', end='')
    server.start()
    print('[OK]')
    server.wait_for_termination()

if __name__ == '__main__':
    run()
