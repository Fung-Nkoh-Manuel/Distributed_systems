import bcrypt
import grpc
from concurrent import futures
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc
from utils import send_otp

# Temporary store for OTPs (in-memory dictionary)
otp_store = {}

class UserServiceSkeleton(cloudsecurity_pb2_grpc.UserServiceServicer):
    def signup(self, request, context):
        # Hash the password
        hashed_pw = bcrypt.hashpw(request.password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        
        # Save to credentials file
        with open('credentials', 'a') as file:
            file.write(f"{request.login},{request.email},{hashed_pw}\n")
        
        return cloudsecurity_pb2.Response(result="Signup successful")

    def login(self, request, context):
        credentials = {}
        emails = {}
        with open('credentials', 'r') as file:
            for line in file:
                username, email, password = line.strip().split(',')
                credentials[username] = password
                emails[username] = email

        # Step 1: check username/password
        if not (credentials.get(request.login) and 
                bcrypt.checkpw(request.password.encode('utf-8'), credentials[request.login].encode('utf-8'))):
            return cloudsecurity_pb2.Response(result="Unauthorized")

        # Step 2: if OTP not provided, send it
        if not request.otp:
            otp = send_otp(emails[request.login])
            otp_store[request.login] = otp
            return cloudsecurity_pb2.Response(result="OTP sent to your email")

        # Step 3: if OTP provided, verify it
        if otp_store.get(request.login) == request.otp:
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
