import sys
import grpc
import cloudsecurity_pb2
import cloudsecurity_pb2_grpc

def run_signup(login, password, email):
    """Signup a new user via Firebase (server handles creation)."""
    with grpc.insecure_channel('localhost:51234') as channel:
        stub = cloudsecurity_pb2_grpc.UserServiceStub(channel)
        response = stub.signup(
            cloudsecurity_pb2.SignupRequest(login=login, password=password, email=email)
        )
        print(f"Result: {response.result}")

def run_login(email, password):
    with grpc.insecure_channel('localhost:51234') as channel:
        stub = cloudsecurity_pb2_grpc.UserServiceStub(channel)

        # Step 1: login with email/password
        response = stub.login(cloudsecurity_pb2.LoginRequest(email=email, password=password))
        print(f"Result: {response.result}")

        if "OTP sent" in response.result:
            entered_otp = input("Enter the OTP sent to your email: ")
            response = stub.verifyOtp(cloudsecurity_pb2.OtpRequest(email=email, otp=entered_otp))
            print(f"Result: {response.result}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python client.py <signup|login> <username> <password> <email>")
        sys.exit(1)

    action = sys.argv[1].lower()

    if action == "signup":
        if len(sys.argv) < 5:
            print("Usage: python client.py signup <username> <password> <email>")
            sys.exit(1)
        login = sys.argv[2]
        password = sys.argv[3]
        email = sys.argv[4]
        run_signup(login, password, email)

    elif action == "login":
        if len(sys.argv) < 4:
            print("Usage: python client.py login <password> <email>")
            sys.exit(1)
        password = sys.argv[2]
        email = sys.argv[3]
        run_login(email, password)

    else:
        print("Invalid action. Use 'signup' or 'login'.")
        sys.exit(1)
