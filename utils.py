import bcrypt
import random
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from params import from_email, from_password   # keep your credentials in params.py


# -----------------------------
# Password hashing
# -----------------------------
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'),
                         bcrypt.gensalt()).decode('utf-8')


# -----------------------------
# OTP generation
# -----------------------------
def generate_otp() -> str:
    return str(random.randint(100000, 999999))


# -----------------------------
# Send OTP via Gmail
# -----------------------------
def send_otp(to_email: str) -> str:
    otp = generate_otp()

    subject = "Your OTP Code for the Cloud Security Simulator"
    body = f"Your OTP code is: {otp}"

    # Build email
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            print("Starting TLS session...")
            server.starttls()
            print("Logging in...")
            server.login(from_email, from_password)
            print("Sending email...")
            server.send_message(msg)
            print(f"OTP sent to {to_email} successfully!")
            return otp   # return OTP for later verification
    except Exception as e:
        print(f"Failed to send email: {e}")
        return None


# -----------------------------
# Main program
# -----------------------------
if __name__ == '__main__':
    # Load plain credentials from file
    credentials = {}
    file_path = 'ids'   # file containing username,password pairs
    with open(file_path, 'r') as file:
        for line in file:
            username, password = line.strip().split(',')
            credentials[username] = password

    # Write hashed credentials to new file
    with open('credentials', 'w') as file:
        for username, password in credentials.items():
            file.write(f'{username},{hash_password(password)}\n')

    # Example: send OTP to a test email
    recipient = "recipient@example.com"
    otp = send_otp(recipient)
    if otp:
        print(f"Generated OTP: {otp}")
