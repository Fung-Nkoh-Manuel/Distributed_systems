# ⚡ Quick Start Guide

Get the Cloud Storage System running in 5 minutes!

## 📋 Prerequisites

- Python 3.8 or higher
- Git
- Firebase Admin SDK credentials (optional, for real auth)

## 🚀 Setup (5 minutes)

### Step 1: Clone & Install (1 minute)
```bash
# Clone repository
git clone https://github.com/yourusername/cloud-storage-system.git
cd cloud-storage-system

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Firebase Setup (Optional, 2 minutes)
If you want real authentication with Firebase:

1. Go to [Firebase Console](https://console.firebase.google.com)
2. Create a new project or select existing one
3. Download service account key (Project Settings → Service Accounts)
4. Save as `grcpauth-firebase-adminsdk-fbsvc-687effe5bf.json` in project root

**Skip this if using demo mode** ✓

### Step 3: Start Services (2 minutes)

Open **4 terminal windows** in the project directory. **Make sure venv is activated in each!**

#### Terminal 1: Auth Server
```bash
python cloudauth.py
```
Expected:
```
Starting Server on port 51234 ............[OK]
```

#### Terminal 2: Controller
```bash
python clean_controller.py
```
Expected:
```
🚀 ENHANCED DISTRIBUTED CLOUD STORAGE CONTROLLER
🌐 Clean Controller started on 0.0.0.0:5000
🔍 Looking for existing node storage folders...
🚀 Spawning 5 new CleanNode instances...
   ➕ Node N1 spawned
   ➕ Node N2 spawned
   ➕ Node N3 spawned
   ➕ Node N4 spawned
   ➕ Node N5 spawned
```

#### Terminal 3: (Nodes are auto-spawned)
✓ Nodes are automatically started by the controller. You're good!

#### Terminal 4: Web UI
```bash
python app.py
```
Expected:
```
🚀 Cloud Storage UI starting...
📡 Controller at localhost:5000
🔐 Auth Server at localhost:51234
👤 User Dashboard at http://localhost:5001/dashboard
👨‍💼 Admin Dashboard at http://localhost:5001/admin-dashboard
   Admin: admin@gmail.com / admin123
```

## 🎯 Try It Out!

### Access the System

| Role | URL | Credentials |
|------|-----|-------------|
| **Admin** | http://localhost:5001/admin-dashboard | `admin@gmail.com` / `admin123` |
| **User** | http://localhost:5001/dashboard | Create new account or use test account |

### Admin Features (3 minutes)
1. Open http://localhost:5001/admin-dashboard
2. Login with: `admin@gmail.com` / `admin123`
3. **Explore the dashboard:**
   - See active nodes (N1-N5)
   - View storage utilization
   - See all files in network
   - Monitor transfers
   - Create new nodes (click "+ Add Node")
   - Toggle nodes online/offline
   - Delete nodes

### User Features (2 minutes)
1. Go to http://localhost:5001 and click "Signup"
2. Create account with:
   - Username: `testuser`
   - Email: `test@example.com`
   - Password: `test123`
3. Login and receive OTP (check console or use demo OTP)
4. **Upload a file:**
   - Click "+ Upload File"
   - Select any file from your computer
   - Watch it upload
5. **Download file:**
   - Click "Download" on your file
   - File downloads to your computer
6. **Delete file:**
   - Click "Delete" on file
   - See storage quota update instantly

## 📊 What You'll See

### Admin Dashboard
```
🌐 Cloud Storage Controller
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📊 Network Status:
   Active Nodes: 5/5
   Storage Usage: 15.2%
   Active Transfers: 0
   Total Files: 3

🖥️ Network Nodes:
   [N1] ✅ 4 CPU • 16GB RAM • 1000GB Storage
   [N2] ✅ 2 CPU • 8GB RAM • 500GB Storage
   [N3] ✅ 6 CPU • 32GB RAM • 2000GB Storage
   [N4] ✅ 4 CPU • 16GB RAM • 1500GB Storage
   [N5] ✅ 2 CPU • 4GB RAM • 250GB Storage

📁 Files & Replicas:
   document.pdf (25 MB) - Replicas: 2/2
   image.jpg (5 MB) - Replicas: 2/2
   video.mp4 (500 MB) - Replicas: 2/2
```

### User Dashboard
```
☁️ CloudVault
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

💾 Storage: 25.5 GB / 100 GB (25.5%)

📄 My Files:
   document.pdf (25 MB) - Downloaded: 2/20 times
   image.jpg (5 MB)
   presentation.pptx (15 MB)

[+ Upload File]
```

## 🔧 Common Commands

### View Logs
```bash
# Watch auth server logs
tail -f *.log

# Check node storage
ls -la node_storage_N1/files/
```

### Add More Nodes (Admin Dashboard)
1. Click "+ Add Node" button
2. Enter Node ID: `N6`
3. Set resources:
   - CPU: 4
   - Memory: 16 GB
   - Storage: 1000 GB
   - Bandwidth: 1000 Mbps
4. Click "Create Node"

### Stop Everything
```bash
# Press Ctrl+C in each terminal window
# Or:
pkill -f "python"
```

### Reset Everything
```bash
# Remove all node data and user data
rm -rf node_storage_* user_storage/ controller_storage/

# Restart the system from Step 3
```

## 🆘 Troubleshooting

### "Port already in use"
```bash
# Find what's using port 5001
lsof -i :5001

# Kill it
kill -9 <PID>
```

### "Auth server won't start"
- Make sure port 51234 is free
- Check if another instance is running

### "No files showing in upload"
- Ensure all 4 services are running
- Check browser console for errors (F12)
- Try uploading small file first (< 10 MB)

### "Download fails"
- Ensure nodes are still running
- Check node_storage_N*/ directories exist
- Try refreshing the page

### "Can't login with OTP"
- Check console output for OTP value
- Use that OTP in the UI
- Or use admin account for testing

## 📝 Default Test Credentials

### Admin (Works immediately)
```
Email: admin@gmail.com
Password: admin123
```

### Create User Account
1. Go to http://localhost:5001
2. Click "Sign Up"
3. Fill form and submit
4. Use OTP from console output

## 🎓 Next Steps

1. **Explore the code:**
   - `app.py` - Main web server
   - `clean_controller.py` - Orchestration logic
   - `clean_node.py` - Storage nodes
   - `cloudauth.py` - Authentication

2. **Try Advanced Features:**
   - Create 10 nodes and see load balancing
   - Upload large files (test replication)
   - Take nodes offline and see recovery
   - Monitor real-time statistics

3. **Customize:**
   - Edit `app.py` to change admin credentials
   - Modify `CHUNK_SIZE` for different file split sizes
   - Adjust node resources for testing

4. **Deploy:**
   - See `README.md` Production Checklist
   - Use `gunicorn` instead of Flask dev server
   - Configure HTTPS/SSL
   - Set up proper logging

## 📚 Documentation

For detailed information, see:
- `README.md` - Full documentation
- Architecture diagrams and API endpoints
- Configuration options
- Deployment guide
- Troubleshooting tips

## 🎉 You're All Set!

Your cloud storage system is now running with:
- ✅ 5 active storage nodes
- ✅ Real-time network monitoring
- ✅ Automatic file replication
- ✅ User authentication
- ✅ Admin control panel

**Happy cloud storing! ☁️**

---

**Need help?** Open an issue on GitHub or check the README.md file.