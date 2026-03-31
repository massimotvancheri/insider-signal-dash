#!/bin/bash
#
# Insider Signal Platform — Simple GCP Deployment (No Docker)
#
# This is the simpler approach: creates a VM, installs Node.js + Python
# directly, and runs the app as a systemd service.
#
# Prerequisites:
#   1. Google Cloud SDK installed: https://cloud.google.com/sdk/docs/install
#   2. Authenticated: gcloud auth login
#   3. Project created at https://console.cloud.google.com
#
# Usage:
#   chmod +x deploy-gcp-simple.sh
#   ./deploy-gcp-simple.sh
#

set -euo pipefail

PROJECT_ID="insidertradingquant"
ZONE="us-central1-a"
INSTANCE_NAME="insider-signal"
MACHINE_TYPE="e2-micro"

echo "=== Insider Signal Platform — Simple GCP Deployment ==="
echo ""

# Set project
gcloud config set project ${PROJECT_ID}

# Enable Compute Engine
gcloud services enable compute.googleapis.com

# Create VM with Ubuntu
echo "[1/4] Creating VM..."
gcloud compute instances create ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --machine-type=${MACHINE_TYPE} \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-standard \
  --tags=http-server \
  --metadata=startup-script='#!/bin/bash
    # This runs on first boot and every reboot
    
    if [ ! -f /opt/insider-signal/.installed ]; then
      echo "First boot — installing dependencies..."
      
      # Install Node.js 20
      curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
      apt-get install -y nodejs python3 python3-pip git unzip
      
      # Install Python packages
      pip3 install --break-system-packages yfinance pandas numpy
      
      # Clone/setup app directory
      mkdir -p /opt/insider-signal
      touch /opt/insider-signal/.installed
      
      echo "Dependencies installed"
    fi
    
    # Start the app if not running
    if ! systemctl is-active --quiet insider-signal; then
      systemctl start insider-signal 2>/dev/null || true
    fi
  '

echo "[2/4] Creating firewall rule..."
gcloud compute firewall-rules create allow-http-insider \
  --allow=tcp:80 \
  --target-tags=http-server \
  --description="Allow HTTP traffic" \
  2>/dev/null || echo "  (already exists)"

echo "[3/4] Waiting for VM to be ready..."
sleep 30

# Get the external IP
EXTERNAL_IP=$(gcloud compute instances describe ${INSTANCE_NAME} \
  --zone=${ZONE} \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')

echo "[4/4] VM ready at ${EXTERNAL_IP}"

echo ""
echo "=== VM Created ==="
echo ""
echo "Next steps — SSH into the VM and deploy the app:"
echo ""
echo "  gcloud compute ssh ${INSTANCE_NAME} --zone=${ZONE}"
echo ""
echo "Then run these commands on the VM:"
echo ""
cat << 'INSTRUCTIONS'
  # 1. Upload the project (from your local machine, run this in a new terminal):
  #    gcloud compute scp --recurse ./insider-signal-dash ${INSTANCE_NAME}:/opt/ --zone=us-central1-a

  # 2. On the VM, install and build:
  cd /opt/insider-signal-dash
  npm install
  npm run build
  
  # 3. Run the SEC backfill (takes ~5 min):
  npx drizzle-kit push
  npx tsx server/sec-backfill.ts 2016
  
  # 4. Run price enrichment (takes ~10 min for 500 tickers):
  python3 scripts/enrich-prices.py 500 2020
  
  # 5. Run factor research:
  python3 scripts/factor-research.py
  
  # 6. Create systemd service for auto-restart:
  sudo tee /etc/systemd/system/insider-signal.service << EOF
[Unit]
Description=Insider Signal Platform
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/insider-signal-dash
Environment=NODE_ENV=production
Environment=PORT=80
ExecStart=/usr/bin/node dist/index.cjs
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

  sudo systemctl daemon-reload
  sudo systemctl enable insider-signal
  sudo systemctl start insider-signal
  
  # 7. Verify it's running:
  sudo systemctl status insider-signal
  curl http://localhost/api/status
INSTRUCTIONS

echo ""
echo "Dashboard will be at: http://${EXTERNAL_IP}"
echo ""
echo "=== Cost ==="
echo "e2-micro in us-central1: FREE (Always Free tier)"
echo "20GB standard disk: ~\$0.80/month"
echo "Total: ~\$0.80/month"
