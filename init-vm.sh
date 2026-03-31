#!/bin/bash
# ONE-TIME VM INITIALIZATION
# Run this once on a fresh VM to set up everything from scratch
set -e

echo "=== Installing dependencies ==="
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs python3 python3-pip unzip sqlite3 git
pip3 install yfinance pandas numpy

echo "=== Cloning from GitHub ==="
cd /opt
git clone https://github.com/massimotvancheri/insider-signal-dash.git
cd insider-signal-dash

echo "=== Building ==="
npm ci
npm run build

echo "=== Setting up database ==="
npx drizzle-kit push

echo "=== Running SEC backfill (10 years) ==="
npx tsx server/sec-backfill.ts 2016

echo "=== Starting enrichment in background ==="
nohup bash -c 'python3 scripts/enrich-prices.py 500 2020 && python3 scripts/factor-research.py' > /var/log/enrichment.log 2>&1 &

echo "=== Creating systemd service ==="
cat > /etc/systemd/system/insider-signal.service << SVCEOF
[Unit]
Description=Insider Signal Platform
After=network.target
[Service]
Type=simple
WorkingDirectory=/opt/insider-signal-dash
Environment=NODE_ENV=production
Environment=PORT=80
ExecStart=/usr/bin/node dist/index.cjs
Restart=always
RestartSec=3
[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable insider-signal
systemctl start insider-signal

EXTERNAL_IP=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor:Google")
echo ""
echo "=== DONE ==="
echo "Dashboard: http://${EXTERNAL_IP}"
