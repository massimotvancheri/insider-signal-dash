#!/bin/bash
#
# Insider Signal Platform — VM Setup Script
# Run this ONCE on a fresh Ubuntu VM after uploading the project.
#
# This script:
#   1. Installs Node.js 20, Python 3, pip packages
#   2. Installs npm dependencies and builds the app
#   3. Pushes the database schema
#   4. Runs the 10-year SEC backfill (~5 min)
#   5. Runs price enrichment for top 500 tickers (~10 min)
#   6. Runs factor research analysis
#   7. Creates a systemd service with auto-restart
#   8. Starts the service on port 80
#
# Usage: sudo bash setup-vm.sh
#

set -euo pipefail

APP_DIR="/opt/insider-signal-dash"
LOG_FILE="/var/log/insider-signal-setup.log"

echo "=== Insider Signal Platform — Full VM Setup ===" | tee ${LOG_FILE}
echo "Started at $(date)" | tee -a ${LOG_FILE}
echo "" | tee -a ${LOG_FILE}

# Step 1: Install system dependencies
echo "[1/8] Installing system dependencies..." | tee -a ${LOG_FILE}
curl -fsSL https://deb.nodesource.com/setup_20.x | bash - >> ${LOG_FILE} 2>&1
apt-get install -y nodejs python3 python3-pip unzip >> ${LOG_FILE} 2>&1
pip3 install yfinance pandas numpy >> ${LOG_FILE} 2>&1 || pip3 install --break-system-packages yfinance pandas numpy >> ${LOG_FILE} 2>&1
echo "  Node.js $(node -v), Python $(python3 --version)" | tee -a ${LOG_FILE}

# Step 2: Install npm dependencies and build
echo "[2/8] Installing npm packages and building..." | tee -a ${LOG_FILE}
cd ${APP_DIR}
npm ci >> ${LOG_FILE} 2>&1
npm run build >> ${LOG_FILE} 2>&1
echo "  Build complete" | tee -a ${LOG_FILE}

# Step 3: Push database schema
echo "[3/8] Creating database tables..." | tee -a ${LOG_FILE}
npx drizzle-kit push >> ${LOG_FILE} 2>&1
echo "  Schema pushed" | tee -a ${LOG_FILE}

# Step 4: SEC Backfill (10 years)
echo "[4/8] Running SEC historical backfill (2016-2025)..." | tee -a ${LOG_FILE}
echo "  This downloads ~400MB of SEC data and takes ~5 minutes..."
npx tsx server/sec-backfill.ts 2016 2>&1 | tee -a ${LOG_FILE}

# Step 5: Price enrichment (top 500 tickers)
echo "[5/8] Running market data enrichment (500 tickers)..." | tee -a ${LOG_FILE}
echo "  This fetches historical prices and takes ~10 minutes..."
python3 scripts/enrich-prices.py 500 2020 2>&1 | tee -a ${LOG_FILE}

# Step 6: Factor research
echo "[6/8] Running factor research engine..." | tee -a ${LOG_FILE}
python3 scripts/factor-research.py 2>&1 | tee -a ${LOG_FILE}

# Step 7: Create systemd service
echo "[7/8] Creating systemd service..." | tee -a ${LOG_FILE}
cat > /etc/systemd/system/insider-signal.service << 'EOF'
[Unit]
Description=Insider Signal Quant Research Platform
After=network.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
User=root
WorkingDirectory=/opt/insider-signal-dash
Environment=NODE_ENV=production
Environment=PORT=80
ExecStart=/usr/bin/node dist/index.cjs
Restart=always
RestartSec=3
StandardOutput=journal
StandardError=journal

# Watchdog — restart if unresponsive
WatchdogSec=120

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable insider-signal
echo "  Service created and enabled" | tee -a ${LOG_FILE}

# Step 8: Start the service
echo "[8/8] Starting the service..." | tee -a ${LOG_FILE}
systemctl start insider-signal
sleep 3

# Verify
if systemctl is-active --quiet insider-signal; then
  echo "" | tee -a ${LOG_FILE}
  echo "=== DEPLOYMENT COMPLETE ===" | tee -a ${LOG_FILE}
  echo "" | tee -a ${LOG_FILE}
  
  # Get external IP
  EXTERNAL_IP=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor: Google" 2>/dev/null || echo "unknown")
  
  echo "Dashboard URL: http://${EXTERNAL_IP}" | tee -a ${LOG_FILE}
  echo "" | tee -a ${LOG_FILE}
  
  # Show stats
  echo "=== Database Stats ===" | tee -a ${LOG_FILE}
  sqlite3 /opt/insider-signal-dash/data.db "SELECT 'Transactions: ' || COUNT(*) FROM insider_transactions WHERE transaction_type='P';" | tee -a ${LOG_FILE}
  sqlite3 /opt/insider-signal-dash/data.db "SELECT 'Signals: ' || COUNT(*) FROM purchase_signals;" | tee -a ${LOG_FILE}
  sqlite3 /opt/insider-signal-dash/data.db "SELECT 'Enriched: ' || COUNT(DISTINCT signal_id) FROM signal_entry_prices;" | tee -a ${LOG_FILE}
  sqlite3 /opt/insider-signal-dash/data.db "SELECT 'Forward returns: ' || COUNT(*) FROM daily_forward_returns;" | tee -a ${LOG_FILE}
  sqlite3 /opt/insider-signal-dash/data.db "SELECT 'Factor analyses: ' || COUNT(*) FROM factor_analysis;" | tee -a ${LOG_FILE}
  sqlite3 /opt/insider-signal-dash/data.db "SELECT 'Model factors: ' || COUNT(*) FROM model_weights;" | tee -a ${LOG_FILE}
  
  echo "" | tee -a ${LOG_FILE}
  echo "=== Poller Status ===" | tee -a ${LOG_FILE}
  sleep 2
  curl -s http://localhost/api/status | python3 -m json.tool 2>/dev/null | tee -a ${LOG_FILE} || echo "  (waiting for server...)"
  
  echo "" | tee -a ${LOG_FILE}
  echo "=== Management Commands ===" | tee -a ${LOG_FILE}
  echo "View logs:     journalctl -u insider-signal -f" | tee -a ${LOG_FILE}
  echo "Restart:       systemctl restart insider-signal" | tee -a ${LOG_FILE}
  echo "Stop:          systemctl stop insider-signal" | tee -a ${LOG_FILE}
  echo "Status:        systemctl status insider-signal" | tee -a ${LOG_FILE}
  echo "" | tee -a ${LOG_FILE}
  echo "Completed at $(date)" | tee -a ${LOG_FILE}
else
  echo "ERROR: Service failed to start!" | tee -a ${LOG_FILE}
  journalctl -u insider-signal --no-pager -n 20 | tee -a ${LOG_FILE}
  exit 1
fi
