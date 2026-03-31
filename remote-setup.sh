#!/bin/bash
# This script runs ON the VM via gcloud compute ssh
# It sets up GitHub deploy key, clones the repo, and deploys everything
set -e

echo "=== Setting up GitHub deploy key ==="
ssh-keygen -t ed25519 -f /root/.ssh/github_deploy -N "" -q 2>/dev/null || true
echo ""
echo "============================================"
echo "ADD THIS DEPLOY KEY TO YOUR GITHUB REPO:"
echo "============================================"
echo ""
cat /root/.ssh/github_deploy.pub
echo ""
echo "Go to: https://github.com/massimotvancheri/insider-signal-dash/settings/keys"
echo "Click 'Add deploy key', paste the key above, check 'Allow write access'"
echo ""
read -p "Press ENTER after you've added the key..."

# Configure SSH to use deploy key for GitHub
cat > /root/.ssh/config << 'SSHEOF'
Host github.com
  IdentityFile /root/.ssh/github_deploy
  StrictHostKeyChecking no
SSHEOF
chmod 600 /root/.ssh/config

echo "=== Cloning repository ==="
cd /opt
rm -rf insider-signal-dash-new
git clone git@github.com:massimotvancheri/insider-signal-dash.git insider-signal-dash-new

# Preserve the database if it exists
if [ -f /opt/insider-signal-dash/data.db ]; then
  cp /opt/insider-signal-dash/data.db /opt/insider-signal-dash-new/data.db
  echo "Database preserved"
fi

# Swap directories
rm -rf /opt/insider-signal-dash-old
mv /opt/insider-signal-dash /opt/insider-signal-dash-old 2>/dev/null || true
mv /opt/insider-signal-dash-new /opt/insider-signal-dash
cd /opt/insider-signal-dash

echo "=== Installing and building ==="
npm ci --production=false 2>&1 | tail -3
npm run build 2>&1 | tail -3

echo "=== Checking database ==="
TXCOUNT=$(sqlite3 data.db "SELECT count(*) FROM insider_transactions WHERE transaction_type='P';" 2>/dev/null || echo "0")
echo "Transactions in DB: $TXCOUNT"

if [ "$TXCOUNT" -lt "1000" ]; then
  echo "=== Database needs backfill - running in background ==="
  npx drizzle-kit push 2>&1 | tail -2
  nohup bash -c 'cd /opt/insider-signal-dash && npx tsx server/sec-backfill.ts 2016 && python3 scripts/enrich-prices.py 500 2020 && python3 scripts/factor-research.py' > /var/log/backfill.log 2>&1 &
  echo "Backfill running in background (PID $!) - check: tail -f /var/log/backfill.log"
fi

echo "=== Setting up auto-deploy cron ==="
cat > /opt/deploy.sh << 'DEPLOYEOF'
#!/bin/bash
cd /opt/insider-signal-dash
git fetch origin master
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/master)
if [ "$LOCAL" != "$REMOTE" ]; then
  echo "$(date) - New changes detected, deploying..."
  git pull origin master
  npm ci --production=false
  npm run build
  systemctl restart insider-signal
  echo "$(date) - Deploy complete"
fi
DEPLOYEOF
chmod +x /opt/deploy.sh

# Check for updates every 60 seconds
(crontab -l 2>/dev/null; echo "* * * * * /opt/deploy.sh >> /var/log/auto-deploy.log 2>&1") | sort -u | crontab -

echo "=== Creating/restarting service ==="
cat > /etc/systemd/system/insider-signal.service << 'SVCEOF'
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
systemctl restart insider-signal
sleep 2

EXTERNAL_IP=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor:Google")

if systemctl is-active --quiet insider-signal; then
  echo ""
  echo "============================================"
  echo "DEPLOYMENT COMPLETE"
  echo "Dashboard: http://${EXTERNAL_IP}"
  echo ""
  echo "Auto-deploy is ON: pushes to GitHub deploy within 60 seconds"
  echo "============================================"
else
  echo "SERVICE FAILED - checking logs:"
  journalctl -u insider-signal --no-pager -n 20
fi
