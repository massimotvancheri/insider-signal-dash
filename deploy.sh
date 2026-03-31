#!/bin/bash
# Auto-deploy: pull latest from GitHub, build, restart
set -e
cd /opt/insider-signal-dash
git pull origin master
npm ci --production=false
npm run build
systemctl restart insider-signal
sleep 2
if systemctl is-active --quiet insider-signal; then
  echo "DEPLOY SUCCESS - $(date)"
  curl -s http://localhost/api/status | python3 -c "import sys,json; d=json.load(sys.stdin); print('Poller:', d.get('mode','unknown'))" 2>/dev/null || true
else
  echo "DEPLOY FAILED"
  journalctl -u insider-signal --no-pager -n 10
fi
