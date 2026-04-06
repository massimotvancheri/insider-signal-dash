#!/bin/bash
# Idempotent systemd setup for both insider-signal services
# Safe to run multiple times — always writes latest unit files

set -e

echo "Setting up systemd services..."

# Web server service
cat > /etc/systemd/system/insider-signal.service << 'EOF'
[Unit]
Description=Insider Signal Dashboard
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/insider-signal-dash
ExecStart=/usr/bin/node dist/index.cjs
Environment=NODE_ENV=production PORT=80
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# EDGAR poller service (separate process)
cat > /etc/systemd/system/insider-signal-poller.service << 'EOF'
[Unit]
Description=Insider Signal EDGAR Poller
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/insider-signal-dash
ExecStart=/usr/bin/node dist/poller.cjs
Environment=NODE_ENV=production
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable insider-signal
systemctl enable insider-signal-poller
systemctl start insider-signal-poller 2>/dev/null || true

echo "Services configured. Run: systemctl restart insider-signal insider-signal-poller"
