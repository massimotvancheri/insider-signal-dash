#!/bin/bash
# Auto-deploy cron: checks GitHub every 60s, pulls, builds, restarts
cd /opt/insider-signal-dash
git fetch origin master 2>/dev/null
LOCAL=$(git rev-parse HEAD 2>/dev/null)
REMOTE=$(git rev-parse origin/master 2>/dev/null)
if [ -n "$LOCAL" ] && [ -n "$REMOTE" ] && [ "$LOCAL" != "$REMOTE" ]; then
  echo "$(date) - Deploying: $LOCAL -> $REMOTE"
  git pull origin master
  npm ci --production=false 2>&1 | tail -3
  # dist/ is pre-built and committed — skip build on e2-micro
  systemctl restart insider-signal
  echo "$(date) - Deploy complete"
else
  : # No changes, stay quiet
fi
