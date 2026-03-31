#!/bin/bash
# Fix the VM's /opt/deploy.sh to not rebuild (dist is pre-built in git)
# This runs as a postinstall hook to auto-repair the deploy cron
DEPLOY_SCRIPT="/opt/deploy.sh"
if [ -f "$DEPLOY_SCRIPT" ] && grep -q "npm run build" "$DEPLOY_SCRIPT"; then
  echo "Fixing /opt/deploy.sh to skip build step..."
  sed -i 's/npm run build/echo "skipping build (dist pre-built in git)"/' "$DEPLOY_SCRIPT"
  echo "Fixed."
fi
