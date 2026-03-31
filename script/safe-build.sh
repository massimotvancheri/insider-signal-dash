#!/bin/bash
# Safe build wrapper: skips build if pre-built dist/index.cjs exists (from git)
# This prevents the e2-micro VM from deleting a working dist/ during a failed build
if [ -f "dist/index.cjs" ]; then
  echo "dist/index.cjs already exists (from git), skipping build"
  exit 0
fi
npx tsx script/build.ts
