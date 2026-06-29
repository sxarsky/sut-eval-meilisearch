#!/usr/bin/env bash
set -euo pipefail

# Poll health endpoint before attempting to use the master key
timeout=300
elapsed=0
until curl -sf http://localhost:7700/health > /dev/null 2>&1; do
  if [ "$elapsed" -ge "$timeout" ]; then
    echo "Timed out waiting for Meilisearch to be ready" >&2
    exit 1
  fi
  sleep 5
  elapsed=$((elapsed + 5))
done

# The Meilisearch master key is used directly as the Bearer token
echo "testbot-master-key"
