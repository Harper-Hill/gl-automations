#!/bin/bash
set -e
cd "$(dirname "$0")"
netlify deploy --prod
# Usage: ADMIN_KEY=... SA_JSON=~/path/to/new-service-account.json ./deploy.sh
: "${ADMIN_KEY:?Set ADMIN_KEY (same value as the Netlify ADMIN_KEY env var)}"
if [ -n "$SA_JSON" ]; then
  curl -sS -X POST "https://gl-automations.netlify.app/.netlify/functions/seed-sa" \
    -H "Content-Type: application/json" -H "X-Admin-Key: $ADMIN_KEY" \
    --data-binary @"$SA_JSON"
else
  echo "SA_JSON not set - skipping service-account seed"
fi
echo ""
echo "Deploy + seed complete."
