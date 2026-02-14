#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "Usage: $0 <BOT_TOKEN> <PUBLIC_BASE_URL> <WEBHOOK_SECRET>"
  exit 1
fi

BOT_TOKEN="$1"
PUBLIC_BASE_URL="${2%/}"
WEBHOOK_SECRET="$3"

curl -sS "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"${PUBLIC_BASE_URL}/telegram/webhook\",\"secret_token\":\"${WEBHOOK_SECRET}\",\"allowed_updates\":[\"message\",\"channel_post\"]}"
echo
