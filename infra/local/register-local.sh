#!/usr/bin/env bash
# Registers Debezium outbox connectors on the local Kafka Connect.
# Usage: ./register-local.sh [trip] [location] [payment] [matching]     (default: all four)
# Run AFTER the corresponding service has started once (Flyway creates its outbox table).
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICES=("$@"); [ ${#SERVICES[@]} -eq 0 ] && SERVICES=(trip location payment matching)
for svc in "${SERVICES[@]}"; do
  f="$DIR/debezium-$svc-outbox.local.json"
  name=$(python3 -c "import json;print(json.load(open('$f'))['name'])")
  echo "Registering $name"
  curl -fsS -X PUT -H 'Content-Type: application/json' \
    --data "$(python3 -c "import json;print(json.dumps(json.load(open('$f'))['config']))")" \
    "$CONNECT_URL/connectors/$name/config" | head -c 120; echo
done
echo; echo "Status:"
for svc in "${SERVICES[@]}"; do
  curl -fsS "$CONNECT_URL/connectors/$svc-outbox-connector/status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(d['name'],d['connector']['state'],[t['state'] for t in d['tasks']])"
done
