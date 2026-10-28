#!/usr/bin/env bash
# Registers (idempotently) the Debezium outbox connectors on Kafka Connect / MSK Connect.
# Prereqs on each PostgreSQL (RDS parameter group): rds.logical_replication=1 (wal_level=logical),
# and a "debezium" role with REPLICATION + SELECT on public.outbox.
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
for f in "$(dirname "$0")"/debezium-*-outbox.json; do
  name=$(basename "$f" .json)
  cname=$(python3 -c "import json,sys;print(json.load(open('$f'))['name'])")
  echo "Registering $cname"
  curl -fsS -X PUT -H 'Content-Type: application/json' \
       --data "$(python3 -c "import json;print(json.dumps(json.load(open('$f'))['config']))")" \
       "$CONNECT_URL/connectors/$cname/config" >/dev/null
done
echo "Done."
