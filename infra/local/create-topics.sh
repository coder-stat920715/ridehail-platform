#!/usr/bin/env bash
# Pre-creates topics (the Kafka REST proxy refuses to produce to topics that don't exist).
set -euo pipefail
for t in outbox.event.trip outbox.event.payment-command outbox.event.matching-command \
         outbox.event.payment outbox.event.matching outbox.event.driver-location; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$t.DLT" --partitions 3 --replication-factor 1
done
