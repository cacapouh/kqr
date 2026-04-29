#!/usr/bin/env bash
# Seed a Kafka topic with a small JSON dataset for manual smoke tests.
#
#   scripts/seed.sh                  # 100 messages to "kqr-demo"
#   scripts/seed.sh bids 1000        # 1000 messages to "bids"
#
# Pre-req: docker compose -f docker/compose.yaml up -d --wait

set -euo pipefail

cd "$(dirname "$0")/.."
COMPOSE="docker/compose.yaml"

TOPIC="${1:-kqr-demo}"
COUNT="${2:-100}"

if ! docker compose -f "$COMPOSE" ps --status running --format '{{.Service}}' | grep -qx kafka; then
    echo "[seed] Kafka container is not running."
    echo "[seed] Start it with: docker compose -f $COMPOSE up -d --wait"
    exit 1
fi

echo "[seed] producing $COUNT messages → topic '$TOPIC'"

now_epoch_ms="$(($(date -u +%s) * 1000))"

awk -v count="$COUNT" -v base_ts="$now_epoch_ms" '
BEGIN {
    sides[0] = "buy"; sides[1] = "sell"
    srand(base_ts)
    for (i = 1; i <= count; i++) {
        ts = base_ts + i * 17
        price = int(rand() * 10000) / 100.0
        side = sides[i % 2]
        printf("{\"id\":%d,\"ts\":%d,\"price\":%.2f,\"side\":\"%s\"}\n", i, ts, price, side)
    }
}' | docker compose -f "$COMPOSE" exec -T kafka \
        /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server localhost:9092 \
            --topic "$TOPIC"

echo "[seed] done"
