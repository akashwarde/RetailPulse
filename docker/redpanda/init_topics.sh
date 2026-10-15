#!/usr/bin/env bash
set -euo pipefail

BROKER="${REDPANDA_BROKER:-redpanda:9092}"
echo "Connecting to Redpanda broker at ${BROKER}..."

TOPICS=("orders_stream" "orders_dlq")

for topic in "${TOPICS[@]}"; do
  echo "Checking topic '${topic}' on broker ${BROKER}..."
  if rpk topic list -X brokers="${BROKER}" 2>/dev/null | grep -qw "${topic}"; then
    echo "Topic '${topic}' already exists. Skipping creation."
  else
    echo "Creating topic '${topic}'..."
    rpk topic create "${topic}" -X brokers="${BROKER}" --partitions 1 --replicas 1
    echo "Topic '${topic}' created successfully."
  fi
done

echo "Verifying topic provisioning..."
for topic in "${TOPICS[@]}"; do
  if ! rpk topic list -X brokers="${BROKER}" | grep -qw "${topic}"; then
    echo "ERROR: Verification failed. Topic '${topic}' was not found on broker ${BROKER}." >&2
    exit 1
  fi
done

echo "All required streaming topics successfully verified on ${BROKER}."
