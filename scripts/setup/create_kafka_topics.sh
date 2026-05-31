#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/setup/create_kafka_topics.sh
# Creates all required Kafka topics.
# Requires Kafka to be running (docker compose --profile messaging up -d kafka)
# =============================================================================

set -euo pipefail

KAFKA_CONTAINER="arcas-kafka"
BOOTSTRAP="localhost:9092"
REPLICATION=1   # Single-broker development setup

echo "============================================================"
echo "ARCAS - Creating Kafka topics"
echo "Bootstrap: $BOOTSTRAP"
echo "============================================================"

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
RETRIES=20
until docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  if [ $RETRIES -eq 0 ]; then
    echo "ERROR: Kafka did not become ready in time."
    exit 1
  fi
  echo "  Still waiting... ($RETRIES retries left)"
  sleep 5
done
echo "Kafka is ready."
echo ""

# Helper function
create_topic() {
  local TOPIC=$1
  local PARTITIONS=${2:-3}
  local RETENTION_MS=${3:-604800000}   # 7 days default

  if docker exec "$KAFKA_CONTAINER" kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --describe --topic "$TOPIC" > /dev/null 2>&1; then
    echo "  SKIP (already exists): $TOPIC"
  else
    docker exec "$KAFKA_CONTAINER" kafka-topics \
      --bootstrap-server "$BOOTSTRAP" \
      --create \
      --topic "$TOPIC" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION" \
      --config "retention.ms=$RETENTION_MS"
    echo "  CREATED: $TOPIC (partitions=$PARTITIONS, retention=${RETENTION_MS}ms)"
  fi
}

echo "--- Raw ingestion topics (per source type) ---"
create_topic "arcas.raw.gazette"         3 86400000    # 1 day - raw BOE / official gazette
create_topic "arcas.raw.procurement"     3 86400000    # 1 day - raw contract portal data
create_topic "arcas.raw.courts"          3 86400000    # 1 day - raw judicial records
create_topic "arcas.raw.media"           3 86400000    # 1 day - raw media articles
create_topic "arcas.raw.social"          3 86400000    # 1 day - raw social media posts
create_topic "arcas.raw.financial"       3 86400000    # 1 day - raw financial disclosures
create_topic "arcas.raw.icij"            1 86400000    # 1 day - ICIJ offshore leaks

echo ""
echo "--- Processing pipeline topics ---"
create_topic "arcas.normalised"          6 259200000   # 3 days - deduplicated, normalised
create_topic "arcas.nlp.extracted"       6 259200000   # 3 days - NLP-extracted entities/relations
create_topic "arcas.processed"           6 604800000   # 7 days - fully processed, pseudonymised

echo ""
echo "--- Knowledge graph topics ---"
create_topic "arcas.kg.entity.proposals" 3 604800000   # 7 days - entity merge proposals
create_topic "arcas.kg.entity.merged"    3 604800000   # 7 days - confirmed entity merges
create_topic "arcas.kg.relation.new"     3 604800000   # 7 days - new relationship proposals

echo ""
echo "--- Alert and detection topics ---"
create_topic "arcas.detection.scores"    3 2592000000  # 30 days - risk scores
create_topic "arcas.alerts.draft"        3 2592000000  # 30 days - draft alerts pending HITL
create_topic "arcas.alerts.validated"    3 -1           # Indefinite - validated alerts
create_topic "arcas.alerts.rejected"     3 2592000000  # 30 days - rejected alerts (feedback)

echo ""
echo "--- HITL workflow topics ---"
create_topic "arcas.hitl.requests"       1 -1           # Indefinite - pending HITL reviews
create_topic "arcas.hitl.decisions"      1 -1           # Indefinite - human decisions (audit)

echo ""
echo "--- Agent governance topics (via SAM, mirrored to Kafka for persistence) ---"
create_topic "arcas.agent.lifecycle"     3 604800000   # 7 days
create_topic "arcas.agent.tasks"         3 604800000   # 7 days
create_topic "arcas.governance.drift"    1 -1           # Indefinite - model drift alerts

echo ""
echo "--- CDC topics (Debezium -> Iceberg lakehouse) ---"
create_topic "arcas.cdc.events"          3 -1           # Indefinite
create_topic "arcas.cdc.actors"          3 -1           # Indefinite
create_topic "arcas.cdc.alerts"          3 -1           # Indefinite
create_topic "arcas.cdc.audit_log"       3 -1           # Indefinite

echo ""
echo "--- Dead letter queue ---"
create_topic "arcas.dead.letter"         3 2592000000  # 30 days

echo ""
echo "--- Debezium internal topics ---"
create_topic "arcas.debezium.configs"    1 -1
create_topic "arcas.debezium.offsets"    1 -1
create_topic "arcas.debezium.status"     1 -1

echo ""
echo "============================================================"
echo "All topics created. Listing all arcas.* topics:"
echo "============================================================"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$BOOTSTRAP" \
  --list | grep "^arcas\." | sort
