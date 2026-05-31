#!/bin/bash
# scripts/setup/pull_and_cache_images.sh
# Run this ONCE before starting development.
 
set -e
 
LOCAL_REGISTRY="localhost:5000"
 
IMAGES=(
  "postgres:16-alpine"
  "neo4j:5.23-community"
  "qdrant/qdrant:latest"
  "minio/minio:latest"
  "redis:7-alpine"
  "confluentinc/cp-kafka:7.7.0"
  "confluentinc/cp-schema-registry:7.7.0"
  "apicurio/apicurio-registry-mem:2.6.0.Final"
  "redpandadata/console:latest"
  "flink:1.19-scala_2.12-java11"
  "ollama/ollama:latest"
  "registry:2"
  "prom/prometheus:v2.54.0"
  "grafana/grafana:11.2.0"
  "grafana/loki:3.1.0"
  "grafana/promtail:3.1.0"
  "grafana/tempo:2.5.0"
  "prom/alertmanager:v0.27.0"
  "otel/opentelemetry-collector-contrib:0.107.0"
  "debezium/connect:2.7.0.Final"
)
 
echo "Starting local registry..."
docker compose --profile registry up -d registry
sleep 3
 
for IMAGE in "${IMAGES[@]}"; do
  echo "Pulling $IMAGE ..."
  docker pull "$IMAGE"
  
  # Tag for local registry
  SHORT=$(echo "$IMAGE" | sed 's|.*/||')
  docker tag "$IMAGE" "$LOCAL_REGISTRY/$SHORT"
  docker push "$LOCAL_REGISTRY/$SHORT"
  echo "  Cached: $LOCAL_REGISTRY/$SHORT"
done
 
echo ""
echo "All images cached in local registry."
echo "Total cache size:"
docker system df
