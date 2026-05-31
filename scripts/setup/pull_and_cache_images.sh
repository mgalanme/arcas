#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/setup/pull_and_cache_images.sh
#
# PURPOSE: Pull all required Docker images from Docker Hub and push them to
# the local registry (localhost:5000). Run this ONCE before starting
# development to avoid Docker Hub rate limits during iterative work.
#
# LESSON LEARNED: Docker Hub limits unauthenticated pulls to 100/6h and
# authenticated free-tier pulls to 200/6h. During active development this
# limit is hit quickly, causing pipeline failures. A local registry
# eliminates this dependency entirely.
#
# PREREQUISITES:
#   - Local registry running: docker compose --profile registry up -d
#   - Docker daemon running
#
# USAGE:
#   bash scripts/setup/pull_and_cache_images.sh
#   bash scripts/setup/pull_and_cache_images.sh --skip-existing
# =============================================================================

set -euo pipefail

LOCAL_REGISTRY="localhost:5000"
SKIP_EXISTING=false

# Parse arguments
for arg in "$@"; do
  case $arg in
    --skip-existing) SKIP_EXISTING=true ;;
  esac
done

# All images required by docker-compose.yml
# Format: "docker-hub-image" -> cached as "localhost:5000/<short-name>"
declare -A IMAGES=(
  # storage profile
  ["postgres:16-alpine"]="postgres:16-alpine"
  ["neo4j:5.23-community"]="neo4j:5.23-community"
  ["qdrant/qdrant:latest"]="qdrant:latest"
  ["minio/minio:latest"]="minio:latest"
  ["redis:7-alpine"]="redis:7-alpine"

  # messaging profile
  ["confluentinc/cp-kafka:7.7.0"]="cp-kafka:7.7.0"
  ["apicurio/apicurio-registry-mem:2.6.0.Final"]="apicurio-registry-mem:2.6.0.Final"
  ["redpandadata/console:latest"]="console:latest"

  # streaming profile
  ["flink:1.19-scala_2.12-java11"]="flink:1.19-scala_2.12-java11"
  ["debezium/connect:2.7"]="connect:2.7"

  # ai profile
  ["ollama/ollama:latest"]="ollama:latest"

  # governance profile
  ["postgres:14-alpine"]="postgres:14-alpine"
  ["infisical/infisical:latest-postgres"]="infisical:latest-postgres"
  ["solace/solace-pubsub-standard:latest"]="solace-pubsub-standard:latest"

  # observability profile
  ["otel/opentelemetry-collector-contrib:0.107.0"]="opentelemetry-collector-contrib:0.107.0"
  ["prom/prometheus:v2.54.0"]="prometheus:v2.54.0"
  ["grafana/grafana:11.2.0"]="grafana:11.2.0"
  ["grafana/loki:3.1.0"]="loki:3.1.0"
  ["grafana/promtail:3.1.0"]="promtail:3.1.0"
  ["grafana/tempo:2.5.0"]="tempo:2.5.0"
  ["prom/alertmanager:v0.27.0"]="alertmanager:v0.27.0"
)

echo "============================================================"
echo "ARCAS - Pre-pulling and caching Docker images"
echo "Local registry: $LOCAL_REGISTRY"
echo "Skip existing:  $SKIP_EXISTING"
echo "Images to process: ${#IMAGES[@]}"
echo "============================================================"

# Ensure local registry is running
if ! curl -sf "http://${LOCAL_REGISTRY}/v2/" > /dev/null 2>&1; then
  echo "ERROR: Local registry not reachable at http://${LOCAL_REGISTRY}"
  echo "Start it with: docker compose --profile registry up -d"
  exit 1
fi
echo "Local registry: OK"
echo ""

SUCCESS=0
SKIPPED=0
FAILED=0
FAILED_IMAGES=()

for HUB_IMAGE in "${!IMAGES[@]}"; do
  LOCAL_TAG="${LOCAL_REGISTRY}/${IMAGES[$HUB_IMAGE]}"

  # Check if already cached
  if [ "$SKIP_EXISTING" = true ]; then
    SHORT_NAME="${IMAGES[$HUB_IMAGE]%%:*}"
    if curl -sf "http://${LOCAL_REGISTRY}/v2/${SHORT_NAME}/tags/list" > /dev/null 2>&1; then
      echo "  SKIP (cached): $HUB_IMAGE"
      SKIPPED=$((SKIPPED + 1))
      continue
    fi
  fi

  echo "  Pulling: $HUB_IMAGE"
  if docker pull "$HUB_IMAGE" > /dev/null 2>&1; then
    docker tag "$HUB_IMAGE" "$LOCAL_TAG"
    if docker push "$LOCAL_TAG" > /dev/null 2>&1; then
      echo "  Cached:  $LOCAL_TAG"
      SUCCESS=$((SUCCESS + 1))
    else
      echo "  WARN: push failed for $LOCAL_TAG"
      FAILED=$((FAILED + 1))
      FAILED_IMAGES+=("$HUB_IMAGE")
    fi
  else
    echo "  WARN: pull failed for $HUB_IMAGE (rate limit or not found)"
    FAILED=$((FAILED + 1))
    FAILED_IMAGES+=("$HUB_IMAGE")
  fi
done

echo ""
echo "============================================================"
echo "Results: $SUCCESS cached, $SKIPPED skipped, $FAILED failed"

if [ ${#FAILED_IMAGES[@]} -gt 0 ]; then
  echo ""
  echo "Failed images (retry manually):"
  for img in "${FAILED_IMAGES[@]}"; do
    echo "  - $img"
  done
fi

echo ""
echo "Total local registry size:"
du -sh /var/lib/docker/volumes/*registry* 2>/dev/null || \
  docker system df --format "table {{.Type}}\t{{.Size}}" | grep -i volume || \
  echo "  (run 'docker system df' to check)"
echo "============================================================"
