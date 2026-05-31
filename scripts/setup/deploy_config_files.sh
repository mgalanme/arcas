#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/setup/deploy_config_files.sh
#
# PURPOSE: Places all downloaded config files and scripts into their correct
# paths within the project. Run this from the project root after downloading
# all files from the document delivery.
#
# USAGE:
#   cd /home/pruebas/formacion/arcas
#   bash scripts/setup/deploy_config_files.sh <source_dir>
#
# ARGUMENTS:
#   source_dir  Directory containing the downloaded files, preserving the
#               same subfolder structure as the project. Defaults to ~/Downloads
#
# EXAMPLE:
#   bash scripts/setup/deploy_config_files.sh ~/Downloads/arcas-files
#
# The script will:
#   1. Create all required directories
#   2. Copy each file to its correct project path
#   3. Set executable permissions on .sh scripts
#   4. Report any missing source files
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOURCE_DIR="${1:-$HOME/Downloads}"

cd "$PROJECT_ROOT"

echo "============================================================"
echo "ARCAS - Deploying config files and scripts"
echo "Project root: $PROJECT_ROOT"
echo "Source dir:   $SOURCE_DIR"
echo "============================================================"

# ---------------------------------------------------------------------------
# FILE MAP: source path (relative to SOURCE_DIR) -> destination (relative to PROJECT_ROOT)
# ---------------------------------------------------------------------------
declare -A FILE_MAP=(
  # Root
  ["docker-compose.yml"]="docker-compose.yml"
  ["pyproject.toml"]="pyproject.toml"

  # Config - postgres
  ["config/postgres/init.sql"]="config/postgres/init.sql"
  ["config/postgres/audit_trigger.sql"]="config/postgres/audit_trigger.sql"

  # Config - qdrant
  ["config/qdrant/config.yaml"]="config/qdrant/config.yaml"

  # Config - kafka
  ["config/kafka/redpanda-console-config.yaml"]="config/kafka/redpanda-console-config.yaml"

  # Config - flink
  ["config/flink/flink-conf.yaml"]="config/flink/flink-conf.yaml"

  # Config - grafana
  ["config/grafana/datasources.yaml"]="config/grafana/datasources.yaml"
  ["config/grafana/dashboards.yaml"]="config/grafana/dashboards.yaml"
  ["config/grafana/dashboards/arcas_overview.json"]="config/grafana/dashboards/arcas_overview.json"

  # Config - loki
  ["config/loki/loki-config.yaml"]="config/loki/loki-config.yaml"
  ["config/loki/promtail-config.yaml"]="config/loki/promtail-config.yaml"

  # Config - otel
  ["config/otel/otel-collector-config.yaml"]="config/otel/otel-collector-config.yaml"

  # Config - prometheus
  ["config/prometheus/prometheus.yml"]="config/prometheus/prometheus.yml"
  ["config/prometheus/alertmanager.yaml"]="config/prometheus/alertmanager.yaml"

  # Config - tempo
  ["config/tempo/tempo.yaml"]="config/tempo/tempo.yaml"

  # Config - infisical (standalone compose)
  ["config/infisical/docker-compose.infisical.yml"]="config/infisical/docker-compose.infisical.yml"

  # Config - SAM (standalone compose)
  ["config/sam/docker-compose.sam.yml"]="config/sam/docker-compose.sam.yml"

  # Scripts - setup
  ["scripts/setup/pull_and_cache_images.sh"]="scripts/setup/pull_and_cache_images.sh"
  ["scripts/setup/create_venvs.sh"]="scripts/setup/create_venvs.sh"
  ["scripts/setup/create_kafka_topics.sh"]="scripts/setup/create_kafka_topics.sh"
  ["scripts/setup/init_minio_buckets.sh"]="scripts/setup/init_minio_buckets.sh"
  ["scripts/setup/init_neo4j_schema.py"]="scripts/setup/init_neo4j_schema.py"
  ["scripts/setup/init_sam_topics.sh"]="scripts/setup/init_sam_topics.sh"

  # Scripts - maintenance
  ["scripts/maintenance/backup.py"]="scripts/maintenance/backup.py"
  ["scripts/maintenance/test_ingestion.py"]="scripts/maintenance/test_ingestion.py"
  ["scripts/maintenance/test_agent.py"]="scripts/maintenance/test_agent.py"
)

# ---------------------------------------------------------------------------
# REQUIRED DIRECTORIES
# ---------------------------------------------------------------------------
DIRS=(
  "config/postgres"
  "config/qdrant"
  "config/kafka"
  "config/flink"
  "config/grafana/dashboards"
  "config/loki"
  "config/otel"
  "config/prometheus"
  "config/tempo"
  "config/infisical"
  "config/sam"
  "scripts/setup"
  "scripts/maintenance"
  ".github/workflows"
)

echo ""
echo "--- Creating directories ---"
for DIR in "${DIRS[@]}"; do
  mkdir -p "$DIR"
  echo "  OK: $DIR"
done

# ---------------------------------------------------------------------------
# COPY FILES
# ---------------------------------------------------------------------------
echo ""
echo "--- Copying files ---"
COPIED=0
MISSING=0
SKIPPED=0

for SRC_REL in "${!FILE_MAP[@]}"; do
  DST_REL="${FILE_MAP[$SRC_REL]}"
  SRC_PATH="${SOURCE_DIR}/${SRC_REL}"
  DST_PATH="${PROJECT_ROOT}/${DST_REL}"

  if [ ! -f "$SRC_PATH" ]; then
    echo "  MISSING: $SRC_REL (expected at $SRC_PATH)"
    MISSING=$((MISSING + 1))
    continue
  fi

  # Skip if destination is identical (avoid unnecessary writes)
  if [ -f "$DST_PATH" ] && cmp -s "$SRC_PATH" "$DST_PATH"; then
    echo "  SAME:    $DST_REL"
    SKIPPED=$((SKIPPED + 1))
    continue
  fi

  mkdir -p "$(dirname "$DST_PATH")"
  cp "$SRC_PATH" "$DST_PATH"
  echo "  COPIED:  $DST_REL"
  COPIED=$((COPIED + 1))
done

# ---------------------------------------------------------------------------
# SET PERMISSIONS
# ---------------------------------------------------------------------------
echo ""
echo "--- Setting permissions ---"
find "$PROJECT_ROOT/scripts" -name "*.sh" -exec chmod +x {} \; -print | \
  sed "s|$PROJECT_ROOT/||" | sed 's/^/  chmod +x: /'

# ---------------------------------------------------------------------------
# SUMMARY
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "Results: $COPIED copied, $SKIPPED unchanged, $MISSING missing"

if [ $MISSING -gt 0 ]; then
  echo ""
  echo "WARNING: $MISSING file(s) not found in $SOURCE_DIR"
  echo "Download the missing files and re-run this script."
  exit 1
else
  echo ""
  echo "All files deployed successfully."
  echo ""
  echo "Next steps:"
  echo "  1. git add -A && git commit -m 'chore: deploy config files'"
  echo "  2. make setup   (first-time full setup)"
  echo "  3. make start   (start development stack)"
fi
echo "============================================================"
