#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/setup/init_minio_buckets.sh
# Creates MinIO buckets and applies lifecycle policies.
# Requires MinIO to be running (docker compose --profile storage up -d minio)
# =============================================================================

set -euo pipefail

# Load environment variables
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MC_ALIAS="arcas"

echo "============================================================"
echo "ARCAS - Initialising MinIO buckets"
echo "Endpoint: $MINIO_ENDPOINT"
echo "============================================================"

# Ensure mc (MinIO client) is available
if ! command -v mc &> /dev/null; then
  echo "Installing MinIO client (mc)..."
  wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc
  chmod +x /tmp/mc
  sudo mv /tmp/mc /usr/local/bin/mc
  echo "mc installed."
fi

# Wait for MinIO to be ready
echo "Waiting for MinIO to be ready..."
RETRIES=20
until curl -sf "http://${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  if [ $RETRIES -eq 0 ]; then
    echo "ERROR: MinIO did not become ready in time."
    exit 1
  fi
  echo "  Still waiting... ($RETRIES retries left)"
  sleep 3
done
echo "MinIO is ready."

# Configure mc alias
mc alias set "$MC_ALIAS" \
  "http://${MINIO_ENDPOINT}" \
  "$MINIO_ACCESS_KEY" \
  "$MINIO_SECRET_KEY" \
  --api S3v4 > /dev/null

echo ""
echo "--- Creating buckets ---"

create_bucket() {
  local BUCKET=$1
  if mc ls "${MC_ALIAS}/${BUCKET}" > /dev/null 2>&1; then
    echo "  SKIP (exists): ${BUCKET}"
  else
    mc mb "${MC_ALIAS}/${BUCKET}"
    echo "  CREATED: ${BUCKET}"
  fi
}

create_bucket "arcas-raw"           # Raw ingested data (90-day lifecycle)
create_bucket "arcas-processed"     # Pseudonymised processed text (1-year lifecycle)
create_bucket "arcas-iceberg"       # Apache Iceberg table data files (permanent)
create_bucket "arcas-models"        # ML model artefacts (permanent)
create_bucket "arcas-backups"       # Database backups (30-day lifecycle)
create_bucket "arcas-exports"       # HITL export documents (30-day lifecycle)

echo ""
echo "--- Applying lifecycle policies ---"

# Raw data: 90-day retention (data minimisation - GDPR)
cat > /tmp/raw_lifecycle.json << 'EOF'
{
  "Rules": [
    {
      "ID": "arcas-raw-90day",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "Expiration": { "Days": 90 }
    }
  ]
}
EOF
mc ilm import "${MC_ALIAS}/arcas-raw" < /tmp/raw_lifecycle.json
echo "  arcas-raw: 90-day lifecycle applied"

# Processed data: 1-year retention
cat > /tmp/processed_lifecycle.json << 'EOF'
{
  "Rules": [
    {
      "ID": "arcas-processed-1year",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "Expiration": { "Days": 365 }
    }
  ]
}
EOF
mc ilm import "${MC_ALIAS}/arcas-processed" < /tmp/processed_lifecycle.json
echo "  arcas-processed: 1-year lifecycle applied"

# Backups: 30-day retention
cat > /tmp/backup_lifecycle.json << 'EOF'
{
  "Rules": [
    {
      "ID": "arcas-backups-30day",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "Expiration": { "Days": 30 }
    }
  ]
}
EOF
mc ilm import "${MC_ALIAS}/arcas-backups" < /tmp/backup_lifecycle.json
echo "  arcas-backups: 30-day lifecycle applied"

# Exports: 30-day retention
mc ilm import "${MC_ALIAS}/arcas-exports" < /tmp/backup_lifecycle.json
echo "  arcas-exports: 30-day lifecycle applied"

echo ""
echo "--- Creating Iceberg warehouse structure ---"
# Create placeholder to establish the warehouse path
echo "placeholder" | mc pipe "${MC_ALIAS}/arcas-iceberg/warehouse/.keep"
echo "  arcas-iceberg/warehouse/ path created"

echo ""
echo "--- Verifying buckets ---"
mc ls "${MC_ALIAS}/"

echo ""
echo "============================================================"
echo "MinIO initialisation complete."
echo ""
echo "Access points:"
echo "  API:     http://$MINIO_ENDPOINT"
echo "  Console: http://localhost:9001"
echo "  Iceberg warehouse: s3a://arcas-iceberg/warehouse"
echo "============================================================"

# Cleanup temp files
rm -f /tmp/raw_lifecycle.json /tmp/processed_lifecycle.json /tmp/backup_lifecycle.json
