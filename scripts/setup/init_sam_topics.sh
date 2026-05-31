#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/setup/init_sam_topics.sh
# Configures Solace PubSub+ (Agent Mesh broker) via SEMP v2 REST API.
# Creates the VPN, queues and topic subscriptions for all ARCAS governance
# event channels.
#
# Requires: Solace broker running (docker compose --profile governance up -d)
# =============================================================================

set -euo pipefail

# Load environment variables
if [ -f ".env" ]; then
  export $(grep -v '^#' .env | grep -v '^$' | xargs)
fi

SAM_HOST="${SAM_HOST:-localhost}"
SAM_MGMT_PORT="${SAM_MGMT_PORT:-8086}"
SAM_ADMIN_USER="${SAM_ADMIN_USER:-admin}"
SAM_ADMIN_PASS="${SAM_ADMIN_PASS:-${SAM_PASSWORD:-admin}}"
SEMP_BASE="http://${SAM_HOST}:${SAM_MGMT_PORT}/SEMP/v2/config"
VPN_NAME="arcas-vpn"

echo "============================================================"
echo "ARCAS - Configuring Solace Agent Mesh"
echo "SEMP endpoint: $SEMP_BASE"
echo "VPN: $VPN_NAME"
echo "============================================================"

# Helper: SEMP REST call
semp() {
  local METHOD=$1
  local PATH=$2
  local BODY=${3:-"{}"}
  curl -s -u "${SAM_ADMIN_USER}:${SAM_ADMIN_PASS}" \
    -X "$METHOD" \
    -H "Content-Type: application/json" \
    -d "$BODY" \
    "${SEMP_BASE}${PATH}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
if 'error' in data and data['error'].get('status') not in ['ALREADY_EXISTS','']:
    print('SEMP ERROR:', json.dumps(data['error'], indent=2), file=sys.stderr)
else:
    code = data.get('meta', {}).get('responseCode', '?')
    print(f'  HTTP {code}: OK')
" 2>&1
}

# Wait for Solace to be ready
echo ""
echo "Waiting for Solace broker to be ready..."
RETRIES=24
until curl -sf -u "${SAM_ADMIN_USER}:${SAM_ADMIN_PASS}" \
    "${SEMP_BASE}/msgVpns" > /dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  if [ $RETRIES -eq 0 ]; then
    echo "ERROR: Solace broker did not become ready."
    exit 1
  fi
  echo "  Still waiting... ($RETRIES retries left)"
  sleep 5
done
echo "Solace broker is ready."

# =============================================================================
# 1. Create Message VPN
# =============================================================================
echo ""
echo "--- Creating Message VPN: $VPN_NAME ---"
semp POST "/msgVpns" "{
  \"msgVpnName\": \"$VPN_NAME\",
  \"enabled\": true,
  \"maxMsgSpoolUsage\": 1500,
  \"maxConnectionCount\": 100,
  \"authenticationBasicEnabled\": true,
  \"authenticationBasicType\": \"internal\"
}"

# =============================================================================
# 2. Create application user for ARCAS services
# =============================================================================
echo ""
echo "--- Creating ARCAS client username ---"
semp POST "/msgVpns/${VPN_NAME}/clientUsernames" "{
  \"clientUsername\": \"arcas-app\",
  \"password\": \"${SAM_PASSWORD}\",
  \"enabled\": true,
  \"msgVpnName\": \"$VPN_NAME\"
}"

# Grant publish and subscribe permissions
semp POST "/msgVpns/${VPN_NAME}/aclProfiles" "{
  \"aclProfileName\": \"arcas-profile\",
  \"msgVpnName\": \"$VPN_NAME\",
  \"publishTopicDefaultAction\": \"allow\",
  \"subscribeTopicDefaultAction\": \"allow\",
  \"clientConnectDefaultAction\": \"allow\"
}"

# =============================================================================
# 3. Create durable queues for each governance topic
# =============================================================================
echo ""
echo "--- Creating governance queues ---"

create_queue() {
  local QUEUE_NAME=$1
  local MAX_SPOOL_MB=${2:-50}

  echo "  Queue: $QUEUE_NAME"
  semp POST "/msgVpns/${VPN_NAME}/queues" "{
    \"queueName\": \"$QUEUE_NAME\",
    \"msgVpnName\": \"$VPN_NAME\",
    \"accessType\": \"non-exclusive\",
    \"egressEnabled\": true,
    \"ingressEnabled\": true,
    \"permission\": \"consume\",
    \"maxMsgSpoolUsage\": $MAX_SPOOL_MB,
    \"maxMsgSize\": 1048576,
    \"respectTtlEnabled\": false
  }"
}

# Agent lifecycle events - all services subscribe
create_queue "q.arcas.agent.lifecycle"     25

# Task assignment and completion
create_queue "q.arcas.agent.tasks"         25

# HITL requests - Dashboard subscribes
create_queue "q.arcas.hitl.requests"       100

# HITL decisions - Orchestrator subscribes
create_queue "q.arcas.hitl.decisions"      100

# Alert notifications - Dashboard + Audit service subscribe
create_queue "q.arcas.alerts.created"      200

# Governance and drift alerts - Dashboard + Auditor subscribe
create_queue "q.arcas.governance.drift"    25

# Audit ingestion queue - Audit service subscribes
create_queue "q.arcas.audit.ingest"        500

# =============================================================================
# 4. Create topic subscriptions on each queue
# =============================================================================
echo ""
echo "--- Binding topic subscriptions to queues ---"

subscribe_queue() {
  local QUEUE_NAME=$1
  local TOPIC=$2
  echo "  $QUEUE_NAME <- $TOPIC"
  semp POST "/msgVpns/${VPN_NAME}/queues/${QUEUE_NAME}/subscriptions" "{
    \"msgVpnName\": \"$VPN_NAME\",
    \"queueName\": \"$QUEUE_NAME\",
    \"subscriptionTopic\": \"$TOPIC\"
  }"
}

# Agent lifecycle subscriptions
subscribe_queue "q.arcas.agent.lifecycle"   "arcas/agent/lifecycle/>"
subscribe_queue "q.arcas.audit.ingest"      "arcas/agent/lifecycle/>"

# Task subscriptions
subscribe_queue "q.arcas.agent.tasks"       "arcas/agent/task/>"
subscribe_queue "q.arcas.audit.ingest"      "arcas/agent/task/>"

# HITL subscriptions
subscribe_queue "q.arcas.hitl.requests"     "arcas/hitl/request/>"
subscribe_queue "q.arcas.hitl.decisions"    "arcas/hitl/decision/>"
subscribe_queue "q.arcas.audit.ingest"      "arcas/hitl/>"

# Alert subscriptions
subscribe_queue "q.arcas.alerts.created"    "arcas/alert/>"
subscribe_queue "q.arcas.audit.ingest"      "arcas/alert/>"

# Governance subscriptions
subscribe_queue "q.arcas.governance.drift"  "arcas/governance/>"
subscribe_queue "q.arcas.audit.ingest"      "arcas/governance/>"

# =============================================================================
# 5. Verify configuration
# =============================================================================
echo ""
echo "--- Verifying configuration ---"
QUEUE_COUNT=$(curl -s -u "${SAM_ADMIN_USER}:${SAM_ADMIN_PASS}" \
  "${SEMP_BASE}/msgVpns/${VPN_NAME}/queues?count=50" | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('data',[])))")

echo "  VPN:    $VPN_NAME"
echo "  Queues: $QUEUE_COUNT"

echo ""
echo "============================================================"
echo "Solace Agent Mesh configuration complete."
echo ""
echo "Access points:"
echo "  SEMP Management: http://$SAM_HOST:$SAM_MGMT_PORT"
echo "  SMF (app):       $SAM_HOST:55555"
echo "  MQTT:            $SAM_HOST:1883"
echo ""
echo "Topic scheme: arcas/{domain}/{action}/{entity}"
echo "  Examples:"
echo "    arcas/agent/lifecycle/started"
echo "    arcas/hitl/request/alert_review"
echo "    arcas/hitl/decision/approved"
echo "    arcas/alert/created/category_a"
echo "    arcas/governance/drift/fraud_agent"
echo "============================================================"
