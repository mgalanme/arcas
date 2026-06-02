"""
ARCAS - src/arcas_audit/audit_service.py

Audit service: subscribes to governance events and persists each
to the append-only audit_log table with HMAC-SHA256 signature.
The audit_log table has database-level triggers preventing UPDATE/DELETE.
"""
import hashlib, hmac, json, logging, os, time
from dotenv import load_dotenv
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

load_dotenv()
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
HMAC_KEY        = os.getenv("ARCAS_AUDIT_HMAC_KEY", "change-me")
PG_DSN          = (
    f"host={os.getenv('POSTGRES_HOST','localhost')} "
    f"port={os.getenv('POSTGRES_PORT','5432')} "
    f"dbname={os.getenv('POSTGRES_DB','arcas')} "
    f"user={os.getenv('POSTGRES_USER','arcas_app')} "
    f"password={os.getenv('POSTGRES_PASSWORD','')}"
)

# Topics to subscribe to for audit events
AUDIT_TOPICS = [
    "arcas.hitl.decisions",
    "arcas.hitl.requests",
    "arcas.alerts.validated",
    "arcas.alerts.rejected",
    "arcas.agent.lifecycle",
    "arcas.agent.tasks",
    "arcas.governance.drift",
]


def compute_hmac(payload: str) -> str:
    """Compute HMAC-SHA256 of the payload string."""
    return hmac.new(HMAC_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()


class AuditService:
    """
    Consumes governance events from Kafka and persists them
    to the append-only audit_log table with cryptographic signature.
    """

    def __init__(self):
        self.conn     = psycopg2.connect(PG_DSN)
        self.consumer = self._create_consumer()

    def _create_consumer(self):
        for i in range(10):
            try:
                return KafkaConsumer(
                    *AUDIT_TOPICS,
                    bootstrap_servers=[KAFKA_BOOTSTRAP],
                    group_id="arcas-audit-service",
                    auto_offset_reset="latest",
                    value_deserializer=lambda v: json.loads(v.decode()),
                    consumer_timeout_ms=1000,
                )
            except NoBrokersAvailable:
                time.sleep(3)
        raise RuntimeError("Cannot connect to Kafka")

    def _persist(self, event_type: str, payload: dict,
                 operator_id: str = None, agent_id: str = None,
                 alert_id: str = None) -> None:
        """Write an immutable audit entry with HMAC signature."""
        payload_str    = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        hmac_signature = compute_hmac(payload_str)

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO audit_log
                    (event_type, operator_id, agent_id, alert_id, payload, hmac_signature)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (event_type, operator_id, agent_id, alert_id,
                  json.dumps(payload), hmac_signature))
            self.conn.commit()

    def log_hitl_decision(self, decision: dict) -> None:
        self._persist(
            event_type="HITL_DECISION",
            payload=decision,
            operator_id=decision.get("operator_id"),
            alert_id=decision.get("alert_id"),
        )

    def log_alert_event(self, event_type: str, alert: dict) -> None:
        self._persist(
            event_type=event_type,
            payload=alert,
            alert_id=alert.get("alert_id"),
        )

    def log_agent_event(self, event: dict) -> None:
        self._persist(
            event_type=f"AGENT_{event.get('lifecycle_event', 'UNKNOWN').upper()}",
            payload=event,
            agent_id=event.get("agent_id"),
        )

    def verify_log_integrity(self, limit: int = 1000) -> dict:
        """Verify HMAC signatures on the most recent audit entries."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT log_id, event_type, payload, hmac_signature
                FROM audit_log ORDER BY created_at DESC LIMIT %s
            """, (limit,))
            rows = cur.fetchall()

        valid = tampered = 0
        for log_id, event_type, payload, stored_hmac in rows:
            expected = compute_hmac(json.dumps(json.loads(payload), sort_keys=True))
            if expected == stored_hmac:
                valid += 1
            else:
                tampered += 1
                log.error(f"INTEGRITY VIOLATION: audit_log entry {log_id} ({event_type})")

        return {"checked": len(rows), "valid": valid, "tampered": tampered}

    def run(self):
        log.info("Audit service running")
        for message in self.consumer:
            topic   = message.topic
            payload = message.value
            try:
                if "hitl.decisions" in topic:
                    self.log_hitl_decision(payload)
                elif "alerts." in topic:
                    event_type = "ALERT_VALIDATED" if "validated" in topic else "ALERT_REJECTED"
                    self.log_alert_event(event_type, payload)
                elif "agent." in topic:
                    self.log_agent_event(payload)
                else:
                    self._persist(event_type=f"KAFKA_{topic.upper()}", payload=payload)
            except Exception as e:
                log.error(f"Audit persist error ({topic}): {e}")

    def close(self):
        self.consumer.close()
        self.conn.close()


if __name__ == "__main__":
    service = AuditService()
    try:
        service.run()
    finally:
        service.close()
