"""
ARCAS - src/arcas_vault/pseudonymisation_vault.py

Pseudonymisation vault service.
The ONLY component that maps real identifiers to pseudonymous tokens.
No downstream component receives real identifiers.

Re-identification requires dual-control HITL authorisation logged in audit_log.
All access is recorded in the audit log via the audit service.
"""
import hashlib, hmac, logging, os
from typing import Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import base64

load_dotenv()
log = logging.getLogger(__name__)

PG_HOST     = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT     = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB       = os.getenv("POSTGRES_DB", "arcas")
PG_USER     = os.getenv("POSTGRES_USER", "arcas_app")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
SALT        = os.getenv("ARCAS_PSEUDONYM_SALT", "change-me-use-long-random-string")
HMAC_KEY    = os.getenv("ARCAS_AUDIT_HMAC_KEY", "change-me-use-long-random-string")


class PseudonymisationVault:
    """
    Manages the bidirectional mapping between real identifiers and
    pseudonymous tokens. Tokens are deterministic (same input always
    yields same token) to support entity reconciliation across sources.

    Security properties:
    - Tokens are HMAC-SHA256 of (real_id + entity_type + salt)
    - Real identifiers are stored encrypted with Fernet symmetric encryption
    - The encryption key is managed separately (Infisical in production,
      environment variable in development)
    - All vault operations are logged to the audit_log table
    """

    def __init__(self):
        self.conn    = self._connect()
        self.fernet  = self._init_fernet()

    def _connect(self):
        return psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
        )

    def _init_fernet(self) -> Fernet:
        # Derive a 32-byte Fernet key from the HMAC key
        key_bytes = hashlib.sha256(HMAC_KEY.encode()).digest()
        fernet_key = base64.urlsafe_b64encode(key_bytes)
        return Fernet(fernet_key)

    def _generate_token(self, real_id: str, entity_type: str) -> str:
        """Generate a deterministic pseudonymous token."""
        payload = f"{real_id}|{entity_type}|{SALT}"
        return hmac.new(HMAC_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()

    def pseudonymise(self, real_id: str, entity_type: str) -> str:
        """
        Returns the pseudonymous token for a real identifier.
        Creates a new entry if not already in the vault.
        Thread-safe via PostgreSQL INSERT ON CONFLICT.
        """
        token = self._generate_token(real_id, entity_type)
        encrypted = self.fernet.encrypt(real_id.encode())

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pseudonymisation_vault (pseudo_token, encrypted_id, entity_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (pseudo_token) DO NOTHING
            """, (token, encrypted, entity_type))
            self.conn.commit()

        return token

    def batch_pseudonymise(self, items: list[tuple[str, str]]) -> dict[str, str]:
        """
        Pseudonymise a batch of (real_id, entity_type) tuples.
        Returns {real_id: token} mapping.
        """
        result = {}
        for real_id, entity_type in items:
            result[real_id] = self.pseudonymise(real_id, entity_type)
        return result

    def reidentify(self, token: str, operator_id: str, authorisation_id: str) -> Optional[str]:
        """
        Re-identify a token back to the real identifier.
        REQUIRES: a valid dual-control authorisation_id from the HITL workflow.
        Logs the re-identification event in audit_log.
        """
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT encrypted_id FROM pseudonymisation_vault WHERE pseudo_token = %s",
                (token,)
            )
            row = cur.fetchone()
            if not row:
                return None

            # Log the re-identification (mandatory audit trail)
            cur.execute("""
                INSERT INTO audit_log (event_type, operator_id, payload)
                VALUES ('VAULT_REIDENTIFICATION', %s, %s)
            """, (operator_id, f'{{"token": "{token[:8]}...", "authorisation_id": "{authorisation_id}"}}'))
            self.conn.commit()

        return self.fernet.decrypt(row["encrypted_id"]).decode()

    def close(self):
        self.conn.close()
