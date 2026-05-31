-- =============================================================================
-- ARCAS - config/postgres/init.sql
-- Initial schema creation. Run automatically by PostgreSQL on first start.
-- Alembic manages subsequent migrations.
-- =============================================================================

-- Ensure the database uses UTC
SET timezone = 'UTC';

-- ---------------------------------------------------------------------------
-- EXTENSIONS
-- ---------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- ENUM TYPES
-- ---------------------------------------------------------------------------
CREATE TYPE actor_type_enum AS ENUM (
    'political', 'corporate', 'judicial', 'law_enforcement',
    'legal', 'media', 'regulator', 'think_tank', 'influencer', 'other'
);

CREATE TYPE alert_status_enum AS ENUM (
    'pending', 'approved', 'rejected', 'modified_approved',
    'evidence_requested', 'escalated', 'archived_monitoring',
    'false_positive', 'reported_authorities', 'emailed'
);

CREATE TYPE alert_category_enum AS ENUM ('A', 'B', 'C', 'D', 'E', 'F');

CREATE TYPE verification_status_enum AS ENUM (
    'unverified', 'false', 'misleading', 'unsubstantiated', 'verified_true'
);

-- ---------------------------------------------------------------------------
-- SOURCES
-- Source registry - every data source the system ingests from
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sources (
    source_id       UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_name     VARCHAR(255) NOT NULL,
    source_type     VARCHAR(50)  NOT NULL,  -- gazette, procurement, courts, media, etc.
    base_url        TEXT,
    jurisdiction    VARCHAR(100),
    language        VARCHAR(10)  DEFAULT 'es',
    is_active       BOOLEAN      DEFAULT TRUE,
    last_fetched_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- ACTORS
-- Pseudonymised registry of all in-scope actors.
-- Real identifiers live ONLY in the pseudonymisation_vault table.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS actors (
    actor_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pseudo_token        VARCHAR(64) UNIQUE NOT NULL,  -- token from vault
    actor_type          actor_type_enum NOT NULL,
    display_name        VARCHAR(255),                 -- pseudonymised label
    jurisdiction        VARCHAR(100),
    risk_score_global   NUMERIC(5,2) DEFAULT 0.00,
    risk_score_cat_a    NUMERIC(5,2) DEFAULT 0.00,    -- procurement fraud
    risk_score_cat_b    NUMERIC(5,2) DEFAULT 0.00,    -- illicit enrichment
    risk_score_cat_c    NUMERIC(5,2) DEFAULT 0.00,    -- judicial patterns
    risk_score_cat_d    NUMERIC(5,2) DEFAULT 0.00,    -- disinformation
    risk_score_cat_e    NUMERIC(5,2) DEFAULT 0.00,    -- influence networks
    risk_score_cat_f    NUMERIC(5,2) DEFAULT 0.00,    -- abuse of function
    first_detected_at   TIMESTAMPTZ  DEFAULT NOW(),
    last_updated_at     TIMESTAMPTZ  DEFAULT NOW(),
    is_synthetic        BOOLEAN      DEFAULT FALSE,   -- TRUE for test records
    metadata            JSONB        DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_actors_type      ON actors(actor_type);
CREATE INDEX IF NOT EXISTS idx_actors_risk      ON actors(risk_score_global DESC);
CREATE INDEX IF NOT EXISTS idx_actors_token     ON actors(pseudo_token);
CREATE INDEX IF NOT EXISTS idx_actors_updated   ON actors(last_updated_at DESC);

-- ---------------------------------------------------------------------------
-- ORGANISATIONS
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS organisations (
    org_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pseudo_token    VARCHAR(64) UNIQUE NOT NULL,
    org_type        VARCHAR(50) NOT NULL,
    display_name    VARCHAR(255),
    sector          VARCHAR(100),
    jurisdiction    VARCHAR(100),
    is_active       BOOLEAN     DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ DEFAULT NOW(),
    is_synthetic    BOOLEAN     DEFAULT FALSE,
    metadata        JSONB       DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_orgs_type         ON organisations(org_type);
CREATE INDEX IF NOT EXISTS idx_orgs_jurisdiction ON organisations(jurisdiction);

-- ---------------------------------------------------------------------------
-- EVENTS
-- Individual public events linked to actors
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS events (
    event_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type          VARCHAR(100) NOT NULL,
    event_date          DATE,
    source_id           UUID REFERENCES sources(source_id),
    source_url          TEXT,
    pseudonymised_text  TEXT,
    content_hash        VARCHAR(64),                  -- SHA-256 of original
    relevance_score     NUMERIC(4,3) DEFAULT 0.000,
    qdrant_embedding_id VARCHAR(64),                  -- reference to Qdrant point
    ingest_at           TIMESTAMPTZ  DEFAULT NOW(),
    is_synthetic        BOOLEAN      DEFAULT FALSE,
    metadata            JSONB        DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_events_date   ON events(event_date DESC);
CREATE INDEX IF NOT EXISTS idx_events_type   ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_id);

-- Actor-Event junction
CREATE TABLE IF NOT EXISTS event_actors (
    event_id UUID REFERENCES events(event_id)  ON DELETE CASCADE,
    actor_id UUID REFERENCES actors(actor_id)  ON DELETE CASCADE,
    role     VARCHAR(100),
    PRIMARY KEY (event_id, actor_id)
);

-- ---------------------------------------------------------------------------
-- ALERTS
-- Generated by agents, reviewed via HITL
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS alerts (
    alert_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    category            alert_category_enum NOT NULL,
    status              alert_status_enum   NOT NULL DEFAULT 'pending',
    confidence_score    NUMERIC(4,3) NOT NULL,
    nl_justification    TEXT,
    reasoning_chain     JSONB  DEFAULT '[]',          -- array of reasoning steps
    supporting_events   UUID[] DEFAULT '{}',
    operator_id         VARCHAR(100),                 -- who reviewed it
    operator_notes      TEXT,
    reviewed_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    is_synthetic        BOOLEAN     DEFAULT FALSE,
    metadata            JSONB       DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_alerts_status     ON alerts(status);
CREATE INDEX IF NOT EXISTS idx_alerts_category   ON alerts(category);
CREATE INDEX IF NOT EXISTS idx_alerts_confidence ON alerts(confidence_score DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_created    ON alerts(created_at DESC);

-- Alert-Actor junction
CREATE TABLE IF NOT EXISTS alert_actors (
    alert_id UUID REFERENCES alerts(alert_id) ON DELETE CASCADE,
    actor_id UUID REFERENCES actors(actor_id) ON DELETE CASCADE,
    PRIMARY KEY (alert_id, actor_id)
);

-- ---------------------------------------------------------------------------
-- EVIDENCE
-- Pseudonymised text fragments that support alerts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS evidence (
    evidence_id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_id           UUID REFERENCES sources(source_id),
    source_url          TEXT NOT NULL,
    pub_date            DATE,
    ingest_date         TIMESTAMPTZ DEFAULT NOW(),
    content_hash        VARCHAR(64) NOT NULL,         -- SHA-256 of original
    pseudonymised_text  TEXT,
    qdrant_embedding_id VARCHAR(64),
    is_synthetic        BOOLEAN DEFAULT FALSE,
    metadata            JSONB   DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_evidence_source ON evidence(source_id);
CREATE INDEX IF NOT EXISTS idx_evidence_date   ON evidence(pub_date DESC);
CREATE INDEX IF NOT EXISTS idx_evidence_hash   ON evidence(content_hash);

-- ---------------------------------------------------------------------------
-- DISINFORMATION RECORDS
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS disinformation_records (
    record_id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    outlet_id            UUID REFERENCES actors(actor_id),
    claim_text           TEXT NOT NULL,
    pub_date             DATE,
    verification_status  verification_status_enum DEFAULT 'unverified',
    verification_sources JSONB DEFAULT '[]',
    beneficiary_actors   UUID[] DEFAULT '{}',
    correction_date      DATE,
    qdrant_embedding_id  VARCHAR(64),
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    is_synthetic         BOOLEAN DEFAULT FALSE,
    metadata             JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_disinfo_outlet ON disinformation_records(outlet_id);
CREATE INDEX IF NOT EXISTS idx_disinfo_status ON disinformation_records(verification_status);
CREATE INDEX IF NOT EXISTS idx_disinfo_date   ON disinformation_records(pub_date DESC);

-- ---------------------------------------------------------------------------
-- JUDICIAL PATTERNS
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS judicial_patterns (
    pattern_id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    judge_pseudo_token            VARCHAR(64) NOT NULL,
    pattern_type                  VARCHAR(100) NOT NULL,
    evidentiary_basis_score       NUMERIC(4,3),        -- 0=hearsay only, 1=full evidence
    political_affiliation_corr    NUMERIC(4,3),        -- correlation coefficient
    confidence                    NUMERIC(4,3),
    supporting_case_refs          JSONB DEFAULT '[]',
    qdrant_embedding_id           VARCHAR(64),
    created_at                    TIMESTAMPTZ DEFAULT NOW(),
    is_synthetic                  BOOLEAN DEFAULT FALSE,
    metadata                      JSONB DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_judicial_judge ON judicial_patterns(judge_pseudo_token);
CREATE INDEX IF NOT EXISTS idx_judicial_type  ON judicial_patterns(pattern_type);

-- ---------------------------------------------------------------------------
-- PSEUDONYMISATION VAULT
-- THE MOST SENSITIVE TABLE IN THE SYSTEM.
-- Maps real identifiers to pseudonymous tokens.
-- Application user has SELECT + INSERT only - no UPDATE, no DELETE.
-- Re-identification requires dual-control HITL authorisation (see audit_log).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pseudonymisation_vault (
    vault_id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pseudo_token    VARCHAR(64) UNIQUE NOT NULL,
    -- real_identifier is stored encrypted with pgcrypto using the app HMAC key
    -- It is never stored in plaintext.
    encrypted_id    BYTEA NOT NULL,
    entity_type     VARCHAR(50) NOT NULL,              -- actor, organisation
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    -- Re-identification is tracked in audit_log, not here.
    CONSTRAINT pseudo_token_format CHECK (pseudo_token ~ '^[a-f0-9]{64}$')
);

-- No indexes on encrypted_id intentionally - prevents timing attacks
CREATE INDEX IF NOT EXISTS idx_vault_token ON pseudonymisation_vault(pseudo_token);
CREATE INDEX IF NOT EXISTS idx_vault_type  ON pseudonymisation_vault(entity_type);

-- REVOKE UPDATE and DELETE from application user (applied after table creation)
-- These GRANTs are applied in the post-init section below.

-- ---------------------------------------------------------------------------
-- AUDIT LOG (APPEND-ONLY)
-- Every system decision and human action is recorded here.
-- The trigger in audit_trigger.sql prevents UPDATE and DELETE at DB level.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    log_id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    event_type      VARCHAR(100) NOT NULL,
    actor_ids       UUID[]       DEFAULT '{}',
    alert_id        UUID,
    operator_id     VARCHAR(100),
    agent_id        VARCHAR(100),
    payload         JSONB        DEFAULT '{}',
    hmac_signature  VARCHAR(64),                       -- HMAC-SHA256 of payload
    created_at      TIMESTAMPTZ  DEFAULT NOW()
    -- No updated_at: this table is append-only by design
);

-- Partial index: fast lookup of pending HITL events
CREATE INDEX IF NOT EXISTS idx_audit_event_type ON audit_log(event_type);
CREATE INDEX IF NOT EXISTS idx_audit_operator   ON audit_log(operator_id) WHERE operator_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_audit_created    ON audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_alert      ON audit_log(alert_id)   WHERE alert_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- LANGGRAPH CHECKPOINTER TABLES
-- Required by LangGraph's PostgreSQL checkpointer for stateful agent flows.
-- These are managed by LangGraph itself; we just ensure the schema exists.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id     TEXT   NOT NULL,
    checkpoint_id TEXT   NOT NULL,
    parent_id     TEXT,
    checkpoint    BYTEA,
    metadata      BYTEA,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (thread_id, checkpoint_id)
);

CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id     TEXT  NOT NULL,
    checkpoint_id TEXT  NOT NULL,
    task_id       TEXT  NOT NULL,
    idx           INT   NOT NULL,
    channel       TEXT  NOT NULL,
    value         BYTEA,
    PRIMARY KEY (thread_id, checkpoint_id, task_id, idx)
);

-- ---------------------------------------------------------------------------
-- SEED DATA: default sources
-- ---------------------------------------------------------------------------
INSERT INTO sources (source_name, source_type, base_url, jurisdiction, language)
VALUES
    ('BOE - Boletín Oficial del Estado',       'gazette',     'https://www.boe.es',                             'ES', 'es'),
    ('PLACSP - Contratación Pública',           'procurement', 'https://contrataciondelestado.es',               'ES', 'es'),
    ('EUR-Lex Official Journal',                'gazette',     'https://eur-lex.europa.eu',                      'EU', 'en'),
    ('TED - Tenders Electronic Daily',          'procurement', 'https://ted.europa.eu',                          'EU', 'en'),
    ('ICIJ Offshore Leaks Database',            'financial',   'https://offshoreleaks.icij.org',                 'GL', 'en'),
    ('OpenCorporates',                          'financial',   'https://opencorporates.com',                     'GL', 'en'),
    ('Tribunal de Cuentas - Financiación',      'financial',   'https://www.tcu.es',                             'ES', 'es'),
    ('Registro de Transparencia UE',            'lobbying',    'https://www.transparencyregister.eu',            'EU', 'en')
ON CONFLICT DO NOTHING;

-- ---------------------------------------------------------------------------
-- PERMISSIONS (applied after table creation)
-- Application user: arcas_app
-- Deny UPDATE and DELETE on audit_log and pseudonymisation_vault at SQL level.
-- The trigger in audit_trigger.sql adds a second layer of protection.
-- ---------------------------------------------------------------------------
REVOKE UPDATE, DELETE ON audit_log              FROM arcas_app;
REVOKE UPDATE, DELETE ON pseudonymisation_vault FROM arcas_app;
