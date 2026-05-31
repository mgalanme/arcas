-- =============================================================================
-- ARCAS - config/postgres/audit_trigger.sql
-- Database-level protection for the audit_log table.
-- Raises an exception on any UPDATE or DELETE attempt,
-- regardless of which user or role makes the request.
-- This is the second layer of protection after the REVOKE in init.sql.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- FUNCTION: prevent any modification to append-only tables
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_prevent_audit_modification()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'Operation % on table % is not permitted. The audit log is append-only.',
        TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'insufficient_privilege';
    RETURN NULL;
END;
$$;

-- ---------------------------------------------------------------------------
-- TRIGGER: audit_log - block UPDATE
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_audit_log_no_update ON audit_log;
CREATE TRIGGER trg_audit_log_no_update
    BEFORE UPDATE ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_audit_modification();

-- ---------------------------------------------------------------------------
-- TRIGGER: audit_log - block DELETE
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_audit_log_no_delete ON audit_log;
CREATE TRIGGER trg_audit_log_no_delete
    BEFORE DELETE ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION fn_prevent_audit_modification();

-- ---------------------------------------------------------------------------
-- FUNCTION: auto-update updated_at on alerts
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION fn_update_timestamp()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_alerts_updated_at ON alerts;
CREATE TRIGGER trg_alerts_updated_at
    BEFORE UPDATE ON alerts
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_timestamp();

-- ---------------------------------------------------------------------------
-- FUNCTION: auto-update last_updated_at on actors
-- ---------------------------------------------------------------------------
DROP TRIGGER IF EXISTS trg_actors_updated_at ON actors;
CREATE TRIGGER trg_actors_updated_at
    BEFORE UPDATE ON actors
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_timestamp();

-- ---------------------------------------------------------------------------
-- Verify triggers were created
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    trigger_count INT;
BEGIN
    SELECT COUNT(*) INTO trigger_count
    FROM information_schema.triggers
    WHERE trigger_name IN (
        'trg_audit_log_no_update',
        'trg_audit_log_no_delete',
        'trg_alerts_updated_at',
        'trg_actors_updated_at'
    );

    IF trigger_count < 4 THEN
        RAISE WARNING 'Expected 4 triggers, found %. Check init logs.', trigger_count;
    ELSE
        RAISE NOTICE 'All % audit and timestamp triggers created successfully.', trigger_count;
    END IF;
END;
$$;
