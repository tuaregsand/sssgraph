ALTER TABLE agent_automations
    ADD COLUMN IF NOT EXISTS dedupe_minutes INTEGER NOT NULL DEFAULT 15,
    ADD COLUMN IF NOT EXISTS retry_max_attempts INTEGER NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS retry_initial_backoff_ms INTEGER NOT NULL DEFAULT 200,
    ADD COLUMN IF NOT EXISTS dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_trigger_fingerprint TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_automations
    DROP CONSTRAINT IF EXISTS agent_automations_dedupe_chk,
    DROP CONSTRAINT IF EXISTS agent_automations_retry_attempts_chk,
    DROP CONSTRAINT IF EXISTS agent_automations_retry_backoff_chk;

ALTER TABLE agent_automations
    ADD CONSTRAINT agent_automations_dedupe_chk CHECK (dedupe_minutes >= 0),
    ADD CONSTRAINT agent_automations_retry_attempts_chk CHECK (retry_max_attempts > 0),
    ADD CONSTRAINT agent_automations_retry_backoff_chk CHECK (retry_initial_backoff_ms > 0);

CREATE TABLE IF NOT EXISTS agent_automation_evaluations (
    id BIGSERIAL PRIMARY KEY,
    automation_id BIGINT NOT NULL REFERENCES agent_automations(id) ON DELETE CASCADE,
    value DOUBLE PRECISION NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    comparator VARCHAR(8) NOT NULL,
    triggered BOOLEAN NOT NULL,
    alert_sent BOOLEAN NOT NULL,
    delivery_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    query TEXT NOT NULL,
    window_from TIMESTAMPTZ NOT NULL,
    window_to TIMESTAMPTZ NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    dedupe_key TEXT NOT NULL DEFAULT '',
    retry_attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_automation_evaluations_comparator_chk CHECK (comparator IN ('gt', 'gte', 'lt', 'lte'))
);

CREATE INDEX IF NOT EXISTS idx_agent_automation_evaluations_automation_id
    ON agent_automation_evaluations (automation_id);
CREATE INDEX IF NOT EXISTS idx_agent_automation_evaluations_evaluated_at
    ON agent_automation_evaluations (evaluated_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_automation_evaluations_automation_eval
    ON agent_automation_evaluations (automation_id, evaluated_at DESC);

CREATE OR REPLACE FUNCTION purge_agent_automation_evaluations(retention INTERVAL)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_rows BIGINT;
BEGIN
    DELETE FROM agent_automation_evaluations
    WHERE created_at < NOW() - retention;

    GET DIAGNOSTICS deleted_rows = ROW_COUNT;
    RETURN deleted_rows;
END;
$$;
