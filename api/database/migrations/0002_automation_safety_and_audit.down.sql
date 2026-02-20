DROP FUNCTION IF EXISTS purge_agent_automation_evaluations(INTERVAL);

DROP INDEX IF EXISTS idx_agent_automation_evaluations_automation_eval;
DROP INDEX IF EXISTS idx_agent_automation_evaluations_evaluated_at;
DROP INDEX IF EXISTS idx_agent_automation_evaluations_automation_id;
DROP TABLE IF EXISTS agent_automation_evaluations;

ALTER TABLE agent_automations
    DROP CONSTRAINT IF EXISTS agent_automations_dedupe_chk,
    DROP CONSTRAINT IF EXISTS agent_automations_retry_attempts_chk,
    DROP CONSTRAINT IF EXISTS agent_automations_retry_backoff_chk;

ALTER TABLE agent_automations
    DROP COLUMN IF EXISTS last_trigger_fingerprint,
    DROP COLUMN IF EXISTS dry_run,
    DROP COLUMN IF EXISTS retry_initial_backoff_ms,
    DROP COLUMN IF EXISTS retry_max_attempts,
    DROP COLUMN IF EXISTS dedupe_minutes;
