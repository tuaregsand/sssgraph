DROP INDEX IF EXISTS idx_agent_automations_due;
DROP INDEX IF EXISTS idx_agent_automations_is_active;
DROP INDEX IF EXISTS idx_agent_automations_program_id;
DROP TABLE IF EXISTS agent_automations;

DROP INDEX IF EXISTS idx_agent_webhooks_is_active;
DROP INDEX IF EXISTS idx_agent_webhooks_event_type;
DROP INDEX IF EXISTS idx_agent_webhooks_program_id;
DROP TABLE IF EXISTS agent_webhooks;

DROP TABLE IF EXISTS idls;
