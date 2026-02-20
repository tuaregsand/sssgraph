CREATE TABLE IF NOT EXISTS idls (
    program_id VARCHAR(64) PRIMARY KEY,
    name TEXT NOT NULL DEFAULT '',
    content JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_webhooks (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    program_id TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_webhooks_program_id ON agent_webhooks (program_id);
CREATE INDEX IF NOT EXISTS idx_agent_webhooks_event_type ON agent_webhooks (event_type);
CREATE INDEX IF NOT EXISTS idx_agent_webhooks_is_active ON agent_webhooks (is_active);

CREATE TABLE IF NOT EXISTS agent_automations (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    program_id TEXT NOT NULL DEFAULT '',
    query_template TEXT NOT NULL,
    comparator VARCHAR(8) NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    schedule_minutes INTEGER NOT NULL DEFAULT 60,
    window_minutes INTEGER NOT NULL DEFAULT 60,
    cooldown_minutes INTEGER NOT NULL DEFAULT 30,
    webhook_url TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_value DOUBLE PRECISION,
    last_evaluated_at TIMESTAMPTZ,
    last_triggered_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT agent_automations_comparator_chk CHECK (comparator IN ('gt', 'gte', 'lt', 'lte')),
    CONSTRAINT agent_automations_schedule_chk CHECK (schedule_minutes > 0),
    CONSTRAINT agent_automations_window_chk CHECK (window_minutes > 0),
    CONSTRAINT agent_automations_cooldown_chk CHECK (cooldown_minutes >= 0)
);

CREATE INDEX IF NOT EXISTS idx_agent_automations_program_id ON agent_automations (program_id);
CREATE INDEX IF NOT EXISTS idx_agent_automations_is_active ON agent_automations (is_active);
CREATE INDEX IF NOT EXISTS idx_agent_automations_due ON agent_automations (is_active, last_evaluated_at);
