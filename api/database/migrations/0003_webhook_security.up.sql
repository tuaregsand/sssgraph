CREATE TABLE IF NOT EXISTS webhook_replay_nonces (
    nonce TEXT PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_webhook_replay_nonces_expires_at
    ON webhook_replay_nonces (expires_at);

CREATE TABLE IF NOT EXISTS agent_inbound_webhooks (
    id BIGSERIAL PRIMARY KEY,
    nonce TEXT NOT NULL DEFAULT '',
    signature TEXT NOT NULL,
    key_id TEXT NOT NULL DEFAULT '',
    source_ip TEXT NOT NULL DEFAULT '',
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_inbound_webhooks_nonce
    ON agent_inbound_webhooks (nonce);
CREATE INDEX IF NOT EXISTS idx_agent_inbound_webhooks_received_at
    ON agent_inbound_webhooks (received_at DESC);
