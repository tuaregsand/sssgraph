DROP INDEX IF EXISTS idx_agent_inbound_webhooks_received_at;
DROP INDEX IF EXISTS idx_agent_inbound_webhooks_nonce;
DROP TABLE IF EXISTS agent_inbound_webhooks;

DROP INDEX IF EXISTS idx_webhook_replay_nonces_expires_at;
DROP TABLE IF EXISTS webhook_replay_nonces;
