# Solana Super-Graph Engine (LaserStream + Go API)

two deployable services:

- `ingester/`: Rust ingestion pipeline
- `api/`: Go API + WebSocket fanout gateway

Rust ingester (`ingester/`)

- Connects to Helius LaserStream Yellowstone gRPC endpoints with token auth.
- Subscribes to transaction/account updates filtered by configured PROGRAM_IDS.
- Implements endpoint failover with exponential backoff.
- Maintains a dynamic IDL cache by reading idls from PostgreSQL (`PG_DSN`) on a refresh interval.
- Maintains a dynamic AI-agent webhook route cache by reading active agent_webhook, rows from PostgreSQL (`AGENT_WEBHOOKS_PG_DSN` or `PG_DSN`) on a refresh interval.
- Normalizes matching events into JSON envelopes.
- Publishes normalized events to Redpanda via Kafka REST Proxy (binary records), partition-keyed by signature/program/account.
- Falls back to stdout publishing when proxy URL is not configured.
- Uses publish retries before writing failed records to a DLQ JSONL file.
- Replays DLQ backlog on an interval and can emit webhook alerts when backlog crosses threshold.

### Go API (`api/`)

- Fiber API with structured runtime config.
- PostgreSQL-backed IDL CRUD endpoints:
  - `POST /api/idls`
  - `GET /api/idls`
  - `GET /api/idls/:programID`
  - `PUT /api/idls/:programID`
  - `DELETE /api/idls/:programID`
- ClickHouse-backed historical endpoint:
  - `GET /api/events?program_id=...&event_type=...&from=...&to=...&limit=...&offset=...`
- Agent intelligence endpoints:
  - `POST /api/agent/query` (NL prompt -> guarded SQL with `mode=sql|verified|analyst_report`)
    - verified execution responses now include `evidence_bundle` with `sql_hash_sha256`, `program_ids`, `signatures`, and `slots`
  - `POST /api/agent/query/report` (dedicated strict verified analyst report alias)
  - `POST /api/agent/backtest/launches` (coin launch backtesting with signal scoring)
  - `POST /api/agent/backtest/launches/score` (backtest-to-live launch scoring with probability band + confidence)
  - `POST /api/agent/automations` (create threshold-based agent monitor)
  - `GET /api/agent/automations`
  - `GET /api/agent/automations/:id/audit` (evaluation audit trail)
  - `PATCH /api/agent/automations/:id`
  - `POST /api/agent/automations/:id/evaluate`
- Health and readiness endpoints:
  - `GET /api/health`
  - `GET /api/ready` (DB + Redis checks, plus ClickHouse when configured)
- WebSocket event fanout:
  - `GET /ws?program_id=...&event_type=...`
  - `GET /ws/:programID?event_type=...`
- Dragonfly/Redis PubSub integration through a native RESP client.
- Slow-client protection via bounded per-connection queue and forced disconnect on overflow.
- AuthN/AuthZ:
  - API key auth (`X-API-Key`) and JWT HS256 auth (`Authorization: Bearer ...`)
  - Role separation (`read` vs `admin`)
  - Program-level access control for IDL/event/ws requests
- Abuse protection:
  - Fixed-window per-IP rate limits on `/api/events` and websocket upgrades
  - WebSocket capacity limits (global + per-IP)
- Observability:
  - Request/WS metrics exposed at `GET /metrics` (Prometheus text format)
  - Request IDs enabled via middleware

## Environment Setup

Copy:

- `api/.env.example` -> `api/.env`
- `ingester/.env.example` -> `ingester/.env`

and fill in real values.

## Run

### 1) API

```bash
cd /api
go run .
```


### 2) Ingester

```bash
cd ingester
cargo run
```

### 3) Interactive CLI (configure/use/monitor)

```bash
./laserctl hub
```

Examples:

```bash
./laserctl hub
./laserctl configure
./laserctl health
./laserctl query-report "What changed in Program X over the last 24h?"
./laserctl backtest
./laserctl custom
```

CI gate is defined in `.github/workflows/ci.yml` and runs:
- Go E2E suite (`TestAPIEndToEndFlow`)
- full Go tests
- Rust tests