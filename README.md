# Solana Super-Graph Engine (LaserStream + Go API)

This repository contains two deployable services:

- `ingester/`: Rust ingestion + normalization + webhook fanout
- `api/`: Go API + WebSocket gateway + agent intelligence endpoints

## Implemented Features

### Rust ingester (`ingester/`)

- Helius LaserStream Yellowstone gRPC ingestion with endpoint failover.
- Dynamic IDL cache refresh from PostgreSQL.
- Dynamic AI-agent webhook route refresh from PostgreSQL.
- Event normalization + Redpanda REST publish + DLQ replay.
- Outbound webhook signing support (`X-Laser-*` HMAC headers).
- Built-in metrics endpoint with ingestion lag gauge (`ingester_ingestion_lag_ms`).

### Go API (`api/`)

- IDL CRUD endpoints backed by PostgreSQL.
- Historical query endpoint backed by ClickHouse.
- Agent query endpoints:
  - `POST /api/agent/query`
  - `POST /api/agent/query/report`
- Query responses include verified evidence bundles (`sql_hash_sha256`, `program_ids`, `signatures`, `slots`).
- Backtesting endpoints:
  - `POST /api/agent/backtest/launches`
  - `POST /api/agent/backtest/launches/score`
- Automation endpoints:
  - `POST /api/agent/automations`
  - `GET /api/agent/automations`
  - `GET /api/agent/automations/:id/audit`
  - `PATCH /api/agent/automations/:id`
  - `POST /api/agent/automations/:id/evaluate`
- Automation safety rails:
  - cooldown (`cooldown_minutes`)
  - dedupe (`dedupe_minutes`)
  - retry policy (`retry_max_attempts`, `retry_initial_backoff_ms`)
  - dry-run mode (`dry_run`)
- Inbound signed webhook receiver with replay protection:
  - `POST /api/agent/webhooks/inbound`
  - `GET /api/agent/webhooks/inbound`
- Explicit versioned DB migrations + schema verification at startup.

## Environment Setup

Copy and configure:

- `api/.env.example` -> `api/.env`
- `ingester/.env.example` -> `ingester/.env`

## Run

### API

```bash
cd api
go run .
```

### Ingester

```bash
cd ingester
cargo run
```

### Interactive CLI

```bash
./laserctl hub
```

## DB Migration Commands

```bash
cd api
./scripts/db_migrate.sh
./scripts/db_rollback_last.sh
# or:
go run ./cmd/dbmigrate up
go run ./cmd/dbmigrate down
go run ./cmd/dbmigrate verify
```

## Validation

```bash
cd api
GOCACHE=/tmp/go-build go test ./...

cd ../ingester
cargo test --locked
```

## CI

CI workflow: `.github/workflows/ci.yml`

It runs:

- API E2E tests
- Full Go test suite
- Service-backed Go integration tests (Postgres + Redis + ClickHouse)
- Rust tests

## Observability Artifacts

- Prometheus alert rules: `ops/prometheus/laserstream-slo-alerts.yml`
- Grafana dashboard: `ops/grafana/laserstream-slo-dashboard.json`

## API Contract + SDK

- OpenAPI spec: `api/openapi/laserstream-agent-api.yaml`
- Typed TypeScript SDK: `sdk/typescript`
