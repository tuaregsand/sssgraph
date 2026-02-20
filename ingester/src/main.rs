use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use futures::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use log::{error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::{
    collections::{HashMap, HashSet},
    env,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Mutex, RwLock},
    time::sleep,
};
use tokio_postgres::{NoTls, Row};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
    SubscribeUpdateAccount, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
};

type HmacSha256 = Hmac<Sha256>;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = Config::from_env()?;
    info!(
        "Starting ingester with {} endpoints and {} program filters",
        config.endpoints.len(),
        config.program_ids.len()
    );

    let metrics = MetricsRegistry::default();
    if config.metrics_enabled {
        let metrics_addr = config.metrics_addr.clone();
        let metrics_registry = metrics.clone();
        tokio::spawn(async move {
            if let Err(err) = run_metrics_server(metrics_addr, metrics_registry).await {
                error!("metrics server failed: {err:#}");
            }
        });
    }

    let idl_cache = IdlCache::default();
    if let Some(idl_dsn) = &config.idl_dsn {
        let repository = IdlRepository::connect(idl_dsn)
            .await
            .context("failed to connect to IDL postgres repository")?;

        match repository.load_all().await {
            Ok(idls) => {
                let count = idls.len();
                idl_cache.replace_all(idls).await;
                info!("Loaded {count} IDLs into cache");
            }
            Err(err) => {
                warn!("Initial IDL cache load failed: {err:#}");
            }
        }

        let refresh_cache = idl_cache.clone();
        let refresh_interval = config.idl_refresh_interval;
        tokio::spawn(async move {
            run_idl_refresh(repository, refresh_cache, refresh_interval).await;
        });
    } else {
        warn!("PG_DSN is not set; dynamic IDL decoding will remain disabled");
    }

    let agent_webhook_cache = AgentWebhookCache::default();
    if config.agent_webhooks_enabled {
        if let Some(agent_webhooks_dsn) = config.agent_webhooks_dsn.as_ref() {
            let repository = AgentWebhookRepository::connect(agent_webhooks_dsn)
                .await
                .context("failed to connect to agent webhook postgres repository")?;

            match repository.load_active().await {
                Ok(webhooks) => {
                    let count = webhooks.len();
                    agent_webhook_cache.replace_all(webhooks).await;
                    info!("Loaded {count} active agent webhooks into cache");
                }
                Err(err) => {
                    warn!("Initial agent webhook cache load failed: {err:#}");
                }
            }

            let refresh_cache = agent_webhook_cache.clone();
            let refresh_interval = config.agent_webhooks_refresh_interval;
            tokio::spawn(async move {
                run_agent_webhook_refresh(repository, refresh_cache, refresh_interval).await;
            });
        } else {
            warn!(
                "AGENT_WEBHOOKS_ENABLED is true but AGENT_WEBHOOKS_PG_DSN/PG_DSN is empty; dynamic agent webhook fanout disabled"
            );
        }
    }

    let publisher = Publisher::from_config(&config)?;
    let alert_notifier = config
        .alert_webhook_url
        .as_ref()
        .map(|url| AlertNotifier::new(url, Duration::from_secs(5)))
        .transpose()?;

    let dlq = if config.dlq_enabled {
        Some(
            DlqStore::new(
                config.dlq_path.clone(),
                config.dlq_replay_batch_size,
                config.dlq_alert_threshold,
                alert_notifier.clone(),
            )
            .await?,
        )
    } else {
        None
    };

    if let Some(dlq_store) = dlq.clone() {
        let publisher_for_replay = publisher.clone();
        let interval = config.dlq_replay_interval;
        tokio::spawn(async move {
            run_dlq_replay_loop(dlq_store, publisher_for_replay, interval).await;
        });
    }

    let agent_notifier = if config.agent_webhooks_enabled || config.agent_webhook_url.is_some() {
        Some(AgentNotifier::new(
            config.agent_webhook_url.clone(),
            agent_webhook_cache,
            config.agent_webhook_timeout,
            config.agent_webhook_max_attempts,
            config.agent_webhook_retry_initial_backoff,
            config.webhook_signing_enabled,
            config.webhook_signing_secret.clone(),
            config.webhook_signing_key_id.clone(),
            metrics.clone(),
        )?)
    } else {
        None
    };

    run_ingestion_supervisor(config, idl_cache, publisher, dlq, agent_notifier, metrics).await
}

#[derive(Debug, Clone)]
struct Config {
    endpoints: Vec<String>,
    x_token: Option<String>,
    program_ids: Vec<String>,
    commitment: CommitmentLevel,
    idl_dsn: Option<String>,
    idl_refresh_interval: Duration,
    reconnect_initial_backoff: Duration,
    reconnect_max_backoff: Duration,
    ping_interval: Duration,
    redpanda_rest_proxy_url: Option<String>,
    redpanda_topic: String,
    redpanda_publish_timeout: Duration,
    publish_max_attempts: usize,
    publish_retry_initial_backoff: Duration,
    dlq_enabled: bool,
    dlq_path: String,
    dlq_replay_interval: Duration,
    dlq_replay_batch_size: usize,
    alert_webhook_url: Option<String>,
    agent_webhooks_enabled: bool,
    agent_webhooks_dsn: Option<String>,
    agent_webhooks_refresh_interval: Duration,
    agent_webhook_url: Option<String>,
    agent_webhook_timeout: Duration,
    agent_webhook_max_attempts: usize,
    agent_webhook_retry_initial_backoff: Duration,
    dlq_alert_threshold: usize,
    metrics_enabled: bool,
    metrics_addr: String,
    webhook_signing_enabled: bool,
    webhook_signing_secret: Option<String>,
    webhook_signing_key_id: String,
}

impl Config {
    fn from_env() -> Result<Self> {
        let endpoints = parse_csv_env("HELIUS_GRPC_ENDPOINTS");
        if endpoints.is_empty() {
            return Err(anyhow!(
                "HELIUS_GRPC_ENDPOINTS must contain at least one gRPC endpoint"
            ));
        }

        let program_ids = parse_csv_env("PROGRAM_IDS");
        if program_ids.is_empty() {
            return Err(anyhow!(
                "PROGRAM_IDS must contain at least one Solana program id"
            ));
        }

        let commitment =
            parse_commitment_level(&env_or_default("LASERSTREAM_COMMITMENT", "processed"))?;

        let idl_refresh_seconds = parse_u64_env("IDL_REFRESH_SECONDS", 30)?;
        let reconnect_initial_seconds = parse_u64_env("RECONNECT_INITIAL_SECONDS", 1)?;
        let reconnect_max_seconds = parse_u64_env("RECONNECT_MAX_SECONDS", 30)?;
        let ping_seconds = parse_u64_env("GEYSER_PING_SECONDS", 15)?;
        let redpanda_timeout_ms = parse_u64_env("REDPANDA_PUBLISH_TIMEOUT_MS", 5_000)?;
        let publish_max_attempts = parse_u64_env("PUBLISH_MAX_ATTEMPTS", 3)? as usize;
        let publish_retry_backoff_ms = parse_u64_env("PUBLISH_RETRY_INITIAL_BACKOFF_MS", 200)?;
        let dlq_replay_interval_seconds = parse_u64_env("DLQ_REPLAY_INTERVAL_SECONDS", 30)?;
        let dlq_replay_batch_size = parse_u64_env("DLQ_REPLAY_BATCH_SIZE", 200)? as usize;
        let dlq_alert_threshold = parse_u64_env("DLQ_ALERT_THRESHOLD", 200)? as usize;
        let agent_webhooks_refresh_seconds = parse_u64_env("AGENT_WEBHOOKS_REFRESH_SECONDS", 30)?;
        let agent_webhook_timeout_ms = parse_u64_env("AGENT_WEBHOOK_TIMEOUT_MS", 5_000)?;
        let agent_webhook_max_attempts = parse_u64_env("AGENT_WEBHOOK_MAX_ATTEMPTS", 2)? as usize;
        let agent_webhook_retry_backoff_ms =
            parse_u64_env("AGENT_WEBHOOK_RETRY_INITIAL_BACKOFF_MS", 200)?;
        let metrics_enabled = parse_bool_env("METRICS_ENABLED", true);
        let metrics_addr = env_or_default("METRICS_ADDR", "0.0.0.0:9102");
        let webhook_signing_enabled = parse_bool_env("WEBHOOK_SIGNING_ENABLED", true);
        let webhook_signing_secret = optional_env("WEBHOOK_SIGNING_SECRET");
        let webhook_signing_key_id = env_or_default("WEBHOOK_SIGNING_KEY_ID", "laserstream-v1");

        if webhook_signing_enabled && webhook_signing_secret.is_none() {
            return Err(anyhow!(
                "WEBHOOK_SIGNING_ENABLED=true but WEBHOOK_SIGNING_SECRET is empty"
            ));
        }

        Ok(Self {
            endpoints,
            x_token: optional_env("HELIUS_X_TOKEN"),
            program_ids,
            commitment,
            idl_dsn: optional_env("PG_DSN"),
            idl_refresh_interval: Duration::from_secs(idl_refresh_seconds),
            reconnect_initial_backoff: Duration::from_secs(reconnect_initial_seconds),
            reconnect_max_backoff: Duration::from_secs(reconnect_max_seconds),
            ping_interval: Duration::from_secs(ping_seconds),
            redpanda_rest_proxy_url: optional_env("REDPANDA_REST_PROXY_URL"),
            redpanda_topic: env_or_default("REDPANDA_TOPIC", "solana.events"),
            redpanda_publish_timeout: Duration::from_millis(redpanda_timeout_ms),
            publish_max_attempts: publish_max_attempts.max(1),
            publish_retry_initial_backoff: Duration::from_millis(publish_retry_backoff_ms.max(1)),
            dlq_enabled: parse_bool_env("DLQ_ENABLED", true),
            dlq_path: env_or_default("DLQ_PATH", "./dlq/events.jsonl"),
            dlq_replay_interval: Duration::from_secs(dlq_replay_interval_seconds.max(1)),
            dlq_replay_batch_size: dlq_replay_batch_size.max(1),
            alert_webhook_url: optional_env("ALERT_WEBHOOK_URL"),
            agent_webhooks_enabled: parse_bool_env("AGENT_WEBHOOKS_ENABLED", true),
            agent_webhooks_dsn: optional_env("AGENT_WEBHOOKS_PG_DSN")
                .or_else(|| optional_env("PG_DSN")),
            agent_webhooks_refresh_interval: Duration::from_secs(
                agent_webhooks_refresh_seconds.max(1),
            ),
            agent_webhook_url: optional_env("AGENT_WEBHOOK_URL"),
            agent_webhook_timeout: Duration::from_millis(agent_webhook_timeout_ms.max(1)),
            agent_webhook_max_attempts: agent_webhook_max_attempts.max(1),
            agent_webhook_retry_initial_backoff: Duration::from_millis(
                agent_webhook_retry_backoff_ms.max(1),
            ),
            dlq_alert_threshold: dlq_alert_threshold.max(1),
            metrics_enabled,
            metrics_addr,
            webhook_signing_enabled,
            webhook_signing_secret,
            webhook_signing_key_id,
        })
    }
}

#[derive(Clone, Default)]
struct IdlCache {
    inner: Arc<RwLock<HashMap<String, IdlEntry>>>,
}

impl IdlCache {
    async fn replace_all(&self, next: HashMap<String, IdlEntry>) {
        let mut guard = self.inner.write().await;
        *guard = next;
    }

    async fn snapshot(&self) -> HashMap<String, IdlEntry> {
        self.inner.read().await.clone()
    }
}

#[derive(Clone, Default)]
struct AgentWebhookCache {
    inner: Arc<RwLock<Vec<AgentWebhookEntry>>>,
}

impl AgentWebhookCache {
    async fn replace_all(&self, next: Vec<AgentWebhookEntry>) {
        let mut guard = self.inner.write().await;
        *guard = next;
    }

    async fn snapshot(&self) -> Vec<AgentWebhookEntry> {
        self.inner.read().await.clone()
    }
}

#[derive(Debug, Clone)]
struct IdlEntry {
    program_id: String,
    name: Option<String>,
    content: Value,
}

#[derive(Debug, Clone)]
struct AgentWebhookEntry {
    id: i64,
    url: String,
    program_id: Option<String>,
    event_type: Option<String>,
}

impl AgentWebhookEntry {
    fn matches_event(&self, event: &NormalizedEvent) -> bool {
        let program_match = match self.program_id.as_ref() {
            Some(program_id) => event.program_id == *program_id,
            None => true,
        };
        let event_type_match = match self.event_type.as_ref() {
            Some(event_type) => event.event_type == *event_type,
            None => true,
        };

        program_match && event_type_match
    }
}

struct IdlRepository {
    client: tokio_postgres::Client,
}

impl IdlRepository {
    async fn connect(dsn: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .context("failed to open postgres connection")?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                error!("IDL repository postgres connection failed: {err}");
            }
        });

        Ok(Self { client })
    }

    async fn load_all(&self) -> Result<HashMap<String, IdlEntry>> {
        let rows = self
            .client
            .query("SELECT program_id, name, content FROM idls", &[])
            .await
            .context("failed to query idls table")?;

        let mut entries = HashMap::new();
        for row in rows {
            let entry = row_to_idl_entry(row)?;
            entries.insert(entry.program_id.clone(), entry);
        }
        Ok(entries)
    }
}

struct AgentWebhookRepository {
    client: tokio_postgres::Client,
}

impl AgentWebhookRepository {
    async fn connect(dsn: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(dsn, NoTls)
            .await
            .context("failed to open postgres connection")?;

        tokio::spawn(async move {
            if let Err(err) = connection.await {
                error!("Agent webhook repository postgres connection failed: {err}");
            }
        });

        Ok(Self { client })
    }

    async fn load_active(&self) -> Result<Vec<AgentWebhookEntry>> {
        let rows = self
            .client
            .query(
                "SELECT id, url, program_id, event_type FROM agent_webhooks WHERE is_active = true",
                &[],
            )
            .await
            .context("failed to query agent_webhooks table")?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            entries.push(row_to_agent_webhook_entry(row)?);
        }
        Ok(entries)
    }
}

fn row_to_idl_entry(row: Row) -> Result<IdlEntry> {
    let program_id: String = row
        .try_get("program_id")
        .context("missing program_id column on idls row")?;
    if program_id.trim().is_empty() {
        return Err(anyhow!("received empty program_id from idls row"));
    }

    let name: Option<String> = row
        .try_get("name")
        .context("invalid name column on idls row")?;
    let content: Value = row
        .try_get("content")
        .context("invalid content column on idls row")?;

    Ok(IdlEntry {
        program_id,
        name,
        content,
    })
}

fn row_to_agent_webhook_entry(row: Row) -> Result<AgentWebhookEntry> {
    let id: i64 = row
        .try_get("id")
        .context("missing id column on agent_webhooks row")?;
    let url: String = row
        .try_get("url")
        .context("missing url column on agent_webhooks row")?;
    let url = url.trim().to_string();
    if url.is_empty() {
        return Err(anyhow!("received empty url from agent_webhooks row"));
    }

    let raw_program_id: String = row
        .try_get("program_id")
        .context("invalid program_id column on agent_webhooks row")?;
    let raw_event_type: String = row
        .try_get("event_type")
        .context("invalid event_type column on agent_webhooks row")?;

    let program_id = match raw_program_id.trim() {
        "" => None,
        value => Some(value.to_string()),
    };
    let event_type = match raw_event_type.trim() {
        "" => None,
        value => Some(value.to_string()),
    };

    Ok(AgentWebhookEntry {
        id,
        url,
        program_id,
        event_type,
    })
}

async fn run_idl_refresh(repository: IdlRepository, cache: IdlCache, refresh_interval: Duration) {
    loop {
        match repository.load_all().await {
            Ok(idls) => {
                let count = idls.len();
                cache.replace_all(idls).await;
                info!("IDL cache refreshed: {count} programs");
            }
            Err(err) => warn!("IDL cache refresh failed: {err:#}"),
        }

        sleep(refresh_interval).await;
    }
}

async fn run_agent_webhook_refresh(
    repository: AgentWebhookRepository,
    cache: AgentWebhookCache,
    refresh_interval: Duration,
) {
    loop {
        match repository.load_active().await {
            Ok(webhooks) => {
                let count = webhooks.len();
                cache.replace_all(webhooks).await;
                info!("Agent webhook cache refreshed: {count} active routes");
            }
            Err(err) => warn!("Agent webhook cache refresh failed: {err:#}"),
        }

        sleep(refresh_interval).await;
    }
}

#[derive(Clone, Default)]
struct MetricsRegistry {
    events_total: Arc<AtomicU64>,
    published_total: Arc<AtomicU64>,
    publish_failures_total: Arc<AtomicU64>,
    agent_webhook_failures_total: Arc<AtomicU64>,
    last_event_observed_ms: Arc<AtomicU64>,
    last_publish_success_ms: Arc<AtomicU64>,
    last_publish_failure_ms: Arc<AtomicU64>,
}

impl MetricsRegistry {
    fn record_event_observed(&self, observed_at_unix_ms: u128) {
        self.events_total.fetch_add(1, Ordering::Relaxed);
        self.last_event_observed_ms
            .store(u128_to_u64(observed_at_unix_ms), Ordering::Relaxed);
    }

    fn record_publish_success(&self) {
        self.published_total.fetch_add(1, Ordering::Relaxed);
        self.last_publish_success_ms
            .store(u128_to_u64(now_unix_ms()), Ordering::Relaxed);
    }

    fn record_publish_failure(&self) {
        self.publish_failures_total.fetch_add(1, Ordering::Relaxed);
        self.last_publish_failure_ms
            .store(u128_to_u64(now_unix_ms()), Ordering::Relaxed);
    }

    fn record_agent_webhook_failure(&self) {
        self.agent_webhook_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn ingestion_lag_ms(&self) -> u64 {
        let last_event = self.last_event_observed_ms.load(Ordering::Relaxed);
        if last_event == 0 {
            return 0;
        }
        let now = u128_to_u64(now_unix_ms());
        now.saturating_sub(last_event)
    }

    fn prometheus(&self) -> String {
        let events_total = self.events_total.load(Ordering::Relaxed);
        let published_total = self.published_total.load(Ordering::Relaxed);
        let publish_failures_total = self.publish_failures_total.load(Ordering::Relaxed);
        let agent_webhook_failures_total =
            self.agent_webhook_failures_total.load(Ordering::Relaxed);
        let last_event_observed_ms = self.last_event_observed_ms.load(Ordering::Relaxed);
        let last_publish_success_ms = self.last_publish_success_ms.load(Ordering::Relaxed);
        let last_publish_failure_ms = self.last_publish_failure_ms.load(Ordering::Relaxed);
        let ingestion_lag_ms = self.ingestion_lag_ms();

        format!(
            "# TYPE ingester_events_total counter\n\
ingester_events_total {events_total}\n\
# TYPE ingester_published_total counter\n\
ingester_published_total {published_total}\n\
# TYPE ingester_publish_failures_total counter\n\
ingester_publish_failures_total {publish_failures_total}\n\
# TYPE ingester_agent_webhook_failures_total counter\n\
ingester_agent_webhook_failures_total {agent_webhook_failures_total}\n\
# TYPE ingester_last_event_observed_unix_ms gauge\n\
ingester_last_event_observed_unix_ms {last_event_observed_ms}\n\
# TYPE ingester_last_publish_success_unix_ms gauge\n\
ingester_last_publish_success_unix_ms {last_publish_success_ms}\n\
# TYPE ingester_last_publish_failure_unix_ms gauge\n\
ingester_last_publish_failure_unix_ms {last_publish_failure_ms}\n\
# TYPE ingester_ingestion_lag_ms gauge\n\
ingester_ingestion_lag_ms {ingestion_lag_ms}\n"
        )
    }
}

async fn run_metrics_server(addr: String, metrics: MetricsRegistry) -> Result<()> {
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind metrics listener on {addr}"))?;
    info!("Ingester metrics listening on http://{addr}/metrics");

    loop {
        let (mut socket, _) = listener.accept().await.context("metrics accept failed")?;
        let registry = metrics.clone();

        tokio::spawn(async move {
            let mut read_buffer = vec![0u8; 2048];
            let read_len = match socket.read(&mut read_buffer).await {
                Ok(len) => len,
                Err(err) => {
                    warn!("metrics read failed: {err}");
                    return;
                }
            };
            if read_len == 0 {
                return;
            }

            let request_line = String::from_utf8_lossy(&read_buffer[..read_len])
                .lines()
                .next()
                .unwrap_or("")
                .to_string();

            let (status_line, body) = if request_line.starts_with("GET /metrics ") {
                ("HTTP/1.1 200 OK", registry.prometheus())
            } else {
                ("HTTP/1.1 404 Not Found", String::from("not found\n"))
            };

            let response = format!(
                "{status_line}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );

            if let Err(err) = socket.write_all(response.as_bytes()).await {
                warn!("metrics write failed: {err}");
            }
        });
    }
}

#[derive(Clone)]
enum Publisher {
    RedpandaProxy(RedpandaRestPublisher),
    Stdout,
}

impl Publisher {
    fn from_config(config: &Config) -> Result<Self> {
        match config.redpanda_rest_proxy_url.as_deref() {
            Some(url) if !url.trim().is_empty() => {
                Ok(Self::RedpandaProxy(RedpandaRestPublisher::new(
                    url,
                    &config.redpanda_topic,
                    config.redpanda_publish_timeout,
                )?))
            }
            _ => {
                warn!("REDPANDA_REST_PROXY_URL is empty; publishing normalized events to stdout");
                Ok(Self::Stdout)
            }
        }
    }

    async fn publish(&self, event: &NormalizedEvent) -> Result<()> {
        match self {
            Publisher::RedpandaProxy(publisher) => publisher.publish(event).await,
            Publisher::Stdout => {
                println!("{}", serde_json::to_string(event)?);
                Ok(())
            }
        }
    }
}

#[derive(Clone)]
struct RedpandaRestPublisher {
    http_client: Client,
    topic_url: String,
}

impl RedpandaRestPublisher {
    fn new(rest_proxy_url: &str, topic: &str, timeout: Duration) -> Result<Self> {
        let base_url = rest_proxy_url.trim_end_matches('/');
        if base_url.is_empty() {
            return Err(anyhow!("REDPANDA_REST_PROXY_URL cannot be empty"));
        }

        Ok(Self {
            http_client: Client::builder()
                .timeout(timeout)
                .build()
                .context("failed to create redpanda REST proxy client")?,
            topic_url: format!("{base_url}/topics/{topic}"),
        })
    }

    async fn publish(&self, event: &NormalizedEvent) -> Result<()> {
        let payload = serde_json::to_vec(event)?;
        let partition_key = if event.partition_key.is_empty() {
            event.program_id.as_bytes()
        } else {
            event.partition_key.as_bytes()
        };

        let response = self
            .http_client
            .post(&self.topic_url)
            .header("Content-Type", "application/vnd.kafka.binary.v2+json")
            .json(&json!({
                "records": [{
                    "key": BASE64_STANDARD.encode(partition_key),
                    "value": BASE64_STANDARD.encode(&payload),
                }]
            }))
            .send()
            .await
            .context("failed to call redpanda REST proxy")?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|_| String::from("<unable to read response body>"));
        Err(anyhow!(
            "redpanda REST publish failed with status {status}: {body}"
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NormalizedEvent {
    event_id: String,
    event_type: String,
    program_id: String,
    slot: u64,
    signature: Option<String>,
    partition_key: String,
    source_endpoint: String,
    observed_at_unix_ms: u128,
    payload: Value,
}

#[derive(Clone)]
struct AlertNotifier {
    url: String,
    client: Client,
}

impl AlertNotifier {
    fn new(url: &str, timeout: Duration) -> Result<Self> {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return Err(anyhow!("alert webhook url cannot be empty"));
        }

        Ok(Self {
            url: trimmed.to_string(),
            client: Client::builder()
                .timeout(timeout)
                .build()
                .context("failed to build alert notifier client")?,
        })
    }

    async fn notify(&self, title: &str, details: &str) {
        let payload = json!({
            "title": title,
            "details": details,
            "service": "laserstream-ingester",
            "at_unix_ms": now_unix_ms(),
        });

        if let Err(err) = self.client.post(&self.url).json(&payload).send().await {
            warn!("Failed to send alert webhook: {err}");
        }
    }
}

#[derive(Clone)]
struct AgentNotifier {
    static_url: Option<String>,
    dynamic_routes: AgentWebhookCache,
    client: Client,
    max_attempts: usize,
    retry_initial_backoff: Duration,
    signing_enabled: bool,
    signing_secret: Option<String>,
    signing_key_id: String,
    metrics: MetricsRegistry,
}

impl AgentNotifier {
    fn new(
        static_url: Option<String>,
        dynamic_routes: AgentWebhookCache,
        timeout: Duration,
        max_attempts: usize,
        retry_initial_backoff: Duration,
        signing_enabled: bool,
        signing_secret: Option<String>,
        signing_key_id: String,
        metrics: MetricsRegistry,
    ) -> Result<Self> {
        let static_url = static_url
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        if signing_enabled && signing_secret.is_none() {
            return Err(anyhow!(
                "WEBHOOK_SIGNING_ENABLED=true but WEBHOOK_SIGNING_SECRET is empty"
            ));
        }

        Ok(Self {
            static_url,
            dynamic_routes,
            client: Client::builder()
                .timeout(timeout)
                .build()
                .context("failed to build agent notifier client")?,
            max_attempts: max_attempts.max(1),
            retry_initial_backoff: retry_initial_backoff.max(Duration::from_millis(1)),
            signing_enabled,
            signing_secret,
            signing_key_id,
            metrics,
        })
    }

    async fn notify_event(&self, event: &NormalizedEvent) {
        let targets = self.targets_for_event(event).await;
        if targets.is_empty() {
            return;
        }

        for target in targets {
            let payload = json!({
                "agent_context": "On-chain event intercepted by LaserStream.",
                "event": event,
                "timestamp_ms": now_unix_ms(),
                "delivery_target": {
                    "source": target.source,
                    "route_id": target.route_id,
                    "program_id_filter": target.program_id_filter,
                    "event_type_filter": target.event_type_filter,
                },
            });

            if let Err(err) = self.send_with_retry(&target.url, &payload).await {
                self.metrics.record_agent_webhook_failure();
                warn!(
                    "Failed to send event_id={} to AI agent webhook route_id={} url={}: {err:#}",
                    event.event_id, target.route_id, target.url
                );
            }
        }
    }

    async fn targets_for_event(&self, event: &NormalizedEvent) -> Vec<AgentDeliveryTarget> {
        let mut targets = Vec::new();
        let mut seen_urls = HashSet::new();

        let dynamic_routes = self.dynamic_routes.snapshot().await;
        for route in dynamic_routes {
            if !route.matches_event(event) {
                continue;
            }
            if !seen_urls.insert(route.url.clone()) {
                continue;
            }

            targets.push(AgentDeliveryTarget {
                route_id: route.id.to_string(),
                url: route.url,
                program_id_filter: route.program_id.unwrap_or_else(|| "*".to_string()),
                event_type_filter: route.event_type.unwrap_or_else(|| "*".to_string()),
                source: "postgres".to_string(),
            });
        }

        if let Some(static_url) = self.static_url.as_ref() {
            if seen_urls.insert(static_url.clone()) {
                targets.push(AgentDeliveryTarget {
                    route_id: "static-env".to_string(),
                    url: static_url.clone(),
                    program_id_filter: "*".to_string(),
                    event_type_filter: "*".to_string(),
                    source: "env".to_string(),
                });
            }
        }

        targets
    }

    async fn send_with_retry(&self, url: &str, payload: &Value) -> Result<()> {
        let mut backoff = self.retry_initial_backoff;
        let mut last_err: Option<anyhow::Error> = None;
        let payload_bytes =
            serde_json::to_vec(payload).context("failed to encode webhook payload")?;

        for attempt in 1..=self.max_attempts {
            let mut request = self
                .client
                .post(url)
                .header("Content-Type", "application/json");

            if self.signing_enabled {
                let secret = self
                    .signing_secret
                    .as_ref()
                    .ok_or_else(|| anyhow!("webhook signing secret is empty"))?;
                let timestamp = (now_unix_ms() / 1000).to_string();
                let nonce = generate_webhook_nonce()?;
                let signature = compute_webhook_signature(
                    secret,
                    &timestamp,
                    &nonce,
                    payload_bytes.as_slice(),
                )?;

                request = request
                    .header("X-Laser-Timestamp", timestamp)
                    .header("X-Laser-Nonce", nonce)
                    .header("X-Laser-Signature", format!("v1={signature}"));
                if !self.signing_key_id.trim().is_empty() {
                    request = request.header("X-Laser-Key-Id", self.signing_key_id.clone());
                }
            }

            match request.body(payload_bytes.clone()).send().await {
                Ok(response) if response.status().is_success() => {
                    return Ok(());
                }
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| String::from("<unable to read response body>"));
                    last_err = Some(anyhow!("status={status} body={body}"));
                }
                Err(err) => {
                    last_err = Some(anyhow!(err));
                }
            }

            if attempt < self.max_attempts {
                sleep(backoff).await;
                backoff = next_backoff(backoff, Duration::from_secs(5));
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("agent webhook delivery failed with unknown error")))
    }
}

#[derive(Debug, Clone)]
struct AgentDeliveryTarget {
    route_id: String,
    url: String,
    program_id_filter: String,
    event_type_filter: String,
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DlqRecord {
    failed_at_unix_ms: u128,
    error: String,
    event: NormalizedEvent,
}

#[derive(Clone)]
struct DlqStore {
    path: Arc<PathBuf>,
    replay_batch_size: usize,
    alert_threshold: usize,
    notifier: Option<AlertNotifier>,
    io_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Default)]
struct DlqReplayStats {
    processed: usize,
    replayed: usize,
    remaining: usize,
}

impl DlqStore {
    async fn new(
        path: String,
        replay_batch_size: usize,
        alert_threshold: usize,
        notifier: Option<AlertNotifier>,
    ) -> Result<Self> {
        let path = PathBuf::from(path);
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context("failed to create DLQ directory")?;
            }
        }

        Ok(Self {
            path: Arc::new(path),
            replay_batch_size: replay_batch_size.max(1),
            alert_threshold: alert_threshold.max(1),
            notifier,
            io_lock: Arc::new(Mutex::new(())),
        })
    }

    async fn append(&self, event: &NormalizedEvent, reason: &str) -> Result<()> {
        let _guard = self.io_lock.lock().await;

        let record = DlqRecord {
            failed_at_unix_ms: now_unix_ms(),
            error: reason.to_string(),
            event: event.clone(),
        };
        let serialized = serde_json::to_string(&record)?;

        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context("failed to create DLQ directory before append")?;
            }
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path.as_path())
            .await
            .context("failed to open DLQ file")?;

        file.write_all(serialized.as_bytes())
            .await
            .context("failed to write DLQ record")?;
        file.write_all(b"\n")
            .await
            .context("failed to finalize DLQ record")?;

        Ok(())
    }

    async fn replay_once(&self, publisher: &Publisher) -> Result<DlqReplayStats> {
        let _guard = self.io_lock.lock().await;

        if !path_exists(self.path.as_path()).await? {
            return Ok(DlqReplayStats::default());
        }

        let content = tokio::fs::read_to_string(self.path.as_path())
            .await
            .context("failed to read DLQ file")?;
        if content.trim().is_empty() {
            return Ok(DlqReplayStats::default());
        }

        let mut stats = DlqReplayStats::default();
        let mut remaining_lines = Vec::new();

        for (index, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            if index >= self.replay_batch_size {
                remaining_lines.push(trimmed.to_string());
                continue;
            }

            stats.processed += 1;
            let record: DlqRecord = match serde_json::from_str(trimmed) {
                Ok(record) => record,
                Err(err) => {
                    warn!("Skipping invalid DLQ line due to JSON error: {err}");
                    remaining_lines.push(trimmed.to_string());
                    continue;
                }
            };

            match publisher.publish(&record.event).await {
                Ok(()) => {
                    stats.replayed += 1;
                }
                Err(err) => {
                    remaining_lines.push(trimmed.to_string());
                    warn!(
                        "DLQ replay failed for event_id={} program_id={}: {err:#}",
                        record.event.event_id, record.event.program_id
                    );
                }
            }
        }

        stats.remaining = remaining_lines.len();
        self.persist_remaining(&remaining_lines).await?;

        if stats.remaining >= self.alert_threshold {
            if let Some(notifier) = &self.notifier {
                notifier
                    .notify(
                        "DLQ backlog threshold reached",
                        &format!(
                            "remaining={} threshold={} path={}",
                            stats.remaining,
                            self.alert_threshold,
                            self.path.display()
                        ),
                    )
                    .await;
            }
        }

        Ok(stats)
    }

    async fn persist_remaining(&self, remaining_lines: &[String]) -> Result<()> {
        if remaining_lines.is_empty() {
            if path_exists(self.path.as_path()).await? {
                tokio::fs::remove_file(self.path.as_path())
                    .await
                    .context("failed to remove drained DLQ file")?;
            }
            return Ok(());
        }

        let temp_path = self.path.with_extension("jsonl.tmp");
        let mut body = remaining_lines.join("\n");
        body.push('\n');

        tokio::fs::write(&temp_path, body)
            .await
            .context("failed to write DLQ temp file")?;
        tokio::fs::rename(&temp_path, self.path.as_path())
            .await
            .context("failed to replace DLQ file")?;

        Ok(())
    }
}

async fn run_dlq_replay_loop(dlq: DlqStore, publisher: Publisher, interval: Duration) {
    loop {
        match dlq.replay_once(&publisher).await {
            Ok(stats) => {
                if stats.processed > 0 {
                    info!(
                        "DLQ replay: processed={} replayed={} remaining={}",
                        stats.processed, stats.replayed, stats.remaining
                    );
                }
            }
            Err(err) => warn!("DLQ replay loop failed: {err:#}"),
        }

        sleep(interval).await;
    }
}

async fn publish_with_retry(
    publisher: &Publisher,
    event: &NormalizedEvent,
    max_attempts: usize,
    initial_backoff: Duration,
    metrics: &MetricsRegistry,
) -> Result<()> {
    let attempts = max_attempts.max(1);
    let mut backoff = initial_backoff.max(Duration::from_millis(1));
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=attempts {
        match publisher.publish(event).await {
            Ok(()) => {
                metrics.record_publish_success();
                return Ok(());
            }
            Err(err) => {
                last_err = Some(err);
                if attempt < attempts {
                    sleep(backoff).await;
                    backoff = next_backoff(backoff, Duration::from_secs(5));
                }
            }
        }
    }

    metrics.record_publish_failure();
    Err(last_err.unwrap_or_else(|| anyhow!("publish failed with unknown error")))
}

async fn run_ingestion_supervisor(
    config: Config,
    idl_cache: IdlCache,
    publisher: Publisher,
    dlq: Option<DlqStore>,
    agent_notifier: Option<AgentNotifier>,
    metrics: MetricsRegistry,
) -> Result<()> {
    let mut endpoint_index = 0usize;
    let mut backoff = config.reconnect_initial_backoff;

    loop {
        let endpoint = config.endpoints[endpoint_index % config.endpoints.len()].clone();
        endpoint_index = endpoint_index.wrapping_add(1);

        info!("Connecting to endpoint: {endpoint}");
        let session_result = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Shutdown signal received");
                return Ok(());
            }
            result = run_endpoint_session(
                &config,
                &idl_cache,
                &publisher,
                dlq.as_ref(),
                agent_notifier.as_ref(),
                &endpoint,
                &metrics,
            ) => result,
        };

        match session_result {
            Ok(()) => {
                backoff = config.reconnect_initial_backoff;
            }
            Err(err) => {
                warn!("Endpoint session failed ({endpoint}): {err:#}");
                info!("Reconnecting with backoff: {:?}", backoff);
                sleep(backoff).await;
                backoff = next_backoff(backoff, config.reconnect_max_backoff);
            }
        }
    }
}

async fn run_endpoint_session(
    config: &Config,
    idl_cache: &IdlCache,
    publisher: &Publisher,
    dlq: Option<&DlqStore>,
    agent_notifier: Option<&AgentNotifier>,
    endpoint: &str,
    metrics: &MetricsRegistry,
) -> Result<()> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())
        .context("failed to create geyser client builder")?
        .x_token(config.x_token.clone())
        .context("failed to configure x-token")?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .tcp_nodelay(true)
        .max_decoding_message_size(64 * 1024 * 1024);

    if endpoint.starts_with("https://") {
        builder = builder
            .tls_config(ClientTlsConfig::new())
            .context("failed to configure TLS for https endpoint")?;
    }

    let mut client = builder
        .connect()
        .await
        .with_context(|| format!("failed to connect to geyser endpoint: {endpoint}"))?;

    let (mut subscribe_tx, mut stream) = client
        .subscribe()
        .await
        .context("failed to open geyser subscription stream")?;

    let subscribe_request = build_subscription_request(&config.program_ids, config.commitment);
    subscribe_tx
        .send(subscribe_request)
        .await
        .context("failed to send initial subscribe request")?;

    info!(
        "Subscribed to {} programs on endpoint {}",
        config.program_ids.len(),
        endpoint
    );

    let mut ping_interval = tokio::time::interval(config.ping_interval);

    loop {
        tokio::select! {
            _ = ping_interval.tick() => {
                let ping_request = SubscribeRequest {
                    ping: Some(SubscribeRequestPing { id: 1 }),
                    ..Default::default()
                };
                subscribe_tx
                    .send(ping_request)
                    .await
                    .context("failed to send keepalive ping")?;
            }
            maybe_update = stream.next() => {
                let update = match maybe_update {
                    Some(Ok(update)) => update,
                    Some(Err(status)) => {
                        return Err(anyhow!("stream returned grpc status: {status}"));
                    }
                    None => {
                        return Err(anyhow!("stream closed by remote endpoint"));
                    }
                };

                let events = normalize_update(update, idl_cache, endpoint).await;
                for event in events {
                    metrics.record_event_observed(event.observed_at_unix_ms);

                    if let Some(agent) = agent_notifier {
                        let agent_clone = agent.clone();
                        let event_clone = event.clone();
                        tokio::spawn(async move {
                            agent_clone.notify_event(&event_clone).await;
                        });
                    }

                    if let Err(err) = publish_with_retry(
                        publisher,
                        &event,
                        config.publish_max_attempts,
                        config.publish_retry_initial_backoff,
                        metrics,
                    ).await {
                        warn!(
                            "Failed to publish event_id={} program_id={}: {err:#}",
                            event.event_id, event.program_id
                        );
                        if let Some(dlq_store) = dlq {
                            if let Err(dlq_err) = dlq_store.append(&event, &err.to_string()).await {
                                error!(
                                    "Failed to append event_id={} to DLQ: {dlq_err:#}",
                                    event.event_id
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn build_subscription_request(
    program_ids: &[String],
    commitment: CommitmentLevel,
) -> SubscribeRequest {
    let mut transactions = HashMap::new();
    transactions.insert(
        "program-transactions".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: None,
            signature: None,
            account_include: program_ids.to_vec(),
            account_exclude: Vec::new(),
            account_required: Vec::new(),
        },
    );

    let mut accounts = HashMap::new();
    accounts.insert(
        "program-accounts".to_string(),
        SubscribeRequestFilterAccounts {
            account: Vec::new(),
            owner: program_ids.to_vec(),
            filters: Vec::new(),
            nonempty_txn_signature: None,
        },
    );

    SubscribeRequest {
        accounts,
        transactions,
        commitment: Some(commitment as i32),
        ..Default::default()
    }
}

async fn normalize_update(
    update: SubscribeUpdate,
    idl_cache: &IdlCache,
    endpoint: &str,
) -> Vec<NormalizedEvent> {
    let idl_snapshot = idl_cache.snapshot().await;
    let filters = update.filters.clone();

    match update.update_oneof {
        Some(UpdateOneof::Transaction(transaction_update)) => {
            normalize_transaction_update(transaction_update, &idl_snapshot, endpoint, &filters)
        }
        Some(UpdateOneof::Account(account_update)) => {
            normalize_account_update(account_update, &idl_snapshot, endpoint, &filters)
        }
        _ => Vec::new(),
    }
}

fn normalize_transaction_update(
    transaction_update: SubscribeUpdateTransaction,
    idls: &HashMap<String, IdlEntry>,
    endpoint: &str,
    filters: &[String],
) -> Vec<NormalizedEvent> {
    let slot = transaction_update.slot;
    let Some(transaction_info) = transaction_update.transaction else {
        return Vec::new();
    };

    let signature = if transaction_info.signature.is_empty() {
        None
    } else {
        Some(signature_to_string(&transaction_info.signature))
    };

    let Some(raw_transaction) = transaction_info.transaction else {
        return Vec::new();
    };
    let Some(message) = raw_transaction.message else {
        return Vec::new();
    };

    let mut account_keys: Vec<String> = message
        .account_keys
        .iter()
        .map(|key| pubkey_to_string(key))
        .collect();
    if let Some(meta) = transaction_info.meta.as_ref() {
        for key in &meta.loaded_writable_addresses {
            account_keys.push(pubkey_to_string(key));
        }
        for key in &meta.loaded_readonly_addresses {
            account_keys.push(pubkey_to_string(key));
        }
    }

    let mut events = Vec::new();
    for (instruction_index, instruction) in message.instructions.iter().enumerate() {
        let program_index = instruction.program_id_index as usize;
        if program_index >= account_keys.len() {
            continue;
        }

        let program_id = account_keys[program_index].clone();
        let Some(idl) = idls.get(&program_id) else {
            continue;
        };

        let event_id = format!(
            "{}:{}:{}",
            signature.as_deref().unwrap_or("unknown-signature"),
            slot,
            instruction_index
        );
        let idl_name = resolve_idl_name(idl);
        let partition_key = signature.clone().unwrap_or_else(|| program_id.clone());

        let event = NormalizedEvent {
            event_id,
            event_type: "transaction".to_string(),
            program_id: program_id.clone(),
            slot,
            signature: signature.clone(),
            partition_key,
            source_endpoint: endpoint.to_string(),
            observed_at_unix_ms: now_unix_ms(),
            payload: json!({
                "idl_name": idl_name,
                "filters": filters,
                "instruction_index": instruction_index,
                "is_vote": transaction_info.is_vote,
                "instruction": {
                    "program_id_index": instruction.program_id_index,
                    "accounts": instruction.accounts,
                    "data_base64": BASE64_STANDARD.encode(&instruction.data),
                },
                "account_keys": account_keys,
                "meta": {
                    "fee": transaction_info.meta.as_ref().map(|meta| meta.fee),
                    "compute_units_consumed": transaction_info.meta.as_ref().and_then(|meta| meta.compute_units_consumed),
                    "log_messages": transaction_info
                        .meta
                        .as_ref()
                        .map(|meta| meta.log_messages.clone())
                        .unwrap_or_default(),
                },
                "idl_version": idl.content.get("version"),
            }),
        };

        events.push(event);
    }

    events
}

fn normalize_account_update(
    account_update: SubscribeUpdateAccount,
    idls: &HashMap<String, IdlEntry>,
    endpoint: &str,
    filters: &[String],
) -> Vec<NormalizedEvent> {
    let Some(account_info) = account_update.account else {
        return Vec::new();
    };

    let owner = pubkey_to_string(&account_info.owner);
    let Some(idl) = idls.get(&owner) else {
        return Vec::new();
    };

    let account_pubkey = pubkey_to_string(&account_info.pubkey);
    let signature = account_info
        .txn_signature
        .as_ref()
        .map(|bytes| signature_to_string(bytes));
    let event_id = format!("{}:{}:account", account_update.slot, account_pubkey);
    let partition_key = signature.clone().unwrap_or_else(|| account_pubkey.clone());

    vec![NormalizedEvent {
        event_id,
        event_type: "account".to_string(),
        program_id: owner.clone(),
        slot: account_update.slot,
        signature,
        partition_key,
        source_endpoint: endpoint.to_string(),
        observed_at_unix_ms: now_unix_ms(),
        payload: json!({
            "idl_name": resolve_idl_name(idl),
            "filters": filters,
            "account_pubkey": account_pubkey,
            "owner": owner,
            "lamports": account_info.lamports,
            "rent_epoch": account_info.rent_epoch,
            "write_version": account_info.write_version,
            "executable": account_info.executable,
            "data_base64": BASE64_STANDARD.encode(&account_info.data),
            "idl_version": idl.content.get("version"),
        }),
    }]
}

fn resolve_idl_name(idl: &IdlEntry) -> String {
    if let Some(name) = idl.name.as_ref().filter(|name| !name.trim().is_empty()) {
        return name.clone();
    }

    idl.content
        .get("name")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "unknown".to_string())
}

fn parse_commitment_level(raw: &str) -> Result<CommitmentLevel> {
    match raw.trim().to_lowercase().as_str() {
        "processed" => Ok(CommitmentLevel::Processed),
        "confirmed" => Ok(CommitmentLevel::Confirmed),
        "finalized" => Ok(CommitmentLevel::Finalized),
        other => Err(anyhow!(
            "invalid LASERSTREAM_COMMITMENT value `{other}`; use processed|confirmed|finalized"
        )),
    }
}

fn parse_csv_env(key: &str) -> Vec<String> {
    optional_env(key)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn parse_u64_env(key: &str, fallback: u64) -> Result<u64> {
    match optional_env(key) {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("{key} must be a valid u64")),
        None => Ok(fallback),
    }
}

fn parse_bool_env(key: &str, fallback: bool) -> bool {
    match optional_env(key) {
        Some(value) => match value.to_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => true,
            "0" | "false" | "no" | "n" | "off" => false,
            _ => fallback,
        },
        None => fallback,
    }
}

async fn path_exists(path: &Path) -> Result<bool> {
    match tokio::fs::metadata(path).await {
        Ok(_) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err.into()),
    }
}

fn optional_env(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_or_default(key: &str, fallback: &str) -> String {
    optional_env(key).unwrap_or_else(|| fallback.to_string())
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

fn u128_to_u64(value: u128) -> u64 {
    value.min(u64::MAX as u128) as u64
}

fn generate_webhook_nonce() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).context("failed to generate webhook nonce")?;
    Ok(hex_encode(&bytes))
}

fn compute_webhook_signature(
    secret: &str,
    timestamp: &str,
    nonce: &str,
    payload: &[u8],
) -> Result<String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .context("failed to initialize webhook hmac signer")?;
    mac.update(timestamp.as_bytes());
    mac.update(b"\n");
    mac.update(nonce.as_bytes());
    mac.update(b"\n");
    mac.update(payload);
    Ok(hex_encode(mac.finalize().into_bytes().as_slice()))
}

fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(LUT[(byte >> 4) as usize] as char);
        output.push(LUT[(byte & 0x0f) as usize] as char);
    }
    output
}

fn pubkey_to_string(bytes: &[u8]) -> String {
    Pubkey::try_from(bytes)
        .map(|pubkey| pubkey.to_string())
        .unwrap_or_else(|_| BASE64_STANDARD.encode(bytes))
}

fn signature_to_string(bytes: &[u8]) -> String {
    Signature::try_from(bytes)
        .map(|signature| signature.to_string())
        .unwrap_or_else(|_| BASE64_STANDARD.encode(bytes))
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    let doubled = current.saturating_mul(2);
    if doubled > max { max } else { doubled }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn sample_event(program_id: &str, event_type: &str) -> NormalizedEvent {
        NormalizedEvent {
            event_id: "event-1".to_string(),
            event_type: event_type.to_string(),
            program_id: program_id.to_string(),
            slot: 1,
            signature: Some("sig-1".to_string()),
            partition_key: "sig-1".to_string(),
            source_endpoint: "endpoint".to_string(),
            observed_at_unix_ms: 1,
            payload: json!({}),
        }
    }

    #[test]
    fn parse_commitment_level_accepts_expected_values() {
        assert_eq!(
            parse_commitment_level("processed").expect("processed should parse"),
            CommitmentLevel::Processed
        );
        assert_eq!(
            parse_commitment_level("confirmed").expect("confirmed should parse"),
            CommitmentLevel::Confirmed
        );
        assert_eq!(
            parse_commitment_level("finalized").expect("finalized should parse"),
            CommitmentLevel::Finalized
        );
    }

    #[test]
    fn parse_commitment_level_rejects_invalid_values() {
        assert!(parse_commitment_level("invalid").is_err());
    }

    #[test]
    fn next_backoff_caps_at_max() {
        assert_eq!(
            next_backoff(Duration::from_secs(1), Duration::from_secs(30)),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(20), Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn agent_webhook_entry_matches_program_and_event_filters() {
        let event = sample_event("ProgramA", "transaction");

        let wildcard_route = AgentWebhookEntry {
            id: 1,
            url: "https://agent.example/wildcard".to_string(),
            program_id: None,
            event_type: None,
        };
        assert!(wildcard_route.matches_event(&event));

        let matching_route = AgentWebhookEntry {
            id: 2,
            url: "https://agent.example/matching".to_string(),
            program_id: Some("ProgramA".to_string()),
            event_type: Some("transaction".to_string()),
        };
        assert!(matching_route.matches_event(&event));

        let wrong_program = AgentWebhookEntry {
            id: 3,
            url: "https://agent.example/wrong-program".to_string(),
            program_id: Some("ProgramB".to_string()),
            event_type: Some("transaction".to_string()),
        };
        assert!(!wrong_program.matches_event(&event));

        let wrong_event_type = AgentWebhookEntry {
            id: 4,
            url: "https://agent.example/wrong-type".to_string(),
            program_id: Some("ProgramA".to_string()),
            event_type: Some("account".to_string()),
        };
        assert!(!wrong_event_type.matches_event(&event));
    }

    #[tokio::test]
    async fn agent_notifier_targets_deduplicate_urls_and_apply_filters() {
        let cache = AgentWebhookCache::default();
        cache
            .replace_all(vec![
                AgentWebhookEntry {
                    id: 1,
                    url: "https://agent.example/program".to_string(),
                    program_id: Some("ProgramA".to_string()),
                    event_type: Some("transaction".to_string()),
                },
                AgentWebhookEntry {
                    id: 2,
                    url: "https://agent.example/all".to_string(),
                    program_id: None,
                    event_type: None,
                },
                AgentWebhookEntry {
                    id: 3,
                    url: "https://agent.example/program".to_string(),
                    program_id: Some("ProgramA".to_string()),
                    event_type: Some("transaction".to_string()),
                },
                AgentWebhookEntry {
                    id: 4,
                    url: "https://agent.example/other".to_string(),
                    program_id: Some("ProgramB".to_string()),
                    event_type: Some("transaction".to_string()),
                },
            ])
            .await;

        let notifier = AgentNotifier::new(
            Some("https://agent.example/program".to_string()),
            cache,
            Duration::from_secs(1),
            1,
            Duration::from_millis(1),
            false,
            None,
            String::new(),
            MetricsRegistry::default(),
        )
        .expect("agent notifier should initialize");

        let event = sample_event("ProgramA", "transaction");
        let targets = notifier.targets_for_event(&event).await;

        let urls: HashSet<String> = targets.iter().map(|target| target.url.clone()).collect();
        assert_eq!(urls.len(), 2, "expected deduplicated URLs");
        assert!(urls.contains("https://agent.example/program"));
        assert!(urls.contains("https://agent.example/all"));
        assert!(!urls.contains("https://agent.example/other"));
    }

    #[tokio::test]
    async fn agent_notifier_uses_static_fallback_when_dynamic_routes_do_not_match() {
        let cache = AgentWebhookCache::default();
        cache
            .replace_all(vec![AgentWebhookEntry {
                id: 1,
                url: "https://agent.example/other-program".to_string(),
                program_id: Some("ProgramB".to_string()),
                event_type: Some("transaction".to_string()),
            }])
            .await;

        let notifier = AgentNotifier::new(
            Some("https://agent.example/static".to_string()),
            cache,
            Duration::from_secs(1),
            1,
            Duration::from_millis(1),
            false,
            None,
            String::new(),
            MetricsRegistry::default(),
        )
        .expect("agent notifier should initialize");

        let event = sample_event("ProgramA", "transaction");
        let targets = notifier.targets_for_event(&event).await;

        assert_eq!(targets.len(), 1, "expected only static fallback target");
        assert_eq!(targets[0].source, "env");
        assert_eq!(targets[0].url, "https://agent.example/static");
    }

    #[tokio::test]
    async fn agent_notifier_returns_no_targets_without_matches_or_static_url() {
        let cache = AgentWebhookCache::default();
        cache
            .replace_all(vec![AgentWebhookEntry {
                id: 1,
                url: "https://agent.example/other-program".to_string(),
                program_id: Some("ProgramB".to_string()),
                event_type: Some("account".to_string()),
            }])
            .await;

        let notifier = AgentNotifier::new(
            None,
            cache,
            Duration::from_secs(1),
            1,
            Duration::from_millis(1),
            false,
            None,
            String::new(),
            MetricsRegistry::default(),
        )
        .expect("agent notifier should initialize");

        let event = sample_event("ProgramA", "transaction");
        let targets = notifier.targets_for_event(&event).await;

        assert!(targets.is_empty(), "expected no delivery targets");
    }

    #[test]
    fn compute_webhook_signature_is_deterministic() {
        let payload = br#"{"event":"x"}"#;
        let signature_a =
            compute_webhook_signature("secret", "1700000000", "nonce-a", payload).unwrap();
        let signature_b =
            compute_webhook_signature("secret", "1700000000", "nonce-a", payload).unwrap();
        let signature_c =
            compute_webhook_signature("secret", "1700000001", "nonce-a", payload).unwrap();

        assert_eq!(signature_a, signature_b);
        assert_ne!(signature_a, signature_c);
    }

    #[test]
    fn metrics_registry_tracks_ingestion_lag() {
        let metrics = MetricsRegistry::default();
        metrics.record_event_observed(1_000);

        assert_eq!(metrics.events_total.load(Ordering::Relaxed), 1);
        assert!(metrics.ingestion_lag_ms() > 0);
    }
}
