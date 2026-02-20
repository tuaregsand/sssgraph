export type QueryMode = "sql" | "verified" | "analyst_report";

export interface AgentQueryRequest {
  prompt: string;
  execute?: boolean;
  limit?: number;
  mode?: QueryMode;
  strict_verified?: boolean;
}

export interface AgentQueryResponse {
  prompt: string;
  sql: string;
  sql_hash_sha256: string;
  status: string;
  mode: QueryMode;
  count?: number;
  limit?: number;
  items?: Record<string, unknown>[];
  verified_answer?: string;
  report?: Record<string, unknown>;
  evidence_bundle?: Record<string, unknown>;
}

export interface LaunchInput {
  name?: string;
  program_id: string;
  launch_at: string;
}

export interface LaunchBacktestThresholds {
  event_growth_ratio?: number;
  unique_signer_growth_ratio?: number;
  transaction_growth_ratio?: number;
}

export interface LaunchBacktestRequest {
  launches: LaunchInput[];
  baseline_hours?: number;
  evaluation_hours?: number;
  min_evaluation_events?: number;
  thresholds?: LaunchBacktestThresholds;
}

export interface LaunchScoreRequest {
  history_launches: LaunchInput[];
  candidate: LaunchInput;
  baseline_hours?: number;
  evaluation_hours?: number;
  min_evaluation_events?: number;
  thresholds?: LaunchBacktestThresholds;
}

export interface LaunchBacktestResponse {
  status: string;
  config: Record<string, unknown>;
  summary: Record<string, unknown>;
  launches: Record<string, unknown>[];
}

export interface AgentWebhookCreateRequest {
  url: string;
  program_id?: string;
  event_type?: string;
}

export interface AgentWebhook {
  id: number;
  url: string;
  program_id: string;
  event_type: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface AutomationCreateRequest {
  name: string;
  description?: string;
  program_id?: string;
  query_template: string;
  comparator: "gt" | "gte" | "lt" | "lte";
  threshold: number;
  schedule_minutes?: number;
  window_minutes?: number;
  cooldown_minutes?: number;
  dedupe_minutes?: number;
  retry_max_attempts?: number;
  retry_initial_backoff_ms?: number;
  dry_run?: boolean;
  webhook_url: string;
  is_active?: boolean;
}

export interface AutomationUpdateRequest {
  name?: string;
  description?: string;
  program_id?: string;
  query_template?: string;
  comparator?: "gt" | "gte" | "lt" | "lte";
  threshold?: number;
  schedule_minutes?: number;
  window_minutes?: number;
  cooldown_minutes?: number;
  dedupe_minutes?: number;
  retry_max_attempts?: number;
  retry_initial_backoff_ms?: number;
  dry_run?: boolean;
  webhook_url?: string;
  is_active?: boolean;
}

export interface Automation extends AutomationCreateRequest {
  id: number;
  created_at: string;
  updated_at: string;
}

export interface AutomationListResponse {
  items: Automation[];
  limit: number;
  offset: number;
  count: number;
}

export interface AutomationEvaluationResponse {
  automation: Automation;
  evaluation: Record<string, unknown>;
}

export interface InboundWebhookResponse {
  status: "accepted";
  id: number;
  received_at: string;
}
