import type {
  AgentQueryRequest,
  AgentQueryResponse,
  AgentWebhook,
  AgentWebhookCreateRequest,
  Automation,
  AutomationCreateRequest,
  AutomationEvaluationResponse,
  AutomationListResponse,
  AutomationUpdateRequest,
  InboundWebhookResponse,
  LaunchBacktestRequest,
  LaunchBacktestResponse,
  LaunchScoreRequest
} from "./types.js";

export interface LaserStreamClientOptions {
  baseUrl: string;
  apiKey?: string;
  bearerToken?: string;
  fetchImpl?: typeof fetch;
}

export interface WebhookSignatureHeaders {
  timestamp: string;
  nonce: string;
  signature: string;
  keyId?: string;
}

export class LaserStreamClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly bearerToken?: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: LaserStreamClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.apiKey = options.apiKey;
    this.bearerToken = options.bearerToken;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }

  query(payload: AgentQueryRequest): Promise<AgentQueryResponse> {
    return this.request<AgentQueryResponse>("/api/agent/query", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }

  queryReport(payload: AgentQueryRequest): Promise<AgentQueryResponse> {
    return this.request<AgentQueryResponse>("/api/agent/query/report", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }

  backtestLaunches(payload: LaunchBacktestRequest): Promise<LaunchBacktestResponse> {
    return this.request<LaunchBacktestResponse>("/api/agent/backtest/launches", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }

  scoreLaunch(payload: LaunchScoreRequest): Promise<Record<string, unknown>> {
    return this.request<Record<string, unknown>>("/api/agent/backtest/launches/score", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }

  createWebhook(payload: AgentWebhookCreateRequest): Promise<AgentWebhook> {
    return this.request<AgentWebhook>("/api/agent/webhooks", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }

  listWebhooks(): Promise<AgentWebhook[]> {
    return this.request<AgentWebhook[]>("/api/agent/webhooks");
  }

  postInboundWebhook(payload: Record<string, unknown>, signatureHeaders: WebhookSignatureHeaders): Promise<InboundWebhookResponse> {
    return this.request<InboundWebhookResponse>("/api/agent/webhooks/inbound", {
      method: "POST",
      headers: {
        "X-Laser-Timestamp": signatureHeaders.timestamp,
        "X-Laser-Nonce": signatureHeaders.nonce,
        "X-Laser-Signature": signatureHeaders.signature,
        ...(signatureHeaders.keyId ? { "X-Laser-Key-Id": signatureHeaders.keyId } : {})
      },
      body: JSON.stringify(payload),
      omitAuth: true
    });
  }

  createAutomation(payload: AutomationCreateRequest): Promise<Automation> {
    return this.request<Automation>("/api/agent/automations", {
      method: "POST",
      body: JSON.stringify(payload)
    });
  }

  listAutomations(limit = 100, offset = 0): Promise<AutomationListResponse> {
    const query = new URLSearchParams({
      limit: String(limit),
      offset: String(offset)
    });
    return this.request<AutomationListResponse>(`/api/agent/automations?${query.toString()}`);
  }

  updateAutomation(id: number, payload: AutomationUpdateRequest): Promise<Automation> {
    return this.request<Automation>(`/api/agent/automations/${id}`, {
      method: "PATCH",
      body: JSON.stringify(payload)
    });
  }

  evaluateAutomation(id: number, options?: { sendWebhook?: boolean; dryRun?: boolean }): Promise<AutomationEvaluationResponse> {
    const params = new URLSearchParams();
    if (typeof options?.sendWebhook === "boolean") {
      params.set("send_webhook", String(options.sendWebhook));
    }
    if (typeof options?.dryRun === "boolean") {
      params.set("dry_run", String(options.dryRun));
    }
    const suffix = params.toString();
    const path = suffix.length > 0
      ? `/api/agent/automations/${id}/evaluate?${suffix}`
      : `/api/agent/automations/${id}/evaluate`;

    return this.request<AutomationEvaluationResponse>(path, {
      method: "POST"
    });
  }

  listAutomationAudit(id: number, limit = 100, offset = 0): Promise<Record<string, unknown>> {
    const query = new URLSearchParams({
      limit: String(limit),
      offset: String(offset)
    });
    return this.request<Record<string, unknown>>(`/api/agent/automations/${id}/audit?${query.toString()}`);
  }

  private async request<T>(
    path: string,
    init: (RequestInit & { omitAuth?: boolean }) = {}
  ): Promise<T> {
    const headers = new Headers(init.headers ?? {});
    if (!headers.has("Content-Type") && init.body != null) {
      headers.set("Content-Type", "application/json");
    }

    if (!init.omitAuth) {
      if (this.apiKey) {
        headers.set("X-API-Key", this.apiKey);
      }
      if (this.bearerToken) {
        headers.set("Authorization", `Bearer ${this.bearerToken}`);
      }
    }

    const response = await this.fetchImpl(`${this.baseUrl}${path}`, {
      ...init,
      headers
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`LaserStream request failed (${response.status}): ${text}`);
    }

    if (response.status === 204) {
      return {} as T;
    }
    return (await response.json()) as T;
  }
}
