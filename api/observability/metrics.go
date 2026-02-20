package observability

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v2"
)

type requestKey struct {
	Method string
	Path   string
	Status int
}

type requestStats struct {
	Count       uint64
	DurationSum uint64
}

type stageCounter map[string]uint64

type Registry struct {
	mu       sync.RWMutex
	requests map[requestKey]requestStats
	queryErr stageCounter
	autoErr  stageCounter

	wsCurrentConnections  int64
	wsRejectedConnections uint64
	wsSlowDrops           uint64
	wsMessagesOut         uint64
	queryExecutions       uint64
	automationEvaluations uint64
}

var Metrics = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{
		requests: make(map[requestKey]requestStats),
		queryErr: make(stageCounter),
		autoErr:  make(stageCounter),
	}
}

func Middleware() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		err := c.Next()

		method := c.Method()
		path := c.Route().Path
		if path == "" {
			path = c.Path()
		}
		status := c.Response().StatusCode()
		durationMicros := uint64(time.Since(start).Microseconds())

		Metrics.recordRequest(method, path, status, durationMicros)
		return err
	}
}

func Handler(c *fiber.Ctx) error {
	c.Set(fiber.HeaderContentType, "text/plain; version=0.0.4")
	return c.SendString(Metrics.prometheus())
}

func (r *Registry) recordRequest(method, path string, status int, durationMicros uint64) {
	if path == "" {
		path = "/unknown"
	}
	if method == "" {
		method = "UNKNOWN"
	}

	key := requestKey{
		Method: method,
		Path:   path,
		Status: status,
	}

	r.mu.Lock()
	stats := r.requests[key]
	stats.Count++
	stats.DurationSum += durationMicros
	r.requests[key] = stats
	r.mu.Unlock()
}

func WSConnectionOpened() {
	atomic.AddInt64(&Metrics.wsCurrentConnections, 1)
}

func WSConnectionClosed() {
	atomic.AddInt64(&Metrics.wsCurrentConnections, -1)
}

func WSConnectionRejected() {
	atomic.AddUint64(&Metrics.wsRejectedConnections, 1)
}

func WSSlowClientDropped() {
	atomic.AddUint64(&Metrics.wsSlowDrops, 1)
}

func WSMessageOut() {
	atomic.AddUint64(&Metrics.wsMessagesOut, 1)
}

func AgentQueryExecution() {
	atomic.AddUint64(&Metrics.queryExecutions, 1)
}

func AgentQueryError(stage string) {
	stage = sanitizeStage(stage)
	Metrics.mu.Lock()
	Metrics.queryErr[stage]++
	Metrics.mu.Unlock()
}

func AgentAutomationEvaluation() {
	atomic.AddUint64(&Metrics.automationEvaluations, 1)
}

func AgentAutomationFailure(stage string) {
	stage = sanitizeStage(stage)
	Metrics.mu.Lock()
	Metrics.autoErr[stage]++
	Metrics.mu.Unlock()
}

func (r *Registry) prometheus() string {
	var builder strings.Builder
	builder.WriteString("# TYPE api_http_requests_total counter\n")
	builder.WriteString("# TYPE api_http_request_duration_microseconds_sum counter\n")
	builder.WriteString("# TYPE api_ws_current_connections gauge\n")
	builder.WriteString("# TYPE api_ws_rejected_connections_total counter\n")
	builder.WriteString("# TYPE api_ws_slow_client_drops_total counter\n")
	builder.WriteString("# TYPE api_ws_messages_out_total counter\n")
	builder.WriteString("# TYPE api_agent_query_executions_total counter\n")
	builder.WriteString("# TYPE api_agent_query_errors_total counter\n")
	builder.WriteString("# TYPE api_agent_automation_evaluations_total counter\n")
	builder.WriteString("# TYPE api_agent_automation_failures_total counter\n")

	r.mu.RLock()
	keys := make([]requestKey, 0, len(r.requests))
	for key := range r.requests {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		if a.Method != b.Method {
			return a.Method < b.Method
		}
		return a.Status < b.Status
	})

	for _, key := range keys {
		stats := r.requests[key]
		labels := fmt.Sprintf(`method=%q,path=%q,status=%q`, key.Method, key.Path, strconv.Itoa(key.Status))
		builder.WriteString("api_http_requests_total{")
		builder.WriteString(labels)
		builder.WriteString("} ")
		builder.WriteString(strconv.FormatUint(stats.Count, 10))
		builder.WriteByte('\n')

		builder.WriteString("api_http_request_duration_microseconds_sum{")
		builder.WriteString(labels)
		builder.WriteString("} ")
		builder.WriteString(strconv.FormatUint(stats.DurationSum, 10))
		builder.WriteByte('\n')
	}
	r.mu.RUnlock()

	builder.WriteString("api_ws_current_connections ")
	builder.WriteString(strconv.FormatInt(atomic.LoadInt64(&r.wsCurrentConnections), 10))
	builder.WriteByte('\n')

	builder.WriteString("api_ws_rejected_connections_total ")
	builder.WriteString(strconv.FormatUint(atomic.LoadUint64(&r.wsRejectedConnections), 10))
	builder.WriteByte('\n')

	builder.WriteString("api_ws_slow_client_drops_total ")
	builder.WriteString(strconv.FormatUint(atomic.LoadUint64(&r.wsSlowDrops), 10))
	builder.WriteByte('\n')

	builder.WriteString("api_ws_messages_out_total ")
	builder.WriteString(strconv.FormatUint(atomic.LoadUint64(&r.wsMessagesOut), 10))
	builder.WriteByte('\n')

	builder.WriteString("api_agent_query_executions_total ")
	builder.WriteString(strconv.FormatUint(atomic.LoadUint64(&r.queryExecutions), 10))
	builder.WriteByte('\n')

	builder.WriteString("api_agent_automation_evaluations_total ")
	builder.WriteString(strconv.FormatUint(atomic.LoadUint64(&r.automationEvaluations), 10))
	builder.WriteByte('\n')

	r.mu.RLock()
	queryErrStages := make([]string, 0, len(r.queryErr))
	for stage := range r.queryErr {
		queryErrStages = append(queryErrStages, stage)
	}
	sort.Strings(queryErrStages)
	for _, stage := range queryErrStages {
		builder.WriteString("api_agent_query_errors_total{stage=")
		builder.WriteString(strconv.Quote(stage))
		builder.WriteString("} ")
		builder.WriteString(strconv.FormatUint(r.queryErr[stage], 10))
		builder.WriteByte('\n')
	}

	autoErrStages := make([]string, 0, len(r.autoErr))
	for stage := range r.autoErr {
		autoErrStages = append(autoErrStages, stage)
	}
	sort.Strings(autoErrStages)
	for _, stage := range autoErrStages {
		builder.WriteString("api_agent_automation_failures_total{stage=")
		builder.WriteString(strconv.Quote(stage))
		builder.WriteString("} ")
		builder.WriteString(strconv.FormatUint(r.autoErr[stage], 10))
		builder.WriteByte('\n')
	}
	r.mu.RUnlock()

	return builder.String()
}

func sanitizeStage(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "unknown"
	}

	var out strings.Builder
	for _, ch := range value {
		switch {
		case ch >= 'a' && ch <= 'z':
			out.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			out.WriteRune(ch)
		case ch == '_' || ch == '-':
			out.WriteRune(ch)
		default:
			out.WriteByte('_')
		}
	}
	if out.Len() == 0 {
		return "unknown"
	}
	return out.String()
}
