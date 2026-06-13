package cdc

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics holds CDC pipeline metrics for monitoring and Prometheus export.
type Metrics struct {
	// Source metrics
	SourceEventsTotal    int64 `json:"source_events_total"`
	SourceLSN            string `json:"source_lsn"`
	SourceRunning        bool  `json:"source_running"`

	// Applier metrics
	ApplierEventsReceived  int64 `json:"applier_events_received"`
	ApplierEventsApplied   int64 `json:"applier_events_applied"`
	ApplierEventsFailed    int64 `json:"applier_events_failed"`
	ApplierEventsSkipped   int64 `json:"applier_events_skipped"`
	ApplierBatchesFlushed  int64 `json:"applier_batches_flushed"`
	ApplierLastLSN         string `json:"applier_last_lsn"`
	ApplierLastError       string `json:"applier_last_error,omitempty"`

	// Lag metrics
	LagSeconds     float64 `json:"lag_seconds"`
	LagEvents      int64   `json:"lag_events"`

	// Throughput
	EventsPerSecond float64 `json:"events_per_second"`
	BytesPerSecond  float64 `json:"bytes_per_second"`

	// Uptime
	UptimeSeconds float64 `json:"uptime_seconds"`

	// Internal
	startTime     time.Time
	lastCalcTime  time.Time
	lastCalcEvents int64
}

// MetricsCollector gathers and exposes CDC metrics.
type MetricsCollector struct {
	mu      sync.RWMutex
	metrics *Metrics

	runner *Runner // for pulling live stats

	// Event counters (atomic for concurrent access)
	eventsProcessed atomic.Int64
	eventsFailed    atomic.Int64
	bytesTransferred atomic.Int64
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: &Metrics{
			startTime: time.Now(),
		},
	}
}

// SetRunner sets the runner for live stat collection.
func (mc *MetricsCollector) SetRunner(runner *Runner) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.runner = runner
}

// RecordEvent increments the processed event counter.
func (mc *MetricsCollector) RecordEvent() {
	mc.eventsProcessed.Add(1)
}

// RecordFailedEvent increments the failed event counter.
func (mc *MetricsCollector) RecordFailedEvent() {
	mc.eventsFailed.Add(1)
}

// RecordBytes adds to the bytes transferred counter.
func (mc *MetricsCollector) RecordBytes(n int64) {
	mc.bytesTransferred.Add(n)
}

// Snapshot returns a copy of current metrics.
func (mc *MetricsCollector) Snapshot() *Metrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	m := *mc.metrics // shallow copy

	// Pull live stats from runner
	if mc.runner != nil {
		stats := mc.runner.Stats()
		if v, ok := stats["source_events"].(int64); ok {
			m.SourceEventsTotal = v
		}
		if v, ok := stats["source_lsn"].(string); ok {
			m.SourceLSN = v
		}
		if v, ok := stats["source_running"].(bool); ok {
			m.SourceRunning = v
		}
		if v, ok := stats["applier_events_received"].(int64); ok {
			m.ApplierEventsReceived = v
		}
		if v, ok := stats["applier_events_applied"].(int64); ok {
			m.ApplierEventsApplied = v
		}
		if v, ok := stats["applier_events_failed"].(int64); ok {
			m.ApplierEventsFailed = v
		}
		if v, ok := stats["applier_events_skipped"].(int64); ok {
			m.ApplierEventsSkipped = v
		}
		if v, ok := stats["applier_batches"].(int64); ok {
			m.ApplierBatchesFlushed = v
		}
		if v, ok := stats["applier_last_lsn"].(string); ok {
			m.ApplierLastLSN = v
		}
		if v, ok := stats["applier_last_error"].(string); ok {
			m.ApplierLastError = v
		}
	}

	// Calculate throughput
	now := time.Now()
	elapsed := now.Sub(m.lastCalcTime).Seconds()
	if elapsed >= 1.0 {
		eventsDelta := m.ApplierEventsApplied - m.lastCalcEvents
		m.EventsPerSecond = float64(eventsDelta) / elapsed
		m.lastCalcTime = now
		m.lastCalcEvents = m.ApplierEventsApplied
	}

	// Calculate lag
	m.LagEvents = m.SourceEventsTotal - m.ApplierEventsApplied
	if m.LagEvents < 0 {
		m.LagEvents = 0
	}

	// Uptime
	m.UptimeSeconds = now.Sub(m.startTime).Seconds()

	return &m
}

// PrometheusText returns metrics in Prometheus text exposition format.
func (mc *MetricsCollector) PrometheusText() string {
	m := mc.Snapshot()

	return fmt.Sprintf(`# HELP pg2tidb_cdc_source_events_total Total events received from PG source
# TYPE pg2tidb_cdc_source_events_total counter
pg2tidb_cdc_source_events_total %d
# HELP pg2tidb_cdc_applier_events_applied Total events applied to TiDB
# TYPE pg2tidb_cdc_applier_events_applied counter
pg2tidb_cdc_applier_events_applied %d
# HELP pg2tidb_cdc_applier_events_failed Total events failed to apply
# TYPE pg2tidb_cdc_applier_events_failed counter
pg2tidb_cdc_applier_events_failed %d
# HELP pg2tidb_cdc_applier_events_skipped Total events skipped
# TYPE pg2tidb_cdc_applier_events_skipped counter
pg2tidb_cdc_applier_events_skipped %d
# HELP pg2tidb_cdc_applier_batches_flushed Total batches flushed
# TYPE pg2tidb_cdc_applier_batches_flushed counter
pg2tidb_cdc_applier_batches_flushed %d
# HELP pg2tidb_cdc_lag_events Event lag (source - applied)
# TYPE pg2tidb_cdc_lag_events gauge
pg2tidb_cdc_lag_events %d
# HELP pg2tidb_cdc_events_per_second Current throughput
# TYPE pg2tidb_cdc_events_per_second gauge
pg2tidb_cdc_events_per_second %.2f
# HELP pg2tidb_cdc_uptime_seconds Uptime in seconds
# TYPE pg2tidb_cdc_uptime_seconds gauge
pg2tidb_cdc_uptime_seconds %.2f
# HELP pg2tidb_cdc_source_running Whether source is running (1=yes)
# TYPE pg2tidb_cdc_source_running gauge
pg2tidb_cdc_source_running %d
`,
		m.SourceEventsTotal,
		m.ApplierEventsApplied,
		m.ApplierEventsFailed,
		m.ApplierEventsSkipped,
		m.ApplierBatchesFlushed,
		m.LagEvents,
		m.EventsPerSecond,
		m.UptimeSeconds,
		boolToInt(m.SourceRunning),
	)
}

// JSON returns metrics as JSON.
func (mc *MetricsCollector) JSON() ([]byte, error) {
	m := mc.Snapshot()
	return json.MarshalIndent(m, "", "  ")
}

// HTTPHandler returns an http.Handler that serves metrics.
func (mc *MetricsCollector) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		if accept == "application/json" {
			w.Header().Set("Content-Type", "application/json")
			data, err := mc.JSON()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Write(data)
		} else {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			w.Write([]byte(mc.PrometheusText()))
		}
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		m := mc.Snapshot()
		if m.SourceRunning {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"healthy"}`))
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte(`{"status":"unhealthy","reason":"source not running"}`))
		}
	})
	return mux
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
