package verify

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

// obsAttempts / obsDelay bound how long each observability round-trip waits for
// the signal to propagate through Prometheus/Loki/Tempo (~2min, matching the
// old shell timeouts).
const (
	obsAttempts = 40
	obsDelay    = 3 * time.Second
	obsWindow   = 600 // seconds of history to query in Loki
)

// checkObservability confirms the real telemetry emitted by the API and the
// scheduler during this run made it through the pipeline. Because the live
// services emit metrics, logs, and traces, there are no synthetic probes — the
// golden-path and schedule→Kafka flows above are the producers.
func (v *Verifier) checkObservability(ctx context.Context) {
	v.rep.Section("Observability — Prometheus / Loki / Tempo round-trips")

	// Prometheus: API request + idempotency metrics.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		n, _, err := v.promCount(ctx, `hatch_api_requests_total{endpoint="POST /v1/schedules"}`)
		return err == nil && n > 0
	}), `Prometheus has hatch_api_requests_total{endpoint="POST /v1/schedules"}`,
		"Prometheus missing hatch_api_requests_total for POST /v1/schedules")

	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		_, val, err := v.promCount(ctx, "hatch_api_idempotency_hits_total")
		return err == nil && val != "" && val != "0"
	}), "Prometheus has hatch_api_idempotency_hits_total > 0",
		"Prometheus has no hatch_api_idempotency_hits_total samples")

	// Prometheus: scheduler poll metric present for each shard.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		n, _, err := v.promCount(ctx, "sum by (pod_index) (hatch_scheduler_poll_emails_loaded_total)")
		return err == nil && n >= v.cfg.SchedReplicas
	}), "Prometheus has hatch_scheduler_poll_emails_loaded_total per shard",
		"Prometheus missing poll_emails_loaded samples per shard")

	// Loki: API "Schedule created" line tagged service=scheduler-api.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		body, err := v.lokiQuery(ctx, `{service_name="api"} |= "Schedule created"`, obsWindow)
		if err != nil {
			return false
		}
		return strings.Contains(body, "scheduler-api") && (v.schedID == "" || strings.Contains(body, v.schedID))
	}), "Loki has \"Schedule created\" line (service=scheduler-api)",
		"Loki did not return the \"Schedule created\" line")

	// Loki: scheduler "wheel slot fired" line tagged service=scheduler-service.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		body, err := v.lokiQuery(ctx, `{service_name="scheduler"} |= "wheel slot fired"`, obsWindow)
		return err == nil && strings.Contains(body, "scheduler-service")
	}), "Loki has \"wheel slot fired\" line (service=scheduler-service)",
		"Loki did not return the \"wheel slot fired\" line")

	// Loki: no error-level lines from the scheduler in the recent window. Uses a
	// backtick LogQL filter so the embedded JSON quotes need no escaping.
	errQuery := `{service_name="scheduler"} |= ` + "`" + `"level":"error"` + "`"
	body, err := v.lokiQuery(ctx, errQuery, 300)
	switch {
	case err != nil:
		v.rep.Failf("Loki error-log query failed: %v", err)
	case lokiEntryCount(body) == 0:
		v.rep.Pass("no error-level scheduler logs in the last 5m")
	default:
		v.rep.Failf("found %d error-level scheduler log line(s) in the last 5m", lokiEntryCount(body))
	}

	// Tempo: traces for both services.
	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		body, err := v.tempoSearch(ctx, "service.name=scheduler-api")
		return err == nil && strings.Contains(body, "traceID")
	}), "Tempo has traces with service.name=scheduler-api",
		"Tempo did not return any scheduler-api trace")

	v.rep.Check(retry(ctx, obsAttempts, obsDelay, func() bool {
		body, err := v.tempoSearch(ctx, "service.name=scheduler-service")
		return err == nil && strings.Contains(body, "traceID")
	}), "Tempo has traces with service.name=scheduler-service",
		"Tempo did not return any scheduler-service trace")
}

// lokiEntryCount totals the log entries across all streams in a Loki
// query_range response body. Returns -1 if the body can't be parsed.
func lokiEntryCount(body string) int {
	var parsed struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		return -1
	}
	n := 0
	for _, r := range parsed.Data.Result {
		n += len(r.Values)
	}
	return n
}
