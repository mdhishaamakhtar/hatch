package verify

import (
	"context"
	"time"
)

// checkTelemetry checks the metrics, logs and traces the audit's own traffic
// produced reached Prometheus, Loki and Tempo.
func (v *verifier) checkTelemetry(ctx context.Context) {
	v.section("Telemetry")

	v.checkMetric(ctx, `sum(hatch_api_requests_total{endpoint="POST /v1/schedules"})`, 1)
	v.checkMetric(ctx, `sum(hatch_api_idempotency_hits_total)`, 1)
	v.checkMetric(ctx, `count(sum by (pod) (hatch_scheduler_poll_emails_loaded_total))`, float64(v.SchedulerReplicas))

	for _, logs := range []struct{ query, want string }{
		{`{service_name="api"} |= "schedule created" |= "` + v.golden.String() + `"`, "the API logged creating schedule " + v.golden.String()},
		{`{service_name="scheduler"} |= "wheel fired"`, "the scheduler logged firing its wheel"},
	} {
		ok := eventually(ctx, telemetryTimeout, func() bool {
			lines, err := v.logLines(ctx, logs.query, 15*time.Minute)
			return err == nil && len(lines) > 0
		})
		v.check(ok, "Loki: %s", logs.want)
	}
	lines, err := v.logLines(ctx, `{service_name="scheduler"} |= `+"`"+`"level":"error"`+"`", 5*time.Minute)
	if err != nil {
		v.fail("Loki: %v", err)
	} else {
		v.check(len(lines) == 0, "Loki: the scheduler logged %d error(s) in the last 5 minutes", len(lines))
	}

	for _, service := range []string{"scheduler-api", "scheduler-service"} {
		ok := eventually(ctx, telemetryTimeout, func() bool { return v.hasTraces(ctx, service) })
		v.check(ok, "Tempo: traces from %s", service)
	}
}
