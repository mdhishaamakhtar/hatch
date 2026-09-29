// Package verify is Hatch's acceptance audit. It runs as a Kubernetes Job
// against a deployed stack and checks each stage in the order a schedule
// passes through them: the database and topics, the API, the scheduler,
// delivery, retries, the reconciliation and archival crons, and the telemetry
// they all emit. It prints one [PASS] or [FAIL] line per check.
package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"uuid"

	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/kafka"
	"github.com/mdhishaamakhtar/hatch/internal/stack"
	"github.com/redis/rueidis"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// Config locates the stack and the dependencies the audit reads directly.
type Config struct {
	stack.Config
	RedisAddr    string   `env:"REDIS_ADDR,required,notEmpty"`
	KafkaBrokers []string `env:"KAFKA_BROKERS,required,notEmpty"`
	LokiURL      string   `env:"LOKI_URL"  envDefault:"http://observability-loki-gateway.observability.svc.cluster.local"`
	TempoURL     string   `env:"TEMPO_URL" envDefault:"http://observability-tempo.observability.svc.cluster.local:3200"`

	// ScheduleLead is how far ahead the audit's schedules are due. It has to
	// clear the API's API_MIN_SCHEDULE_HORIZON.
	ScheduleLead time.Duration `env:"VERIFY_SCHEDULE_LEAD" envDefault:"150s"`

	// The Resend check sends a real email with this key.
	ResendAPIKey string `env:"VERIFY_RESEND_API_KEY"`
	ResendFrom   string `env:"VERIFY_RESEND_FROM" envDefault:"verify@nexia.hishaam.dev"`
	ResendTo     string `env:"VERIFY_RESEND_TO"   envDefault:"delivered@resend.dev"`

	// FailRecipient is the address the delivery workers' mock provider always
	// fails to send to.
	FailRecipient string `env:"MOCK_PROVIDER_FAIL_RECIPIENT" envDefault:"fail@mock.test"`
}

// telemetryTimeout bounds the wait for a signal to reach Prometheus, Loki or
// Tempo: a scrape or a batch export, then ingestion.
const telemetryTimeout = 2 * time.Minute

type verifier struct {
	*stack.Stack
	cfg      Config
	lg       *zap.Logger
	redis    rueidis.Client
	producer *kgo.Client
	runID    string
	failures int

	// State the API check leaves for the checks after it: the audit's client,
	// a schedule it created, and the batch the scheduler check fired.
	client    uuid.UUID
	clientKey string
	golden    uuid.UUID
	fired     []uuid.UUID
}

// Run audits the stack and returns an error if any check failed.
func Run(ctx context.Context, lg *zap.Logger, cfg Config) error {
	st, err := stack.Connect(ctx, cfg.Config, &http.Client{Timeout: 15 * time.Second})
	if err != nil {
		return err
	}
	defer st.Close()
	redis, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{cfg.RedisAddr}})
	if err != nil {
		return err
	}
	defer redis.Close()
	producer, err := kafka.NewProducer(cfg.KafkaBrokers, lg)
	if err != nil {
		return err
	}
	defer producer.Close()

	v := &verifier{Stack: st, cfg: cfg, lg: lg, redis: redis, producer: producer, runID: "verify-" + uuid.New().String()}
	fmt.Println("Verifying Hatch, run", v.runID)
	for _, check := range []func(context.Context){
		v.checkFoundation,
		v.checkAPI,
		v.checkScheduler,
		v.checkDelivery,
		v.checkResend,
		v.checkRetries,
		v.checkReconciliation,
		v.checkArchival,
		v.checkTelemetry,
		v.checkClientDeletion,
	} {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		check(ctx)
	}

	if v.failures > 0 {
		return fmt.Errorf("%d check(s) failed", v.failures)
	}
	fmt.Println("\nAll checks passed.")
	return nil
}

func (v *verifier) section(name string) { fmt.Printf("\n== %s ==\n", name) }

// check records a check. The message should describe what was observed, so it
// reads right whichever way the check went.
func (v *verifier) check(ok bool, format string, args ...any) {
	mark := "PASS"
	if !ok {
		mark = "FAIL"
		v.failures++
	}
	fmt.Printf("  [%s] %s\n", mark, fmt.Sprintf(format, args...))
}

// fail records a check that could not run.
func (v *verifier) fail(format string, args ...any) { v.check(false, format, args...) }

// expect checks that a request succeeded with status want.
func (v *verifier) expect(what string, resp stack.Response, err error, want int) bool {
	switch {
	case err != nil:
		v.fail("%s: %v", what, err)
		return false
	case resp.Code != want:
		v.fail("%s → %d, want %d: %s", what, resp.Code, want, resp.Body)
		return false
	}
	v.check(true, "%s → %d", what, resp.Code)
	return true
}

// eventually calls ok every two seconds until it returns true or timeout has
// passed, and reports whether it returned true.
func eventually(ctx context.Context, timeout time.Duration, ok func() bool) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for !ok() {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
	return true
}

// email is the body of a request to schedule an email due at deliverAt.
func (v *verifier) email(deliverAt time.Time, to string) map[string]any {
	return map[string]any{
		"deliver_at":      deliverAt.UnixMilli(),
		"recipient_email": to,
		"from_email":      "verify@hatch.test",
		"from_name":       "Hatch Verify",
		"subject":         v.runID,
		"body":            "<p>" + v.runID + "</p>",
	}
}

// createSchedule schedules an email as the client whose API key is key.
func (v *verifier) createSchedule(ctx context.Context, key string, email map[string]any) (uuid.UUID, error) {
	resp, err := v.API(ctx, http.MethodPost, "/v1/schedules", key, email)
	if err != nil {
		return uuid.Nil(), err
	}
	if resp.Code != http.StatusCreated {
		return uuid.Nil(), fmt.Errorf("%d %s", resp.Code, resp.Body)
	}
	return uuid.Parse(resp.Field("schedule_id"))
}

// schedules reads the schedules among ids that exist.
func (v *verifier) schedules(ctx context.Context, ids ...uuid.UUID) ([]db.ScheduledEmail, error) {
	var params db.GetSchedulesParams
	for _, id := range ids {
		params.Ids = append(params.Ids, id[:])
		params.DeliverAts = append(params.DeliverAts, db.ScheduleDeliverAt(id))
	}
	return db.New(v.DB).GetSchedules(ctx, params)
}

// awaitStatus waits up to timeout for every schedule in ids to reach status,
// and returns how many had by the end.
func (v *verifier) awaitStatus(ctx context.Context, timeout time.Duration, ids []uuid.UUID, status db.ScheduleStatus) int {
	var n int
	eventually(ctx, timeout, func() bool {
		rows, err := v.schedules(ctx, ids...)
		n = 0
		for _, row := range rows {
			if row.Status == status {
				n++
			}
		}
		return err == nil && n == len(ids)
	})
	return n
}

// awaitDue waits up to timeout for each of ids to be put on emails.due after
// since, and returns how many were.
func (v *verifier) awaitDue(ctx context.Context, since time.Time, ids []uuid.UUID, timeout time.Duration) int {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(v.cfg.KafkaBrokers...),
		kgo.ConsumeTopics(kafka.TopicDue),
		// Record timestamps come from the producers' clocks: allow for skew.
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(since.Add(-time.Minute).UnixMilli())),
	)
	if err != nil {
		v.fail("consume %s: %v", kafka.TopicDue, err)
		return 0
	}
	defer cl.Close()

	missing := make(map[uuid.UUID]bool, len(ids))
	for _, id := range ids {
		missing[id] = true
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for len(missing) > 0 && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			if id, err := kafka.ScheduleID(r); err == nil {
				delete(missing, id)
			}
		})
	}
	return len(ids) - len(missing)
}

// checkMetric waits for the PromQL expr to evaluate to at least min.
func (v *verifier) checkMetric(ctx context.Context, expr string, min float64) {
	ok := eventually(ctx, telemetryTimeout, func() bool {
		values, err := v.Query(ctx, expr)
		return err == nil && len(values) > 0 && values[0] >= min
	})
	v.check(ok, "Prometheus: %s ≥ %g", expr, min)
}

// logLines returns the lines the LogQL query matches from the last window.
func (v *verifier) logLines(ctx context.Context, query string, window time.Duration) ([]string, error) {
	now := time.Now()
	params := url.Values{
		"query": {query},
		"start": {strconv.FormatInt(now.Add(-window).UnixNano(), 10)},
		"end":   {strconv.FormatInt(now.UnixNano(), 10)},
	}
	resp, err := v.Request(ctx, http.MethodGet, v.cfg.LokiURL+"/loki/api/v1/query_range?"+params.Encode(), "", nil)
	if err != nil {
		return nil, err
	}
	if resp.Code != http.StatusOK {
		return nil, fmt.Errorf("loki: %d %s", resp.Code, resp.Body)
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Values [][2]string `json:"values"` // [timestamp, line]
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, err
	}
	var lines []string
	for _, stream := range parsed.Data.Result {
		for _, entry := range stream.Values {
			lines = append(lines, entry[1])
		}
	}
	return lines, nil
}

// hasTraces reports whether Tempo holds recent traces from service.
func (v *verifier) hasTraces(ctx context.Context, service string) bool {
	params := url.Values{"tags": {"service.name=" + service}, "limit": {"5"}}
	resp, err := v.Request(ctx, http.MethodGet, v.cfg.TempoURL+"/api/search?"+params.Encode(), "", nil)
	if err != nil || resp.Code != http.StatusOK {
		return false
	}
	var parsed struct {
		Traces []json.RawMessage `json:"traces"`
	}
	return json.Unmarshal(resp.Body, &parsed) == nil && len(parsed.Traces) > 0
}
