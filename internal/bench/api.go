package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// apiClient is the harness's view of scheduler-api: provision a client, post
// schedules as that client, tear it down.
//
// It keeps its own *http.Client with a generous connection pool — the default
// transport caps idle connections per host at 2, which would throttle the load
// generator before the server ever became the bottleneck and quietly turn a
// server measurement into a client one.
type apiClient struct {
	base string
	http *http.Client
}

func newAPIClient(base string, maxConns int) *apiClient {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = maxConns
	tr.MaxIdleConnsPerHost = maxConns
	tr.MaxConnsPerHost = maxConns
	return &apiClient{
		base: base,
		http: &http.Client{Transport: tr, Timeout: 30 * time.Second},
	}
}

// response is the slice of an HTTP response the harness cares about.
type response struct {
	code int
	body []byte
}

func (c *apiClient) do(ctx context.Context, method, path, bearer string, body any) (response, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return response{}, fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return response{}, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{code: resp.StatusCode, body: b}, nil
}

// benchClient is the throwaway client a run provisions for itself. Every
// schedule the run creates belongs to it, which is what lets the harness count
// its own rows in Postgres without touching anyone else's data.
type benchClient struct {
	ID     string
	APIKey string
}

// provisionClient creates the benchmark client and attaches a mock provider.
//
// maxRPS is set well above the intended load on purpose: the per-client rate
// limiter is not what this harness is trying to measure, and leaving it at the
// schema default of 100 would cap every ingest run at 100 RPS regardless of what
// the server could actually do.
func (c *apiClient) provisionClient(ctx context.Context, adminKey, name string, maxRPS int) (benchClient, error) {
	resp, err := c.do(ctx, http.MethodPost, "/admin/clients", adminKey,
		map[string]any{"name": name, "max_rps": maxRPS})
	if err != nil {
		return benchClient{}, fmt.Errorf("create client: %w", err)
	}
	if resp.code != http.StatusCreated {
		return benchClient{}, fmt.Errorf("create client: %d: %s", resp.code, resp.body)
	}
	var created struct {
		ClientID string `json:"client_id"`
		APIKey   string `json:"api_key"`
	}
	if err := json.Unmarshal(resp.body, &created); err != nil {
		return benchClient{}, fmt.Errorf("decode client: %w", err)
	}

	// vendor=mock keeps the run offline and deterministic: no real provider is
	// ever contacted, and send latency is whatever MOCK_PROVIDER_LATENCY_MS says.
	resp, err = c.do(ctx, http.MethodPost, "/admin/clients/"+created.ClientID+"/providers", adminKey,
		map[string]any{"vendor": "mock", "credentials": map[string]any{"api_key": "bench"}})
	if err != nil {
		return benchClient{}, fmt.Errorf("attach mock provider: %w", err)
	}
	if resp.code != http.StatusCreated {
		return benchClient{}, fmt.Errorf("attach mock provider: %d: %s", resp.code, resp.body)
	}
	return benchClient{ID: created.ClientID, APIKey: created.APIKey}, nil
}

// deleteClient soft-deletes the benchmark client. Its schedules stay in
// Postgres for post-run inspection; only the credential is retired.
func (c *apiClient) deleteClient(ctx context.Context, adminKey, clientID string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/admin/clients/"+clientID, adminKey, nil)
	if err != nil {
		return err
	}
	if resp.code != http.StatusNoContent {
		return fmt.Errorf("delete client: %d: %s", resp.code, resp.body)
	}
	return nil
}

// createSchedule posts one schedule and reports the status code. Errors are
// returned rather than logged so the load generator can attribute them.
func (c *apiClient) createSchedule(ctx context.Context, apiKey string, deliverAt time.Time, seq int) (int, error) {
	resp, err := c.do(ctx, http.MethodPost, "/v1/schedules", apiKey, map[string]any{
		"deliver_at":      deliverAt.UnixMilli(),
		"recipient_email": fmt.Sprintf("bench+%d@mock.test", seq),
		"from_email":      "bench@hatch.test",
		"subject":         "hatch benchmark",
		"body":            "benchmark payload",
	})
	if err != nil {
		return 0, err
	}
	return resp.code, nil
}

// forcePoll asks one scheduler pod to run an out-of-band poll cycle. Returns
// nil only on 202 — a silently-failed poll would look like a scheduler that
// never fired, which is exactly the kind of false result a benchmark must not
// produce.
func forcePoll(ctx context.Context, hc *http.Client, url, adminKey string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/internal/poll", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+adminKey)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("%s/internal/poll: %d", url, resp.StatusCode)
	}
	return nil
}
