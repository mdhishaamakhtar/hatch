// Package stack drives a deployed Hatch stack from inside its cluster, for
// the two tools that exercise it end to end: the acceptance audit
// (internal/verify) and the benchmark harness (internal/bench). Both run as
// Kubernetes Jobs and reach everything by cluster DNS, so a long run never
// depends on a port-forward staying up.
package stack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/internal/db"
)

// Config locates the stack. The defaults are its in-cluster addresses, and
// the secrets come from the hatch-secrets Secret.
type Config struct {
	AdminKey    string `env:"ADMIN_API_KEY,required,notEmpty"`
	DatabaseURL string `env:"DATABASE_URL,required,notEmpty"`
	APIURL      string `env:"HATCH_API_URL"  envDefault:"http://api.hatch.svc.cluster.local:9021"`
	PromURL     string `env:"PROMETHEUS_URL" envDefault:"http://observability-kps-prometheus.observability.svc.cluster.local:9090"`

	// SchedulerURL is one scheduler pod's admin address, with %d standing for
	// the pod's ordinal. Shards are reached one by one rather than through a
	// load balancer, because a poll only loads the shard that receives it.
	SchedulerURL      string `env:"SCHEDULER_ADMIN_URL" envDefault:"http://scheduler-%d.scheduler.hatch.svc.cluster.local:9022"`
	SchedulerReplicas int    `env:"SCHEDULER_REPLICAS"  envDefault:"2"`
}

// Stack is a connection to a deployed stack.
type Stack struct {
	Config
	DB   *pgxpool.Pool
	http *http.Client
}

// Connect opens a connection to the stack's database. HTTP requests go through
// httpClient.
func Connect(ctx context.Context, cfg Config, httpClient *http.Client) (*Stack, error) {
	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	return &Stack{Config: cfg, DB: pool, http: httpClient}, nil
}

// Close closes the database connection.
func (s *Stack) Close() { s.DB.Close() }

// Response is an HTTP response, read in full.
type Response struct {
	Code int
	Body []byte
}

// Field returns a top-level string field of a JSON response body, or "".
func (r Response) Field(key string) string {
	var m map[string]any
	_ = json.Unmarshal(r.Body, &m)
	s, _ := m[key].(string)
	return s
}

// Request sends an HTTP request, with body as JSON unless it is nil and with
// bearer as the token unless it is empty.
func (s *Stack) Request(ctx context.Context, method, url, bearer string, body any) (Response, error) {
	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return Response{}, err
		}
		payload = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, payload)
	if err != nil {
		return Response{}, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return Response{Code: resp.StatusCode, Body: b}, err
}

// API sends a request to a path on the API.
func (s *Stack) API(ctx context.Context, method, path, bearer string, body any) (Response, error) {
	return s.Request(ctx, method, s.APIURL+path, bearer, body)
}

// NewClient creates an API client with one provider, and returns the client's
// id and API key.
func (s *Stack) NewClient(ctx context.Context, name string, maxRPS int, vendor string, creds any) (id uuid.UUID, key string, err error) {
	resp, err := s.API(ctx, http.MethodPost, "/admin/clients", s.AdminKey, map[string]any{"name": name, "max_rps": maxRPS})
	if err != nil {
		return uuid.Nil, "", err
	}
	if resp.Code != http.StatusCreated {
		return uuid.Nil, "", fmt.Errorf("create client: %d %s", resp.Code, resp.Body)
	}
	if id, err = uuid.Parse(resp.Field("client_id")); err != nil {
		return uuid.Nil, "", fmt.Errorf("create client: %w", err)
	}
	key = resp.Field("api_key")
	resp, err = s.API(ctx, http.MethodPost, "/admin/clients/"+id.String()+"/providers", s.AdminKey,
		map[string]any{"vendor": vendor, "credentials": creds})
	if err != nil {
		return uuid.Nil, "", err
	}
	if resp.Code != http.StatusCreated {
		return uuid.Nil, "", fmt.Errorf("register %s provider: %d %s", vendor, resp.Code, resp.Body)
	}
	return id, key, nil
}

// DeleteClient deactivates an API client.
func (s *Stack) DeleteClient(ctx context.Context, id uuid.UUID) error {
	resp, err := s.API(ctx, http.MethodDelete, "/admin/clients/"+id.String(), s.AdminKey, nil)
	if err == nil && resp.Code != http.StatusNoContent {
		err = fmt.Errorf("delete client: %d %s", resp.Code, resp.Body)
	}
	return err
}

// Scheduler returns scheduler pod i's admin address.
func (s *Stack) Scheduler(i int) string { return fmt.Sprintf(s.SchedulerURL, i) }

// Poll has every scheduler shard load newly created schedules now, instead of
// at its next hourly poll.
func (s *Stack) Poll(ctx context.Context) error {
	for i := range s.SchedulerReplicas {
		resp, err := s.Request(ctx, http.MethodPost, s.Scheduler(i)+"/internal/poll", s.AdminKey, nil)
		if err == nil && resp.Code != http.StatusAccepted {
			err = fmt.Errorf("%d %s", resp.Code, resp.Body)
		}
		if err != nil {
			return fmt.Errorf("poll scheduler %d: %w", i, err)
		}
	}
	return nil
}

// Query runs an instant PromQL query and returns each series' value.
func (s *Stack) Query(ctx context.Context, expr string) ([]float64, error) {
	resp, err := s.Request(ctx, http.MethodGet, s.PromURL+"/api/v1/query?query="+url.QueryEscape(expr), "", nil)
	if err != nil {
		return nil, err
	}
	if resp.Code != http.StatusOK {
		return nil, fmt.Errorf("prometheus: %d %s", resp.Code, resp.Body)
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"` // [timestamp, "value"]
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body, &parsed); err != nil {
		return nil, err
	}
	values := make([]float64, 0, len(parsed.Data.Result))
	for _, r := range parsed.Data.Result {
		// Prometheus writes a value as a string, and a quantile over no data as "NaN".
		s, _ := r.Value[1].(string)
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("prometheus value %q: %w", s, err)
		}
		values = append(values, v)
	}
	return values, nil
}
