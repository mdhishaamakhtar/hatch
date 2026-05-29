package verify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// httpResp is the minimal slice of an HTTP response the checks care about.
type httpResp struct {
	code int
	body []byte
}

// do issues an HTTP request with an optional Bearer token and JSON body, and
// reads the full response. A non-nil body is marshalled to JSON.
func (v *Verifier) do(ctx context.Context, method, rawURL, bearer string, body any) (httpResp, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return httpResp{}, fmt.Errorf("marshal body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rdr)
	if err != nil {
		return httpResp{}, err
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return httpResp{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return httpResp{}, err
	}
	return httpResp{code: resp.StatusCode, body: b}, nil
}

// jsonField pulls a top-level string field out of a JSON object body.
func jsonField(body []byte, key string) string {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return ""
	}
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

// promCount runs an instant Prometheus query and returns the number of series
// in the result plus the first series' scalar value (empty if none).
func (v *Verifier) promCount(ctx context.Context, query string) (int, string, error) {
	u := v.cfg.PromURL + "/api/v1/query?query=" + url.QueryEscape(query)
	resp, err := v.do(ctx, http.MethodGet, u, "", nil)
	if err != nil {
		return 0, "", err
	}
	if resp.code != http.StatusOK {
		return 0, "", fmt.Errorf("prometheus %d: %s", resp.code, resp.body)
	}
	var parsed struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.body, &parsed); err != nil {
		return 0, "", err
	}
	first := ""
	if len(parsed.Data.Result) > 0 && len(parsed.Data.Result[0].Value) == 2 {
		if s, ok := parsed.Data.Result[0].Value[1].(string); ok {
			first = s
		}
	}
	return len(parsed.Data.Result), first, nil
}

// lokiQuery runs a LogQL range query over the last sinceSec seconds and returns
// the raw response body. Callers grep the body for expected tokens, mirroring
// the old shell checks (Loki JSON-encodes each line, so inner quotes are
// escaped — match bare tokens, not full JSON shapes).
func (v *Verifier) lokiQuery(ctx context.Context, logql string, sinceSec int) (string, error) {
	now := time.Now()
	q := url.Values{}
	q.Set("query", logql)
	q.Set("start", strconv.FormatInt(now.Add(-time.Duration(sinceSec)*time.Second).UnixNano(), 10))
	q.Set("end", strconv.FormatInt(now.UnixNano(), 10))
	u := v.cfg.LokiURL + "/loki/api/v1/query_range?" + q.Encode()
	resp, err := v.do(ctx, http.MethodGet, u, "", nil)
	if err != nil {
		return "", err
	}
	if resp.code != http.StatusOK {
		return "", fmt.Errorf("loki %d: %s", resp.code, resp.body)
	}
	return string(resp.body), nil
}

// tempoSearch runs a Tempo tag search and returns the raw response body.
func (v *Verifier) tempoSearch(ctx context.Context, tags string) (string, error) {
	q := url.Values{}
	q.Set("tags", tags)
	q.Set("limit", "5")
	u := v.cfg.TempoURL + "/api/search?" + q.Encode()
	resp, err := v.do(ctx, http.MethodGet, u, "", nil)
	if err != nil {
		return "", err
	}
	if resp.code != http.StatusOK {
		return "", fmt.Errorf("tempo %d: %s", resp.code, resp.body)
	}
	return string(resp.body), nil
}

// retry calls fn up to attempts times, sleeping delay between tries, until fn
// returns true or ctx is cancelled. Returns whether fn ever succeeded.
func retry(ctx context.Context, attempts int, delay time.Duration, fn func() bool) bool {
	for i := 0; i < attempts; i++ {
		if fn() {
			return true
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}
	}
	return false
}
