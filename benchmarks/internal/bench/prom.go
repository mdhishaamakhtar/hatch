package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// promClient runs instant queries against the Prometheus HTTP API.
//
// Every benchmark query is evaluated at a single instant with an explicit
// lookback window covering the run (`...[15m]`), rather than as a range query
// that would need stitching. The harness only ever asks "what did this number
// come to over the run", never "what did it look like second by second" — that
// is Grafana's job, and it reads the same metrics.
type promClient struct {
	base string
	http *http.Client
}

func newPromClient(base string) *promClient {
	return &promClient{base: base, http: &http.Client{Timeout: 20 * time.Second}}
}

// promResult is the subset of Prometheus's response format the harness reads.
type promResult struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []any             `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// query runs an instant query and returns every series with its scalar value.
func (p *promClient) query(ctx context.Context, expr string) (promResult, error) {
	u := p.base + "/api/v1/query?query=" + url.QueryEscape(expr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return promResult{}, err
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return promResult{}, fmt.Errorf("prometheus query %q: %w", expr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return promResult{}, fmt.Errorf("prometheus query %q: http %d", expr, resp.StatusCode)
	}
	var out promResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return promResult{}, fmt.Errorf("decode prometheus response: %w", err)
	}
	if out.Status != "success" {
		return promResult{}, fmt.Errorf("prometheus query %q: status %s", expr, out.Status)
	}
	return out, nil
}

// scalar runs a query expected to yield one series and returns its value.
//
// ok=false means the query returned no series — which for a histogram_quantile
// over a window with no observations is the normal, expected answer, not an
// error. Reporting it as a distinct state stops an absent metric from being
// rendered as a very confident 0.
func (p *promClient) scalar(ctx context.Context, expr string) (v float64, ok bool, err error) {
	res, err := p.query(ctx, expr)
	if err != nil {
		return 0, false, err
	}
	if len(res.Data.Result) == 0 || len(res.Data.Result[0].Value) != 2 {
		return 0, false, nil
	}
	s, isString := res.Data.Result[0].Value[1].(string)
	if !isString {
		return 0, false, nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// Prometheus renders a missing quantile as NaN, which is "no data",
		// not a malformed response.
		return 0, false, nil
	}
	return f, true, nil
}

// Quantiles are the three e2e latency percentiles the SLA is stated in.
type Quantiles struct {
	P50, P95, P99 float64
	Present       bool
}

// e2eQuantiles reads deliver_at → delivered latency from the delivery worker's
// histogram over the given window.
func (p *promClient) e2eQuantiles(ctx context.Context, window time.Duration) (Quantiles, error) {
	w := promDuration(window)
	var q Quantiles
	for _, spec := range []struct {
		q   float64
		dst *float64
	}{{0.50, &q.P50}, {0.95, &q.P95}, {0.99, &q.P99}} {
		expr := fmt.Sprintf(
			`histogram_quantile(%.2f, sum by (le) (rate(hatch_delivery_e2e_latency_seconds_bucket[%s])))`,
			spec.q, w)
		v, ok, err := p.scalar(ctx, expr)
		if err != nil {
			return Quantiles{}, err
		}
		if !ok {
			return Quantiles{}, nil // no observations in the window
		}
		*spec.dst = v
		q.Present = true
	}
	return q, nil
}

// counterIncrease returns how much a counter grew over the window, summed
// across all label combinations.
func (p *promClient) counterIncrease(ctx context.Context, metric string, window time.Duration) (float64, bool, error) {
	expr := fmt.Sprintf(`sum(increase(%s[%s]))`, metric, promDuration(window))
	return p.scalar(ctx, expr)
}

// histogramQuantile returns one quantile of any histogram over the window.
func (p *promClient) histogramQuantile(ctx context.Context, metric string, q float64, window time.Duration) (float64, bool, error) {
	expr := fmt.Sprintf(`histogram_quantile(%.2f, sum by (le) (rate(%s_bucket[%s])))`,
		q, metric, promDuration(window))
	return p.scalar(ctx, expr)
}

// promDuration renders a Go duration in Prometheus's duration syntax. Seconds
// resolution is enough for benchmark windows and avoids Go's "1h0m0s" form,
// which Prometheus rejects.
func promDuration(d time.Duration) string {
	secs := int(d.Seconds())
	if secs < 1 {
		secs = 1
	}
	return strconv.Itoa(secs) + "s"
}
