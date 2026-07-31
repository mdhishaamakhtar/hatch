package retry

import (
	"net/http"

	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
	"github.com/twmb/franz-go/pkg/kgo"
)

// AdminHandler is the retry consumer's health/observability HTTP surface. The
// service has no query API, so this is just liveness, readiness, and /metrics.
// broker is the producer client, reused for the readiness ping.
func AdminHandler(broker *kgo.Client) http.Handler {
	return httpx.Traced("retry-consumer", httpx.AdminRouter(
		httpx.Dependency{Name: "kafka", Ping: broker.Ping},
	))
}
