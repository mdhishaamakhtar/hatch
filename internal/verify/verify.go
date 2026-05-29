package verify

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	hredis "github.com/mdhishaamakhtar/hatch/pkg/redis"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

// Verifier holds the shared connections and the state threaded between check
// sections (the verify client's id/key and the schedule ids it created).
type Verifier struct {
	cfg  Config
	lg   *zap.Logger
	rep  *Reporter
	http *http.Client
	pool *pgxpool.Pool
	rc   rueidis.Client

	runID     string
	marker    string // unique plaintext used for the Tink encryption check
	clientID  string // verify client UUID (string form)
	clientKey string // verify client API key
	schedID   string // the golden-path lifecycle schedule
	postedIDs []string
}

// Run executes the full cumulative acceptance suite and returns a process exit
// code (0 = all checks passed). It owns the Postgres pool and Redis client for
// the duration of the run.
func Run(ctx context.Context, lg *zap.Logger) int {
	rep := &Reporter{}

	cfg, err := LoadConfig()
	if err != nil {
		rep.Fail("config: " + err.Error())
		return 1
	}

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		rep.Fail("connect postgres: " + err.Error())
		return 1
	}
	defer pool.Close()

	rc, err := hredis.NewClient(cfg.RedisAddr)
	if err != nil {
		rep.Fail("connect redis: " + err.Error())
		return 1
	}
	defer rc.Close()

	v := &Verifier{
		cfg:    cfg,
		lg:     lg,
		rep:    rep,
		http:   &http.Client{Timeout: 15 * time.Second},
		pool:   pool,
		rc:     rc,
		runID:  "verify-" + uuid.NewString(),
		marker: "re_marker_" + uuid.NewString(),
	}
	fmt.Printf("RUN_ID=%s\n", v.runID)

	v.checkFoundation(ctx)
	v.checkAPIGoldenPath(ctx)
	v.checkScheduler(ctx)
	v.checkObservability(ctx)
	v.cleanup(ctx)

	fmt.Println()
	if rep.Failures() == 0 {
		fmt.Println("Hatch verified — all checks PASS.")
		return 0
	}
	fmt.Printf("Hatch NOT verified — %d check(s) failed.\n", rep.Failures())
	return 1
}

// cleanup soft-deletes the verify client and confirms its key is then rejected
// — this doubles as the Phase 1 "deleted client → 401" acceptance check.
func (v *Verifier) cleanup(ctx context.Context) {
	v.rep.Section("Cleanup — soft-delete verify client")
	if v.clientID == "" {
		v.rep.Pass("no verify client to clean up")
		return
	}

	resp, err := v.do(ctx, http.MethodDelete, v.cfg.APIBase+"/admin/clients/"+v.clientID, v.cfg.AdminKey, nil)
	if err != nil {
		v.rep.Failf("DELETE /admin/clients/:id: %v", err)
		return
	}
	v.rep.Check(resp.code == http.StatusNoContent,
		"DELETE /admin/clients/:id → 204",
		fmt.Sprintf("DELETE /admin/clients/:id → %d", resp.code))

	if v.clientKey == "" || v.schedID == "" {
		return
	}
	after, err := v.do(ctx, http.MethodGet, v.cfg.APIBase+"/v1/schedules/"+v.schedID, v.clientKey, nil)
	if err != nil {
		v.rep.Failf("post-delete client request: %v", err)
		return
	}
	v.rep.Check(after.code == http.StatusUnauthorized,
		"client key rejected with 401 after soft delete",
		fmt.Sprintf("post-delete client request → %d, want 401", after.code))
}
