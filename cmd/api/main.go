// scheduler-api — Hatch's client-facing HTTP API. Handles client schedule
// CRUD and admin client/provider provisioning. See internal/api for handler
// details.
//
//	@title			Hatch Scheduler API
//	@version		1.0
//	@description	Schedule emails for future delivery. Admin endpoints provision
//	@description	clients and per-vendor provider credentials.
//	@host			localhost:9021
//	@BasePath		/
//	@schemes		http
//
//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization
//	@description				"Bearer <api_key>" — client key for /v1/*, admin key for /admin/*.
package main

import (
	"fmt"

	_ "github.com/mdhishaamakhtar/hatch/docs"
	"github.com/mdhishaamakhtar/hatch/internal/api"
	"github.com/mdhishaamakhtar/hatch/pkg/config"
	"github.com/mdhishaamakhtar/hatch/pkg/crypto"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/redis"
	"github.com/mdhishaamakhtar/hatch/pkg/service"
	"go.uber.org/zap"
)

func main() { service.Main("scheduler-api", run) }

func run(lg *zap.Logger) error {
	cfg, err := config.Load[api.Config]()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, cancel := service.SignalContext()
	defer cancel()

	flushTraces, err := service.InitTracer(ctx, lg, "scheduler-api", cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer flushTraces()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	rc, err := redis.NewClient(cfg.RedisAddr)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	defer rc.Close()

	cipher, err := crypto.LoadCipher(cfg.ProviderCredKey)
	if err != nil {
		return fmt.Errorf("cipher: %w", err)
	}

	srv := api.NewServer(cfg, lg, pool, rc, cipher)
	return service.Serve(ctx, lg, "scheduler-api", cfg.Port, srv.Handler(), cfg.ShutdownTimeout)
}
