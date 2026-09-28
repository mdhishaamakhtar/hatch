// Command api serves the scheduler API.
//
//	@title			Hatch Scheduler API
//	@version		1.0
//	@description	Schedule emails for future delivery. Admin endpoints provision clients and their providers' credentials.
//	@host			localhost:9021
//	@BasePath		/
//	@schemes		http
//
//	@securityDefinitions.apikey	BearerAuth
//	@in							header
//	@name						Authorization
//	@description				"Bearer <api_key>": a client's key for /v1, the admin key for /admin.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	_ "github.com/mdhishaamakhtar/hatch/docs"
	"github.com/mdhishaamakhtar/hatch/internal/api"
	"github.com/mdhishaamakhtar/hatch/internal/crypto"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/redis/rueidis"
	"go.uber.org/zap"
)

func main() {
	service.Run("scheduler-api", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[api.Config]()
		if err != nil {
			return err
		}
		cipher, err := crypto.New(cfg.ProviderCredKey)
		if err != nil {
			return err
		}
		pool, err := db.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		redis, err := rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{cfg.RedisAddr}})
		if err != nil {
			return err
		}
		defer redis.Close()

		return service.Serve(ctx, lg, cfg.Port, api.New(cfg, lg, pool, redis, cipher).Handler())
	})
}
