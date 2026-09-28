// Command verify is Hatch's acceptance audit. It runs as a Kubernetes Job
// against a deployed stack; see internal/verify.
package main

import (
	"context"

	"github.com/caarlos0/env/v11"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"github.com/mdhishaamakhtar/hatch/internal/verify"
	"go.uber.org/zap"
)

func main() {
	service.Run("verify", func(ctx context.Context, lg *zap.Logger) error {
		cfg, err := env.ParseAs[verify.Config]()
		if err != nil {
			return err
		}
		return verify.Run(ctx, lg, cfg)
	})
}
