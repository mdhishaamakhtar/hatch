// Package db is Hatch's Postgres access layer: the sqlc-generated queries from
// queries/*.sql, plus the few helpers every service needs around them.
package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a connection pool and pings it, so a service fails at startup
// rather than on its first query when Postgres is unreachable.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}
