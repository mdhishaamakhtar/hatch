// Package db is the project-wide PostgreSQL access layer, built on pgx/v5
// native (not database/sql). It also exposes UUID <-> bytea helpers since
// IDs are stored as raw 16 bytes per the LLD.
package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool dials Postgres and returns a connection pool.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// UUIDToBytes returns the raw 16-byte representation of u.
func UUIDToBytes(u uuid.UUID) []byte {
	b := make([]byte, 16)
	copy(b, u[:])
	return b
}

// BytesToUUID parses raw 16 bytes back into a uuid.UUID.
func BytesToUUID(b []byte) (uuid.UUID, error) {
	if len(b) != 16 {
		return uuid.Nil, errors.New("uuid bytes must be exactly 16")
	}
	var u uuid.UUID
	copy(u[:], b)
	return u, nil
}
