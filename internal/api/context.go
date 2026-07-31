package api

import (
	"context"

	"github.com/google/uuid"
)

type ctxKey int

const (
	ctxKeyClientID ctxKey = iota
	ctxKeyMaxRPS
)

func withClientID(ctx context.Context, id uuid.UUID) context.Context {
	return context.WithValue(ctx, ctxKeyClientID, id)
}

// ClientIDFromCtx returns the authenticated client ID. Zero UUID + false
// indicates the request did not pass through ClientAuth.
func ClientIDFromCtx(ctx context.Context) (uuid.UUID, bool) {
	v, ok := ctx.Value(ctxKeyClientID).(uuid.UUID)
	return v, ok
}

func withMaxRPS(ctx context.Context, rps int32) context.Context {
	return context.WithValue(ctx, ctxKeyMaxRPS, rps)
}

func maxRPSFromCtx(ctx context.Context) (int32, bool) {
	v, ok := ctx.Value(ctxKeyMaxRPS).(int32)
	return v, ok
}
