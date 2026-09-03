package api

import (
	"crypto/sha256"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
	"go.uber.org/zap"
)

// sha256Bytes is the digest stored in clients.api_key_lookup. It is both the
// index the client row is found by and the credential check itself.
//
// A fast digest is the right choice here because an API key is 32 bytes from
// crypto/rand, not a human-chosen password. A slow, salted hash buys resistance
// to guessing, which only matters when the secret is drawn from a space small
// enough to search — an attacker cannot search a uniformly random 256-bit one at
// any hash speed. This is the same reason API tokens are conventionally stored
// as a single fast digest while passwords are not.
func sha256Bytes(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// ClientAuth resolves the inbound Bearer token to a clients row and injects
// (client_id, max_rps) into the request context. 401 on any failure.
func ClientAuth(q *gen.Queries, lg *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := httpx.BearerToken(r)
			if tok == "" {
				writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "missing_bearer")
				return
			}
			// Finding the row is the authentication: only a caller holding the
			// key can produce a digest that matches the stored one.
			row, err := q.GetClientByAPIKeyLookup(r.Context(), sha256Bytes(tok))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "unknown_key")
					return
				}
				lg.Error("client auth db lookup failed", zap.Error(err))
				writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
				return
			}
			clientID, err := db.BytesToUUID(row.ID)
			if err != nil {
				lg.Error("client uuid decode failed", zap.Error(err))
				writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
				return
			}
			ctx := withClientID(r.Context(), clientID)
			ctx = withMaxRPS(ctx, row.MaxRps)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
