package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

// bearerToken extracts the token from an "Authorization: Bearer <x>" header.
// Returns "" if the header is missing or malformed.
func bearerToken(r *http.Request) string {
	v := r.Header.Get("Authorization")
	if v == "" {
		return ""
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(v, prefix) {
		return ""
	}
	return strings.TrimSpace(v[len(prefix):])
}

// sha256Bytes is the deterministic per-key lookup value stored in
// clients.api_key_lookup. bcrypt would be non-deterministic and therefore not
// indexable; sha256 lets us hit a unique index, and we still verify with
// bcrypt against api_key_hash for credential security.
func sha256Bytes(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}

// ClientAuth resolves the inbound Bearer token to a clients row and injects
// (client_id, max_rps) into the request context. 401 on any failure.
func ClientAuth(q *gen.Queries, lg *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := bearerToken(r)
			if tok == "" {
				writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "missing_bearer")
				return
			}
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
			if err := bcrypt.CompareHashAndPassword([]byte(row.ApiKeyHash), []byte(tok)); err != nil {
				// bcrypt mismatch with the lookup-row means the sha256 collided
				// against a different key (astronomically rare). Treat as 401.
				writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "bad_credentials")
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

// AdminAuth gates /admin and /internal routes behind a single static
// shared key. Constant-time compare against the configured admin key.
func AdminAuth(adminKey string) func(http.Handler) http.Handler {
	expected := []byte(adminKey)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := []byte(bearerToken(r))
			if subtle.ConstantTimeCompare(got, expected) != 1 {
				writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
