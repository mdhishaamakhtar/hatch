package api

import (
	"net/http"

	"github.com/mdhishaamakhtar/hatch/pkg/httpx"
)

// apiError is the error response body. It aliases the shared shape so the
// generated OpenAPI spec can name the type while every service still returns
// byte-identical errors.
type apiError = httpx.ErrorBody

// writeError is the single path for non-2xx responses. Body shape is stable
// across endpoints so clients can branch on `error` and (when present) `reason`.
func writeError(w http.ResponseWriter, status int, code, reason string) {
	httpx.WriteError(w, status, code, reason)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	httpx.WriteJSON(w, status, v)
}

// Error codes — kept centralised so handlers, tests, and clients agree.
const (
	ErrCodeUnauthorized      = "unauthorized"
	ErrCodeRateLimited       = "rate_limited"
	ErrCodeValidationFailed  = "validation_failed"
	ErrCodeNoActiveProviders = "no_active_providers"
	ErrCodeNotFound          = "not_found"
	ErrCodeConflict          = "conflict"
	ErrCodePayloadTooLarge   = "payload_too_large"
	ErrCodeUnsupportedMedia  = "unsupported_media_type"
	ErrCodeInternal          = "internal"
)
