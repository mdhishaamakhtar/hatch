package api

import (
	"encoding/json"
	"net/http"
)

type apiError struct {
	Error  string `json:"error"`
	Reason string `json:"reason,omitempty"`
}

// writeError is the single path for non-2xx responses. Body shape is stable
// across endpoints so clients can branch on `error` and (when present) `reason`.
func writeError(w http.ResponseWriter, status int, code, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(apiError{Error: code, Reason: reason})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
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
