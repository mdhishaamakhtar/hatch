package api

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mdhishaamakhtar/hatch/gen"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
)

type createClientRequest struct {
	Name   string `json:"name"`
	MaxRPS int32  `json:"max_rps"`
}

type createClientResponse struct {
	ClientID string `json:"client_id"`
	Name     string `json:"name"`
	MaxRPS   int32  `json:"max_rps"`
	APIKey   string `json:"api_key"`
}

// POST /admin/clients
func (s *Server) handleCreateClient(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "body_read")
		return
	}
	var in createClientRequest
	if err := json.Unmarshal(body, &in); err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "json")
		return
	}
	if in.Name == "" {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "name_required")
		return
	}
	if in.MaxRPS <= 0 {
		in.MaxRPS = 100
	}

	// Generate a 32-byte random key, base64url encoded.
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		s.lg.Error("rand read failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	apiKey := base64.RawURLEncoding.EncodeToString(raw)
	lookup := sha256Bytes(apiKey)
	hash, err := bcrypt.GenerateFromPassword([]byte(apiKey), s.cfg.BcryptCost)
	if err != nil {
		s.lg.Error("bcrypt hash failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	id, err := uuid.NewV7()
	if err != nil {
		s.lg.Error("uuidv7 failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	row, err := s.queries.CreateClient(r.Context(), gen.CreateClientParams{
		ID:           db.UUIDToBytes(id),
		Name:         in.Name,
		ApiKeyLookup: lookup,
		ApiKeyHash:   string(hash),
		MaxRps:       in.MaxRPS,
	})
	if err != nil {
		s.lg.Error("create client failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	clientID, _ := db.BytesToUUID(row.ID)
	s.lg.Info("Client created", zap.String("client_id", clientID.String()))
	writeJSON(w, http.StatusCreated, createClientResponse{
		ClientID: clientID.String(),
		Name:     row.Name,
		MaxRPS:   row.MaxRps,
		APIKey:   apiKey,
	})
}

// DELETE /admin/clients/{client_id}
func (s *Server) handleDeleteClient(w http.ResponseWriter, r *http.Request) {
	id, ok := s.parseClientIDParam(w, r, "client_id")
	if !ok {
		return
	}
	if err := s.queries.SoftDeleteClient(r.Context(), db.UUIDToBytes(id)); err != nil {
		s.lg.Error("soft delete client failed", zap.Error(err), zap.String("client_id", id.String()))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	if err := invalidateClientCache(r.Context(), s.redis, id); err != nil {
		s.lg.Warn("redis cache invalidation failed", zap.Error(err), zap.String("client_id", id.String()))
	}
	w.WriteHeader(http.StatusNoContent)
}

type upsertProviderRequest struct {
	Vendor      string          `json:"vendor"`
	Credentials json.RawMessage `json:"credentials"`
}

type upsertProviderResponse struct {
	ProviderID string `json:"provider_id"`
	ClientID   string `json:"client_id"`
	Vendor     string `json:"vendor"`
	IsActive   bool   `json:"is_active"`
}

var allowedVendors = map[string]struct{}{
	"resend":   {},
	"ses":      {},
	"sendgrid": {},
	"smtp":     {},
}

// POST /admin/clients/{client_id}/providers
func (s *Server) handleUpsertProvider(w http.ResponseWriter, r *http.Request) {
	clientID, ok := s.parseClientIDParam(w, r, "client_id")
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16*1024))
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "body_read")
		return
	}
	var in upsertProviderRequest
	if err := json.Unmarshal(body, &in); err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "json")
		return
	}
	if _, ok := allowedVendors[in.Vendor]; !ok {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "vendor_invalid")
		return
	}
	if len(in.Credentials) == 0 || string(in.Credentials) == "null" {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "credentials_required")
		return
	}

	enc, err := s.cipher.EncryptCredentials(in.Credentials)
	if err != nil {
		s.lg.Error("encrypt credentials failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	pid, err := uuid.NewV7()
	if err != nil {
		s.lg.Error("uuidv7 failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	row, err := s.queries.UpsertClientProvider(r.Context(), gen.UpsertClientProviderParams{
		ID:          db.UUIDToBytes(pid),
		ClientID:    db.UUIDToBytes(clientID),
		Vendor:      in.Vendor,
		Credentials: enc,
	})
	if err != nil {
		s.lg.Error("upsert provider failed", zap.Error(err), zap.String("client_id", clientID.String()))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	if err := invalidateClientCache(r.Context(), s.redis, clientID); err != nil {
		s.lg.Warn("redis cache invalidation failed", zap.Error(err))
	}
	provID, _ := db.BytesToUUID(row.ID)
	writeJSON(w, http.StatusCreated, upsertProviderResponse{
		ProviderID: provID.String(),
		ClientID:   clientID.String(),
		Vendor:     row.Vendor,
		IsActive:   row.IsActive,
	})
}

// DELETE /admin/clients/{client_id}/providers/{vendor}
func (s *Server) handleDeleteProvider(w http.ResponseWriter, r *http.Request) {
	clientID, ok := s.parseClientIDParam(w, r, "client_id")
	if !ok {
		return
	}
	vendor := chi.URLParam(r, "vendor")
	if _, ok := allowedVendors[vendor]; !ok {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "vendor_invalid")
		return
	}
	if err := s.queries.SoftDeleteClientProvider(r.Context(), gen.SoftDeleteClientProviderParams{
		ClientID: db.UUIDToBytes(clientID),
		Vendor:   vendor,
	}); err != nil {
		s.lg.Error("soft delete provider failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	if err := invalidateClientCache(r.Context(), s.redis, clientID); err != nil {
		s.lg.Warn("redis cache invalidation failed", zap.Error(err))
	}
	w.WriteHeader(http.StatusNoContent)
}

// parseClientIDParam parses {client_id} from the URL. Writes 400 on failure
// and returns ok=false.
func (s *Server) parseClientIDParam(w http.ResponseWriter, r *http.Request, key string) (uuid.UUID, bool) {
	raw := chi.URLParam(r, key)
	id, err := uuid.Parse(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "client_id_invalid")
		return uuid.Nil, false
	}
	return id, true
}

// Silence unused-import warnings on builds where this file is the only one
// touching pgx/errors (handlers don't otherwise use these directly).
var (
	_ = errors.Is
	_ = pgx.ErrNoRows
)
