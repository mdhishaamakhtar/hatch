package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/provider"
	"go.uber.org/zap"
)

// pgForeignKeyViolation is the SQLSTATE of an insert that references a row
// that doesn't exist.
const pgForeignKeyViolation = "23503"

type createClientRequest struct {
	Name   string `json:"name"`
	MaxRPS int32  `json:"max_rps,omitempty"` // defaults to 100
}

type createClientResponse struct {
	ClientID string `json:"client_id"`
	Name     string `json:"name"`
	MaxRPS   int32  `json:"max_rps"`
	APIKey   string `json:"api_key"`
}

type upsertProviderRequest struct {
	Vendor      string          `json:"vendor" enums:"mock,resend"`
	Credentials json.RawMessage `json:"credentials" swaggertype:"object"`
}

type upsertProviderResponse struct {
	ProviderID string `json:"provider_id"`
	ClientID   string `json:"client_id"`
	Vendor     string `json:"vendor"`
	IsActive   bool   `json:"is_active"`
}

// createClient creates a client. Its API key is in the response and nowhere
// else: only the key's SHA-256 is stored.
//
//	@Summary	Create a client
//	@Tags		admin
//	@Accept		json
//	@Produce	json
//	@Param		body	body		createClientRequest	true	"The client"
//	@Success	201		{object}	createClientResponse
//	@Failure	400		{object}	httpx.ErrorBody
//	@Failure	401		{object}	httpx.ErrorBody
//	@Security	BearerAuth
//	@Router		/admin/clients [post]
func (s *Server) createClient(w http.ResponseWriter, r *http.Request) {
	var in createClientRequest
	if !decode(w, r, 4<<10, &in) {
		return
	}
	if in.Name == "" {
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "name_required")
		return
	}
	if in.MaxRPS <= 0 {
		in.MaxRPS = 100
	}

	secret := make([]byte, 32)
	rand.Read(secret)
	apiKey := base64.RawURLEncoding.EncodeToString(secret)
	digest := sha256.Sum256([]byte(apiKey))
	id := uuid.Must(uuid.NewV7())
	err := s.queries.CreateClient(r.Context(), db.CreateClientParams{ID: id[:], Name: in.Name, ApiKeyLookup: digest[:], MaxRps: in.MaxRPS})
	if err != nil {
		s.internalError(w, r, "create client", err)
		return
	}
	s.lg.Info("client created", zap.Stringer("client_id", id))
	httpx.WriteJSON(w, http.StatusCreated, createClientResponse{ClientID: id.String(), Name: in.Name, MaxRPS: in.MaxRPS, APIKey: apiKey})
}

// deleteClient deactivates a client: its key stops working and its pending
// schedules are cancelled when they come due.
//
//	@Summary	Delete a client
//	@Tags		admin
//	@Param		client_id	path	string	true	"Client id"
//	@Success	204
//	@Failure	400	{object}	httpx.ErrorBody
//	@Failure	401	{object}	httpx.ErrorBody
//	@Security	BearerAuth
//	@Router		/admin/clients/{client_id} [delete]
func (s *Server) deleteClient(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "client_id")
	if !ok {
		return
	}
	if err := s.queries.DeactivateClient(r.Context(), id[:]); err != nil {
		s.internalError(w, r, "deactivate client", err)
		return
	}
	s.invalidateClient(r.Context(), id)
	w.WriteHeader(http.StatusNoContent)
}

// upsertProvider registers a client's credentials for a vendor, replacing any
// it had. They are checked by building the provider, then stored encrypted.
//
//	@Summary	Register a client's provider
//	@Tags		admin
//	@Accept		json
//	@Produce	json
//	@Param		client_id	path		string					true	"Client id"
//	@Param		body		body		upsertProviderRequest	true	"Vendor and credentials, such as an api_key for resend"
//	@Success	201			{object}	upsertProviderResponse
//	@Failure	400			{object}	httpx.ErrorBody
//	@Failure	401			{object}	httpx.ErrorBody
//	@Failure	404			{object}	httpx.ErrorBody
//	@Security	BearerAuth
//	@Router		/admin/clients/{client_id}/providers [post]
func (s *Server) upsertProvider(w http.ResponseWriter, r *http.Request) {
	clientID, ok := pathID(w, r, "client_id")
	if !ok {
		return
	}
	var in upsertProviderRequest
	if !decode(w, r, 16<<10, &in) {
		return
	}
	switch {
	case !slices.Contains(provider.Vendors, in.Vendor):
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "vendor_invalid")
		return
	case len(in.Credentials) == 0 || string(in.Credentials) == "null":
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "credentials_required")
		return
	}
	if _, err := provider.New(in.Vendor, in.Credentials, provider.MockConfig{}); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "credentials_invalid")
		return
	}

	sealed, err := s.cipher.Encrypt(in.Credentials, clientID[:], in.Vendor)
	if err != nil {
		s.internalError(w, r, "encrypt credentials", err)
		return
	}
	newID := uuid.Must(uuid.NewV7())
	providerID, err := s.queries.UpsertClientProvider(r.Context(), db.UpsertClientProviderParams{
		ID: newID[:], ClientID: clientID[:], Vendor: in.Vendor, Credentials: sealed,
	})
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation {
		httpx.WriteError(w, http.StatusNotFound, codeNotFound, "client")
		return
	}
	if err != nil {
		s.internalError(w, r, "upsert provider", err)
		return
	}
	s.invalidateClient(r.Context(), clientID)
	httpx.WriteJSON(w, http.StatusCreated, upsertProviderResponse{
		ProviderID: uuid.UUID(providerID).String(),
		ClientID:   clientID.String(),
		Vendor:     in.Vendor,
		IsActive:   true,
	})
}

// deleteProvider deactivates a client's provider for a vendor.
//
//	@Summary	Delete a client's provider
//	@Tags		admin
//	@Param		client_id	path	string	true	"Client id"
//	@Param		vendor		path	string	true	"Vendor"	Enums(mock, resend)
//	@Success	204
//	@Failure	400	{object}	httpx.ErrorBody
//	@Failure	401	{object}	httpx.ErrorBody
//	@Security	BearerAuth
//	@Router		/admin/clients/{client_id}/providers/{vendor} [delete]
func (s *Server) deleteProvider(w http.ResponseWriter, r *http.Request) {
	clientID, ok := pathID(w, r, "client_id")
	if !ok {
		return
	}
	vendor := chi.URLParam(r, "vendor")
	if !slices.Contains(provider.Vendors, vendor) {
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, "vendor_invalid")
		return
	}
	err := s.queries.DeactivateClientProvider(r.Context(), db.DeactivateClientProviderParams{ClientID: clientID[:], Vendor: vendor})
	if err != nil {
		s.internalError(w, r, "deactivate provider", err)
		return
	}
	s.invalidateClient(r.Context(), clientID)
	w.WriteHeader(http.StatusNoContent)
}
