package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/mail"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/mdhishaamakhtar/hatch/gen"
	hdb "github.com/mdhishaamakhtar/hatch/pkg/db"
	"github.com/mdhishaamakhtar/hatch/pkg/logger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

// pgUniqueViolation is the SQLSTATE for a unique-constraint conflict.
const pgUniqueViolation = "23505"

type createScheduleRequest struct {
	DeliverAt      int64           `json:"deliver_at"`
	RecipientEmail string          `json:"recipient_email"`
	FromEmail      string          `json:"from_email"`
	FromName       string          `json:"from_name,omitempty"`
	Subject        string          `json:"subject"`
	Body           string          `json:"body"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}

type scheduleResponse struct {
	ScheduleID string `json:"schedule_id"`
	Status     string `json:"status"`
	DeliverAt  int64  `json:"deliver_at"`
}

type scheduleFullResponse struct {
	ScheduleID     string          `json:"schedule_id"`
	Status         string          `json:"status"`
	DeliverAt      int64           `json:"deliver_at"`
	RecipientEmail string          `json:"recipient_email"`
	FromEmail      string          `json:"from_email"`
	FromName       string          `json:"from_name,omitempty"`
	Subject        string          `json:"subject"`
	Body           string          `json:"body"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
	RetryCount     int16           `json:"retry_count"`
	LastProvider   string          `json:"last_provider,omitempty"`
	FailureReason  string          `json:"failure_reason,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// handleCreateSchedule enqueues an email for future delivery. Honors Idempotency-Key.
//
//	@Summary		Schedule an email
//	@Tags			schedules
//	@Accept			json
//	@Produce		json
//	@Param			body	body		createScheduleRequest	true	"schedule payload"
//	@Success		201		{object}	scheduleResponse
//	@Success		200		{object}	scheduleResponse	"idempotent replay"
//	@Failure		400		{object}	apiError
//	@Failure		401		{object}	apiError
//	@Failure		413		{object}	apiError
//	@Failure		415		{object}	apiError
//	@Failure		429		{object}	apiError
//	@Security		BearerAuth
//	@Router			/v1/schedules [post]
func (s *Server) handleCreateSchedule(w http.ResponseWriter, r *http.Request) {
	clientID, ok := ClientIDFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "")
		return
	}
	lg := logger.WithCtx(r.Context(), s.lg).With(zap.String("client_id", clientID.String()))

	in, ok := s.decodeCreateRequest(w, r, lg)
	if !ok {
		return
	}

	if !s.hasActiveProvider(w, r, clientID, lg) {
		return
	}

	// An existing key means this request was already accepted — replay its result
	// rather than scheduling a second email.
	if in.IdempotencyKey != "" && !s.replayIdempotent(r.Context(), w, clientID, in.IdempotencyKey, lg) {
		return // either the replay or an error response has been written
	}

	ctx, span := otel.Tracer("scheduler-api").Start(r.Context(), "api.schedule.create")
	defer span.End()
	span.SetAttributes(
		attribute.String("client_id", clientID.String()),
		attribute.String("deliver_at", time.UnixMilli(in.DeliverAt).Format(time.RFC3339)),
		attribute.String("idempotency_key", in.IdempotencyKey),
	)

	s.insertSchedule(ctx, w, clientID, in, lg)
}

// decodeCreateRequest reads, size-limits, parses, and validates the body. It
// writes the failure response itself and returns ok=false.
func (s *Server) decodeCreateRequest(w http.ResponseWriter, r *http.Request, lg *zap.Logger) (createScheduleRequest, bool) {
	var in createScheduleRequest

	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, ErrCodeUnsupportedMedia, ct)
		return in, false
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, ErrCodePayloadTooLarge, "")
			return in, false
		}
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "body_read")
		return in, false
	}
	if err := json.Unmarshal(body, &in); err != nil {
		mValidationFailures.WithLabelValues("json").Inc()
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "json")
		return in, false
	}
	if reason := validateCreateSchedule(in, s.cfg.MinScheduleHorizon, s.cfg.MaxScheduleHorizon); reason != "" {
		mValidationFailures.WithLabelValues(reason).Inc()
		lg.Warn("Validation failure", zap.String("reason", reason))
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, reason)
		return in, false
	}
	return in, true
}

// hasActiveProvider gates creation on the client having somewhere to send from.
func (s *Server) hasActiveProvider(w http.ResponseWriter, r *http.Request, clientID uuid.UUID, lg *zap.Logger) bool {
	provs, err := s.queries.ListClientActiveProviders(r.Context(), hdb.UUIDToBytes(clientID))
	if err != nil {
		lg.Error("list active providers failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return false
	}
	if len(provs) == 0 {
		mNoProviderRejections.Inc()
		lg.Warn("No active providers - rejecting request")
		writeError(w, http.StatusBadRequest, ErrCodeNoActiveProviders, "")
		return false
	}
	return true
}

// replayIdempotent looks the key up in the side table and, if it's a repeat,
// writes the 200 replay response. It returns false when the caller must stop —
// either the replay was written or a lookup error was reported.
func (s *Server) replayIdempotent(ctx context.Context, w http.ResponseWriter, clientID uuid.UUID, key string, lg *zap.Logger) bool {
	row, err := s.queries.GetScheduleIdempotencyByKey(ctx, gen.GetScheduleIdempotencyByKeyParams{
		ClientID:       hdb.UUIDToBytes(clientID),
		IdempotencyKey: key,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return true // first time we have seen this key
	}
	if err != nil {
		lg.Error("idempotency lookup failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return false
	}
	scheduleID, _ := hdb.BytesToUUID(row.ScheduleID)
	lg.Info("Duplicate idempotency key - returning existing",
		zap.String("schedule_id", scheduleID.String()),
		zap.String("idempotency_key", key),
	)
	s.writeReplay(ctx, w, clientID, scheduleID, row.DeliverAt)
	return false
}

// writeReplay renders the 200 response for a repeated idempotency key. The side
// table doesn't track status, so the schedule row is re-read for it — a replay
// must not claim `pending` for a schedule that has since delivered or been
// cancelled. If that read fails the response is omitted rather than guessed.
func (s *Server) writeReplay(ctx context.Context, w http.ResponseWriter, clientID, scheduleID uuid.UUID, deliverAt pgtype.Timestamptz) {
	mIdempotencyHits.Inc()
	resp := scheduleResponse{
		ScheduleID: scheduleID.String(),
		DeliverAt:  deliverAt.Time.UnixMilli(),
	}
	row, err := s.queries.GetScheduleByID(ctx, gen.GetScheduleByIDParams{
		ID:       hdb.UUIDToBytes(scheduleID),
		ClientID: hdb.UUIDToBytes(clientID),
	})
	if err != nil {
		s.lg.Warn("idempotent replay could not read current status", zap.Error(err))
	} else {
		resp.Status = string(row.Status)
	}
	writeJSON(w, http.StatusOK, resp)
}

// insertSchedule writes the schedule row and (when a key was supplied) its
// idempotency side-table row in one transaction, then responds 201. A unique
// violation on the side table means a concurrent request won the race, so the
// winner's schedule is replayed with 200 instead.
func (s *Server) insertSchedule(ctx context.Context, w http.ResponseWriter, clientID uuid.UUID, in createScheduleRequest, lg *zap.Logger) {
	scheduleID, err := uuid.NewV7()
	if err != nil {
		lg.Error("uuidv7 failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}

	deliverAt := pgtype.Timestamptz{Time: time.UnixMilli(in.DeliverAt), Valid: true}
	scheduleIDBytes := hdb.UUIDToBytes(scheduleID)
	clientIDBytes := hdb.UUIDToBytes(clientID)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		lg.Error("tx begin failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.queries.WithTx(tx)

	insertCtx, insertSpan := otel.Tracer("scheduler-api").Start(ctx, "db.schedule.insert")
	row, err := qtx.CreateSchedule(insertCtx, gen.CreateScheduleParams{
		ID:             scheduleIDBytes,
		ClientID:       clientIDBytes,
		IdempotencyKey: optionalString(in.IdempotencyKey),
		DeliverAt:      deliverAt,
		RecipientEmail: in.RecipientEmail,
		FromEmail:      in.FromEmail,
		FromName:       optionalString(in.FromName),
		Subject:        in.Subject,
		Body:           in.Body,
		Metadata:       jsonOrNull(in.Metadata),
	})
	insertSpan.End()
	if err != nil {
		lg.Error("Postgres write failure", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}

	if in.IdempotencyKey != "" {
		err := qtx.CreateScheduleIdempotency(ctx, gen.CreateScheduleIdempotencyParams{
			ClientID:       clientIDBytes,
			IdempotencyKey: in.IdempotencyKey,
			ScheduleID:     scheduleIDBytes,
			DeliverAt:      deliverAt,
		})
		var pgErr *pgconn.PgError
		switch {
		case err == nil:
		case errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation:
			// A concurrent request beat us. Roll back and replay the winner.
			_ = tx.Rollback(ctx)
			existing, lookupErr := s.queries.GetScheduleIdempotencyByKey(ctx, gen.GetScheduleIdempotencyByKeyParams{
				ClientID:       clientIDBytes,
				IdempotencyKey: in.IdempotencyKey,
			})
			if lookupErr != nil {
				lg.Error("idempotency race re-lookup failed", zap.Error(lookupErr))
				writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
				return
			}
			winnerID, _ := hdb.BytesToUUID(existing.ScheduleID)
			s.writeReplay(ctx, w, clientID, winnerID, existing.DeliverAt)
			return
		default:
			lg.Error("idempotency insert failed", zap.Error(err))
			writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		lg.Error("tx commit failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}

	lg.Info("Schedule created",
		zap.String("schedule_id", scheduleID.String()),
		zap.Time("deliver_at", deliverAt.Time),
	)
	writeJSON(w, http.StatusCreated, scheduleResponse{
		ScheduleID: scheduleID.String(),
		Status:     string(row.Status),
		DeliverAt:  row.DeliverAt.Time.UnixMilli(),
	})
}

// handleGetSchedule fetches a single schedule the caller owns.
//
//	@Summary		Get a schedule
//	@Tags			schedules
//	@Produce		json
//	@Param			schedule_id	path		string	true	"schedule UUID"
//	@Success		200			{object}	scheduleFullResponse
//	@Failure		400			{object}	apiError
//	@Failure		401			{object}	apiError
//	@Failure		404			{object}	apiError
//	@Security		BearerAuth
//	@Router			/v1/schedules/{schedule_id} [get]
func (s *Server) handleGetSchedule(w http.ResponseWriter, r *http.Request) {
	clientID, ok := ClientIDFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "")
		return
	}
	scheduleID, ok := parseScheduleIDParam(w, r)
	if !ok {
		return
	}
	row, err := s.queries.GetScheduleByID(r.Context(), gen.GetScheduleByIDParams{
		ID:       hdb.UUIDToBytes(scheduleID),
		ClientID: hdb.UUIDToBytes(clientID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, ErrCodeNotFound, "")
			return
		}
		s.lg.Error("get schedule failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	writeJSON(w, http.StatusOK, toFullResponse(row))
}

// handleCancelSchedule cancels a pending schedule. 409 if already in a terminal state.
//
//	@Summary		Cancel a schedule
//	@Tags			schedules
//	@Produce		json
//	@Param			schedule_id	path	string	true	"schedule UUID"
//	@Success		204
//	@Failure		400	{object}	apiError
//	@Failure		401	{object}	apiError
//	@Failure		404	{object}	apiError
//	@Failure		409	{object}	apiError
//	@Security		BearerAuth
//	@Router			/v1/schedules/{schedule_id} [delete]
func (s *Server) handleCancelSchedule(w http.ResponseWriter, r *http.Request) {
	clientID, ok := ClientIDFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, ErrCodeUnauthorized, "")
		return
	}
	scheduleID, ok := parseScheduleIDParam(w, r)
	if !ok {
		return
	}
	_, err := s.queries.CancelSchedule(r.Context(), gen.CancelScheduleParams{
		ID:       hdb.UUIDToBytes(scheduleID),
		ClientID: hdb.UUIDToBytes(clientID),
	})
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		s.lg.Error("cancel schedule failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	// No row updated — disambiguate: not found vs. terminal status.
	existing, gerr := s.queries.GetScheduleByID(r.Context(), gen.GetScheduleByIDParams{
		ID:       hdb.UUIDToBytes(scheduleID),
		ClientID: hdb.UUIDToBytes(clientID),
	})
	if gerr != nil {
		writeError(w, http.StatusNotFound, ErrCodeNotFound, "")
		return
	}
	writeError(w, http.StatusConflict, ErrCodeConflict, "status_"+string(existing.Status))
}

func parseScheduleIDParam(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "schedule_id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "schedule_id_invalid")
		return uuid.Nil, false
	}
	return id, true
}

// Field limits. Kept here so the validator and its test read against one source.
const (
	maxIdempotencyKeyLen = 255
	maxMetadataBytes     = 8 * 1024
)

// validateCreateSchedule returns "" when the request is acceptable, otherwise
// the machine-readable reason reported to the caller and the metric.
func validateCreateSchedule(in createScheduleRequest, minHorizon, maxHorizon time.Duration) string {
	switch {
	case in.DeliverAt == 0:
		return "deliver_at_required"
	case in.DeliverAt < 0:
		return "deliver_at_format"
	}
	until := time.Until(time.UnixMilli(in.DeliverAt))
	switch {
	case until < minHorizon:
		return "deliver_at_too_soon"
	case until > maxHorizon:
		// Past the pre-created partition runway the INSERT would fail deep in
		// Postgres ("no partition of relation") and surface as a 500.
		return "deliver_at_too_far"
	}
	if _, err := mail.ParseAddress(in.RecipientEmail); err != nil {
		return "recipient_email_invalid"
	}
	if _, err := mail.ParseAddress(in.FromEmail); err != nil {
		return "from_email_invalid"
	}
	switch {
	case in.Subject == "":
		return "subject_required"
	case in.Body == "":
		return "body_required"
	case len(in.IdempotencyKey) > maxIdempotencyKeyLen:
		return "idempotency_key_too_long"
	case len(in.Metadata) > maxMetadataBytes:
		return "metadata_too_large"
	}
	return ""
}

func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func jsonOrNull(raw json.RawMessage) []byte {
	if len(raw) == 0 {
		return nil
	}
	return raw
}

func toFullResponse(row gen.ScheduledEmail) scheduleFullResponse {
	scheduleID, _ := hdb.BytesToUUID(row.ID)
	resp := scheduleFullResponse{
		ScheduleID:     scheduleID.String(),
		Status:         string(row.Status),
		DeliverAt:      row.DeliverAt.Time.UnixMilli(),
		RecipientEmail: row.RecipientEmail,
		FromEmail:      row.FromEmail,
		Subject:        row.Subject,
		Body:           row.Body,
		RetryCount:     row.RetryCount,
		CreatedAt:      row.CreatedAt.Time,
		UpdatedAt:      row.UpdatedAt.Time,
		IdempotencyKey: deref(row.IdempotencyKey),
		FromName:       deref(row.FromName),
		LastProvider:   deref(row.LastProvider),
		FailureReason:  deref(row.FailureReason),
	}
	if len(row.Metadata) > 0 {
		resp.Metadata = row.Metadata
	}
	return resp
}

// deref reads an optional text column, treating NULL as the empty string so the
// `omitempty` JSON tags drop it from the response.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
