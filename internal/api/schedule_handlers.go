package api

import (
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
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

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

	if ct := r.Header.Get("Content-Type"); ct != "" && ct != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, ErrCodeUnsupportedMedia, ct)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, ErrCodePayloadTooLarge, "")
			return
		}
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "body_read")
		return
	}
	var in createScheduleRequest
	if err := json.Unmarshal(body, &in); err != nil {
		s.bumpValidation("json")
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "json")
		return
	}
	// Validate.
	if reason := validateCreateSchedule(in, s.cfg.MinScheduleHorizon); reason != "" {
		s.bumpValidation(reason)
		lg.Warn("Validation failure", zap.String("reason", reason))
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, reason)
		return
	}
	deliverAt := time.UnixMilli(in.DeliverAt)

	// Active providers gate.
	provs, err := s.queries.ListClientActiveProviders(r.Context(), hdb.UUIDToBytes(clientID))
	if err != nil {
		lg.Error("list active providers failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	if len(provs) == 0 {
		mNoProviderRejections.With(prometheus.Labels{"client_id": clientID.String()}).Inc()
		lg.Warn("No active providers - rejecting request")
		writeError(w, http.StatusBadRequest, ErrCodeNoActiveProviders, "")
		return
	}

	// Idempotency lookup (only when key supplied).
	if in.IdempotencyKey != "" {
		row, err := s.queries.GetScheduleIdempotencyByKey(r.Context(), gen.GetScheduleIdempotencyByKeyParams{
			ClientID:       hdb.UUIDToBytes(clientID),
			IdempotencyKey: in.IdempotencyKey,
		})
		if err == nil {
			mIdempotencyHits.WithLabelValues().Inc()
			scheduleID, _ := hdb.BytesToUUID(row.ScheduleID)
			lg.Info("Duplicate idempotency key - returning existing",
				zap.String("schedule_id", scheduleID.String()),
				zap.String("idempotency_key", in.IdempotencyKey),
			)
			writeJSON(w, http.StatusOK, scheduleResponse{
				ScheduleID: scheduleID.String(),
				Status:     "pending",
				DeliverAt:  row.DeliverAt.Time.UnixMilli(),
			})
			return
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			lg.Error("idempotency lookup failed", zap.Error(err))
			writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
			return
		}
	}

	// Insert under api.schedule.create span.
	ctx, span := otel.Tracer("scheduler-api").Start(r.Context(), "api.schedule.create")
	span.SetAttributes(
		attribute.String("client_id", clientID.String()),
		attribute.String("deliver_at", deliverAt.Format(time.RFC3339)),
		attribute.String("idempotency_key", in.IdempotencyKey),
	)
	defer span.End()

	scheduleID, err := uuid.NewV7()
	if err != nil {
		lg.Error("uuidv7 failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}

	deliverTS := pgtype.Timestamptz{Time: deliverAt, Valid: true}
	insertID := hdb.UUIDToBytes(scheduleID)
	cid := hdb.UUIDToBytes(clientID)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		lg.Error("tx begin failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	qtx := s.queries.WithTx(tx)

	insSpanCtx, insSpan := otel.Tracer("scheduler-api").Start(ctx, "db.schedule.insert")
	row, err := qtx.CreateSchedule(insSpanCtx, gen.CreateScheduleParams{
		ID:             insertID,
		ClientID:       cid,
		IdempotencyKey: optionalString(in.IdempotencyKey),
		DeliverAt:      deliverTS,
		RecipientEmail: in.RecipientEmail,
		FromEmail:      in.FromEmail,
		FromName:       optionalString(in.FromName),
		Subject:        in.Subject,
		Body:           in.Body,
		Metadata:       jsonOrNull(in.Metadata),
	})
	insSpan.End()
	if err != nil {
		lg.Error("Postgres write failure", zap.Error(err))
		writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
		return
	}

	if in.IdempotencyKey != "" {
		err := qtx.CreateScheduleIdempotency(ctx, gen.CreateScheduleIdempotencyParams{
			ClientID:       cid,
			IdempotencyKey: in.IdempotencyKey,
			ScheduleID:     insertID,
			DeliverAt:      deliverTS,
		})
		if err != nil {
			// Unique violation on (client_id, idempotency_key) means a concurrent
			// request beat us. Roll back, re-query the side table, return 200 with
			// the winner's schedule_id.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				_ = tx.Rollback(ctx)
				existing, qerr := s.queries.GetScheduleIdempotencyByKey(ctx, gen.GetScheduleIdempotencyByKeyParams{
					ClientID:       cid,
					IdempotencyKey: in.IdempotencyKey,
				})
				if qerr != nil {
					lg.Error("idempotency race re-lookup failed", zap.Error(qerr))
					writeError(w, http.StatusInternalServerError, ErrCodeInternal, "")
					return
				}
				mIdempotencyHits.WithLabelValues().Inc()
				existingID, _ := hdb.BytesToUUID(existing.ScheduleID)
				writeJSON(w, http.StatusOK, scheduleResponse{
					ScheduleID: existingID.String(),
					Status:     "pending",
					DeliverAt:  existing.DeliverAt.Time.UnixMilli(),
				})
				return
			}
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
		zap.Time("deliver_at", deliverAt),
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
	raw := chi.URLParam(r, "schedule_id")
	id, err := uuid.Parse(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrCodeValidationFailed, "schedule_id_invalid")
		return uuid.Nil, false
	}
	return id, true
}

func validateCreateSchedule(in createScheduleRequest, minHorizon time.Duration) string {
	if in.DeliverAt == 0 {
		return "deliver_at_required"
	}
	if in.DeliverAt < 0 {
		return "deliver_at_format"
	}
	deliverAt := time.UnixMilli(in.DeliverAt)
	if time.Until(deliverAt) < minHorizon {
		return "deliver_at_too_soon"
	}
	if _, err := mail.ParseAddress(in.RecipientEmail); err != nil {
		return "recipient_email_invalid"
	}
	if _, err := mail.ParseAddress(in.FromEmail); err != nil {
		return "from_email_invalid"
	}
	if in.Subject == "" {
		return "subject_required"
	}
	if in.Body == "" {
		return "body_required"
	}
	if len(in.IdempotencyKey) > 255 {
		return "idempotency_key_too_long"
	}
	if len(in.Metadata) > 8*1024 {
		return "metadata_too_large"
	}
	return ""
}

func (s *Server) bumpValidation(reason string) {
	mValidationFailures.With(prometheus.Labels{"reason": reason}).Inc()
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
	return []byte(raw)
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
	}
	if row.IdempotencyKey != nil {
		resp.IdempotencyKey = *row.IdempotencyKey
	}
	if row.FromName != nil {
		resp.FromName = *row.FromName
	}
	if row.LastProvider != nil {
		resp.LastProvider = *row.LastProvider
	}
	if row.FailureReason != nil {
		resp.FailureReason = *row.FailureReason
	}
	if len(row.Metadata) > 0 {
		resp.Metadata = row.Metadata
	}
	return resp
}
