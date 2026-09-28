package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/mail"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mdhishaamakhtar/hatch/internal/db"
	"github.com/mdhishaamakhtar/hatch/internal/httpx"
	"github.com/mdhishaamakhtar/hatch/internal/service"
	"go.uber.org/zap"
)

const (
	// maxScheduleHorizon keeps deliver_at well inside the monthly partitions
	// migration 004 creates. Past them an insert would fail inside Postgres.
	maxScheduleHorizon = 10 * 365 * 24 * time.Hour

	maxScheduleBytes     = 64 << 10
	maxIdempotencyKeyLen = 255
	maxMetadataBytes     = 8 << 10
)

type createScheduleRequest struct {
	DeliverAt      int64           `json:"deliver_at"` // Unix milliseconds
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

type scheduleDetail struct {
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

// createSchedule schedules an email. A request that repeats an idempotency key
// gets the schedule the key first created, with 200 instead of 201.
//
//	@Summary	Schedule an email
//	@Tags		schedules
//	@Accept		json
//	@Produce	json
//	@Param		body	body		createScheduleRequest	true	"The email and when to send it"
//	@Success	201		{object}	scheduleResponse
//	@Success	200		{object}	scheduleResponse	"Idempotent replay"
//	@Failure	400		{object}	httpx.ErrorBody
//	@Failure	401		{object}	httpx.ErrorBody
//	@Failure	413		{object}	httpx.ErrorBody
//	@Failure	415		{object}	httpx.ErrorBody
//	@Failure	429		{object}	httpx.ErrorBody
//	@Security	BearerAuth
//	@Router		/v1/schedules [post]
func (s *Server) createSchedule(w http.ResponseWriter, r *http.Request) {
	c := clientFrom(r.Context())
	var in createScheduleRequest
	if !decode(w, r, maxScheduleBytes, &in) {
		return
	}
	if reason := in.validate(time.Now(), s.cfg.MinScheduleHorizon); reason != "" {
		validationFailures.WithLabelValues(reason).Inc()
		httpx.WriteError(w, http.StatusBadRequest, codeValidation, reason)
		return
	}
	if !c.hasProvider {
		noProviderRejections.Inc()
		httpx.WriteError(w, http.StatusBadRequest, codeNoActiveProviders, "")
		return
	}

	deliverAt := time.UnixMilli(in.DeliverAt)
	id := db.NewScheduleID(deliverAt)
	var status db.ScheduleStatus
	var err error
	if in.IdempotencyKey == "" {
		status, err = s.queries.CreateSchedule(r.Context(), db.CreateScheduleParams{
			ID:             id[:],
			ClientID:       c.id[:],
			DeliverAt:      deliverAt,
			RecipientEmail: in.RecipientEmail,
			FromEmail:      in.FromEmail,
			FromName:       nullable(in.FromName),
			Subject:        in.Subject,
			Body:           in.Body,
			Metadata:       in.Metadata,
		})
	} else {
		status, err = s.queries.CreateScheduleWithKey(r.Context(), db.CreateScheduleWithKeyParams{
			ID:             id[:],
			ClientID:       c.id[:],
			IdempotencyKey: in.IdempotencyKey,
			DeliverAt:      deliverAt,
			RecipientEmail: in.RecipientEmail,
			FromEmail:      in.FromEmail,
			FromName:       nullable(in.FromName),
			Subject:        in.Subject,
			Body:           in.Body,
			Metadata:       in.Metadata,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			s.replay(w, r, in.IdempotencyKey)
			return
		}
	}
	if err != nil {
		s.internalError(w, r, "create schedule", err)
		return
	}

	service.WithTrace(r.Context(), s.lg).Info("schedule created",
		zap.Stringer("client_id", c.id), zap.Stringer("schedule_id", id), zap.Time("deliver_at", deliverAt))
	httpx.WriteJSON(w, http.StatusCreated, scheduleResponse{ScheduleID: id.String(), Status: string(status), DeliverAt: in.DeliverAt})
}

// replay answers a create whose idempotency key was already used with the
// schedule that key created, as it stands now.
func (s *Server) replay(w http.ResponseWriter, r *http.Request, key string) {
	c := clientFrom(r.Context())
	idempotencyHits.Inc()
	stored, err := s.queries.GetScheduleIDByKey(r.Context(), db.GetScheduleIDByKeyParams{ClientID: c.id[:], IdempotencyKey: key})
	if err != nil {
		s.internalError(w, r, "look up idempotency key", err)
		return
	}
	id := uuid.UUID(stored)
	row, err := s.queries.GetSchedule(r.Context(), db.GetScheduleParams{ID: id[:], DeliverAt: db.ScheduleDeliverAt(id), ClientID: c.id[:]})
	if err != nil {
		s.internalError(w, r, "read replayed schedule", err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, scheduleResponse{ScheduleID: id.String(), Status: string(row.Status), DeliverAt: row.DeliverAt.UnixMilli()})
}

// getSchedule returns one of the client's schedules.
//
//	@Summary	Get a schedule
//	@Tags		schedules
//	@Produce	json
//	@Param		schedule_id	path		string	true	"Schedule id"
//	@Success	200			{object}	scheduleDetail
//	@Failure	400			{object}	httpx.ErrorBody
//	@Failure	401			{object}	httpx.ErrorBody
//	@Failure	404			{object}	httpx.ErrorBody
//	@Security	BearerAuth
//	@Router		/v1/schedules/{schedule_id} [get]
func (s *Server) getSchedule(w http.ResponseWriter, r *http.Request) {
	c := clientFrom(r.Context())
	id, ok := pathID(w, r, "schedule_id")
	if !ok {
		return
	}
	row, err := s.queries.GetSchedule(r.Context(), db.GetScheduleParams{ID: id[:], DeliverAt: db.ScheduleDeliverAt(id), ClientID: c.id[:]})
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.WriteError(w, http.StatusNotFound, codeNotFound, "")
		return
	}
	if err != nil {
		s.internalError(w, r, "get schedule", err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, scheduleDetail{
		ScheduleID:     id.String(),
		Status:         string(row.Status),
		DeliverAt:      row.DeliverAt.UnixMilli(),
		RecipientEmail: row.RecipientEmail,
		FromEmail:      row.FromEmail,
		FromName:       value(row.FromName),
		Subject:        row.Subject,
		Body:           row.Body,
		IdempotencyKey: value(row.IdempotencyKey),
		Metadata:       row.Metadata,
		RetryCount:     row.RetryCount,
		LastProvider:   value(row.LastProvider),
		FailureReason:  value(row.FailureReason),
		CreatedAt:      row.CreatedAt,
		UpdatedAt:      row.UpdatedAt,
	})
}

// cancelSchedule cancels a schedule that has not finished yet.
//
//	@Summary	Cancel a schedule
//	@Tags		schedules
//	@Produce	json
//	@Param		schedule_id	path	string	true	"Schedule id"
//	@Success	204
//	@Failure	400	{object}	httpx.ErrorBody
//	@Failure	401	{object}	httpx.ErrorBody
//	@Failure	404	{object}	httpx.ErrorBody
//	@Failure	409	{object}	httpx.ErrorBody	"Already delivered, failed or cancelled"
//	@Security	BearerAuth
//	@Router		/v1/schedules/{schedule_id} [delete]
func (s *Server) cancelSchedule(w http.ResponseWriter, r *http.Request) {
	c := clientFrom(r.Context())
	id, ok := pathID(w, r, "schedule_id")
	if !ok {
		return
	}
	deliverAt := db.ScheduleDeliverAt(id)
	n, err := s.queries.CancelSchedule(r.Context(), db.CancelScheduleParams{ID: id[:], DeliverAt: deliverAt, ClientID: c.id[:]})
	if err != nil {
		s.internalError(w, r, "cancel schedule", err)
		return
	}
	if n == 1 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Nothing was cancelled: the schedule doesn't exist, or it has finished.
	row, err := s.queries.GetSchedule(r.Context(), db.GetScheduleParams{ID: id[:], DeliverAt: deliverAt, ClientID: c.id[:]})
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.WriteError(w, http.StatusNotFound, codeNotFound, "")
		return
	}
	if err != nil {
		s.internalError(w, r, "get schedule", err)
		return
	}
	httpx.WriteError(w, http.StatusConflict, codeConflict, "status_"+string(row.Status))
}

// validate returns why a create request is unacceptable, or "" if it is fine.
func (in createScheduleRequest) validate(now time.Time, minHorizon time.Duration) string {
	switch {
	case in.DeliverAt == 0:
		return "deliver_at_required"
	case in.DeliverAt < 0:
		return "deliver_at_format"
	}
	switch lead := time.UnixMilli(in.DeliverAt).Sub(now); {
	case lead < minHorizon:
		return "deliver_at_too_soon"
	case lead > maxScheduleHorizon:
		return "deliver_at_too_far"
	}
	switch {
	case !isAddress(in.RecipientEmail):
		return "recipient_email_invalid"
	case !isAddress(in.FromEmail):
		return "from_email_invalid"
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

// isAddress reports whether s is one bare email address: no display name, and
// nothing before or after it.
func isAddress(s string) bool {
	a, err := mail.ParseAddress(s)
	return err == nil && a.Address == s
}

// nullable maps "" to a NULL column.
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// value reads a nullable column, with NULL as "".
func value(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
