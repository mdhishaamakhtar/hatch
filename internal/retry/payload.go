package retry

import "encoding/json"

// duePayload is the JSON shape carried on emails.due and every retry tier — a
// thin envelope of just the schedule id (all delivery state lives in Postgres).
type duePayload struct {
	ScheduleID string `json:"schedule_id"`
}

// scheduleIDFromValue extracts the schedule_id from a record value for logging.
// Returns "" on malformed payloads — the value is still re-enqueued verbatim.
func scheduleIDFromValue(value []byte) string {
	var p duePayload
	if err := json.Unmarshal(value, &p); err != nil {
		return ""
	}
	return p.ScheduleID
}
