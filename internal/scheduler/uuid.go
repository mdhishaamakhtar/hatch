package scheduler

import "github.com/google/uuid"

// uuidString renders a [16]byte id in canonical UUID form. Centralised so the
// scheduler never leaks raw binary ids in logs, spans, JSON, or Kafka payloads.
func uuidString(id [16]byte) string { return uuid.UUID(id).String() }
