package delivery

import (
	"github.com/google/uuid"
	"github.com/mdhishaamakhtar/hatch/pkg/db"
)

// uuidString renders a 16-byte schedule/client id as its canonical UUID string.
// Returns "" if the bytes are not a valid UUID.
func uuidString(b []byte) string {
	u, err := db.BytesToUUID(b)
	if err != nil {
		return ""
	}
	return u.String()
}

// parseUUID parses a canonical UUID string.
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}
