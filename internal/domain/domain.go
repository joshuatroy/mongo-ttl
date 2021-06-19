package domain

import (
	"time"

	"github.com/google/uuid"
)

type Record struct {
	ID        uuid.UUID
	Timestamp time.Time
}
