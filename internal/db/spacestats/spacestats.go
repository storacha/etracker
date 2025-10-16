package spacestats

import (
	"context"
	"time"

	"github.com/storacha/go-ucanto/did"
)

type DailyStats struct {
	Date   time.Time
	Egress uint64
}

type SpaceStatsTable interface {
	GetDailyStats(ctx context.Context, space did.DID, since time.Time) ([]DailyStats, error)
}
