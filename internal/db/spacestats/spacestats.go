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
	Record(ctx context.Context, space did.DID, egress uint64) error
	GetDailyStats(ctx context.Context, space did.DID, from time.Time, to time.Time) ([]DailyStats, error)
}
