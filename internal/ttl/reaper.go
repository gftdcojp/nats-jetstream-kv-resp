package ttl

import (
	"context"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
	"go.uber.org/zap"
)

// Reaper runs a background loop that cleans up expired keys.
type Reaper struct {
	tracker  *Tracker
	backend  store.Backend
	interval time.Duration
	logger   *zap.Logger
}

func NewReaper(tracker *Tracker, backend store.Backend, interval time.Duration, logger *zap.Logger) *Reaper {
	if interval <= 0 {
		interval = time.Second
	}
	return &Reaper{
		tracker:  tracker,
		backend:  backend,
		interval: interval,
		logger:   logger,
	}
}

// Run starts the reaper loop. Blocks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			count := r.tracker.ReapExpired(ctx, r.backend)
			if count > 0 {
				r.logger.Debug("reaped expired keys", zap.Int("count", count))
			}
		}
	}
}
