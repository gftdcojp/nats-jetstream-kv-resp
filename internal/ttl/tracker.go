package ttl

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
	"go.uber.org/zap"
)

const ttlPrefix = "_ttl."

type ttlEntry struct {
	ExpiresAt int64 `json:"expires_at"` // unix milliseconds
}

// Tracker manages per-key TTL metadata.
type Tracker struct {
	mu       sync.RWMutex
	expiries map[string]int64 // key -> unix_millis
	logger   *zap.Logger
}

func NewTracker(logger *zap.Logger) *Tracker {
	return &Tracker{
		expiries: make(map[string]int64),
		logger:   logger,
	}
}

// LoadFromBackend populates the in-memory cache from NATS KV on startup.
func (t *Tracker) LoadFromBackend(ctx context.Context, backend store.Backend) {
	keys, err := backend.Keys(ctx)
	if err != nil {
		t.logger.Warn("failed to load TTL keys", zap.Error(err))
		return
	}
	loaded := 0
	for _, key := range keys {
		if !strings.HasPrefix(key, ttlPrefix) {
			continue
		}
		data, err := backend.Get(ctx, key)
		if err != nil {
			continue
		}
		var entry ttlEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		originalKey := key[len(ttlPrefix):]
		t.mu.Lock()
		t.expiries[originalKey] = entry.ExpiresAt
		t.mu.Unlock()
		loaded++
	}
	t.logger.Info("loaded TTL entries", zap.Int("count", loaded))
}

// SetExpiry sets a TTL for a key.
func (t *Tracker) SetExpiry(ctx context.Context, backend store.Backend, key string, dur time.Duration) {
	expiresAt := time.Now().Add(dur).UnixMilli()
	t.mu.Lock()
	t.expiries[key] = expiresAt
	t.mu.Unlock()

	entry := ttlEntry{ExpiresAt: expiresAt}
	data, _ := json.Marshal(entry)
	if _, err := backend.Put(ctx, ttlPrefix+key, data); err != nil {
		t.logger.Warn("failed to persist TTL", zap.String("key", key), zap.Error(err))
	}
}

// RemoveExpiry removes the TTL for a key.
func (t *Tracker) RemoveExpiry(ctx context.Context, backend store.Backend, key string) {
	t.mu.Lock()
	delete(t.expiries, key)
	t.mu.Unlock()

	backend.Delete(ctx, ttlPrefix+key)
}

// IsExpired checks if a key has expired (O(1) in-memory check).
func (t *Tracker) IsExpired(key string) bool {
	t.mu.RLock()
	expiresAt, ok := t.expiries[key]
	t.mu.RUnlock()
	if !ok {
		return false
	}
	return time.Now().UnixMilli() >= expiresAt
}

// GetTTLMillis returns remaining TTL in milliseconds, -1 if no expiry.
func (t *Tracker) GetTTLMillis(key string) int64 {
	t.mu.RLock()
	expiresAt, ok := t.expiries[key]
	t.mu.RUnlock()
	if !ok {
		return -1
	}
	remaining := expiresAt - time.Now().UnixMilli()
	if remaining <= 0 {
		return 0
	}
	return remaining
}

// CleanupExpired deletes the data key and TTL metadata for an expired key.
func (t *Tracker) CleanupExpired(ctx context.Context, backend store.Backend, key string) {
	backend.Delete(ctx, key)
	t.RemoveExpiry(ctx, backend, key)
}

// ReapExpired scans and deletes all expired keys. Called by the reaper goroutine.
func (t *Tracker) ReapExpired(ctx context.Context, backend store.Backend) int {
	now := time.Now().UnixMilli()
	t.mu.RLock()
	var expired []string
	for key, expiresAt := range t.expiries {
		if now >= expiresAt {
			expired = append(expired, key)
		}
	}
	t.mu.RUnlock()

	for _, key := range expired {
		backend.Delete(ctx, key)
		t.RemoveExpiry(ctx, backend, key)
	}
	return len(expired)
}
