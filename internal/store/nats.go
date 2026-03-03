package store

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// NATSBackend implements Backend using NATS JetStream KV.
type NATSBackend struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	buckets []jetstream.KeyValue
	names   []string
	active  int
	mu      sync.RWMutex
	timeout time.Duration
	logger  *zap.Logger
}

func NewNATSBackend(nc *nats.Conn, js jetstream.JetStream, bucketNames []string, timeout time.Duration, logger *zap.Logger) (*NATSBackend, error) {
	if len(bucketNames) == 0 {
		return nil, errors.New("at least one bucket name required")
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	b := &NATSBackend{
		nc:      nc,
		js:      js,
		buckets: make([]jetstream.KeyValue, len(bucketNames)),
		names:   bucketNames,
		timeout: timeout,
		logger:  logger,
	}

	// Open the first bucket eagerly
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	kv, err := js.KeyValue(ctx, bucketNames[0])
	if err != nil {
		return nil, err
	}
	b.buckets[0] = kv

	return b, nil
}

func (b *NATSBackend) kvCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), b.timeout)
}

func (b *NATSBackend) kv() jetstream.KeyValue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.buckets[b.active]
}

func (b *NATSBackend) Get(ctx context.Context, key string) ([]byte, error) {
	entry, err := b.kv().Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, ErrKeyNotFound
		}
		return nil, err
	}
	return entry.Value(), nil
}

func (b *NATSBackend) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return b.kv().Put(ctx, key, value)
}

func (b *NATSBackend) Delete(ctx context.Context, key string) error {
	return b.kv().Delete(ctx, key)
}

func (b *NATSBackend) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	rev, err := b.kv().Create(ctx, key, value)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, ErrKeyExists
		}
		return 0, err
	}
	return rev, nil
}

func (b *NATSBackend) Keys(ctx context.Context) ([]string, error) {
	keys, err := b.kv().Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	return keys, nil
}

func (b *NATSBackend) ActiveBucket() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.names[b.active]
}

func (b *NATSBackend) SwitchBucket(ctx context.Context, index int) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if index < 0 || index >= len(b.names) {
		return ErrInvalidDB
	}

	// Lazy open
	if b.buckets[index] == nil {
		kv, err := b.js.KeyValue(ctx, b.names[index])
		if err != nil {
			return err
		}
		b.buckets[index] = kv
	}

	b.active = index
	return nil
}

func (b *NATSBackend) BucketKeyCount(ctx context.Context) (int, error) {
	status, err := b.kv().Status(ctx)
	if err != nil {
		return 0, err
	}
	return int(status.Values()), nil
}

func (b *NATSBackend) PurgeAll(ctx context.Context) error {
	keys, err := b.Keys(ctx)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := b.kv().Purge(ctx, key); err != nil {
			b.logger.Warn("purge key failed", zap.String("key", key), zap.Error(err))
		}
	}
	return nil
}

func (b *NATSBackend) IsConnected() bool {
	return b.nc.IsConnected()
}
