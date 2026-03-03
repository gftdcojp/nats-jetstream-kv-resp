package store

import (
	"context"
	"errors"
)

// ErrKeyNotFound is returned when a key does not exist.
var ErrKeyNotFound = errors.New("key not found")

// ErrKeyExists is returned when Create() fails because the key already exists.
var ErrKeyExists = errors.New("key already exists")

// ErrInvalidDB is returned when SELECT index is out of range.
var ErrInvalidDB = errors.New("invalid DB index")

// Backend abstracts NATS JetStream KV operations for Redis command handlers.
type Backend interface {
	// String operations
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) (uint64, error)
	Delete(ctx context.Context, key string) error
	Create(ctx context.Context, key string, value []byte) (uint64, error)

	// Key enumeration
	Keys(ctx context.Context) ([]string, error)

	// Bucket management
	ActiveBucket() string
	SwitchBucket(ctx context.Context, index int) error
	BucketKeyCount(ctx context.Context) (int, error)
	PurgeAll(ctx context.Context) error

	// Health
	IsConnected() bool
}
