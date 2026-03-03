package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
	server "github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func startTestServer(t *testing.T) (*server.Server, *nats.Conn, jetstream.JetStream) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	ns := natsserver.RunServer(&opts)
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	// Create test KV bucket
	_, err = js.CreateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	return ns, nc, js
}

func newBackend(t *testing.T, nc *nats.Conn, js jetstream.JetStream, buckets []string) store.Backend {
	t.Helper()
	be, err := store.NewNATSBackend(nc, js, buckets, 5*time.Second, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return be
}

func TestNATSBackend_GetPut(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	be := newBackend(t, nc, js, []string{"test"})

	// Put
	_, err := be.Put(ctx, "hello", []byte("world"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get
	val, err := be.Get(ctx, "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "world" {
		t.Fatalf("expected 'world', got %q", val)
	}

	// Delete
	err = be.Delete(ctx, "hello")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err = be.Get(ctx, "hello")
	if err == nil {
		t.Fatal("expected error after delete")
	}
}

func TestNATSBackend_Create(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	be := newBackend(t, nc, js, []string{"test"})

	_, err := be.Create(ctx, "unique", []byte("value"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Create again should fail (CAS)
	_, err = be.Create(ctx, "unique", []byte("other"))
	if err == nil {
		t.Fatal("expected error on duplicate Create")
	}
}

func TestNATSBackend_Keys(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	be := newBackend(t, nc, js, []string{"test"})

	be.Put(ctx, "a", []byte("1"))
	be.Put(ctx, "b", []byte("2"))
	be.Put(ctx, "c", []byte("3"))

	keys, err := be.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
}

func TestNATSBackend_SwitchBucket(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()

	// Create second bucket
	_, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "sessions"})
	if err != nil {
		t.Fatal(err)
	}

	be := newBackend(t, nc, js, []string{"test", "sessions"})

	if be.ActiveBucket() != "test" {
		t.Fatalf("expected active bucket 'test', got %q", be.ActiveBucket())
	}

	err = be.SwitchBucket(ctx, 1)
	if err != nil {
		t.Fatalf("SwitchBucket: %v", err)
	}
	if be.ActiveBucket() != "sessions" {
		t.Fatalf("expected active bucket 'sessions', got %q", be.ActiveBucket())
	}
}

func TestNATSBackend_IsConnected(t *testing.T) {
	_, nc, js := startTestServer(t)
	be := newBackend(t, nc, js, []string{"test"})

	if !be.IsConnected() {
		t.Fatal("expected IsConnected=true")
	}
}

func TestNATSBackend_PurgeAll(t *testing.T) {
	_, nc, js := startTestServer(t)
	ctx := context.Background()
	be := newBackend(t, nc, js, []string{"test"})

	be.Put(ctx, "x", []byte("y"))
	be.Put(ctx, "z", []byte("w"))

	err := be.PurgeAll(ctx)
	if err != nil {
		t.Fatalf("PurgeAll: %v", err)
	}

	// After purge, keys should be gone
	time.Sleep(50 * time.Millisecond)

	keys, _ := be.Keys(ctx)
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys after purge, got %d", len(keys))
	}
}
