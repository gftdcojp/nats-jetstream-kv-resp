# nats-jetstream-kv-resp

Redis RESP protocol bridge for [NATS JetStream KeyValue](https://docs.nats.io/nats-concepts/jetstream/key-value-store).

Use any Redis client (redis-cli, go-redis, ioredis, etc.) to read and write NATS JetStream KV buckets.

## Features

- Full RESP2 protocol (inline + multi-bulk)
- 50+ Redis commands across 6 categories
- Multi-bucket support via `SELECT`
- Per-key TTL with background reaper
- Prometheus metrics + health endpoints
- Embeddable as a Go library (`pkg/bridge`)
- Docker + Kubernetes ready

## Supported Commands

| Category | Commands |
|----------|----------|
| **String** | `GET` `SET` (NX/XX/EX/PX) `MGET` `MSET` `SETNX` `APPEND` `GETSET` `STRLEN` `SETEX` `PSETEX` `GETDEL` |
| **Key** | `DEL` `EXISTS` `KEYS` `SCAN` `TYPE` `RENAME` `EXPIRE` `TTL` `PEXPIRE` `PTTL` `PERSIST` `DBSIZE` `FLUSHDB` |
| **Hash** | `HGET` `HSET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN` `HMGET` `HMSET` `HEXISTS` `HSCAN` |
| **List** | `LPUSH` `RPUSH` `LPOP` `RPOP` `LRANGE` `LLEN` `LINDEX` `LSET` |
| **Connection** | `PING` `SELECT` `QUIT` `ECHO` `AUTH` `CLIENT` |
| **Server** | `INFO` `DBSIZE` `COMMAND` |

## NATS KV Mapping

| Redis Data Type | NATS KV Key Format | Notes |
|---|---|---|
| String | `{key}` | Direct 1:1 mapping. `SETNX` uses `kv.Create()` (CAS) |
| Hash | `_h.{key}.{field}` + `_hm.{key}` | Metadata tracks field list for HGETALL/HKEYS |
| List | `_l.{key}` | JSON-encoded `[]string` |
| TTL metadata | `_ttl.{key}` | In-memory cache + NATS KV persistence |

Internal prefixes (`_h.`, `_hm.`, `_l.`, `_ttl.`) are excluded from `KEYS`/`SCAN` results.

## Quick Start

### Binary

```bash
go install github.com/gftdcojp/nats-jetstream-kv-resp/cmd/nats-jetstream-kv-resp@latest
nats-jetstream-kv-resp -config config.yaml
```

### Docker Compose

```bash
docker compose -f deploy/docker/docker-compose.yaml up
```

### Test with redis-cli

```bash
redis-cli -p 6380 PING
# PONG

redis-cli -p 6380 SET greeting "hello world"
# OK

redis-cli -p 6380 GET greeting
# "hello world"

redis-cli -p 6380 HSET user:1 name Alice age 30
# (integer) 2

redis-cli -p 6380 HGETALL user:1
# 1) "name"
# 2) "Alice"
# 3) "age"
# 4) "30"

redis-cli -p 6380 KEYS '*'
# 1) "greeting"
# 2) "user:1"
```

## Configuration

```yaml
nats:
  url: "nats://localhost:4222"
  connection_name: "kv-resp-bridge"
  max_reconnects: -1
  reconnect_wait: "2s"

server:
  listen_addr: ":6380"
  max_connections: 1024

buckets:
  - name: "performers"   # SELECT 0
  - name: "sessions"     # SELECT 1

auth:
  enabled: false
  password: ""

ttl:
  enabled: true
  reaper_interval: "1s"

observability:
  metrics:
    enabled: true
    listen: ":9090"
    path: "/metrics"
  health:
    enabled: true
    listen: ":6381"
    liveness_path: "/healthz"
    readiness_path: "/readyz"
  logging:
    level: "info"
    format: "json"
```

## Library Usage

```go
package main

import (
    "context"
    "log"

    "github.com/gftdcojp/nats-jetstream-kv-resp/internal/config"
    "github.com/gftdcojp/nats-jetstream-kv-resp/pkg/bridge"
    "go.uber.org/zap"
)

func main() {
    cfg, _ := config.Load("config.yaml")
    logger, _ := zap.NewProduction()

    b := bridge.New(cfg, logger)
    defer b.Close()

    if err := b.Run(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

Or with an existing NATS connection:

```go
b := bridge.NewWithConn(cfg, existingNC, logger)
```

## Kubernetes

```bash
kubectl apply -f deploy/kubernetes/deployment.yaml
```

Deploys to `spinkube` namespace with:
- RESP on port 6380
- Health on port 6381 (`/healthz`, `/readyz`)
- Metrics on port 9090 (`/metrics`)

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `resp_bridge_connections_total` | Counter | - |
| `resp_bridge_commands_total` | Counter | `command`, `status` |
| `resp_bridge_command_duration_seconds` | Histogram | `command` |
| `resp_bridge_nats_ops_total` | Counter | `operation`, `status` |
| `resp_bridge_ttl_expirations_total` | Counter | - |

### Health Checks

- `GET :6381/healthz` — liveness (always OK)
- `GET :6381/readyz` — readiness (checks NATS connectivity)

## Building

```bash
make build        # Build binary
make test         # Run tests
make docker       # Build Docker image
make docker-ghcr  # Build and push to GHCR (multi-platform)
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
