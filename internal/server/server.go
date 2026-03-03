package server

import (
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/command"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/metrics"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/resp"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/store"
	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/ttl"
	"go.uber.org/zap"
)

// Server listens on TCP and serves RESP connections.
type Server struct {
	addr     string
	backend  store.Backend
	registry *command.Registry
	tracker  *ttl.Tracker
	maxConns int
	maxArgs  int
	maxBulk  int64
	logger   *zap.Logger
}

type Config struct {
	Addr           string
	Backend        store.Backend
	Registry       *command.Registry
	TTLTracker     *ttl.Tracker
	MaxConnections int
	MaxArgs        int
	MaxBulkLen     int64
	Logger         *zap.Logger
}

func New(cfg Config) *Server {
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 1024
	}
	return &Server{
		addr:     cfg.Addr,
		backend:  cfg.Backend,
		registry: cfg.Registry,
		tracker:  cfg.TTLTracker,
		maxConns: cfg.MaxConnections,
		maxArgs:  cfg.MaxArgs,
		maxBulk:  cfg.MaxBulkLen,
		logger:   cfg.Logger,
	}
}

// Run starts the TCP server. Blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}

	s.logger.Info("RESP server listening", zap.String("addr", s.addr))

	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				wg.Wait()
				return nil
			default:
				s.logger.Error("accept error", zap.Error(err))
				continue
			}
		}

		metrics.ConnectionsTotal.Inc()
		metrics.ConnectionsCurrent.Inc()

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer metrics.ConnectionsCurrent.Dec()
			s.handleConn(ctx, conn)
		}()
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	reader := resp.NewReader(conn, s.maxArgs, s.maxBulk)
	writer := resp.NewWriter(conn)
	state := &command.ConnState{}

	for {
		args, err := reader.ReadCommand()
		if err != nil {
			if err != io.EOF {
				s.logger.Debug("read error", zap.Error(err))
			}
			return
		}
		if len(args) == 0 {
			continue
		}

		cmdCtx := &command.Context{
			Ctx:       ctx,
			Writer:    writer,
			Backend:   s.backend,
			TTL:       s.tracker,
			ConnState: state,
			StartTime: time.Now(),
		}

		cmdName := strings.ToUpper(args[0])
		err = s.registry.Execute(cmdCtx, args)

		if err != nil {
			if command.IsQuitError(err) {
				return
			}
			s.logger.Debug("command error", zap.String("cmd", cmdName), zap.Error(err))
			metrics.CommandsTotal.WithLabelValues(cmdName, "error").Inc()
		} else {
			metrics.CommandsTotal.WithLabelValues(cmdName, "ok").Inc()
		}
		metrics.CommandDuration.WithLabelValues(cmdName).Observe(time.Since(cmdCtx.StartTime).Seconds())

		if err := writer.Flush(); err != nil {
			return
		}
	}
}
