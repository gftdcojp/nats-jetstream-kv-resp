package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gftdcojp/nats-jetstream-kv-resp/internal/config"
	"github.com/nats-io/nats.go"
)

type HealthStatus struct {
	OK     bool    `json:"ok"`
	Checks []Check `json:"checks,omitempty"`
}

type Check struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type HealthChecker struct {
	natsConn *nats.Conn
}

func NewHealthChecker(nc *nats.Conn) *HealthChecker {
	return &HealthChecker{natsConn: nc}
}

func (h *HealthChecker) Liveness() HealthStatus {
	return HealthStatus{OK: true}
}

func (h *HealthChecker) Readiness() HealthStatus {
	status := HealthStatus{OK: true}
	if h.natsConn != nil && !h.natsConn.IsConnected() {
		status.OK = false
		status.Checks = append(status.Checks, Check{Name: "nats", Status: "disconnected"})
	} else {
		status.Checks = append(status.Checks, Check{Name: "nats", Status: "connected"})
	}
	return status
}

func RunHealthServer(ctx context.Context, cfg config.HealthConfig, checker *HealthChecker) error {
	mux := http.NewServeMux()

	livenessPath := cfg.LivenessPath
	if livenessPath == "" {
		livenessPath = "/healthz"
	}
	readinessPath := cfg.ReadinessPath
	if readinessPath == "" {
		readinessPath = "/readyz"
	}

	mux.HandleFunc(livenessPath, func(w http.ResponseWriter, r *http.Request) {
		status := checker.Liveness()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc(readinessPath, func(w http.ResponseWriter, r *http.Request) {
		status := checker.Readiness()
		code := http.StatusOK
		if !status.OK {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(status)
	})

	srv := &http.Server{Addr: cfg.Listen, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
