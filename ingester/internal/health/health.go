// Package health exposes /healthz (liveness) and /readyz (readiness) plus the
// Prometheus /metrics endpoint on a single HTTP server.
package health

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is a small HTTP server for probes and metrics.
type Server struct {
	srv   *http.Server
	ready atomic.Bool
	live  atomic.Bool
}

// New builds the server. `reg` is the Prometheus registry to expose.
func New(addr string, reg *prometheus.Registry) *Server {
	s := &Server{}
	s.live.Store(true) // process is live as soon as it starts

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if s.live.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.ready.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
	})

	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// SetReady toggles readiness (e.g. false while ClickHouse is unreachable).
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// SetLive toggles liveness (set false only on unrecoverable state).
func (s *Server) SetLive(live bool) { s.live.Store(live) }

// Start runs the server until Shutdown is called. Errors are returned via the
// returned channel.
func (s *Server) Start() <-chan error {
	errc := make(chan error, 1)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errc <- err
		}
	}()
	return errc
}

// Shutdown gracefully stops the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}
