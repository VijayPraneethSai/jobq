// Package server is the coordinator's HTTP API: submit and inspect jobs, lease/heartbeat/complete for workers,
// health endpoints and Prometheus metrics. It is a thin layer: all state rules live in package queue.
package server

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/metrics"
	"github.com/VijayPraneethSai/jobq/internal/queue"
)

// Config of the API layer.
type Config struct {
	Logger       *slog.Logger
	AuthToken    string        // if set, every endpoint except /healthz and /readyz needs "Authorization: Bearer <token>"
	DefaultTTL   time.Duration // lease length when the worker does not ask for one
	MaxTTL       time.Duration // upper bound on a requested lease length
	CompactBytes int64         // compact the WAL when it grows past this many bytes (0 = never)
}

// Server wires the store to HTTP.
type Server struct {
	store *queue.Store
	cfg   Config
	log   *slog.Logger
	reg   *metrics.Registry

	httpReqs map[string]*metrics.Counter // by status class
	finished *metrics.Histogram          // submit -> success, seconds
}

// New builds a server around an open store.
func New(store *queue.Store, cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 30 * time.Second
	}
	if cfg.MaxTTL <= 0 {
		cfg.MaxTTL = 10 * time.Minute
	}
	s := &Server{store: store, cfg: cfg, log: cfg.Logger, reg: metrics.NewRegistry(), httpReqs: map[string]*metrics.Counter{}}
	s.registerMetrics()
	return s
}

func (s *Server) registerMetrics() {
	r := s.reg
	r.GaugeVec("jobq_jobs", "Jobs by state (queued = runnable now, delayed = waiting out a retry backoff).", "state", func() map[string]float64 {
		st := s.store.Stats()
		return map[string]float64{"queued": float64(st.Queued), "delayed": float64(st.Delayed), "running": float64(st.Running),
			"succeeded": float64(st.Succeeded), "dead": float64(st.Dead)}
	})
	r.CounterFunc("jobq_retries_total", "Failed attempts that were scheduled for another try.", func() uint64 { return s.store.Stats().Retries })
	r.CounterFunc("jobq_lease_expirations_total", "Leases that ran out because the worker stopped heartbeating.", func() uint64 { return s.store.Stats().LeaseExpirations })
	r.CounterFunc("jobq_stale_reports_total", "Heartbeats or completions rejected because the lease was no longer current.", func() uint64 { return s.store.Stats().StaleReports })
	r.Gauge("jobq_wal_bytes", "Current size of the write-ahead log.", func() float64 { return float64(s.store.Stats().WALBytes) })
	s.finished = metrics.NewHistogram(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)
	r.Histogram("jobq_job_latency_seconds", "Time from submit to successful completion.", s.finished)
	for _, c := range []string{"2xx", "4xx", "5xx"} {
		s.httpReqs[c] = r.Counter("jobq_http_requests_total", "HTTP requests by status class.", map[string]string{"class": c})
	}
}

// Handler returns the routed, instrumented handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.submit)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("POST /lease", s.lease)
	mux.HandleFunc("POST /jobs/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /jobs/{id}/complete", s.complete)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ready\n")) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.reg.Render(w)
	})
	return s.middleware(mux)
}

// ---- middleware: request id, auth, access log, status metrics ----

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func reqID() string { var b [6]byte; _, _ = rand.Read(b[:]); return hex.EncodeToString(b[:]) }

func (s *Server) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			id = reqID()
		}
		w.Header().Set("X-Request-Id", id)
		sw := &statusWriter{ResponseWriter: w, code: 200}
		open := r.URL.Path == "/healthz" || r.URL.Path == "/readyz"
		if s.cfg.AuthToken != "" && !open {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if subtle.ConstantTimeCompare([]byte(got), []byte(s.cfg.AuthToken)) != 1 {
				writeErr(sw, http.StatusUnauthorized, "missing or wrong bearer token")
				s.account(sw.code)
				return
			}
		}
		next.ServeHTTP(sw, r)
		s.account(sw.code)
		if r.URL.Path != "/healthz" && r.URL.Path != "/metrics" { // keep scrapes and probes out of the log
			s.log.Info("request", "id", id, "method", r.Method, "path", r.URL.Path, "status", sw.code, "dur_ms", float64(time.Since(start).Microseconds())/1000)
		}
	})
}

func (s *Server) account(code int) {
	switch {
	case code >= 500:
		s.httpReqs["5xx"].Inc()
	case code >= 400:
		s.httpReqs["4xx"].Inc()
	default:
		s.httpReqs["2xx"].Inc()
	}
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// decode reads a JSON body of at most 1 MiB and rejects unknown fields (typos should fail loudly).
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "bad JSON body: "+err.Error())
		return false
	}
	return true
}

func (s *Server) ttl(sec int) time.Duration {
	d := time.Duration(sec) * time.Second
	if d <= 0 {
		d = s.cfg.DefaultTTL
	}
	if d > s.cfg.MaxTTL {
		d = s.cfg.MaxTTL
	}
	return d
}

func (s *Server) storeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrStaleLease):
		writeErr(w, http.StatusConflict, err.Error())
	case errors.Is(err, queue.ErrInvalid):
		writeErr(w, http.StatusBadRequest, err.Error())
	default:
		s.log.Error("store error", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

// ---- handlers ----

type submitBody struct {
	Key        string   `json:"key"`
	Command    string   `json:"command"`
	Args       []string `json:"args"`
	TimeoutSec int      `json:"timeout_sec"`
	MaxRetries int      `json:"max_retries"`
	Priority   int      `json:"priority"`
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	var b submitBody
	if !decode(w, r, &b) {
		return
	}
	j, created, err := s.store.Submit(queue.SubmitReq{Key: b.Key, Command: b.Command, Args: b.Args, TimeoutSec: b.TimeoutSec, MaxRetries: b.MaxRetries, Priority: b.Priority})
	if err != nil {
		s.storeErr(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK // same idempotency key: the existing job
	}
	writeJSON(w, code, j)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	j, ok := s.store.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, http.StatusNotFound, "job not found")
		return
	}
	j.LeaseToken = "" // the token is a credential of the lease holder, not for observers
	writeJSON(w, http.StatusOK, j)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	jobs := s.store.List(queue.State(r.URL.Query().Get("state")), limit)
	for i := range jobs {
		jobs[i].LeaseToken = ""
	}
	writeJSON(w, http.StatusOK, jobs)
}

type leaseBody struct {
	Worker string `json:"worker"`
	TTLSec int    `json:"ttl_sec"`
}

func (s *Server) lease(w http.ResponseWriter, r *http.Request) {
	var b leaseBody
	if !decode(w, r, &b) {
		return
	}
	if b.Worker == "" {
		writeErr(w, http.StatusBadRequest, "worker is required")
		return
	}
	j, err := s.store.Lease(b.Worker, s.ttl(b.TTLSec))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if j == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, j) // includes lease_token: only the leaseholder ever sees it
}

type heartbeatBody struct {
	Token  string `json:"token"`
	TTLSec int    `json:"ttl_sec"`
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var b heartbeatBody
	if !decode(w, r, &b) {
		return
	}
	if err := s.store.Heartbeat(r.PathValue("id"), b.Token, s.ttl(b.TTLSec)); err != nil {
		s.storeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type completeBody struct {
	Token      string `json:"token"`
	ExitCode   int    `json:"exit_code"`
	Output     string `json:"output"`
	DurationMS int64  `json:"duration_ms"`
	TimedOut   bool   `json:"timed_out"`
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	var b completeBody
	if !decode(w, r, &b) {
		return
	}
	j, err := s.store.Complete(r.PathValue("id"), b.Token, queue.Result{ExitCode: b.ExitCode, Output: b.Output, DurationMS: b.DurationMS, TimedOut: b.TimedOut})
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if j.State == queue.Succeeded {
		s.finished.Observe(float64(j.FinishedAt-j.SubmittedAt) / 1e9)
	}
	j.LeaseToken = ""
	writeJSON(w, http.StatusOK, j)
}

// ---- background loop ----

// RunReaper expires dead leases every interval and compacts the WAL when it is large, until ctx is cancelled.
func (s *Server) RunReaper(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n := s.store.ExpireLeases(); n > 0 {
				s.log.Warn("leases expired", "count", n)
			}
			if s.cfg.CompactBytes > 0 && s.store.Stats().WALBytes > s.cfg.CompactBytes {
				if err := s.store.Compact(); err != nil {
					s.log.Error("compaction failed", "err", err)
				} else {
					s.log.Info("wal compacted")
				}
			}
		}
	}
}
