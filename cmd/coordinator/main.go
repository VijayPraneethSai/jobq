// Command coordinator runs the job queue server.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/queue"
	"github.com/VijayPraneethSai/jobq/internal/server"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("data", "./jobq-data", "directory for the WAL and snapshot (empty = in memory, nothing survives a restart)")
	syncPol := flag.String("sync", "batch", "WAL fsync policy: batch (every 50 ms), always, never")
	token := flag.String("token", os.Getenv("JOBQ_TOKEN"), "bearer token required on the API (default: $JOBQ_TOKEN; empty = no auth)")
	reaper := flag.Duration("reaper", time.Second, "how often expired leases are collected")
	compact := flag.Int64("compact-bytes", 64<<20, "compact the WAL when it exceeds this size (0 = never)")
	base := flag.Duration("backoff-base", time.Second, "first retry delay; doubles per attempt")
	max := flag.Duration("backoff-max", 30*time.Second, "retry delay cap")
	ttl := flag.Duration("lease-ttl", 30*time.Second, "default lease length")
	jsonLog := flag.Bool("log-json", false, "log as JSON")
	flag.Parse()

	var h slog.Handler = slog.NewTextHandler(os.Stderr, nil)
	if *jsonLog {
		h = slog.NewJSONHandler(os.Stderr, nil)
	}
	log := slog.New(h)

	pol := queue.SyncBatch
	switch *syncPol {
	case "always":
		pol = queue.SyncAlways
	case "never":
		pol = queue.SyncNever
	case "batch":
	default:
		log.Error("unknown --sync value", "value", *syncPol)
		os.Exit(2)
	}
	store, err := queue.Open(queue.Config{Dir: *dir, Sync: pol, BackoffBase: *base, BackoffMax: *max})
	if err != nil {
		log.Error("cannot open store", "err", err)
		os.Exit(1)
	}
	if r := store.Recovery; r.SnapshotJobs+r.Events > 0 || r.TruncatedBytes > 0 {
		log.Info("recovered", "snapshot_jobs", r.SnapshotJobs, "wal_events", r.Events, "skipped", r.Skipped, "truncated_bytes", r.TruncatedBytes)
	}
	srv := server.New(store, server.Config{Logger: log, AuthToken: *token, DefaultTTL: *ttl, CompactBytes: *compact})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go srv.RunReaper(ctx, *reaper)

	hs := &http.Server{Addr: *addr, Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second,
		WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()
	log.Info("listening", "addr", *addr, "data", *dir, "sync", *syncPol, "auth", *token != "")
	if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", "err", err)
		os.Exit(1)
	}
	if err := store.Close(); err != nil {
		log.Error("closing store", "err", err)
		os.Exit(1)
	}
}
