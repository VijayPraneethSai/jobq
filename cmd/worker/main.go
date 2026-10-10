// Command worker pulls jobs from a coordinator and runs them.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/client"
	"github.com/VijayPraneethSai/jobq/internal/worker"
)

func main() {
	coord := flag.String("coordinator", "http://127.0.0.1:8080", "coordinator base URL")
	token := flag.String("token", os.Getenv("JOBQ_TOKEN"), "bearer token (default $JOBQ_TOKEN)")
	host, _ := os.Hostname()
	id := flag.String("id", fmt.Sprintf("%s-%d", host, os.Getpid()), "worker id")
	conc := flag.Int("concurrency", 2, "jobs run in parallel")
	ttl := flag.Duration("lease-ttl", 30*time.Second, "lease length; heartbeats every third of it")
	poll := flag.Duration("poll", 200*time.Millisecond, "sleep between polls when the queue is empty")
	timeout := flag.Duration("default-timeout", 10*time.Minute, "per-attempt timeout for jobs without one")
	grace := flag.Duration("grace", 20*time.Second, "on SIGTERM, how long running jobs may finish")
	allow := flag.String("allow", "", "comma-separated list of commands this worker may run (empty = any; set this on shared hosts)")
	flag.Parse()

	var allowed []string
	if *allow != "" {
		allowed = strings.Split(*allow, ",")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("worker started", "id", *id, "coordinator", *coord, "concurrency", *conc)
	worker.Run(ctx, worker.Config{Client: client.New(*coord, *token), ID: *id, Concurrency: *conc, LeaseTTL: *ttl, PollInterval: *poll,
		DefaultTimeout: *timeout, Grace: *grace, AllowedCommands: allowed, Logger: log})
	log.Info("worker stopped")
}
