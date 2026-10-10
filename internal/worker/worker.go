// Package worker pulls jobs from the coordinator, runs them as child processes and reports the outcome.
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/client"
	"github.com/VijayPraneethSai/jobq/internal/queue"
)

// Config of a worker process.
type Config struct {
	Client          *client.Client
	ID              string
	Concurrency     int           // jobs run in parallel (default 1)
	LeaseTTL        time.Duration // requested lease length; heartbeats go out every third of it
	PollInterval    time.Duration // sleep between lease attempts when the queue is empty
	DefaultTimeout  time.Duration // per-attempt timeout for jobs that do not set one
	MaxOutput       int           // bytes of output kept (the tail)
	Grace           time.Duration // on shutdown, how long running jobs may finish before they are killed
	AllowedCommands []string      // if non-empty, only these exact command strings may run (the coordinator is not trusted blindly)
	Logger          *slog.Logger
}

func (c *Config) defaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 1
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 30 * time.Second
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 10 * time.Minute
	}
	if c.MaxOutput <= 0 {
		c.MaxOutput = 64 << 10
	}
	if c.Grace <= 0 {
		c.Grace = 20 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Run starts Concurrency loops and blocks until ctx is cancelled and the running jobs have finished or been killed.
// Cancelling ctx means "stop taking new jobs": jobs already running get Grace to finish and report; after that they
// are killed WITHOUT reporting, so their leases expire and the coordinator retries them (at-least-once delivery).
func Run(ctx context.Context, cfg Config) {
	cfg.defaults()
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loop(ctx, &cfg)
		}()
	}
	wg.Wait()
}

func loop(ctx context.Context, cfg *Config) {
	idle := cfg.PollInterval
	for ctx.Err() == nil {
		j, err := cfg.Client.Lease(ctx, cfg.ID, cfg.LeaseTTL)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			cfg.Logger.Warn("lease failed", "err", err)
			sleep(ctx, 2*time.Second) // coordinator down or unreachable: back off
			continue
		}
		if j == nil {
			sleep(ctx, idle)
			continue
		}
		runOne(ctx, cfg, j)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// runOne executes one leased job: heartbeat in the background, run the process, report the result.
func runOne(shutdown context.Context, cfg *Config, j *queue.Job) {
	log := cfg.Logger.With("job", j.ID, "attempt", j.Attempt)
	// base is deliberately NOT derived from `shutdown`: a graceful stop lets the job finish. It is cancelled when the
	// lease is lost (the coordinator gave the job to someone else) or when the grace period after shutdown ran out.
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	var abandoned bool
	var mu sync.Mutex
	abandon := func(why string) {
		mu.Lock()
		abandoned = true
		mu.Unlock()
		log.Warn("abandoning job", "why", why)
		cancelBase()
	}

	hbDone := make(chan struct{})
	go func() { // heartbeats + shutdown watcher
		defer close(hbDone)
		t := time.NewTicker(cfg.LeaseTTL / 3)
		defer t.Stop()
		var graceTimer <-chan time.Time
		sd := shutdown.Done()
		for {
			select {
			case <-base.Done():
				return
			case <-sd:
				graceTimer = time.After(cfg.Grace)
				sd = nil
			case <-graceTimer:
				abandon("shutdown grace period elapsed")
				return
			case <-t.C:
				hctx, cancel := context.WithTimeout(base, 5*time.Second)
				err := cfg.Client.Heartbeat(hctx, j.ID, j.LeaseToken, cfg.LeaseTTL)
				cancel()
				if errors.Is(err, client.ErrLeaseLost) || errors.Is(err, client.ErrNotFound) {
					abandon("lease lost")
					return
				}
				if err != nil && base.Err() == nil {
					log.Warn("heartbeat failed", "err", err) // transient: the next beat may succeed before the lease expires
				}
			}
		}
	}()

	res := execute(base, cfg, j)
	cancelBase() // stop the heartbeat goroutine
	<-hbDone

	mu.Lock()
	gone := abandoned
	mu.Unlock()
	if gone {
		return // no report: the job belongs to another attempt now, or will be re-leased after expiry
	}
	rep := client.Report{Token: j.LeaseToken, ExitCode: res.exit, Output: res.output, DurationMS: res.dur.Milliseconds(), TimedOut: res.timedOut}
	for try := 0; try < 4; try++ {
		rctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := cfg.Client.Complete(rctx, j.ID, rep)
		cancel()
		if err == nil {
			log.Info("job reported", "exit", res.exit, "timed_out", res.timedOut, "dur_ms", res.dur.Milliseconds())
			return
		}
		if errors.Is(err, client.ErrLeaseLost) || errors.Is(err, client.ErrNotFound) {
			log.Warn("result rejected: lease no longer current")
			return
		}
		log.Warn("report failed, retrying", "err", err)
		time.Sleep(time.Duration(200<<try) * time.Millisecond)
	}
	log.Error("giving up reporting; the lease will expire and the job will be retried")
}

type result struct {
	exit     int
	output   string
	dur      time.Duration
	timedOut bool
}

func allowed(cfg *Config, cmd string) bool {
	if len(cfg.AllowedCommands) == 0 {
		return true
	}
	for _, a := range cfg.AllowedCommands {
		if a == cmd {
			return true
		}
	}
	return false
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	max int
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > 2*t.max { // trim lazily to avoid copying on every write
		t.buf = append([]byte(nil), t.buf[len(t.buf)-t.max:]...)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	b := t.buf
	if len(b) > t.max {
		b = b[len(b)-t.max:]
	}
	return string(bytes.ToValidUTF8(b, nil))
}

// execute runs the command in its own process group and enforces the timeout by killing the whole group, so children
// the command spawned do not outlive it.
func execute(base context.Context, cfg *Config, j *queue.Job) result {
	start := time.Now()
	if !allowed(cfg, j.Command) {
		return result{exit: 126, output: fmt.Sprintf("command %q is not allowed on this worker", j.Command), dur: time.Since(start)}
	}
	timeout := time.Duration(j.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = cfg.DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(base, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, j.Command, j.Args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) } // negative pid = the whole group
	cmd.WaitDelay = 2 * time.Second                                                      // do not hang on pipes held by stray grandchildren
	out := &tailBuffer{max: cfg.MaxOutput}
	cmd.Stdout, cmd.Stderr = out, out
	err := cmd.Run()
	res := result{output: out.String(), dur: time.Since(start)}
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		res.timedOut, res.exit = true, -1
	case err == nil:
		res.exit = 0
	default:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			res.exit = ee.ExitCode() // -1 if killed by a signal
		} else {
			res.exit = 127 // could not start (not found, not executable)
			res.output = err.Error()
		}
	}
	return res
}
