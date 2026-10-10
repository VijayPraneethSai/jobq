package worker_test

// End-to-end tests: a real coordinator (HTTP handler over a real store), real workers and real child processes.
// They check the guarantees the README claims: retries, dead-lettering, lease expiry after a worker dies, rejection
// of stale completions, timeouts that kill grandchildren, and recovery after a coordinator restart.

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/VijayPraneethSai/jobq/internal/client"
	"github.com/VijayPraneethSai/jobq/internal/queue"
	"github.com/VijayPraneethSai/jobq/internal/server"
	"github.com/VijayPraneethSai/jobq/internal/worker"
)

type env struct {
	t      *testing.T
	store  *queue.Store
	ts     *httptest.Server
	cli    *client.Client
	cancel context.CancelFunc
}

func newEnv(t *testing.T, dir, token string) *env {
	t.Helper()
	st, err := queue.Open(queue.Config{Dir: dir, Sync: queue.SyncNever, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(st, server.Config{AuthToken: token})
	ctx, cancel := context.WithCancel(context.Background())
	go srv.RunReaper(ctx, 50*time.Millisecond)
	ts := httptest.NewServer(srv.Handler())
	e := &env{t: t, store: st, ts: ts, cli: client.New(ts.URL, token), cancel: cancel}
	t.Cleanup(e.close)
	return e
}

func (e *env) close() {
	e.cancel()
	e.ts.Close()
	_ = e.store.Close() // second Close is harmless
}

func (e *env) startWorker(id string, conc int, grace time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx, worker.Config{Client: e.cli, ID: id, Concurrency: conc, LeaseTTL: time.Second,
			PollInterval: 10 * time.Millisecond, Grace: grace})
	}()
	return func() { cancel(); <-done }
}

func (e *env) waitState(id string, want queue.State, within time.Duration) queue.Job {
	e.t.Helper()
	deadline := time.Now().Add(within)
	for {
		j, err := e.cli.Get(context.Background(), id)
		if err != nil {
			e.t.Fatal(err)
		}
		if j.State == want {
			return j
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("job %s: state %s after %v, want %s (attempt %d, last error %q)", id, j.State, within, want, j.Attempt, j.LastError)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func submit(t *testing.T, c *client.Client, r client.SubmitReq) queue.Job {
	t.Helper()
	j, _, err := c.Submit(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestManyJobsAllSucceedOnce(t *testing.T) {
	e := newEnv(t, "", "")
	stop := e.startWorker("w1", 4, time.Second)
	defer stop()
	const n = 60
	ids := make([]string, n)
	for i := range ids {
		ids[i] = submit(t, e.cli, client.SubmitReq{Command: "echo", Args: []string{strconv.Itoa(i)}}).ID
	}
	for i, id := range ids {
		j := e.waitState(id, queue.Succeeded, 10*time.Second)
		if strings.TrimSpace(j.Output) != strconv.Itoa(i) || j.Attempt != 1 {
			t.Fatalf("job %s: output %q attempt %d", id, j.Output, j.Attempt)
		}
	}
}

func TestFailingJobRetriesThenDies(t *testing.T) {
	e := newEnv(t, "", "")
	defer e.startWorker("w1", 1, time.Second)()
	j := submit(t, e.cli, client.SubmitReq{Command: "sh", Args: []string{"-c", "echo boom; exit 3"}, MaxRetries: 2})
	got := e.waitState(j.ID, queue.Dead, 10*time.Second)
	if got.Attempt != 3 || got.ExitCode != 3 || !strings.Contains(got.Output, "boom") {
		t.Fatalf("dead job: attempt %d exit %d output %q", got.Attempt, got.ExitCode, got.Output)
	}
	if st := e.store.Stats(); st.Retries != 2 || st.Dead != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestIdempotencyKey(t *testing.T) {
	e := newEnv(t, "", "")
	a, c1, _ := e.cli.Submit(context.Background(), client.SubmitReq{Key: "k", Command: "true"})
	b, c2, _ := e.cli.Submit(context.Background(), client.SubmitReq{Key: "k", Command: "true"})
	if !c1 || c2 || a.ID != b.ID {
		t.Fatalf("created %v/%v ids %s/%s", c1, c2, a.ID, b.ID)
	}
}

func TestAuth(t *testing.T) {
	e := newEnv(t, "", "s3cret")
	resp, err := http.Get(e.ts.URL + "/jobs")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	resp, _ = http.Get(e.ts.URL + "/healthz")
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("healthz must stay open: %d", resp.StatusCode)
	}
	if _, err := e.cli.List(context.Background(), "", 1); err != nil {
		t.Fatalf("with token: %v", err)
	}
	bad := client.New(e.ts.URL, "wrong")
	if _, err := bad.List(context.Background(), "", 1); err == nil {
		t.Fatal("wrong token accepted")
	}
}

func TestStaleCompletionRejected(t *testing.T) {
	e := newEnv(t, "", "")
	ctx := context.Background()
	j := submit(t, e.cli, client.SubmitReq{Command: "true", MaxRetries: 3})
	a, err := e.cli.Lease(ctx, "slow", time.Second)
	if err != nil || a == nil {
		t.Fatal(err)
	}
	time.Sleep(1200 * time.Millisecond) // lease (1 s) runs out, the reaper requeues the job
	var b *queue.Job
	for i := 0; i < 100 && b == nil; i++ {
		if b, err = e.cli.Lease(ctx, "fast", time.Minute); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if b == nil || b.ID != j.ID || b.LeaseToken == a.LeaseToken {
		t.Fatalf("second lease %+v", b)
	}
	// the presumed-dead worker wakes up and reports success with its old token
	if _, err := e.cli.Complete(ctx, j.ID, client.Report{Token: a.LeaseToken, ExitCode: 0, Output: "stale"}); !errors.Is(err, client.ErrLeaseLost) {
		t.Fatalf("stale completion: %v", err)
	}
	if err := e.cli.Heartbeat(ctx, j.ID, a.LeaseToken, time.Second); !errors.Is(err, client.ErrLeaseLost) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	done, err := e.cli.Complete(ctx, j.ID, client.Report{Token: b.LeaseToken, ExitCode: 0, Output: "fresh"})
	if err != nil || done.State != queue.Succeeded || done.Output != "fresh" || done.Attempt != 2 {
		t.Fatalf("fresh completion: %+v %v", done, err)
	}
	if st := e.store.Stats(); st.StaleReports < 2 || st.LeaseExpirations != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestWorkerDiesMidJobAndJobIsRetried(t *testing.T) {
	e := newEnv(t, "", "")
	dir := t.TempDir()
	marker := filepath.Join(dir, "runs")
	// Appends a line per start, then takes a while. Killed on the first attempt, completes on the second.
	script := fmt.Sprintf("echo start >> %s; if [ $(wc -l < %s) -ge 2 ]; then exit 0; fi; sleep 30", marker, marker)
	j := submit(t, e.cli, client.SubmitReq{Command: "sh", Args: []string{"-c", script}, MaxRetries: 2})

	stopA := e.startWorker("A", 1, 50*time.Millisecond)
	e.waitState(j.ID, queue.Running, 5*time.Second)
	time.Sleep(200 * time.Millisecond) // let the script start
	stopA()                            // "crash": grace is 50 ms, then the child is killed and nothing is reported

	if got, _ := e.cli.Get(context.Background(), j.ID); got.State != queue.Running {
		t.Fatalf("right after the crash the coordinator should still believe it is running, got %s", got.State)
	}
	defer e.startWorker("B", 1, time.Second)()
	got := e.waitState(j.ID, queue.Succeeded, 10*time.Second)
	if got.Attempt != 2 || got.Worker != "B" {
		t.Fatalf("attempt %d worker %s", got.Attempt, got.Worker)
	}
	b, _ := os.ReadFile(marker)
	if n := strings.Count(string(b), "start"); n != 2 {
		t.Fatalf("command started %d times, want 2 (at-least-once)", n)
	}
	if st := e.store.Stats(); st.Succeeded != 1 || st.LeaseExpirations != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func TestTimeoutKillsGrandchildren(t *testing.T) {
	e := newEnv(t, "", "")
	defer e.startWorker("w", 1, time.Second)()
	pidfile := filepath.Join(t.TempDir(), "pid")
	j := submit(t, e.cli, client.SubmitReq{Command: "sh", Args: []string{"-c", fmt.Sprintf("sleep 60 & echo $! > %s; wait", pidfile)}, TimeoutSec: 1})
	got := e.waitState(j.ID, queue.Dead, 10*time.Second)
	if got.Attempt != 1 {
		t.Fatalf("attempt %d", got.Attempt)
	}
	raw, err := os.ReadFile(pidfile)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	deadline := time.Now().Add(3 * time.Second)
	for alive(pid) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if alive(pid) {
		syscall.Kill(pid, syscall.SIGKILL)
		t.Fatalf("grandchild %d survived the timeout", pid)
	}
}

func TestAllowlist(t *testing.T) {
	e := newEnv(t, "", "")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx, worker.Config{Client: e.cli, ID: "w", PollInterval: 10 * time.Millisecond, AllowedCommands: []string{"echo"}})
	}()
	defer func() { cancel(); <-done }()
	ok := submit(t, e.cli, client.SubmitReq{Command: "echo", Args: []string{"hi"}})
	bad := submit(t, e.cli, client.SubmitReq{Command: "rm", Args: []string{"-rf", "/nonexistent"}})
	e.waitState(ok.ID, queue.Succeeded, 5*time.Second)
	got := e.waitState(bad.ID, queue.Dead, 5*time.Second)
	if got.ExitCode != 126 {
		t.Fatalf("exit %d output %q", got.ExitCode, got.Output)
	}
}

func TestCoordinatorRestartLosesNothing(t *testing.T) {
	dir := t.TempDir()
	out := t.TempDir()
	const n = 40
	e1 := newEnv(t, dir, "")
	ids := make([]string, n)
	for i := range ids {
		// each run appends its id to a per-job file, so duplicates would be visible
		f := filepath.Join(out, fmt.Sprintf("%d", i))
		ids[i] = submit(t, e1.cli, client.SubmitReq{Key: fmt.Sprintf("k%d", i), Command: "sh", Args: []string{"-c", "echo x >> " + f}, MaxRetries: 3}).ID
	}
	stop := e1.startWorker("w1", 2, time.Second)
	// let roughly half of the jobs finish, then "crash" the coordinator
	for e1.store.Stats().Succeeded < n/2 {
		time.Sleep(5 * time.Millisecond)
	}
	stop()
	e1.close()

	e2 := newEnv(t, dir, "")
	if e2.store.Recovery.Events == 0 {
		t.Fatalf("nothing replayed: %+v", e2.store.Recovery)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); defer e2.startWorker("w2", 2, time.Second)(); waitAll(t, e2, ids) }()
	wg.Wait()

	// resubmitting with the old keys must return the existing jobs, not create new ones
	for i := range ids {
		j, created, err := e2.cli.Submit(context.Background(), client.SubmitReq{Key: fmt.Sprintf("k%d", i), Command: "true"})
		if err != nil || created || j.ID != ids[i] {
			t.Fatalf("resubmit %d: created=%v id=%s err=%v", i, created, j.ID, err)
		}
	}
	st := e2.store.Stats()
	if st.Succeeded != n || st.Dead != 0 || st.Queued+st.Running+st.Delayed != 0 {
		t.Fatalf("stats after restart %+v", st)
	}
	// A job that was Running at the crash may legitimately run twice (at-least-once); nothing may run zero times.
	for i := range ids {
		b, err := os.ReadFile(filepath.Join(out, fmt.Sprint(i)))
		if err != nil || len(b) == 0 {
			t.Fatalf("job %d never ran: %v", i, err)
		}
		if c := strings.Count(string(b), "x"); c > 2 {
			t.Fatalf("job %d ran %d times", i, c)
		}
	}
}

func waitAll(t *testing.T, e *env, ids []string) {
	t.Helper()
	for _, id := range ids {
		e.waitState(id, queue.Succeeded, 20*time.Second)
	}
}
