package queue

import (
	"errors"
	"testing"
	"time"
)

// fakeClock is advanced by hand so lease expiry and backoff are deterministic.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestStore(t *testing.T, dir string) (*Store, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s, err := Open(Config{Dir: dir, Sync: SyncNever, Clock: clk, Rand: func() float64 { return 1 }, BackoffBase: time.Second, BackoffMax: 8 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, clk
}

func mustSubmit(t *testing.T, s *Store, r SubmitReq) Job {
	t.Helper()
	j, _, err := s.Submit(r)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestPriorityThenFIFO(t *testing.T) {
	s, _ := newTestStore(t, "")
	a := mustSubmit(t, s, SubmitReq{Command: "a"})
	b := mustSubmit(t, s, SubmitReq{Command: "b", Priority: 5})
	c := mustSubmit(t, s, SubmitReq{Command: "c"})
	d := mustSubmit(t, s, SubmitReq{Command: "d", Priority: 5})
	var got []string
	for i := 0; i < 4; i++ {
		j, err := s.Lease("w", time.Minute)
		if err != nil || j == nil {
			t.Fatalf("lease %d: %v %v", i, j, err)
		}
		got = append(got, j.ID)
	}
	want := []string{b.ID, d.ID, a.ID, c.ID}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order %v, want %v", got, want)
		}
	}
	if j, _ := s.Lease("w", time.Minute); j != nil {
		t.Fatalf("expected empty queue, got %v", j.ID)
	}
}

func TestIdempotencyKey(t *testing.T) {
	s, _ := newTestStore(t, "")
	j1, created1, _ := s.Submit(SubmitReq{Command: "x", Key: "build-42"})
	j2, created2, _ := s.Submit(SubmitReq{Command: "x", Key: "build-42"})
	if !created1 || created2 || j1.ID != j2.ID {
		t.Fatalf("key reuse: %v %v %s %s", created1, created2, j1.ID, j2.ID)
	}
	if n := len(s.List("", 0)); n != 1 {
		t.Fatalf("want 1 job, have %d", n)
	}
}

func TestValidation(t *testing.T) {
	s, _ := newTestStore(t, "")
	for _, r := range []SubmitReq{{}, {Command: " "}, {Command: "x", MaxRetries: -1}, {Command: "x", MaxRetries: 51}, {Command: "x", TimeoutSec: -1}} {
		if _, _, err := s.Submit(r); !errors.Is(err, ErrInvalid) {
			t.Errorf("%+v: want ErrInvalid, got %v", r, err)
		}
	}
}

func TestSuccessPath(t *testing.T) {
	s, _ := newTestStore(t, "")
	j := mustSubmit(t, s, SubmitReq{Command: "true"})
	l, _ := s.Lease("w1", time.Minute)
	if l.State != Running || l.Attempt != 1 || l.LeaseToken == "" {
		t.Fatalf("leased job %+v", l)
	}
	done, err := s.Complete(j.ID, l.LeaseToken, Result{ExitCode: 0, Output: "hi", DurationMS: 5})
	if err != nil || done.State != Succeeded || done.Output != "hi" {
		t.Fatalf("complete: %+v %v", done, err)
	}
}

// Retry with backoff: attempt n fails -> queued again, not runnable until the backoff has elapsed; after MaxRetries
// retries the job is dead. Rand returns 1, so the delay is exactly base*2^(n-1) (capped at 8 s).
func TestRetryBackoffAndDeadLetter(t *testing.T) {
	s, clk := newTestStore(t, "")
	j := mustSubmit(t, s, SubmitReq{Command: "false", MaxRetries: 2})
	wantDelay := []time.Duration{time.Second, 2 * time.Second}
	for attempt := 1; attempt <= 3; attempt++ {
		l, _ := s.Lease("w", time.Minute)
		if l == nil || l.Attempt != attempt {
			t.Fatalf("attempt %d: leased %+v", attempt, l)
		}
		out, err := s.Complete(j.ID, l.LeaseToken, Result{ExitCode: 3})
		if err != nil {
			t.Fatal(err)
		}
		if attempt == 3 {
			if out.State != Dead {
				t.Fatalf("after %d attempts want dead, got %s", attempt, out.State)
			}
			break
		}
		if out.State != Queued {
			t.Fatalf("want queued after attempt %d, got %s", attempt, out.State)
		}
		if x, _ := s.Lease("w", time.Minute); x != nil {
			t.Fatalf("job runnable before its backoff elapsed")
		}
		clk.Advance(wantDelay[attempt-1] - time.Millisecond)
		if x, _ := s.Lease("w", time.Minute); x != nil {
			t.Fatalf("job runnable 1 ms early")
		}
		clk.Advance(time.Millisecond)
	}
	st := s.Stats()
	if st.Dead != 1 || st.Retries != 2 {
		t.Fatalf("stats %+v", st)
	}
}

func TestBackoffCap(t *testing.T) {
	s, _ := newTestStore(t, "")
	if d := s.backoff(10); d != 8*time.Second {
		t.Fatalf("backoff(10) = %v, want the 8 s cap", d)
	}
	s.cfg.Rand = func() float64 { return 0 }
	if d := s.backoff(3); d != 2*time.Second { // 4 s base delay, half fixed + 0 random
		t.Fatalf("backoff(3) with zero jitter = %v", d)
	}
}

func TestTimeoutCountsAsFailure(t *testing.T) {
	s, _ := newTestStore(t, "")
	j := mustSubmit(t, s, SubmitReq{Command: "sleep", MaxRetries: 0})
	l, _ := s.Lease("w", time.Minute)
	out, _ := s.Complete(j.ID, l.LeaseToken, Result{ExitCode: -1, TimedOut: true})
	if out.State != Dead || out.LastError != "timeout" {
		t.Fatalf("%+v", out)
	}
}

// The core safety property: a worker whose lease expired must not be able to record a result.
func TestStaleLeaseRejected(t *testing.T) {
	s, clk := newTestStore(t, "")
	j := mustSubmit(t, s, SubmitReq{Command: "x", MaxRetries: 3})
	old, _ := s.Lease("slow", 10*time.Second)
	clk.Advance(11 * time.Second)
	if n := s.ExpireLeases(); n != 1 {
		t.Fatalf("expired %d leases", n)
	}
	clk.Advance(5 * time.Second) // past the 1 s backoff
	fresh, _ := s.Lease("fast", time.Minute)
	if fresh == nil || fresh.Attempt != 2 || fresh.LeaseToken == old.LeaseToken {
		t.Fatalf("re-lease: %+v", fresh)
	}
	if _, err := s.Complete(j.ID, old.LeaseToken, Result{ExitCode: 0}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale completion: want ErrStaleLease, got %v", err)
	}
	if err := s.Heartbeat(j.ID, old.LeaseToken, time.Minute); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale heartbeat: want ErrStaleLease, got %v", err)
	}
	if got, _ := s.Get(j.ID); got.State != Running || got.Worker != "fast" {
		t.Fatalf("stale report changed the job: %+v", got)
	}
	if _, err := s.Complete(j.ID, fresh.LeaseToken, Result{ExitCode: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Complete(j.ID, fresh.LeaseToken, Result{ExitCode: 0}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("double completion must be rejected, got %v", err)
	}
	if st := s.Stats(); st.Succeeded != 1 || st.StaleReports < 3 || st.LeaseExpirations != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	s, clk := newTestStore(t, "")
	j := mustSubmit(t, s, SubmitReq{Command: "x"})
	l, _ := s.Lease("w", 10*time.Second)
	for i := 0; i < 5; i++ {
		clk.Advance(8 * time.Second)
		if err := s.Heartbeat(j.ID, l.LeaseToken, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		if n := s.ExpireLeases(); n != 0 {
			t.Fatalf("lease expired despite heartbeats")
		}
	}
	clk.Advance(11 * time.Second)
	if n := s.ExpireLeases(); n != 1 {
		t.Fatalf("lease should expire once heartbeats stop, expired %d", n)
	}
}

func TestOutputTruncated(t *testing.T) {
	s, _ := newTestStore(t, "")
	s.cfg.MaxOutput = 10
	j := mustSubmit(t, s, SubmitReq{Command: "x"})
	l, _ := s.Lease("w", time.Minute)
	out, _ := s.Complete(j.ID, l.LeaseToken, Result{Output: "0123456789ABCDEFGHIJ"})
	if out.Output != "...[truncated]...ABCDEFGHIJ" {
		t.Fatalf("output %q", out.Output)
	}
}

func TestNotFound(t *testing.T) {
	s, _ := newTestStore(t, "")
	if _, err := s.Complete("j999", "t", Result{}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.Heartbeat("j999", "t", time.Second); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
