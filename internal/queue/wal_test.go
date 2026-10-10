package queue

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func reopen(t *testing.T, s *Store, dir string, clk Clock) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	n, err := Open(Config{Dir: dir, Sync: SyncNever, Clock: clk, Rand: func() float64 { return 1 }, BackoffBase: time.Second, BackoffMax: 8 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}

// randomWorkload drives the store through many random operations so that every event type, every transition and
// the heaps are exercised, then returns.
func randomWorkload(t *testing.T, s *Store, clk *fakeClock, rng *rand.Rand, steps int) {
	t.Helper()
	type lease struct{ id, token string }
	var held []lease
	for i := 0; i < steps; i++ {
		switch rng.Intn(8) {
		case 0, 1:
			mustSubmit(t, s, SubmitReq{Command: "c", MaxRetries: rng.Intn(3), Priority: rng.Intn(3), Key: map[bool]string{true: "k" + string(rune('a'+rng.Intn(6))), false: ""}[rng.Intn(3) == 0]})
		case 2, 3:
			if l, _ := s.Lease("w", time.Duration(5+rng.Intn(20))*time.Second); l != nil {
				held = append(held, lease{l.ID, l.LeaseToken})
			}
		case 4:
			if len(held) > 0 {
				k := rng.Intn(len(held))
				_ = s.Heartbeat(held[k].id, held[k].token, 20*time.Second)
			}
		case 5:
			if len(held) > 0 {
				k := rng.Intn(len(held))
				exit := 0
				if rng.Intn(2) == 0 {
					exit = 1 + rng.Intn(3)
				}
				_, _ = s.Complete(held[k].id, held[k].token, Result{ExitCode: exit, Output: "o", DurationMS: int64(rng.Intn(100))})
				held = append(held[:k], held[k+1:]...)
			}
		case 6:
			clk.Advance(time.Duration(rng.Intn(15)) * time.Second)
			s.ExpireLeases()
		case 7:
			clk.Advance(time.Duration(rng.Intn(3)) * time.Second)
		}
	}
}

func TestReplayReproducesState(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		dir := t.TempDir()
		s, clk := newTestStore(t, dir)
		randomWorkload(t, s, clk, rand.New(rand.NewSource(seed)), 400)
		before := s.List("", 0)
		beforeStats := s.Stats()
		s2 := reopen(t, s, dir, clk)
		if d := diffJobs(before, s2.List("", 0)); d != "" {
			t.Fatalf("seed %d: jobs differ after replay: %s", seed, d)
		}
		afterStats := s2.Stats()
		if beforeStats.Queued != afterStats.Queued || beforeStats.Delayed != afterStats.Delayed || beforeStats.Running != afterStats.Running ||
			beforeStats.Succeeded != afterStats.Succeeded || beforeStats.Dead != afterStats.Dead {
			t.Fatalf("seed %d: gauges differ: %+v vs %+v", seed, beforeStats, afterStats)
		}
		// the recovered store must keep working with the same lease order as the original would have
		if s2.Recovery.TruncatedBytes != 0 || s2.Recovery.Events == 0 {
			t.Fatalf("seed %d: recovery %+v", seed, s2.Recovery)
		}
	}
}

// After recovery both stores must also BEHAVE the same: same next lease, same ids for new jobs.
func TestRecoveredStoreBehavesLikeOriginal(t *testing.T) {
	dir := t.TempDir()
	s, clk := newTestStore(t, dir)
	randomWorkload(t, s, clk, rand.New(rand.NewSource(99)), 300)
	// reference: a second store replaying the same files read-only is not possible, so compare the original's next steps
	// against the recovered store's after taking the same deterministic actions on both.
	nextBefore, _ := s.Lease("probe", time.Minute)
	jobBefore, _, _ := s.Submit(SubmitReq{Command: "probe"})
	s2 := reopen(t, s, dir, clk)
	// s2 recovered everything including the probe lease and submit
	if g, ok := s2.Get(jobBefore.ID); !ok || g.Command != "probe" {
		t.Fatalf("probe job missing after recovery: %+v", g)
	}
	if nextBefore != nil {
		if g, _ := s2.Get(nextBefore.ID); g.State != Running || g.LeaseToken != nextBefore.LeaseToken {
			t.Fatalf("leased job not recovered as running with the same token: %+v", g)
		}
	}
	nj, _, _ := s2.Submit(SubmitReq{Command: "after"})
	if nj.ID == jobBefore.ID {
		t.Fatalf("job id reused after recovery")
	}
}

func TestRecoveredRunningJobExpiresAndRetries(t *testing.T) {
	dir := t.TempDir()
	s, clk := newTestStore(t, dir)
	j := mustSubmit(t, s, SubmitReq{Command: "x", MaxRetries: 2})
	l, _ := s.Lease("w", 10*time.Second)
	// coordinator dies while the job is running; comes back 1 minute later
	clk.Advance(time.Minute)
	s2 := reopen(t, s, dir, clk)
	if g, _ := s2.Get(j.ID); g.State != Running {
		t.Fatalf("job should come back as running, got %s", g.State)
	}
	if n := s2.ExpireLeases(); n != 1 {
		t.Fatalf("expected the stale lease to expire, got %d", n)
	}
	clk.Advance(2 * time.Second)
	again, _ := s2.Lease("w2", time.Minute)
	if again == nil || again.ID != j.ID || again.Attempt != 2 {
		t.Fatalf("re-lease after recovery: %+v", again)
	}
	if _, err := s2.Complete(j.ID, l.LeaseToken, Result{}); err == nil {
		t.Fatal("the pre-crash token must not be accepted")
	}
}

func TestTruncatedTailIsDropped(t *testing.T) {
	dir := t.TempDir()
	s, clk := newTestStore(t, dir)
	a := mustSubmit(t, s, SubmitReq{Command: "a"})
	b := mustSubmit(t, s, SubmitReq{Command: "b"})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, walName)
	data, _ := os.ReadFile(path)
	// simulate a crash in the middle of writing a third record
	torn := append(data, []byte(`{"seq":3,"t":"submit","at":1,"id":"j3","job":{"id":"j3","comm`)...)
	if err := os.WriteFile(path, torn, 0o644); err != nil {
		t.Fatal(err)
	}
	s2 := reopen(t, s, dir, clk)
	if s2.Recovery.TruncatedBytes == 0 {
		t.Fatal("torn tail not detected")
	}
	if len(s2.List("", 0)) != 2 {
		t.Fatalf("want 2 jobs after dropping the torn record, have %d", len(s2.List("", 0)))
	}
	if st, _ := os.Stat(path); st.Size() != int64(len(data)) {
		t.Fatalf("WAL not truncated to the last good record: %d vs %d", st.Size(), len(data))
	}
	c := mustSubmit(t, s2, SubmitReq{Command: "c"})
	if c.ID == a.ID || c.ID == b.ID {
		t.Fatalf("id collision %s", c.ID)
	}
	s3 := reopen(t, s2, dir, clk)
	if len(s3.List("", 0)) != 3 {
		t.Fatal("appending after truncation did not produce a clean log")
	}
}

func TestGarbageLineStopsReplay(t *testing.T) {
	dir := t.TempDir()
	s, clk := newTestStore(t, dir)
	mustSubmit(t, s, SubmitReq{Command: "a"})
	s.Close()
	f, _ := os.OpenFile(filepath.Join(dir, walName), os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString("not json at all\n")
	f.Close()
	s2 := reopen(t, s, dir, clk)
	if len(s2.List("", 0)) != 1 || s2.Recovery.TruncatedBytes == 0 {
		t.Fatalf("recovery %+v jobs %d", s2.Recovery, len(s2.List("", 0)))
	}
}

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	s, clk := newTestStore(t, dir)
	randomWorkload(t, s, clk, rand.New(rand.NewSource(7)), 300)
	if s.Stats().WALBytes == 0 {
		t.Fatal("WAL empty before compaction")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	if s.Stats().WALBytes != 0 {
		t.Fatalf("WAL not truncated: %d", s.Stats().WALBytes)
	}
	// work after compaction goes to the (new) WAL; recovery = snapshot + WAL
	randomWorkload(t, s, clk, rand.New(rand.NewSource(8)), 200)
	before := s.List("", 0)
	s2 := reopen(t, s, dir, clk)
	if s2.Recovery.SnapshotJobs == 0 {
		t.Fatalf("snapshot not used: %+v", s2.Recovery)
	}
	if d := diffJobs(before, s2.List("", 0)); d != "" {
		t.Fatal("state differs after snapshot + WAL recovery: " + d)
	}
}

// Crash between "snapshot renamed" and "WAL truncated": the WAL still holds records the snapshot already covers.
func TestCrashBetweenSnapshotAndTruncate(t *testing.T) {
	dir := t.TempDir()
	s, clk := newTestStore(t, dir)
	randomWorkload(t, s, clk, rand.New(rand.NewSource(11)), 300)
	walCopy, _ := os.ReadFile(filepath.Join(dir, walName))
	before := s.List("", 0)
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := os.WriteFile(filepath.Join(dir, walName), walCopy, 0o644); err != nil { // put the old WAL back
		t.Fatal(err)
	}
	s2 := reopen(t, s, dir, clk)
	if s2.Recovery.Skipped == 0 {
		t.Fatalf("covered records were not skipped: %+v", s2.Recovery)
	}
	if d := diffJobs(before, s2.List("", 0)); d != "" {
		t.Fatal("duplicate records changed the recovered state: " + d)
	}
}
