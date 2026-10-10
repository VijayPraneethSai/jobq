package queue

import (
	"container/heap"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrNotFound   = errors.New("job not found")
	ErrStaleLease = errors.New("stale lease: the job is not running under this token")
	ErrInvalid    = errors.New("invalid request")
)

// Config for Open. Zero values are usable (in-memory, real clock, 1 s .. 30 s backoff).
type Config struct {
	Dir         string // directory for the WAL and snapshot; "" = in memory only
	Sync        SyncPolicy
	Clock       Clock
	Rand        func() float64 // [0,1) for backoff jitter; tests inject a fixed value
	BackoffBase time.Duration
	BackoffMax  time.Duration
	MaxOutput   int // bytes of output kept per attempt (default 64 KiB)
}

// Store is the whole queue state behind one mutex. One lock keeps WAL order identical to apply order, which makes
// replay exact; the price is that throughput is bounded by one core, which is plenty for tens of thousands of jobs/s
// of small state changes (see the loadgen numbers in the README) but is not a horizontally scalable design.
type Store struct {
	cfg Config

	mu      sync.Mutex
	jobs    map[string]*Job
	byKey   map[string]string
	ready   readyHeap
	delayed delayedHeap
	running map[string]*Job
	jobSeq  uint64 // last job number handed out
	evSeq   uint64 // last WAL sequence number
	wal     *wal
	closed  bool

	// counters (monotonic since process start; not persisted)
	retries, leaseExpired, succeeded, dead, stale uint64
	Recovery                                      replayResult
}

// Stats is a point-in-time copy for /metrics.
type Stats struct {
	Queued, Delayed, Running, Succeeded, Dead int
	Retries, LeaseExpirations, StaleReports   uint64
	WALBytes                                  int64
}

// Open creates the store and, if cfg.Dir is set, recovers it from the snapshot and WAL found there.
func Open(cfg Config) (*Store, error) {
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Rand == nil {
		cfg.Rand = func() float64 {
			var b [8]byte
			_, _ = rand.Read(b[:])
			return float64(uint64(b[0])<<8|uint64(b[1])) / 65536.0
		}
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 30 * time.Second
	}
	if cfg.MaxOutput <= 0 {
		cfg.MaxOutput = 64 << 10
	}
	s := &Store{cfg: cfg, jobs: map[string]*Job{}, byKey: map[string]string{}, running: map[string]*Job{}}
	if cfg.Dir == "" {
		return s, nil
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	hdr, snap, err := readSnapshot(cfg.Dir)
	if err != nil {
		return nil, fmt.Errorf("recover: %w", err)
	}
	for _, j := range snap {
		s.load(j)
	}
	s.jobSeq, s.evSeq = hdr.JobSeq, hdr.LastSeq
	s.Recovery.SnapshotJobs = len(snap)
	n, trunc, err := readEvents(cfg.Dir, func(e *Event) {
		if e.Seq <= hdr.LastSeq { // already inside the snapshot (crash between snapshot rename and WAL truncate)
			s.Recovery.Skipped++
			return
		}
		s.apply(e)
		if e.Seq > s.evSeq {
			s.evSeq = e.Seq
		}
	})
	if err != nil {
		return nil, fmt.Errorf("recover: %w", err)
	}
	s.Recovery.Events, s.Recovery.TruncatedBytes = n, trunc
	if s.wal, err = openWAL(cfg.Dir, cfg.Sync); err != nil {
		return nil, err
	}
	return s, nil
}

// Close flushes and closes the WAL.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.wal != nil {
		return s.wal.close()
	}
	return nil
}

// ---- apply: the single place where state changes ----

// load inserts a job read from a snapshot, placing it according to its state.
func (s *Store) load(j *Job) {
	j.heapIdx = -1
	j.seq = jobNum(j.ID)
	s.jobs[j.ID] = j
	if j.Key != "" {
		s.byKey[j.Key] = j.ID
	}
	switch j.State {
	case Queued:
		s.place(j)
	case Running:
		s.running[j.ID] = j
	}
	if j.seq > s.jobSeq {
		s.jobSeq = j.seq
	}
}

func jobNum(id string) uint64 {
	n, _ := strconv.ParseUint(strings.TrimPrefix(id, "j"), 10, 64)
	return n
}

// place puts a queued job into the ready or delayed heap by its NextRun.
func (s *Store) place(j *Job) {
	if j.NextRun > 0 {
		j.inReady = false
		heap.Push(&s.delayed, j)
		return
	}
	j.inReady = true
	heap.Push(&s.ready, j)
}

func (s *Store) unplace(j *Job) {
	if j.heapIdx < 0 {
		return
	}
	if j.inReady {
		heap.Remove(&s.ready, j.heapIdx)
	} else {
		heap.Remove(&s.delayed, j.heapIdx)
	}
}

// apply performs one state transition. It is used both for live changes and for WAL replay, and it is idempotent
// where a duplicate can occur (a snapshot written but the WAL not yet truncated when the process died).
func (s *Store) apply(e *Event) {
	switch e.Type {
	case EvSubmit:
		if _, dup := s.jobs[e.Job.ID]; dup {
			return
		}
		j := *e.Job
		j.State, j.heapIdx = Queued, -1
		j.seq = jobNum(j.ID)
		s.jobs[j.ID] = &j
		if j.Key != "" {
			s.byKey[j.Key] = j.ID
		}
		if j.seq > s.jobSeq {
			s.jobSeq = j.seq
		}
		s.place(&j)
	case EvLease:
		j := s.jobs[e.ID]
		if j == nil || j.State != Queued || e.Attempt <= j.Attempt {
			return
		}
		s.unplace(j)
		j.State, j.Attempt, j.Worker, j.LeaseToken, j.LeaseExpiry, j.NextRun = Running, e.Attempt, e.Worker, e.Token, e.Expiry, 0
		s.running[j.ID] = j
	case EvHeartbeat:
		if j := s.jobs[e.ID]; j != nil && j.State == Running && j.LeaseToken == e.Token {
			j.LeaseExpiry = e.Expiry
		}
	case EvSucceed:
		j := s.jobs[e.ID]
		if j == nil || j.State != Running || j.Attempt != e.Attempt {
			return
		}
		delete(s.running, j.ID)
		j.State, j.LeaseToken, j.LeaseExpiry = Succeeded, "", 0
		j.ExitCode, j.Output, j.DurationMS, j.LastError, j.FinishedAt = e.ExitCode, e.Output, e.DurationMS, "", e.At
		s.succeeded++
	case EvFail:
		j := s.jobs[e.ID]
		if j == nil || j.State != Running || j.Attempt != e.Attempt {
			return
		}
		delete(s.running, j.ID)
		j.LeaseToken, j.LeaseExpiry = "", 0
		j.ExitCode, j.Output, j.DurationMS, j.LastError = e.ExitCode, e.Output, e.DurationMS, e.Reason
		if e.NewState == Dead {
			j.State, j.FinishedAt = Dead, e.At
			s.dead++
			return
		}
		j.State, j.NextRun = Queued, e.NextRun
		s.retries++
		s.place(j)
	}
}

// commit assigns the WAL sequence number, applies the event and appends it. If the append fails the in-memory state
// has already changed; the error is returned so the caller can report a 5xx, and the operator sees the log error.
// (A stricter design would write first and apply after; here apply is cheap to reason about because it cannot fail.)
func (s *Store) commit(e *Event) error {
	s.evSeq++
	e.Seq = s.evSeq
	s.apply(e)
	if s.wal != nil {
		return s.wal.append(e)
	}
	return nil
}

func (s *Store) now() int64 { return s.cfg.Clock.Now().UnixNano() }

// promote moves delayed jobs whose backoff has elapsed into the ready heap. It is derived from the clock, not an
// event: after a restart the delayed jobs are rebuilt from their persisted NextRun and promoted the same way.
func (s *Store) promote(now int64) {
	for len(s.delayed) > 0 && s.delayed[0].NextRun <= now {
		j := heap.Pop(&s.delayed).(*Job)
		j.inReady = true
		heap.Push(&s.ready, j)
	}
}

func (s *Store) backoff(attempt int) time.Duration {
	d := float64(s.cfg.BackoffBase) * math.Pow(2, float64(attempt-1))
	if d > float64(s.cfg.BackoffMax) || math.IsInf(d, 0) {
		d = float64(s.cfg.BackoffMax)
	}
	// "equal jitter": half fixed, half random, so retries of many jobs that failed together spread out
	return time.Duration(d/2 + s.cfg.Rand()*d/2)
}

// ---- public operations ----

// SubmitReq is the input of Submit.
type SubmitReq struct {
	Key        string
	Command    string
	Args       []string
	TimeoutSec int
	MaxRetries int
	Priority   int
}

// Submit queues a job. With a Key that was seen before, the existing job is returned and created is false.
func (s *Store) Submit(r SubmitReq) (job Job, created bool, err error) {
	if strings.TrimSpace(r.Command) == "" || r.TimeoutSec < 0 || r.MaxRetries < 0 || r.MaxRetries > 50 || len(r.Key) > 200 {
		return Job{}, false, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Job{}, false, errors.New("store closed")
	}
	if r.Key != "" {
		if id, ok := s.byKey[r.Key]; ok {
			return *s.jobs[id], false, nil
		}
	}
	id := "j" + strconv.FormatUint(s.jobSeq+1, 10)
	j := &Job{ID: id, Key: r.Key, Command: r.Command, Args: r.Args, TimeoutSec: r.TimeoutSec, MaxRetries: r.MaxRetries,
		Priority: r.Priority, SubmittedAt: s.now()}
	if err := s.commit(&Event{Type: EvSubmit, At: j.SubmittedAt, ID: id, Job: j}); err != nil {
		return *s.jobs[id], true, err
	}
	return *s.jobs[id], true, nil
}

func newToken() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Lease hands the next runnable job to worker for ttl, or returns nil if nothing is runnable now.
func (s *Store) Lease(worker string, ttl time.Duration) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.promote(now)
	if len(s.ready) == 0 {
		return nil, nil
	}
	j := s.ready[0]
	e := &Event{Type: EvLease, At: now, ID: j.ID, Worker: worker, Token: newToken(), Attempt: j.Attempt + 1, Expiry: now + int64(ttl)}
	if err := s.commit(e); err != nil {
		return nil, err
	}
	out := *j
	return &out, nil
}

// Heartbeat extends the lease of a running job. ErrStaleLease tells the worker it has lost the job and must stop.
func (s *Store) Heartbeat(id, token string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil {
		return ErrNotFound
	}
	if j.State != Running || j.LeaseToken != token {
		s.stale++
		return ErrStaleLease
	}
	now := s.now()
	return s.commit(&Event{Type: EvHeartbeat, At: now, ID: id, Token: token, Expiry: now + int64(ttl)})
}

// Result is what a worker reports for one attempt.
type Result struct {
	ExitCode   int
	Output     string
	DurationMS int64
	TimedOut   bool
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "...[truncated]..." + s[len(s)-n:]
}

// Complete records the outcome of an attempt. Only the holder of the current lease token is accepted: a worker that
// was presumed dead (lease expired, job re-leased) gets ErrStaleLease and its result is discarded, so a job is
// never recorded as finished by an attempt that is no longer the current one.
func (s *Store) Complete(id, token string, r Result) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil {
		return Job{}, ErrNotFound
	}
	if j.State != Running || j.LeaseToken != token {
		s.stale++
		return *j, ErrStaleLease
	}
	out := tail(r.Output, s.cfg.MaxOutput)
	now := s.now()
	var err error
	switch {
	case !r.TimedOut && r.ExitCode == 0:
		err = s.commit(&Event{Type: EvSucceed, At: now, ID: id, Attempt: j.Attempt, ExitCode: 0, Output: out, DurationMS: r.DurationMS})
	default:
		reason := fmt.Sprintf("exit code %d", r.ExitCode)
		if r.TimedOut {
			reason = "timeout"
		}
		err = s.fail(j, reason, r.ExitCode, out, r.DurationMS, now)
	}
	return *j, err
}

// fail ends the current attempt: retry with backoff while attempts remain, else move to dead.
func (s *Store) fail(j *Job, reason string, exit int, out string, durMS int64, now int64) error {
	e := &Event{Type: EvFail, At: now, ID: j.ID, Attempt: j.Attempt, ExitCode: exit, Output: out, DurationMS: durMS, Reason: reason}
	if j.Attempt <= j.MaxRetries {
		e.NewState, e.NextRun = Queued, now+int64(s.backoff(j.Attempt))
	} else {
		e.NewState = Dead
	}
	return s.commit(e)
}

// ExpireLeases fails every running attempt whose lease ran out (the worker died, hung or lost the network). Called
// by the coordinator's reaper loop and after recovery. Returns how many leases expired.
func (s *Store) ExpireLeases() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var expired []*Job
	for _, j := range s.running {
		if j.LeaseExpiry < now {
			expired = append(expired, j)
		}
	}
	sort.Slice(expired, func(a, b int) bool { return expired[a].seq < expired[b].seq }) // deterministic order for the WAL
	for _, j := range expired {
		s.leaseExpired++
		_ = s.fail(j, "lease expired", -1, j.Output, 0, now)
	}
	return len(expired)
}

// Get returns a copy of one job.
func (s *Store) Get(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobs[id]
	if j == nil {
		return Job{}, false
	}
	return *j, true
}

// List returns up to limit jobs in submit order, optionally filtered by state.
func (s *Store) List(state State, limit int) []Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Job
	for _, j := range s.jobs {
		if state == "" || j.State == state {
			out = append(out, *j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].seq < out[b].seq })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Stats returns counters and per-state gauges.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promote(s.now()) // so Queued/Delayed reflect the clock, not the last time somebody leased
	st := Stats{Retries: s.retries, LeaseExpirations: s.leaseExpired, StaleReports: s.stale, Running: len(s.running)}
	for _, j := range s.jobs {
		switch j.State {
		case Succeeded:
			st.Succeeded++
		case Dead:
			st.Dead++
		}
	}
	// Queued = runnable now; Delayed = waiting out a retry backoff (moved to ready on the next Lease once due).
	st.Queued, st.Delayed = len(s.ready), len(s.delayed)
	if s.wal != nil {
		st.WALBytes = s.wal.size()
	}
	return st
}

// Compact writes a snapshot of all jobs and empties the WAL. Records are numbered, so a crash between the two steps
// is safe: recovery skips WAL records the snapshot already covers.
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	jobs := make([]*Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(a, b int) bool { return jobs[a].seq < jobs[b].seq })
	if err := writeSnapshot(s.cfg.Dir, snapshotHeader{LastSeq: s.evSeq, JobSeq: s.jobSeq}, jobs); err != nil {
		return err
	}
	return s.wal.reset()
}
