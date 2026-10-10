// Package queue holds the job state machine and its write-ahead log.
//
// Design in one paragraph: every state change is an Event. The live code builds an event, applies it to the
// in-memory state with apply(), and appends it to the WAL. Recovery replays the same events through the same
// apply(), so a restarted coordinator reaches exactly the state it had. Delivery is at-least-once: a job whose
// worker dies is re-run, so commands should be idempotent. A completion is only accepted from the holder of the
// current lease (token), which is what stops a slow, presumed-dead worker from overwriting a retry's result.
package queue

import "time"

// State of a job. A job moves queued -> running -> (succeeded | queued again after a failed attempt | dead).
type State string

const (
	Queued    State = "queued"    // waiting for a worker (possibly delayed by retry backoff)
	Running   State = "running"   // leased to a worker
	Succeeded State = "succeeded" // exit code 0 reported by the current lease holder
	Dead      State = "dead"      // retries exhausted; kept for inspection (dead-letter)
)

// Job is the unit of work: run Command with Args on a worker.
type Job struct {
	ID         string   `json:"id"`
	Key        string   `json:"key,omitempty"` // idempotency key supplied by the submitter
	Command    string   `json:"command"`
	Args       []string `json:"args,omitempty"`
	TimeoutSec int      `json:"timeout_sec"` // per attempt; 0 = worker default
	MaxRetries int      `json:"max_retries"` // retries after the first attempt (max attempts = MaxRetries+1)
	Priority   int      `json:"priority"`    // higher runs first; FIFO inside one priority

	State       State  `json:"state"`
	Attempt     int    `json:"attempt"` // number of attempts started so far
	Worker      string `json:"worker,omitempty"`
	LeaseToken  string `json:"lease_token,omitempty"`
	LeaseExpiry int64  `json:"lease_expiry,omitempty"` // unix nanoseconds
	NextRun     int64  `json:"next_run,omitempty"`     // unix nanoseconds, earliest start (retry backoff)

	SubmittedAt int64  `json:"submitted_at"`
	FinishedAt  int64  `json:"finished_at,omitempty"`
	ExitCode    int    `json:"exit_code"`
	Output      string `json:"output,omitempty"` // tail of stdout+stderr of the last attempt, capped by the worker
	LastError   string `json:"last_error,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"` // of the last attempt

	seq     uint64 // submit order, FIFO tie-break
	heapIdx int    // position in whichever heap holds the job, -1 if none
	inReady bool   // true: in the ready heap, false: in the delayed heap (only meaningful when heapIdx >= 0)
}

// Clock lets tests control time.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// EventType names a WAL record.
type EventType string

const (
	EvSubmit    EventType = "submit"
	EvLease     EventType = "lease"
	EvHeartbeat EventType = "heartbeat"
	EvSucceed   EventType = "succeed"
	EvFail      EventType = "fail" // one attempt failed: retry (state queued) or give up (state dead)
)

// Event is one WAL record. Seq is the position in the log; replay skips records already covered by a snapshot.
type Event struct {
	Seq  uint64    `json:"seq"`
	Type EventType `json:"t"`
	At   int64     `json:"at"` // unix nanoseconds
	ID   string    `json:"id,omitempty"`

	Job *Job `json:"job,omitempty"` // submit

	Worker  string `json:"worker,omitempty"` // lease
	Token   string `json:"token,omitempty"`  // lease
	Attempt int    `json:"attempt,omitempty"`
	Expiry  int64  `json:"expiry,omitempty"` // lease, heartbeat

	ExitCode   int    `json:"exit,omitempty"` // succeed, fail
	Output     string `json:"out,omitempty"`
	DurationMS int64  `json:"dur_ms,omitempty"`
	Reason     string `json:"reason,omitempty"` // fail
	NewState   State  `json:"state,omitempty"`  // fail: queued or dead
	NextRun    int64  `json:"next_run,omitempty"`
}
