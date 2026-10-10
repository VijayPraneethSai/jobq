# Design notes

## State machine

```
            lease                 exit 0 + current token
  queued ───────────▶ running ─────────────────────────────▶ succeeded
    ▲                    │
    │  retries left      │ non-zero exit / timeout / lease expired
    └────────────────────┤
                         │ retries exhausted
                         └──────────────────────────────────▶ dead
```

A retry puts the job back to `queued` with `next_run = now + backoff(attempt)`; until then it sits in a second
heap and is not handed out. `max_retries` counts retries, so `max_retries: 2` allows three attempts. An expired lease
counts as a failed attempt (otherwise a job that crashes its worker every time would loop forever).

## Everything is an event

Each change is an `Event` (submit, lease, heartbeat, succeed, fail). The live path creates the event, passes it to
`apply()`, then appends it to the WAL. Startup replays the log through the same `apply()`. There is no second code
path that reconstructs state, which is the usual source of "works until you restart it" bugs.
`TestReplayReproducesState` runs random workloads (including a fake clock so backoff and expiry are
exercised) and compare the live store with a store rebuilt from the log.

Two decisions inside `apply`:

* Time never comes from the clock inside `apply`; the event carries it. That is what makes replay deterministic.
* Tokens, job ids and the backoff jitter are drawn *before* the event is built and stored in it, for the same reason.

## Delivery semantics

At-least-once. If a worker dies after doing the work but before reporting, the lease expires and another worker runs
the job again. Commands should therefore be idempotent. Exactly-once is not claimed; what is guaranteed is that the
*recorded* outcome of a job comes from exactly one lease holder: `Complete` and `Heartbeat` require the token of the
current lease. A slow worker that was presumed dead and wakes up later gets `409 Conflict` and its output is
discarded (`TestStaleCompletionRejected`).

## Durability

`wal.jsonl` is append-only JSON lines. Policies: `always` (fsync before acknowledging), `batch` (default; a background
fsync every 50 ms, so a *power* failure can lose up to ~50 ms of acknowledged changes, a process crash loses
nothing), `never`.

Compaction writes `snapshot.jsonl` (temp file, fsync, rename) and then truncates the WAL. Every record has a sequence
number, so a crash between the rename and the truncate is harmless: replay skips records the snapshot covers
(`TestCrashBetweenSnapshotAndTruncate`).

A torn last line (crash mid-write) is detected on startup and truncated. Records are **not checksummed**: a flipped
bit in the middle of the log that still parses as JSON would be replayed. Adding a CRC per record is the obvious next
step.

## Concurrency

One mutex around the whole store. It keeps log order identical to apply order, which is what makes replay exact, and
it is simple to reason about. The price is that state changes are serialised on one core and the log is written under
the lock. The load numbers in the README are bounded by process creation for the jobs themselves, not by the store,
but this is not a design that scales horizontally.

## Workers

* One loop per concurrency slot: lease → run → report. Heartbeats go out every TTL/3 in a separate goroutine.
* Each command runs in its own process group; a timeout kills the group (`kill(-pgid)`), so children and
  grandchildren die with it (`TestTimeoutKillsGrandchildren`).
* If a heartbeat gets 409/404 the worker abandons the job (kills it, reports nothing).
* SIGTERM stops leasing new jobs and gives running ones `-grace` to finish; after that they are killed unreported and
  will be retried once their lease expires.
* `-allow` restricts which commands a worker will run. Without it the worker runs whatever the coordinator hands it,
  which means anyone who can submit jobs can run code on every worker.

## Not done / known gaps

* Single coordinator: no replication, no failover. If it is down, workers back off and retry.
* No WAL checksums (above).
* Job index is in memory; the whole data set must fit in RAM. Terminal jobs are never evicted.
* Auth is one shared bearer token over plain HTTP. Put TLS in front of it if it leaves localhost.
* No job dependencies, cron, per-queue concurrency limits or cancellation of running jobs.
