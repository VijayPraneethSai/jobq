# jobq

A small job queue in Go: a coordinator with an HTTP API, workers that run shell commands as child processes, and a
write-ahead log so the queue survives restarts. It is at-least-once, single-node, and meant to be read and run, not
to replace Sidekiq or SQS. Design notes and trade-offs are in [docs/DESIGN.md](docs/DESIGN.md).

* Leases with heartbeats: a worker that dies loses its job after the lease expires and another worker picks it up.
* Completions are accepted only from the current lease holder, so a slow worker cannot overwrite a retry.
* Retries with exponential backoff and jitter, then a dead-letter state.
* Idempotency keys on submit, priorities, per-attempt timeouts (the whole process group is killed).
* Event-sourced state: the live path and crash recovery go through the same `apply()`; snapshot + log compaction.
* `/metrics` in Prometheus text format (written by hand, no client library), `/healthz`, `/readyz`, structured logs.
* Standard library only. No third-party dependencies.

## Run it

```
make build
bin/coordinator -addr 127.0.0.1:8080 -data ./jobq-data &
bin/worker -coordinator http://127.0.0.1:8080 -concurrency 2 &

curl -XPOST localhost:8080/jobs -d '{"key":"hello-1","command":"echo","args":["hello"],"max_retries":2}'
bin/jobqctl submit -key hello-1 -- echo hello      # same key: returns the existing job
bin/jobqctl list
bin/jobqctl wait j1
curl localhost:8080/metrics
```

Set `JOBQ_TOKEN` (or `-token`) on the coordinator, workers and CLI to require a bearer token. Run workers with
`-allow echo,make` on shared machines; without it a worker executes whatever it is given.

API: `POST /jobs`, `GET /jobs/{id}`, `GET /jobs?state=&limit=`, `POST /lease`, `POST /jobs/{id}/heartbeat`,
`POST /jobs/{id}/complete`.

## Tests

```
make test    # unit + end-to-end
make race    # go test -race -count=3
make lint    # gofmt, go vet, golangci-lint
```

What the tests exercise (real child processes and a real HTTP handler where relevant):

| Scenario | Test |
|---|---|
| WAL replay reproduces the live state, over random workloads with a fake clock | `TestReplayReproducesState` |
| Torn last line is dropped, garbage stops replay | `TestTruncatedTailIsDropped`, `TestGarbageLineStopsReplay` |
| Crash between snapshot rename and WAL truncate | `TestCrashBetweenSnapshotAndTruncate` |
| Worker killed mid-job: job re-run on another worker, one recorded success | `TestWorkerDiesMidJobAndJobIsRetried` |
| Stale completion / heartbeat gets 409 | `TestStaleCompletionRejected` |
| Timeout kills grandchildren | `TestTimeoutKillsGrandchildren` |
| Coordinator restart: no job lost, resubmitted keys return the old jobs | `TestCoordinatorRestartLosesNothing` |
| Retry, backoff cap, dead-letter | `TestRetryBackoffAndDeadLetter`, `TestFailingJobRetriesThenDies` |

I broke the lease-token check on purpose to confirm these catch it (three tests failed). Statement coverage from
`go test -cover`: queue 84%, worker 86%, metrics 93%; `cmd/` and `internal/server` have no tests of their own (the
server is exercised through the end-to-end tests).

Ran here: `go vet`, `gofmt`, `golangci-lint` (0 issues, config in `.golangci.yml`), `go test -race -count=8` on the
end-to-end package. Not run here: the GitHub Actions workflow and the Dockerfile (no Actions runner and no Docker
daemon where this was built), so treat both as untested.

## Load numbers

`cmd/loadgen` starts a coordinator and workers in one process, submits N jobs from 4 clients as fast as it can, and
waits for all of them. The jobs are `/bin/true`, so this mostly measures fork/exec plus the queue's bookkeeping, not
useful work. Machine: 2 vCPU shared container, linux/amd64, Go 1.24.7, disk type unknown; the load generator,
coordinator and workers compete for the same two cores. Raw output: `results/loadgen-2026-10-11.txt`.

`bin/loadgen -n 2000 -workers W -sync S`, one run each, no repeats, so treat small differences as noise:

| workers | WAL fsync | submit rate (jobs/s) | end-to-end (jobs/s) | latency p50 / p99 (ms) |
|---:|---|---:|---:|---:|
| 2 | batch (50 ms) | 4796 | 758 | 1148 / 2187 |
| 2 | always | 1116 | 378 | 2269 / 3472 |
| 8 | batch | 903 | 618 | 633 / 1006 |
| 8 | always | 583 | 513 | 363 / 488 |
| 32 | batch | 518 | 516 | 11 / 40 |
| 32 | always | 331 | 330 | 17 / 33 |

How to read it: all 2000 jobs are submitted up front, so with few workers the latency is mostly time spent waiting in
the queue; with 32 workers the queue never builds up and latency is a few milliseconds. `sync=always` costs roughly a
third to a half of the throughput on this disk. I would not quote the end-to-end figures as "the queue's throughput":
they are limited by process creation on two shared cores, and I did not profile the store separately.

## Limitations

* One coordinator, no replication or failover.
* WAL records are not checksummed.
* The whole job table is in memory and finished jobs are never evicted.
* At-least-once delivery: commands must be safe to run twice.
* Auth is a single shared token over plain HTTP.
* No cancellation of running jobs, no dependencies between jobs, no time-based scheduling.
