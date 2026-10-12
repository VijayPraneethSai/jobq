# Service level objectives

These are targets for a single-node coordinator, chosen so that the alerts in `deploy/prometheus/alerts.yml` have
something concrete to defend. They have not been measured over a real 30-day window; the numbers come from the load
runs in the README and from what the design can promise.

| # | SLI (what is measured) | Objective | Window | Metric |
|---|---|---|---|---|
| 1 | Share of coordinator HTTP requests that do not return 5xx | 99.9% | 30 days | `jobq_http_requests_total{class}` |
| 2 | Share of jobs that succeed within 60 s of submit, while the queue is not saturated | 99% | 30 days | `jobq_job_latency_seconds` |
| 3 | Acknowledged jobs that are lost after a worker crash or a coordinator restart | 0 | always | covered by tests, not a metric |

## Notes on each objective

**1. Availability.** 99.9% over 30 days is about 43 minutes of budget. Because there is one coordinator with no
failover, a restart counts against it. A restart replays the WAL (see the runbook); replay time grows with log size and has not been benchmarked here. 4xx responses are not errors here: a 409 from a stale worker is the lease check doing its job.

**2. Latency.** The histogram only observes jobs that succeed, so a job that dies after retries never shows up in it.
Watch the dead-letter alert alongside it. Time spent waiting out a retry backoff counts toward latency, and the
histogram does not separate first-attempt jobs from retried ones. The target is also workload-dependent: it assumes jobs shorter than a few seconds and enough workers that the queue does not build
(the README load table shows queue wait dominating latency when there are too few workers).

**3. Durability.** The guarantee is at-least-once with a single recorded outcome per job (see `docs/DESIGN.md`).
With `-sync batch` a power failure can lose up to about 50 ms of acknowledged changes; a process crash loses nothing.
Use `-sync always` if that window is not acceptable. This objective is enforced by tests
(`TestWorkerDiesMidJobAndJobIsRetried`, `TestCoordinatorRestartLosesNothing`) rather than by a runtime metric.

## Error budget policy

* Budget for objective 1 is 0.1% of requests over 30 days. If more than half is spent in the first week, stop
  feature work and fix reliability until the burn rate is back under 1x.
* A single incident that spends more than 25% of the budget gets a short written post-mortem.

## Alerts that defend these objectives

| Alert | Defends | Severity |
|---|---|---|
| `JobqCoordinatorDown` | 1 | page |
| `JobqServerErrors` | 1 | page |
| `JobqBacklogNotDraining` | 2 | page |
| `JobqLatencyP99High` | 2 | ticket |
| `JobqHighLeaseExpiryRate`, `JobqDeadLetterGrowing` | 2, early warning | ticket |
| `JobqWALLarge` | 1, 3 (a huge WAL means slow restarts) | ticket |
