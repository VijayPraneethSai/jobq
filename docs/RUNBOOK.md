# Runbook

One section per alert in `deploy/prometheus/alerts.yml`. Commands assume the Docker Compose setup
(`docker compose up --build --scale worker=3`); for a bare-metal install replace `docker compose exec` / `logs` with
the equivalent. Dashboard: Grafana "jobq overview" at http://localhost:3000.

First look, for any alert:

```
docker compose ps
docker compose logs --tail=100 coordinator
curl -s localhost:8080/metrics | grep -E '^jobq_jobs|lease_exp|retries|wal_bytes'
```

## Coordinator down or restarting

Alert: `JobqCoordinatorDown`. Workers cannot lease or report; they back off and retry, and running jobs keep running.
Their leases may expire while the coordinator is away, in which case those jobs are retried (at-least-once).

1. `docker compose ps coordinator` and `docker compose logs coordinator`. Look for a panic or a WAL error at startup.
2. If the container is restarting in a loop, check that the data volume is mounted and writable by uid 65532.
3. On start the coordinator replays `snapshot.jsonl` and then `wal.jsonl`. A torn last line (crash mid-write) is
   dropped automatically. Replay stops at the first line that does not parse, so a corrupted line in the middle of the
   log means everything after it is ignored: copy the volume aside **before** touching anything.
4. When it is back, confirm `jobq_jobs{state="running"}` recovers and `jobq_lease_expirations_total` stops climbing.

## Lease expiries rising

Alert: `JobqHighLeaseExpiryRate`. Leases expire when a worker stops heartbeating (every TTL/3, default 10 s).
Each expiry counts as a failed attempt, so this also drives retries and eventually dead jobs.

1. `docker compose ps worker` and `docker compose logs worker`. Look for OOM kills, restarts, "heartbeat rejected".
2. Check `docker stats`: workers starved of CPU cannot heartbeat. Lower `-concurrency` or add workers.
3. If jobs legitimately run longer than the lease and the worker is healthy, raise `-lease-ttl` on coordinator and workers.
4. A worker that was presumed dead and wakes up gets 409 and abandons the job. `jobq_stale_reports_total` rising
   alongside is the lease check working, not a bug.

## Jobs in the dead-letter state

Alert: `JobqDeadLetterGrowing`. Retries are exhausted (`max_retries + 1` attempts).

1. `docker compose exec coordinator /usr/local/bin/jobqctl list -state dead` (or `curl 'localhost:8080/jobs?state=dead'`).
2. Read `exit_code`, `last_error` and `output` on a job: a command that always fails, a command not in the worker's
   `-allow` list (exit 126, "is not allowed on this worker"), or a timeout.
3. Fix the cause, then resubmit with a **new** idempotency key; resubmitting the old key returns the dead job.
4. There is no "requeue" command and no eviction of finished jobs, so dead jobs stay until the data is reset.

## Backlog not draining

Alert: `JobqBacklogNotDraining`. `queued > 0` with `running == 0` for 2 minutes means nothing is pulling work.
Jobs waiting out a retry backoff are counted as `delayed`, not `queued`, so this is not a backoff artefact.

1. `docker compose ps worker`: are any running? `docker compose up -d --scale worker=3`.
2. Workers up but idle: check their logs for 401 (token mismatch with the coordinator) or connection errors
   (wrong `-coordinator` URL).
3. If the workers have an `-allow` list, jobs for other commands are leased and fail with exit 126 rather than sit; check the dead state.

## HTTP 5xx errors

Alert: `JobqServerErrors`. More than 1% of requests failed server-side for 5 minutes.

1. `docker compose logs coordinator | grep -i error`: every request is logged with an `id` that matches the `X-Request-Id` response header.
2. Disk full or read-only volume on the WAL is the usual cause (appends fail). `df -h` on the host; free space or
   grow the volume, then check that writes succeed again (submit a test job). If they do not, restart the coordinator.
3. If it started with a deploy, roll back the image tag.

## Latency SLO breach

Alert: `JobqLatencyP99High`. p99 of submit-to-success is above 60 s for 10 minutes.

1. Dashboard: is `queued` growing? Then it is capacity: scale workers, or reduce submit rate.
2. `queued` flat but latency high: jobs are slow themselves or retrying. Compare with `jobq_retries_total`.
3. Check `-sync always` on the coordinator: it roughly halves throughput on slow disks (README load table).

## WAL keeps growing

Alert: `JobqWALLarge`. Compaction triggers at `-compact-bytes` (default 64 MiB), so over 128 MiB means it is failing
or the threshold was changed.

1. Look for snapshot errors in the coordinator log (disk space, permissions).
2. A large WAL only costs restart time; it is safe to leave until the disk problem is fixed.
3. Compaction is attempted from the coordinator's periodic loop (log lines "compaction failed" / "wal compacted"), so the failure reason is in the log.

## Worker crash (drill)

Not an alert; useful to rehearse. With the stack up and a long job running:

```
docker compose exec coordinator /usr/local/bin/jobqctl submit -key drill-1 -- sh -c 'sleep 20; echo done'
docker compose kill -s KILL worker      # simulate a crash
```

Expected: the lease expires after the TTL (30 s by default), `jobq_lease_expirations_total` increments, the job is
re-queued and finishes on another worker once one is running. `docker compose up -d --scale worker=3` brings workers back.
