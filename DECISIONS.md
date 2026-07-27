# Design & Architectural Decisions — QueueCTL

This file documents my technical choices, concurrency handling, and trade-offs made while building `queuectl`.

---

## 1. Atomic Job Claiming Across Processes

### Code Reference
The job claiming logic lives in [`queuectl_engine/db.py:L103-L125`](file:///home/vaish/Flam/queuectl_engine/db.py#L103-L125) inside `Database.claim_job()`:

```python
conn.execute("BEGIN IMMEDIATE")

row = conn.execute("""
    SELECT id FROM jobs
    WHERE state = 'pending'
       OR (state = 'failed' AND (run_at IS NULL OR run_at <= ?))
       OR (state = 'processing' AND (last_heartbeat IS NULL OR last_heartbeat <= ?))
    ORDER BY created_at ASC LIMIT 1
""", (now_str, stale_thresh)).fetchone()

if not row:
    conn.execute("COMMIT")
    return None

job_id = row["id"]
conn.execute("""
    UPDATE jobs
    SET state = 'processing', worker_id = ?, last_heartbeat = ?, updated_at = ?
    WHERE id = ?
""", (worker_id, now_str, now_str, job_id))
conn.execute("COMMIT")
```

### How it guarantees single-worker claims
I used SQLite in WAL mode (`PRAGMA journal_mode=WAL;`) with a 10-second busy timeout (`PRAGMA busy_timeout=10000;`).

When multiple worker processes run `claim_job()` at the exact same time from different terminals:
1. `BEGIN IMMEDIATE` grabs a reserved write lock on the database before doing any reads or updates.
2. Only one worker process gets the lock first. Any other process attempting to claim at the same instant is queued by SQLite and waits up to 10 seconds.
3. The first worker selects the next available job, updates its state to `processing`, records its `worker_id` and timestamp, and calls `COMMIT`.
4. Once the commit releases the write lock, the second worker's query runs — but since the candidate job is now marked `processing`, it selects the *next* pending job (or returns `None` if the queue is empty).

This prevents two workers from ever grabbing or executing the same job.

---

## 2. SIGKILL Crash Recovery & Delay Calculation

### Walkthrough of a Crash
Say worker `worker-1` picks up a job (`job-101`) and starts executing it.

1. **Normal execution**: `job-101` state becomes `processing`, assigned to `worker-1` with a `last_heartbeat` timestamp set when claimed (and updated during long loops in [`queuectl_engine/worker.py:L60`](file:///home/vaish/Flam/queuectl_engine/worker.py#L60)).
2. **SIGKILL occurs**: `worker-1` is killed ungracefully (`kill -9`). Because `SIGKILL` cannot be caught, no cleanup code runs. The process dies immediately, leaving `job-101` stuck in SQLite with state `processing`.
3. **Detection**: The next time any worker runs `claim_job()`, the query checks for jobs stuck in `processing` where `last_heartbeat` is older than `stale_timeout` (default 30 seconds):
   ```sql
   OR (state = 'processing' AND (last_heartbeat IS NULL OR last_heartbeat <= :stale_threshold))
   ```
4. **Recovery**: The healthy worker picks up `job-101`, updates `worker_id` to itself, resets `last_heartbeat`, and re-runs the command.

### Worst-Case Recovery Delay
- **Stale window**: 30 seconds (default `stale-timeout`, adjustable via `queuectl config set stale-timeout <seconds>`).
- **Worker poll loop**: 0.5 seconds sleep when idle.
- **Worst-case delay**: **30.5 seconds** (30s stale window + 0.5s max poll loop delay). This guarantees recovery well under the 60-second rule.

---

## 3. DLQ Retry Attempt Counter Policy

### My Decision
When running `queuectl dlq retry <job_id>`, I reset `attempts` back to `0` and set the state back to `pending`.

### Why I Chose This
When a job ends up in the Dead Letter Queue (`state = 'dead'`), it means all automated retries have failed. Moving it out of DLQ is an explicit manual action by a developer or operator after fixing whatever caused the failure (e.g. fixing a bad API endpoint, database connection, or script bug).

Once the underlying issue is fixed, the job should get a fresh retry lifecycle with the full set of configured retries (`max_retries`) and exponential backoff delays ($base^1, base^2, ...$). If we kept `attempts = max_retries`, a single transient glitch after retrying would throw the job right back into the DLQ without giving exponential backoff a chance to work.

---

## 4. Cross-Process Worker Stop Trade-Offs

### What I Rejected
- **PID File only (`/tmp/queuectl.pid`)**: Stale PID files break easily if workers crash uncleanly or run inside separate containers/namespaces.
- **IPC Sockets / HTTP Server**: Setting up a local socket server (e.g., `/tmp/queuectl.sock`) adds unnecessary socket file cleanup edge cases and permission issues.

### What I Implemented
A hybrid database status approach ([`queuectl_engine/worker.py:L105-L116`](file:///home/vaish/Flam/queuectl_engine/worker.py#L105-L116)):

1. Running `queuectl worker stop` sets `status = 'stopping'` in the shared `workers` table in SQLite.
2. It queries active worker PIDs from the DB and sends a `SIGTERM` signal to each process.
3. Workers catch `SIGTERM` or check `status == 'stopping'` in their main loop.
4. If a worker is mid-job, it finishes executing its current in-flight job before unregistering from the database and exiting cleanly.

---

## 5. Adding Job Priorities (Extensibility Analysis)

If we needed to add job priorities (e.g., `high`, `medium`, `low`) in the future:

### What Remains Unchanged
- **Database Concurrency & Locking**: The SQLite `BEGIN IMMEDIATE` transaction locking mechanism in `claim_job()` stays 100% identical.
- **Worker Execution & Signals**: Command execution (`subprocess.run`), signal traps, and worker stop routines don't care about job priority.
- **Exponential Retries & DLQ**: Backoff math and DLQ state transitions stay the same.

### What Would Change
1. **Database Schema**: Add a `priority INTEGER DEFAULT 0` column to the `jobs` table (where higher values mean higher priority).
2. **Claim SQL Query**: Update the `ORDER BY` clause in `claim_job()` from:
   ```sql
   ORDER BY created_at ASC
   ```
   to:
   ```sql
   ORDER BY priority DESC, created_at ASC
   ```
3. **CLI Parser**: Add a `--priority` option to `queuectl enqueue` (e.g. `queuectl enqueue --priority 10 '{"id":"job1",...}'`).
