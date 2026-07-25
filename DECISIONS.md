# Architecture & Design Decisions - QueueCTL

This document details the critical architectural decisions, concurrency mechanics, crash recovery workflows, and trade-offs for `queuectl`.

---

## 1. Atomic Job Claiming Across Processes

### Exact Line Reference
The cross-process atomic job claiming logic is implemented in [`queuectl_engine/db.py:L125-L150`](file:///home/vaish/Flam/queuectl_engine/db.py#L125-L150) inside `Database.claim_job()`:

```python
conn.execute("BEGIN IMMEDIATE")

cursor = conn.execute("""
    SELECT id, state, attempts, max_retries, command FROM jobs
    WHERE state = 'pending'
       OR (state = 'failed' AND (run_at IS NULL OR run_at <= ?))
       OR (state = 'processing' AND (last_heartbeat IS NULL OR last_heartbeat <= ?))
    ORDER BY created_at ASC
    LIMIT 1
""", (now_str, stale_threshold_str))
row = cursor.fetchone()

if not row:
    conn.execute("COMMIT")
    return None

job_id = row["id"]

conn.execute("""
    UPDATE jobs
    SET state = 'processing',
        worker_id = ?,
        last_heartbeat = ?,
        updated_at = ?
    WHERE id = ?
""", (worker_id, now_str, now_str, job_id))

conn.execute("COMMIT")
```

### Why it is Atomic Across Separate OS Processes
1. **SQLite WAL Mode & `BEGIN IMMEDIATE` Locks**: SQLite is configured in Write-Ahead Logging (WAL) mode with `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=10000;`.
2. **Reserved Writer Lock**: Executing `BEGIN IMMEDIATE` acquires a reserved lock on the database before reading the candidate rows. This prevents any other OS process from starting a write transaction concurrently.
3. **Serial Execution of Claim**: When multiple worker processes run `claim_job()` simultaneously from different terminals, only ONE process obtains the write lock. The second worker blocks up to `busy_timeout` (10 seconds) until the first process updates the candidate job's state to `'processing'` and releases the lock via `COMMIT`.
4. **Zero Race Conditions**: Because read and write occur inside a single `BEGIN IMMEDIATE` block, no two processes can select the same row in state `'pending'` simultaneously.

---

## 2. SIGKILL Crash Recovery Walkthrough & Worst-Case Delay

### Step-by-Step Walkthrough
Suppose Worker A is executing a job (`job_id = "job-42"`).

1. **State during execution**: `job-42` has `state = 'processing'`, `worker_id = 'worker-A'`, and a continuously updated `last_heartbeat` timestamp (refreshed every 5 seconds by a background heartbeat thread in [`queuectl_engine/worker.py:L40-L55`](file:///home/vaish/Flam/queuectl_engine/worker.py#L40-L55)).
2. **SIGKILL event**: Worker A receives a `SIGKILL` signal (or experiences a sudden OS crash / power loss). No cleanup handler or signal trap executes; the process dies instantly. `job-42` remains in SQLite with `state = 'processing'` and the last recorded `last_heartbeat` timestamp at the time of death.
3. **Detection by Active Workers**: When any worker (or a newly restarted worker) calls `claim_job()`, the SQL query evaluates:
   ```sql
   WHERE state = 'pending'
      OR (state = 'failed' AND (run_at IS NULL OR run_at <= :now))
      OR (state = 'processing' AND (last_heartbeat IS NULL OR last_heartbeat <= :stale_threshold))
   ```
4. **Re-claiming the Job**: Once `current_time - last_heartbeat` exceeds `stale_timeout` (default: 30 seconds, configurable via `queuectl config set stale-timeout <seconds>`), the query identifies `job-42` as abandoned/stale.
5. **Re-execution**: The active worker updates `job-42`'s `worker_id` to itself, resets `last_heartbeat` to `now`, and executes the command from scratch.

### Worst-Case Delay Before Recovery
- **Default configuration**: `stale-timeout` is set to 30 seconds. Worker polling interval is 0.5 seconds.
- **Worst-Case Delay**: **30.5 seconds** (30s stale window + 0.5s max poll loop delay). This strictly fulfills the assignment constraint requiring worst-case recovery under **60 seconds**.

---

## 3. DLQ Retry Attempt Counter Policy

### Decision
`queuectl dlq retry <id>` **resets the `attempts` counter to 0** and sets `state = 'pending'`.

### Justification
1. **Manual Intervention Context**: Moving a job to the Dead Letter Queue (`state = 'dead'`) indicates that all automated exponential backoff retries (`max_retries`) have been exhausted.
2. **Root Cause Resolution**: Operators inspect DLQ jobs after fixing an underlying dependency (e.g., restoring a broken API endpoint, resolving a database deadlock, or updating system permissions).
3. **Fresh Retry Lifecycle**: When an operator explicitly issues `dlq retry <id>`, the intent is to grant the job a brand-new lifecycle with the full allocation of retry attempts and initial backoff delays (`base^1`, `base^2`, ...). Preserving `attempts = max_retries` would cause a single subsequent failure to immediately return the job to the DLQ without giving exponential retries a chance to work.

---

## 4. Cross-Process Worker Stop Design Trade-Offs

### Rejected Designs
1. **PID File / Process Traversal Only**:
   - *Design*: Write master PID to `/tmp/queuectl.pid` and kill processes via `os.kill(pid, SIGTERM)`.
   - *Why Rejected*: Fragile if workers crash uncleanly, leaving stale PID files behind. Also fails when workers run across container boundaries or non-shared process namespaces.
2. **UNIX Control Sockets / IPC Server**:
   - *Design*: Run an IPC socket server (e.g. `/tmp/queuectl.sock`) inside the worker manager.
   - *Why Rejected*: Adds unnecessary network/socket setup overhead, socket file cleanup bugs on crashes, and permissions issues across multi-user environments.

### Chosen Design: Hybrid DB Signaling + Signal Escalation
- **Mechanism**: [`queuectl_engine/worker.py:L140-L155`](file:///home/vaish/Flam/queuectl_engine/worker.py#L140-L155)
- **Implementation**:
  1. `queuectl worker stop` updates the shared `workers` DB table, setting `status = 'stopping'`.
  2. It queries active worker PIDs from the table and sends `SIGTERM` to each process.
  3. Running workers check `status == 'stopping'` during their loop and trap `SIGTERM`/`SIGINT`.
  4. In-flight jobs are allowed to complete execution before the worker exits cleanly and unregisters from the `workers` table.

---

## 5. System Extensibility: Priority Queue Analysis

If job priorities (e.g. `high`, `medium`, `low`) were added tomorrow:

### Parts That Survive Unchanged
1. **Database Connection & WAL Concurrency**: SQLite `BEGIN IMMEDIATE` transactions and atomic job locking mechanisms remain 100% identical.
2. **Worker Execution Loop**: Job command execution, signal handling (`SIGINT`/`SIGTERM`), heartbeats, and error handling remain unchanged.
3. **Config & DLQ Mechanics**: Exponential backoff calculations, DLQ retry, and configuration parameters operate identically.

### Parts That Change / Break
1. **Schema Migration**: The `jobs` table requires a new column `priority INTEGER DEFAULT 0` (where higher numbers indicate higher priority).
2. **Claiming SQL Query**: The `ORDER BY` clause in `claim_job()` must be updated:
   ```sql
   /* Old query */
   ORDER BY created_at ASC

   /* New query */
   ORDER BY priority DESC, created_at ASC
   ```
3. **CLI Enqueue Parser**: `queuectl enqueue` needs a `--priority` option (e.g. `--priority high` or `-p 10`) and JSON payload validation for priority fields.
