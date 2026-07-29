# Architecture & Design Decisions — QueueCTL

---

## 1. Which exact line(s) prevent two workers from claiming the same job, and why is that operation atomic across separate OS processes?

### Code Reference
The atomic job claiming logic is implemented in [`queuectl_engine/db.py:L103-L125`](file:///home/vaish/Flam/queuectl_engine/db.py#L103-L125) inside `Database.claim_job()`:

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

### Why this operation is atomic
Both the `SELECT` candidate query and the `UPDATE` state query happen inside a single `BEGIN IMMEDIATE` transaction.

When a worker calls `claim_job()`, `BEGIN IMMEDIATE` acquires a write lock on the SQLite database file before reading candidate rows. If another worker process calls `claim_job()` at the same instant, SQLite blocks the second process until the first worker's transaction completes (`COMMIT`).

By the time the second worker acquires the write lock, the job has already been marked as `processing`. The second worker's `SELECT` query skips it and claims the next `pending` job.

---

## 2. A worker is SIGKILLed halfway through a job. Walk through, step by step, what state the job is in and how it eventually runs again. What is the worst-case delay before recovery?

### Walkthrough
Imagine Worker A claims `job-1`:

1. **Execution**: `job-1` has state `processing`, `worker_id` set to Worker A, and a `last_heartbeat` timestamp set when claimed (and updated during long loops in [`queuectl_engine/worker.py:L60`](file:///home/vaish/Flam/queuectl_engine/worker.py#L60)).
2. **SIGKILL Crash**: `SIGKILL` kills Worker A instantly. No cleanup runs. The job stays in SQLite marked as `processing`.
3. **Detection**: Active workers periodically polling SQLite check for candidate jobs, matching `processing` jobs whose `last_heartbeat` timestamp is older than `stale_timeout` (30 seconds):
   ```sql
   OR (state = 'processing' AND (last_heartbeat IS NULL OR last_heartbeat <= :stale_timeout))
   ```
4. **Re-claiming & Execution**: A healthy worker detects `job-1` as stale, updates `worker_id` to itself, resets `last_heartbeat` to `now`, and executes the command from scratch.

---

## 3. Does dlq retry reset attempts? Why is that the right call?

Yes, `queuectl dlq retry <job_id>` resets `attempts` to `0` and state to `pending`.

Moving a job to the Dead Letter Queue (`state = 'dead'`) means automated retries were fully exhausted. Manually issuing `dlq retry` is an explicit action taken after an operator investigates and fixes the underlying issues.

Once the issue is resolved, the job gets a brand-new lifecycle with the full `max_retries`.

---

## 4. What designs did you consider and reject for worker stop (cross-process signaling), and why?

### Rejected Designs
1. **PID File only**: Fragile when workers crash ungracefully, leaving stale PID files behind. Also fails across isolated container namespaces.
2. **UNIX Socket Server**: Adds unnecessary network/socket setup and socket file cleanup complexity.

### Chosen Design
Database signaling + signal escalation ([`queuectl_engine/worker.py:L105-L116`](file:///home/vaish/Flam/queuectl_engine/worker.py#L105-L116)):
1. `queuectl worker stop` updates the shared `workers` table in SQLite, setting `status = 'stopping'`.
2. It queries active worker PIDs and sends `SIGTERM` to each process.
3. Workers check `status == 'stopping'` during their loop and handle `SIGTERM`.
4. In-flight jobs are allowed to complete execution before the worker exits cleanly and unregisters from the database.

---

## 5. If priorities were added tomorrow (high-priority jobs jump the queue), which parts of your design survive unchanged and which break?

### Parts That Survive Unchanged
- **Database Concurrency & Locking**: SQLite `BEGIN IMMEDIATE` transactions and atomic claim locks in `claim_job()` remain 100% identical.
- **Worker Execution Loop**: Shell command execution, signal handling, heartbeats, and graceful stop handlers remain unchanged.
- **Retry & DLQ Mechanics**: Exponential backoff math and DLQ state transitions operate identically.

### Parts That Change
1. **Database Schema**: Add a `priority INTEGER DEFAULT 0` column to the `jobs` table.
2. **Claim SQL Query**: Update the `ORDER BY` clause in `claim_job()` from:
   ```sql
   ORDER BY created_at ASC
   ```
   to:
   ```sql
   ORDER BY priority DESC, created_at ASC
   ```
3. **CLI Parser**: Add a `--priority` option for `queuectl enqueue`.
