# Architecture & Design Decisions – QueueCTL

This document explains the main design decisions behind `queuectl`, why they were made, and how they behave under real-world scenarios.

---

## 1. Atomic Job Claiming Across Processes

### Relevant Code
[`queuectl_engine/db.py:L103-L125`](file:///home/vaish/Flam/queuectl_engine/db.py#L103-L125) (`Database.claim_job()`)

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

### Why this prevents two workers from claiming the same job
The important detail here is that both the `SELECT` and the `UPDATE` happen inside a single `BEGIN IMMEDIATE` transaction.

As soon as the transaction begins, SQLite acquires a write reservation. If another worker process reaches `claim_job()` at the same time, it cannot start its own write transaction immediately. Instead, it waits until the current transaction commits (up to the configured `busy_timeout`).

By the time the second worker gets access to the database, the first worker has already updated the selected job to `processing`. That means the second worker simply won't see the same row as available anymore.

Using WAL mode (`PRAGMA journal_mode=WAL`) also helps here because readers can continue working while writes are serialized safely. In practice, even if multiple workers are started from different terminals at exactly the same time, only one of them can successfully claim a particular job.

---

## 2. What Happens if a Worker is Killed?

Imagine Worker A is currently executing `job-42`.

When the worker claims the job, it marks the row as `processing`, records its `worker_id`, and periodically updates `last_heartbeat` while the job is running.

If the process is terminated with `SIGKILL` (or the machine suddenly loses power), there is no opportunity for cleanup code to run. The database still contains a job in the `processing` state along with the last recorded heartbeat.

Every worker that looks for new work checks not only `pending` jobs, but also `processing` jobs whose heartbeat has become stale:

```sql
WHERE state = 'processing'
  AND (last_heartbeat IS NULL OR last_heartbeat <= :stale_threshold)
```

Once the heartbeat is older than the configured `stale-timeout` (30 seconds by default), another worker assumes the original worker is no longer alive. It updates the ownership information, refreshes the heartbeat, and starts executing the job from the beginning.

Recovery isn't immediate because the system intentionally waits long enough to avoid reclaiming work from a temporarily slow worker. With the default configuration, the timeout is 30 seconds and workers poll every 0.5 seconds, so the worst-case recovery time is about **30.5 seconds**, comfortably below the assignment requirement of 60 seconds.

---

## 3. Why Reset the Retry Counter from the DLQ?

When a job reaches the Dead Letter Queue, it has already exhausted every automatic retry allowed by `max_retries`.

I decided that `queuectl dlq retry <id>` should reset `attempts` back to zero before moving the job to the `pending` state.

The reasoning is simple. A manual retry usually happens after someone has investigated the failure and fixed whatever caused it—perhaps a database outage, a permission problem, or a missing dependency. At that point, it makes more sense to treat the job like a fresh execution instead of continuing from the old retry count.

If the previous attempt count were preserved, another failure would often send the job straight back to the DLQ, leaving almost no opportunity for the normal exponential backoff logic to do its job.

---

## 4. Why I Chose the Worker Stop Mechanism

I considered a couple of different approaches before settling on the final design.

Using only PID files seemed simple, but it quickly becomes unreliable if a worker crashes unexpectedly. Stale PID files are common, and the approach also becomes awkward across containers or isolated process namespaces.

Another option was to build a small UNIX socket server that workers could listen to for control commands. While this works, it introduces extra infrastructure that isn't really necessary for a small queue system.

Instead, I used the database itself as the coordination point (see [`queuectl_engine/worker.py:L105-L116`](file:///home/vaish/Flam/queuectl_engine/worker.py#L105-L116)).

`queuectl worker stop` updates the shared `workers` table by marking workers as `stopping`, then sends each worker a `SIGTERM`. During their normal polling loop, workers check this status while also handling termination signals. If a worker is already executing a job, it finishes that job first, unregisters itself from the database, and exits cleanly.

This approach keeps coordination centralized in one place while avoiding the complexity of running a separate control service.

---

## 5. Extending the System with Job Priorities

If priorities (`high`, `medium`, `low`) needed to be added later, most of the existing architecture could stay exactly as it is.

The concurrency model wouldn't change because jobs are still claimed using the same atomic transaction. Heartbeats, retry logic, worker shutdown, and DLQ handling would also continue to work without modification.

The main change would be deciding which eligible job should be claimed first:

1. The database schema would need a new column:
   ```sql
   priority INTEGER DEFAULT 0
   ```

2. The selection query would change from:
   ```sql
   ORDER BY created_at ASC
   ```
   to:
   ```sql
   ORDER BY priority DESC, created_at ASC
   ```

3. Finally, the CLI would need a `--priority` option when enqueuing jobs.

Apart from those changes, the rest of the worker pipeline could remain unchanged because the execution flow itself is independent of scheduling order.
