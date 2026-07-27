# Architecture Decisions

### 1. Atomic Job Claiming Across Processes
Implemented in [`queuectl_engine/db.py:L103-L125`](file:///home/vaish/Flam/queuectl_engine/db.py#L103-L125).

Uses SQLite WAL mode (`PRAGMA journal_mode=WAL;`) with `BEGIN IMMEDIATE` transactions. When multiple workers try to claim a job at the same time, SQLite grants a reserved write lock to only one process. The write lock blocks other workers until the state is updated to `processing` and committed, preventing duplicate job execution across processes.

### 2. SIGKILL Crash Recovery
If a worker crashes mid-execution (`SIGKILL`), the job remains marked as `processing`. When active workers poll the database, they look for jobs in `processing` state where `last_heartbeat` is older than `stale_timeout` (30s default).

Worst-case recovery delay is **30.5 seconds** (30s stale window + 0.5s poll loop delay).

### 3. DLQ Retry Policy
`queuectl dlq retry <job_id>` resets `attempts` to 0 and state to `pending`.

Moving a job to DLQ means automated retries were exhausted. Retrying a DLQ job implies the underlying issue was manually fixed, so it gets a fresh lifecycle with full retries and exponential backoff.

### 4. Cross-Process Worker Stop
`queuectl worker stop` updates the `workers` table status to `stopping` and sends `SIGTERM` to active worker PIDs (see [`queuectl_engine/worker.py:L105-L116`](file:///home/vaish/Flam/queuectl_engine/worker.py#L105-L116)).

Rejected PID files (stale files on unclean crashes) and IPC sockets (unnecessary complexity).

### 5. Priority Queue Extensibility
Adding job priorities later:
- **Unchanged**: SQLite locking, worker loop, backoff math, DLQ handling.
- **Changes**: Add `priority` column to `jobs` table, update `claim_job()` SQL query `ORDER BY priority DESC, created_at ASC`, and add CLI flag.
