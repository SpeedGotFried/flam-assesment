# QueueCTL — Background Job Queue System

QueueCTL is a minimal background job queue system built in Python 3 without any external package dependencies. It manages background jobs with worker processes, retries failures with exponential backoff, maintains a Dead Letter Queue (DLQ), recovers automatically from process crashes (`SIGKILL`), and guarantees cross-process atomic job execution.

---

## Features

- **CLI-based Job Queue**: Executable commands (`queuectl`) to enqueue, monitor, inspect, and manage jobs.
- **Cross-Process Concurrency**: Multiple workers running in separate terminal sessions execute jobs in parallel without race conditions or duplicate execution.
- **Atomic Locking Engine**: SQLite WAL mode (`PRAGMA journal_mode=WAL;`) + `BEGIN IMMEDIATE` transactions guarantee strict single-worker job claims across OS processes.
- **Automatic Retries & Exponential Backoff**: Failed jobs retry automatically after $delay = base^{attempts}$ seconds (default base = 2).
- **Dead Letter Queue (DLQ)**: Jobs exceeding `max_retries` transition to `dead` state and can be inspected or re-enqueued (resetting `attempts` to 0).
- **Crash Recovery (< 60s)**: Detects `SIGKILL` or worker crashes, automatically recovering abandoned jobs in ~30 seconds.
- **Interface Contract Compliance**: Default JSON array output (`queuectl list --state <state>`) and cross-terminal worker management (`queuectl worker stop`).

---

## System Requirements

- **Python**: 3.8+ (No external package dependencies required)
- **Environment**: Linux / macOS / Unix

---

## Installation & Setup

1. **Make the `queuectl` executable runnable**:
   ```bash
   chmod +x queuectl
   ```

2. **Run directly**:
   ```bash
   queuectl list
   ```
   *or using local path:*
   ```bash
   ./queuectl list
   ```

---

## CLI Reference & Usage Examples

### 1. Enqueueing Jobs

Pass job specifications as a raw JSON string:

```bash
queuectl enqueue '{"id":"job1","command":"sleep 2"}'
```

### 2. Managing Workers

Workers run in the foreground and handle incoming jobs concurrently:

```bash
# Terminal 1: Start 3 worker processes in the foreground
queuectl worker start --count 3
```

To stop all active workers gracefully from another terminal:

```bash
# Terminal 2: Stop all active workers cleanly
queuectl worker stop
```

### 3. Monitoring System Status

```bash
queuectl status
```

*Example Output*:
```text
=== QueueCTL Status ===
Active Workers : 3
Job Breakdown:
  Pending    : 2
  Processing : 1
  Completed  : 15
  Failed     : 0
  Dead       : 1
=======================
```

### 4. Listing Jobs by State

Lists jobs in JSON array format by default:

```bash
queuectl list --state pending
```

*Example Output*:
```json
[
  {
    "id": "job1",
    "command": "sleep 2",
    "state": "pending",
    "attempts": 0,
    "max_retries": 3,
    "created_at": "2026-07-27T05:04:02Z",
    "updated_at": "2026-07-27T05:04:02Z"
  }
]
```

### 5. Dead Letter Queue (DLQ)

```bash
# List permanently failed jobs in DLQ
queuectl dlq list

# Re-enqueue a DLQ job (resets attempts to 0 and state to pending)
queuectl dlq retry job1
```

### 6. Configuration Management

```bash
# View configuration
queuectl config get

# Update max retries
queuectl config set max-retries 5

# Update backoff base multiplier
queuectl config set backoff-base 2
```

---

## Architecture Overview

```text
+-------------------------------------------------------+
|                     queuectl CLI                      |
+-------------------------------------------------------+
                           |
       +-------------------+-------------------+
       |                   |                   |
       v                   v                   v
+--------------+   +---------------+   +---------------+
| Worker Process|   | Worker Process|   | Worker Process|
|  (Terminal 1)|   |  (Terminal 2)|   |  (Terminal 3)|
+--------------+   +---------------+   +---------------+
       |                   |                   |
       +-------------------+-------------------+
                           |
            BEGIN IMMEDIATE (Atomic Claim)
                           v
+-------------------------------------------------------+
|             SQLite Engine (queuectl.db)               |
|               - WAL Mode (journal_mode=WAL)           |
|               - Tables: jobs, config, workers         |
+-------------------------------------------------------+
```

---

## Testing Scenarios

QueueCTL includes an automated test suite verifying all 5 mandatory evaluation scenarios:

1. **Scenario 1**: Basic job completes successfully.
2. **Scenario 2**: Failing job retries with exponential backoff and transitions to DLQ (`dead`).
3. **Scenario 3**: Multiple parallel workers process batch jobs with zero duplicate execution.
4. **Scenario 4**: `SIGKILL` mid-job crash recovery guarantees job completion under 30 seconds.
5. **Scenario 5**: Job states persist intact across full system restarts.

Run the test suite:

```bash
python3 -m unittest discover tests
```

---

## Design Decisions

See [DECISIONS.md](file:///home/vaish/Flam/DECISIONS.md) for detailed answers to the 5 mandatory interview defense questions regarding atomic locking line numbers, `SIGKILL` recovery walkthroughs, DLQ attempt counter policies, cross-process signaling, and priority queue extensibility.
