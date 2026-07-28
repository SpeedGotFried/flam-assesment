#!/usr/bin/env python3
"""
QueueCTL Stress Test Suite

Stress tests QueueCTL under high concurrency, high load, SQLite lock contention,
and mid-job worker crashes (SIGKILL).

Configured via stress-test/data/config.sh:
- MAX_RETRIES=5
- BACKOFF_BASE=1
- STALE_TIMEOUT=5
- DB_PATH=stress-test/data/test.db
"""

import os
import sys
import time
import json
import signal
import shutil
import subprocess
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
STRESS_DIR = PROJECT_ROOT / "stress-test"
DATA_DIR = STRESS_DIR / "data"
CONFIG_FILE = DATA_DIR / "config.sh"
DB_PATH = DATA_DIR / "test.db"
CLI_BIN = PROJECT_ROOT / "queuectl"

# Defaults from config.sh
MAX_RETRIES = 5
BACKOFF_BASE = 1
STALE_TIMEOUT = 5


def parse_config():
    global MAX_RETRIES, BACKOFF_BASE, STALE_TIMEOUT, DB_PATH
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k == "MAX_RETRIES":
                        MAX_RETRIES = int(v)
                    elif k == "BACKOFF_BASE":
                        BACKOFF_BASE = int(v)
                    elif k == "STALE_TIMEOUT":
                        STALE_TIMEOUT = int(v)
                    elif k == "DB_PATH":
                        DB_PATH = PROJECT_ROOT / v
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def run_cli(args, check=True):
    cmd = [sys.executable, str(CLI_BIN), "--db", str(DB_PATH)] + args
    res = subprocess.run(cmd, capture_output=True, text=True)
    if check and res.returncode != 0:
        print(f"[ERROR] CLI failed: {cmd}\nStderr: {res.stderr}", file=sys.stderr)
    return res


def cleanup_db():
    for f in [DB_PATH, Path(str(DB_PATH) + "-wal"), Path(str(DB_PATH) + "-shm")]:
        if f.exists():
            try:
                f.unlink()
            except Exception:
                pass


def setup_stress_test():
    print("=" * 60)
    print(" QueueCTL Stress Test Setup")
    print("=" * 60)
    parse_config()
    cleanup_db()
    print(f"Config: MAX_RETRIES={MAX_RETRIES}, BACKOFF_BASE={BACKOFF_BASE}, STALE_TIMEOUT={STALE_TIMEOUT}")
    print(f"DB Path: {DB_PATH}")

    # Set DB config
    run_cli(["config", "set", "max-retries", str(MAX_RETRIES)])
    run_cli(["config", "set", "backoff-base", str(BACKOFF_BASE)])
    run_cli(["config", "set", "stale-timeout", str(STALE_TIMEOUT)])
    print("[OK] Initialized DB configuration.\n")


def test_high_volume_concurrency():
    print("=" * 60)
    print(" Phase 1: High-Volume Concurrent Job Processing")
    print("=" * 60)

    NUM_FAST_JOBS = 200
    NUM_FAIL_JOBS = 50
    TOTAL_JOBS = NUM_FAST_JOBS + NUM_FAIL_JOBS
    NUM_WORKER_PROCS = 4
    WORKERS_PER_PROC = 2  # Total 8 worker threads across 4 processes

    print(f"Enqueuing {TOTAL_JOBS} jobs ({NUM_FAST_JOBS} fast jobs, {NUM_FAIL_JOBS} permanent fail jobs)...")
    start_enq = time.time()

    # Enqueue fast jobs
    for i in range(NUM_FAST_JOBS):
        payload = json.dumps({"id": f"fast_job_{i}", "command": "echo hello"})
        res = run_cli(["enqueue", payload])
        if res.returncode != 0:
            print(f"[FAIL] Failed to enqueue fast_job_{i}")
            return False

    # Enqueue failing jobs (command 'exit 1' will exhaust retries and land in DLQ)
    for i in range(NUM_FAIL_JOBS):
        payload = json.dumps({"id": f"fail_job_{i}", "command": "exit 1"})
        res = run_cli(["enqueue", payload])
        if res.returncode != 0:
            print(f"[FAIL] Failed to enqueue fail_job_{i}")
            return False

    enq_dur = time.time() - start_enq
    print(f"[OK] Enqueued {TOTAL_JOBS} jobs in {enq_dur:.2f}s ({TOTAL_JOBS/enq_dur:.1f} jobs/sec)")

    print(f"Launching {NUM_WORKER_PROCS} worker processes ({NUM_WORKER_PROCS * WORKERS_PER_PROC} workers total)...")
    start_proc = time.time()
    worker_procs = []

    for p_idx in range(NUM_WORKER_PROCS):
        cmd = [sys.executable, str(CLI_BIN), "--db", str(DB_PATH), "worker", "start", "--count", str(WORKERS_PER_PROC)]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        worker_procs.append(proc)

    # Poll status until all jobs are processed (completed or dead)
    timeout = 45
    deadline = time.time() + timeout
    completed, dead = 0, 0

    while time.time() < deadline:
        res = run_cli(["status"])
        output = res.stdout
        # Check if all jobs are finished
        # Pending + Processing == 0
        c_res = run_cli(["list", "--state", "completed", "--json"])
        d_res = run_cli(["list", "--state", "dead", "--json"])

        try:
            completed_jobs = json.loads(c_res.stdout) if c_res.stdout else []
            dead_jobs = json.loads(d_res.stdout) if d_res.stdout else []
            completed = len(completed_jobs)
            dead = len(dead_jobs)
        except Exception:
            pass

        if completed + dead >= TOTAL_JOBS:
            print(f"[OK] All jobs finished! Completed: {completed}, DLQ (Dead): {dead}")
            break

        time.sleep(0.5)

    proc_dur = time.time() - start_proc

    # Stop workers cleanly
    run_cli(["worker", "stop"])
    for p in worker_procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()

    print(f"Phase 1 Duration: {proc_dur:.2f}s (Throughput: {completed/proc_dur:.1f} jobs/sec)")

    # Assertions
    if completed != NUM_FAST_JOBS:
        print(f"[FAIL] Expected {NUM_FAST_JOBS} completed jobs, got {completed}")
        return False
    if dead != NUM_FAIL_JOBS:
        print(f"[FAIL] Expected {NUM_FAIL_JOBS} dead jobs, got {dead}")
        return False

    print("[PASS] Phase 1: High-Volume Concurrency Passed Successfully!\n")
    return True


def test_sigkill_crash_recovery_under_load():
    print("=" * 60)
    print(" Phase 2: SIGKILL Worker Crash & Stale Job Recovery Under Load")
    print("=" * 60)

    NUM_JOBS = 40
    print(f"Enqueuing {NUM_JOBS} slow jobs ('sleep 1.5')...")

    for i in range(NUM_JOBS):
        payload = json.dumps({"id": f"slow_job_{i}", "command": "sleep 1.5"})
        run_cli(["enqueue", payload])

    print("Launching 3 worker processes...")
    worker_procs = []
    for _ in range(3):
        cmd = [sys.executable, str(CLI_BIN), "--db", str(DB_PATH), "worker", "start", "--count", "2"]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        worker_procs.append(p)

    time.sleep(1.0)  # Wait for workers to claim jobs and start processing

    print("SIGKILLing 2 of the worker processes mid-execution...")
    worker_procs[0].kill()
    worker_procs[1].kill()
    print("[KILLED] 2 worker processes forcibly terminated with SIGKILL.")

    print(f"Waiting for STALE_TIMEOUT ({STALE_TIMEOUT}s) + recovery window...")
    time.sleep(STALE_TIMEOUT + 3.0)

    print("Starting new replacement workers to process recovered jobs...")
    repl_p = subprocess.Popen(
        [sys.executable, str(CLI_BIN), "--db", str(DB_PATH), "worker", "start", "--count", "4"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    deadline = time.time() + 30
    all_completed = False
    while time.time() < deadline:
        c_res = run_cli(["list", "--state", "completed", "--json"])
        try:
            completed_jobs = json.loads(c_res.stdout) if c_res.stdout else []
            if len(completed_jobs) >= NUM_JOBS + 200:  # 200 from Phase 1 + 40 from Phase 2
                all_completed = True
                print(f"[OK] All {NUM_JOBS} slow jobs completed after SIGKILL crash recovery!")
                break
        except Exception:
            pass
        time.sleep(1.0)

    # Stop all workers
    run_cli(["worker", "stop"])
    for p in worker_procs + [repl_p]:
        try:
            p.wait(timeout=3)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass

    if not all_completed:
        print("[FAIL] Not all jobs completed after SIGKILL crash!")
        return False

    print("[PASS] Phase 2: SIGKILL Crash & Recovery Passed Successfully!\n")
    return True


def main():
    setup_stress_test()
    success1 = test_high_volume_concurrency()
    success2 = test_sigkill_crash_recovery_under_load()

    print("=" * 60)
    print(" STRESS TEST SUMMARY")
    print("=" * 60)
    if success1 and success2:
        print(">>> ALL STRESS TESTS PASSED SUCCESSFULLY! <<<")
        sys.exit(0)
    else:
        print(">>> STRESS TEST FAILED! <<<")
        sys.exit(1)


if __name__ == "__main__":
    main()
