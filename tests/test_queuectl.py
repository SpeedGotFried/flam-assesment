"""
Comprehensive test suite for QueueCTL covering Scenarios 1-5 from assignment PDF.
"""

import os
import sys
import time
import json
import signal
import shutil
import tempfile
import unittest
import subprocess
import threading
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from queuectl_engine.db import Database
from queuectl_engine.config import ConfigManager
from queuectl_engine.worker import Worker, WorkerManager


class TestQueueCTL(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.temp_dir, "test_queuectl.db")
        self.db = Database(self.db_path)
        self.cli_bin = os.path.join(PROJECT_ROOT, "queuectl")

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def run_cli(self, args_list):
        cmd = [sys.executable, self.cli_bin, "--db", self.db_path] + args_list
        res = subprocess.run(cmd, capture_output=True, text=True)
        return res

    def test_scenario_1_basic_job_completes(self):
        """Scenario 1: A basic job completes."""
        res = self.run_cli(["enqueue", '{"id":"job1","command":"echo hello_world"}'])
        self.assertEqual(res.returncode, 0, f"Error: {res.stderr}")

        job = self.db.get_job("job1")
        self.assertIsNotNone(job)
        self.assertEqual(job["state"], "pending")

        # Run worker to process one job
        worker = Worker(self.db, "test-worker-1")
        processed = worker.run_single()
        self.assertTrue(processed)

        job_after = self.db.get_job("job1")
        self.assertEqual(job_after["state"], "completed")

        # Test CLI list output --json contract
        res_list = self.run_cli(["list", "--state", "completed", "--json"])
        self.assertEqual(res_list.returncode, 0)
        jobs_json = json.loads(res_list.stdout)
        self.assertEqual(len(jobs_json), 1)
        self.assertEqual(jobs_json[0]["id"], "job1")
        self.assertEqual(jobs_json[0]["state"], "completed")

    def test_scenario_2_failing_job_retries_and_dlq(self):
        """Scenario 2: A failing job retries with backoff and lands in DLQ."""
        # Set max-retries to 2 and backoff-base to 1 for fast test execution
        self.db.config_set("max-retries", "2")
        self.db.config_set("backoff-base", "1")

        res = self.run_cli(["enqueue", '{"id":"fail_job","command":"exit 1"}'])
        self.assertEqual(res.returncode, 0)

        worker = Worker(self.db, "test-worker-2")

        # Attempt 1: Fails -> state 'failed' (attempts=1)
        processed1 = worker.run_single()
        self.assertTrue(processed1)
        job1 = self.db.get_job("fail_job")
        self.assertEqual(job1["state"], "failed")
        self.assertEqual(job1["attempts"], 1)

        # Wait 1.1s for backoff run_at
        time.sleep(1.1)

        # Attempt 2: Fails -> attempts=2 >= max_retries(2) -> state 'dead' (DLQ)
        processed2 = worker.run_single()
        self.assertTrue(processed2)
        job2 = self.db.get_job("fail_job")
        self.assertEqual(job2["state"], "dead")
        self.assertEqual(job2["attempts"], 2)

        # Test DLQ list CLI
        dlq_res = self.run_cli(["dlq", "list", "--json"])
        dlq_jobs = json.loads(dlq_res.stdout)
        self.assertEqual(len(dlq_jobs), 1)
        self.assertEqual(dlq_jobs[0]["id"], "fail_job")

        # Test DLQ retry CLI
        retry_res = self.run_cli(["dlq", "retry", "fail_job"])
        self.assertEqual(retry_res.returncode, 0)
        job_retried = self.db.get_job("fail_job")
        self.assertEqual(job_retried["state"], "pending")
        self.assertEqual(job_retried["attempts"], 0)

    def test_scenario_3_many_jobs_multiple_workers_exactly_once(self):
        """Scenario 3: Many jobs across multiple workers — every job runs exactly once."""
        num_jobs = 20
        num_workers = 4

        for i in range(num_jobs):
            self.db.enqueue_job(f"mjob_{i}", "echo batch_job")

        # Start multi-worker manager in thread
        manager = WorkerManager(self.db)
        mgr_thread = threading.Thread(target=manager.start_workers, args=(num_workers,))
        mgr_thread.start()

        # Wait for all jobs to complete
        timeout = 15
        start_t = time.time()
        while time.time() - start_t < timeout:
            counts = self.db.get_job_counts()
            if counts.get("completed", 0) == num_jobs:
                break
            time.sleep(0.5)

        # Stop workers gracefully
        WorkerManager.stop_all_workers(self.db)
        mgr_thread.join(timeout=3)

        counts = self.db.get_job_counts()
        self.assertEqual(counts.get("completed", 0), num_jobs)
        self.assertEqual(counts.get("pending", 0), 0)
        self.assertEqual(counts.get("processing", 0), 0)

    def test_scenario_4_sigkill_recovery(self):
        """Scenario 4: Workers are SIGKILLed mid-job; after restart every job still completes and nothing stuck in processing."""
        # Set stale timeout to 2 seconds for fast test
        self.db.config_set("stale-timeout", "2")

        # Enqueue a job with a sleep
        self.db.enqueue_job("crash_job", "sleep 0.1")

        # Claim the job manually to simulate a worker crashing mid-job
        crashed_worker_id = "crashed-worker-999"
        job = self.db.claim_job(crashed_worker_id, stale_timeout_seconds=2)
        self.assertIsNotNone(job)
        self.assertEqual(job["state"], "processing")

        # Wait for job to become stale (> 2 seconds without heartbeat)
        time.sleep(3.1)

        # A new worker attempts to claim jobs -> should recover and claim the stale job!
        new_worker = Worker(self.db, "new-worker-100")
        processed = new_worker.run_single()
        self.assertTrue(processed)

        job_final = self.db.get_job("crash_job")
        self.assertEqual(job_final["state"], "completed")

    def test_scenario_5_jobs_survive_full_restart(self):
        """Scenario 5: Jobs survive a full system restart."""
        # Enqueue job
        self.db.enqueue_job("persist_job", "echo persist_test")

        # Create new Database connection pointing to same file
        db2 = Database(self.db_path)
        job = db2.get_job("persist_job")
        self.assertIsNotNone(job)
        self.assertEqual(job["state"], "pending")

        # Worker processes it using db2
        worker = Worker(db2, "worker-db2")
        worker.run_single()

        # Re-check via original db instance
        job_after = self.db.get_job("persist_job")
        self.assertEqual(job_after["state"], "completed")


if __name__ == "__main__":
    unittest.main()
