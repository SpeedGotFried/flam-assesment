"""
Worker process management module for QueueCTL.
Handles worker lifecycles, signal handling (SIGINT/SIGTERM), job execution, heartbeats, and graceful shutdowns.
"""

import os
import sys
import time
import signal
import subprocess
import threading
import uuid
from typing import Optional, List
from queuectl_engine.db import Database


class Worker:
    def __init__(self, db: Database, worker_id: Optional[str] = None):
        self.db = db
        self.worker_id = worker_id or f"worker-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.running = False
        self.shutdown_requested = False
        self.current_job_id: Optional[str] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_heartbeat_event = threading.Event()

    def _setup_signal_handlers(self):
        def handle_signal(signum, frame):
            # Graceful shutdown requested
            self.shutdown_requested = True

        try:
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
        except (ValueError, OSError):
            # Signals might fail if called in non-main thread
            pass

    def _start_job_heartbeat(self, job_id: str):
        self._stop_heartbeat_event.clear()
        
        def heartbeat_loop():
            while not self._stop_heartbeat_event.is_set():
                try:
                    self.db.heartbeat_job(job_id, self.worker_id)
                except Exception:
                    pass
                self._stop_heartbeat_event.wait(5.0)

        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def _stop_job_heartbeat(self):
        self._stop_heartbeat_event.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=1.0)
        self._heartbeat_thread = None

    def run_single(self) -> bool:
        """
        Attempt to claim and execute one job.
        Returns True if a job was processed, False otherwise.
        """
        stale_seconds = int(self.db.config_get("stale-timeout", "30"))
        job = self.db.claim_job(self.worker_id, stale_timeout_seconds=stale_seconds)

        if not job:
            return False

        job_id = job["id"]
        command = job["command"]
        self.current_job_id = job_id

        # Start job heartbeat
        self._start_job_heartbeat(job_id)

        try:
            # Execute command in shell
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True
            )

            self._stop_job_heartbeat()

            if result.returncode == 0:
                self.db.complete_job(job_id, self.worker_id)
            else:
                error_msg = result.stderr.strip() or f"Command exited with status {result.returncode}"
                self.db.fail_job(job_id, self.worker_id, error_msg)

        except Exception as exc:
            self._stop_job_heartbeat()
            self.db.fail_job(job_id, self.worker_id, str(exc))
        finally:
            self.current_job_id = None

        return True

    def start_loop(self):
        """Run worker loop until shutdown requested."""
        self._setup_signal_handlers()
        self.running = True
        self.db.register_worker(self.worker_id, os.getpid())

        try:
            while not self.shutdown_requested:
                # Check DB status in case worker stop was called from another terminal
                active_workers = self.db.get_active_workers()
                my_worker_record = next((w for w in active_workers if w["worker_id"] == self.worker_id), None)
                if my_worker_record and my_worker_record["status"] == "stopping":
                    self.shutdown_requested = True

                self.db.worker_heartbeat(self.worker_id)

                processed = self.run_single()

                if not processed:
                    # Sleep briefly when idle
                    time.sleep(0.5)

                if self.shutdown_requested:
                    break
        finally:
            self.running = False
            self.db.unregister_worker(self.worker_id)


class WorkerManager:
    def __init__(self, db: Database):
        self.db = db
        self.threads: List[threading.Thread] = []
        self.workers: List[Worker] = []
        self.shutdown_requested = False

    def start_workers(self, count: int):
        """
        Start N workers in the foreground (blocks until stopped/signaled).
        """
        def handle_signal(signum, frame):
            self.shutdown_requested = True
            for w in self.workers:
                w.shutdown_requested = True

        try:
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
        except (ValueError, OSError):
            pass

        self.workers = [Worker(self.db, f"worker-{os.getpid()}-{i+1}") for i in range(count)]

        for worker in self.workers:
            t = threading.Thread(target=worker.start_loop, daemon=False)
            t.start()
            self.threads.append(t)

        try:
            while any(t.is_alive() for t in self.threads):
                # Check if stop requested via DB table from another terminal
                active = self.db.get_active_workers()
                if any(w["status"] == "stopping" for w in active):
                    for w in self.workers:
                        w.shutdown_requested = True

                time.sleep(0.5)
        except KeyboardInterrupt:
            for w in self.workers:
                w.shutdown_requested = True

        for t in self.threads:
            t.join()

    @staticmethod
    def stop_all_workers(db: Database):
        """
        Send stop signal to all running workers from another terminal.
        """
        active_workers = db.get_active_workers()
        db.set_workers_stopping()

        # Send SIGTERM to worker process PIDs
        signaled_pids = set()
        for w in active_workers:
            pid = w["pid"]
            if pid != os.getpid() and pid not in signaled_pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                    signaled_pids.add(pid)
                except OSError:
                    pass
