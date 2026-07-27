"""
Bare-bones worker process management for QueueCTL.
"""

import os
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
        self.shutdown_requested = False

    def run_single(self) -> bool:
        stale_sec = int(self.db.config_get("stale-timeout", "30"))
        job = self.db.claim_job(self.worker_id, stale_timeout_seconds=stale_sec)
        if not job:
            return False

        job_id = job["id"]
        cmd = job["command"]

        try:
            res = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if res.returncode == 0:
                self.db.complete_job(job_id, self.worker_id)
            else:
                err = res.stderr.strip() or f"Exited with code {res.returncode}"
                self.db.fail_job(job_id, self.worker_id, err)
        except Exception as exc:
            self.db.fail_job(job_id, self.worker_id, str(exc))

        return True

    def start_loop(self):
        def handle_sig(sig, frame):
            self.shutdown_requested = True

        try:
            signal.signal(signal.SIGINT, handle_sig)
            signal.signal(signal.SIGTERM, handle_sig)
        except (ValueError, OSError):
            pass

        self.db.register_worker(self.worker_id, os.getpid())
        try:
            while not self.shutdown_requested:
                active = self.db.get_active_workers()
                me = next((w for w in active if w["worker_id"] == self.worker_id), None)
                if me and me["status"] == "stopping":
                    break

                self.db.worker_heartbeat(self.worker_id)
                processed = self.run_single()
                if not processed:
                    time.sleep(0.5)
        finally:
            self.db.unregister_worker(self.worker_id)


class WorkerManager:
    def __init__(self, db: Database):
        self.db = db
        self.threads: List[threading.Thread] = []
        self.workers: List[Worker] = []

    def start_workers(self, count: int):
        def handle_sig(sig, frame):
            for w in self.workers:
                w.shutdown_requested = True

        try:
            signal.signal(signal.SIGINT, handle_sig)
            signal.signal(signal.SIGTERM, handle_sig)
        except (ValueError, OSError):
            pass

        self.workers = [Worker(self.db, f"worker-{os.getpid()}-{i+1}") for i in range(count)]
        for w in self.workers:
            t = threading.Thread(target=w.start_loop)
            t.start()
            self.threads.append(t)

        try:
            while any(t.is_alive() for t in self.threads):
                if any(w["status"] == "stopping" for w in self.db.get_active_workers()):
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
        active = db.get_active_workers()
        db.set_workers_stopping()
        signaled = set()
        for w in active:
            pid = w["pid"]
            if pid != os.getpid() and pid not in signaled:
                try:
                    os.kill(pid, signal.SIGTERM)
                    signaled.add(pid)
                except OSError:
                    pass
