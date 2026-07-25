"""
Database engine for QueueCTL.
Provides SQLite storage with Write-Ahead Logging (WAL) and atomic job claiming across processes.
"""

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Generator


def get_iso_now() -> str:
    """Return ISO 8601 formatted string in UTC, e.g. 2025-11-04T10:30:00Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso(ts_str: str) -> datetime:
    """Parse ISO 8601 string to datetime object in UTC."""
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)


class Database:
    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_dir = os.path.expanduser("~/.queuectl")
            os.makedirs(db_dir, exist_ok=True)
            db_path = os.path.join(db_dir, "queuectl.db")
        self.db_path = db_path
        self.init_db()

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, timeout=10.0, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=10000;")
        try:
            yield conn
        finally:
            conn.close()

    def init_db(self):
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    command TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_heartbeat TEXT,
                    worker_id TEXT,
                    run_at TEXT,
                    error_message TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    heartbeat_at TEXT NOT NULL
                )
            """)
            # Set default configs if not present
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('max-retries', '3')")
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('backoff-base', '2')")
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('stale-timeout', '30')")

    def config_get(self, key: str, default: str) -> str:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT value FROM config WHERE key = ?", (key,))
            row = cursor.fetchone()
            return row["value"] if row else default

    def config_set(self, key: str, value: str):
        with self.get_connection() as conn:
            conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))

    def enqueue_job(self, job_id: str, command: str, max_retries: Optional[int] = None) -> Dict[str, Any]:
        if max_retries is None:
            max_retries = int(self.config_get("max-retries", "3"))

        now = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at)
                VALUES (?, ?, 'pending', 0, ?, ?, ?)
            """, (job_id, command, max_retries, now, now))
        
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def claim_job(self, worker_id: str, stale_timeout_seconds: int = 30) -> Optional[Dict[str, Any]]:
        """
        Atomically claims a job for the worker.
        Eligible jobs:
        1. state == 'pending'
        2. state == 'failed' AND (run_at IS NULL OR run_at <= now)
        3. state == 'processing' AND (last_heartbeat IS NULL OR last_heartbeat < stale_threshold) [CRASH RECOVERY]

        Uses BEGIN IMMEDIATE transaction to guarantee atomic claiming across processes.
        """
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        stale_threshold_dt = now_dt - timedelta(seconds=stale_timeout_seconds)
        stale_threshold_str = stale_threshold_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.get_connection() as conn:
            try:
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
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                raise

        return self.get_job(job_id)

    def heartbeat_job(self, job_id: str, worker_id: str):
        now_str = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE jobs
                SET last_heartbeat = ?, updated_at = ?
                WHERE id = ? AND worker_id = ? AND state = 'processing'
            """, (now_str, now_str, job_id, worker_id))

    def complete_job(self, job_id: str, worker_id: str):
        now_str = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE jobs
                SET state = 'completed',
                    worker_id = NULL,
                    updated_at = ?
                WHERE id = ? AND worker_id = ?
            """, (now_str, job_id, worker_id))

    def fail_job(self, job_id: str, worker_id: str, error_msg: str) -> str:
        """
        Record job execution failure.
        Increments attempts count.
        If attempts >= max_retries -> transition to 'dead' (DLQ).
        Else -> transition to 'failed' with run_at delay = base ^ attempts seconds.
        Returns final state ('failed' or 'dead').
        """
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        job = self.get_job(job_id)
        if not job:
            return "unknown"

        new_attempts = job["attempts"] + 1
        max_retries = job["max_retries"]
        backoff_base = float(self.config_get("backoff-base", "2"))

        with self.get_connection() as conn:
            if new_attempts >= max_retries:
                # Move to DLQ
                conn.execute("""
                    UPDATE jobs
                    SET state = 'dead',
                        attempts = ?,
                        error_message = ?,
                        worker_id = NULL,
                        updated_at = ?
                    WHERE id = ?
                """, (new_attempts, error_msg, now_str, job_id))
                return "dead"
            else:
                # Calculate backoff delay: base ^ attempts
                delay_seconds = int(backoff_base ** new_attempts)
                run_at_dt = now_dt + timedelta(seconds=delay_seconds)
                run_at_str = run_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                conn.execute("""
                    UPDATE jobs
                    SET state = 'failed',
                        attempts = ?,
                        error_message = ?,
                        run_at = ?,
                        worker_id = NULL,
                        updated_at = ?
                    WHERE id = ?
                """, (new_attempts, error_msg, run_at_str, now_str, job_id))
                return "failed"

    def re_enqueue_dlq_job(self, job_id: str) -> bool:
        """
        Re-enqueue a dead job from DLQ. Resets attempts counter to 0.
        """
        now_str = get_iso_now()
        with self.get_connection() as conn:
            cursor = conn.execute("""
                UPDATE jobs
                SET state = 'pending',
                    attempts = 0,
                    error_message = NULL,
                    worker_id = NULL,
                    run_at = NULL,
                    updated_at = ?
                WHERE id = ? AND state = 'dead'
            """, (now_str, job_id))
            return cursor.rowcount > 0

    def get_jobs_by_state(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            if state:
                cursor = conn.execute("""
                    SELECT id, command, state, attempts, max_retries, created_at, updated_at
                    FROM jobs
                    WHERE state = ?
                    ORDER BY created_at ASC
                """, (state,))
            else:
                cursor = conn.execute("""
                    SELECT id, command, state, attempts, max_retries, created_at, updated_at
                    FROM jobs
                    ORDER BY created_at ASC
                """)
            return [dict(row) for row in cursor.fetchall()]

    def get_job_counts(self) -> Dict[str, int]:
        counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state")
            for row in cursor.fetchall():
                if row["state"] in counts:
                    counts[row["state"]] = row["count"]
        return counts

    def register_worker(self, worker_id: str, pid: int):
        now_str = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO workers (worker_id, pid, status, started_at, heartbeat_at)
                VALUES (?, ?, 'active', ?, ?)
            """, (worker_id, pid, now_str, now_str))

    def worker_heartbeat(self, worker_id: str):
        now_str = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE workers SET heartbeat_at = ? WHERE worker_id = ?
            """, (now_str, worker_id))

    def unregister_worker(self, worker_id: str):
        with self.get_connection() as conn:
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))

    def set_workers_stopping(self):
        with self.get_connection() as conn:
            conn.execute("UPDATE workers SET status = 'stopping'")

    def get_active_workers(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT * FROM workers WHERE status != 'stopped'")
            return [dict(row) for row in cursor.fetchall()]
