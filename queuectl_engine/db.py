"""
Bare-bones Database engine for QueueCTL.
Provides SQLite storage with Write-Ahead Logging (WAL) and atomic job claiming.
"""

import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Generator


def get_iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or "queuectl.db"
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
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('max-retries', '3')")
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('backoff-base', '2')")
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('stale-timeout', '30')")

    def config_get(self, key: str, default: str) -> str:
        with self.get_connection() as conn:
            row = conn.execute("SELECT value FROM config WHERE key = ?", (key,)).fetchone()
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
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return dict(row) if row else None

    def claim_job(self, worker_id: str, stale_timeout_seconds: int = 30) -> Optional[Dict[str, Any]]:
        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        stale_thresh = (now_dt - timedelta(seconds=stale_timeout_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.get_connection() as conn:
            try:
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
                UPDATE jobs SET last_heartbeat = ?, updated_at = ?
                WHERE id = ? AND worker_id = ? AND state = 'processing'
            """, (now_str, now_str, job_id, worker_id))

    def complete_job(self, job_id: str, worker_id: str):
        now_str = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE jobs SET state = 'completed', worker_id = NULL, updated_at = ?
                WHERE id = ? AND worker_id = ?
            """, (now_str, job_id, worker_id))

    def fail_job(self, job_id: str, worker_id: str, error_msg: str) -> str:
        job = self.get_job(job_id)
        if not job:
            return "unknown"

        now_dt = datetime.now(timezone.utc)
        now_str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        new_attempts = job["attempts"] + 1
        max_retries = job["max_retries"]
        base = float(self.config_get("backoff-base", "2"))

        with self.get_connection() as conn:
            if new_attempts >= max_retries:
                conn.execute("""
                    UPDATE jobs SET state = 'dead', attempts = ?, error_message = ?, worker_id = NULL, updated_at = ?
                    WHERE id = ?
                """, (new_attempts, error_msg, now_str, job_id))
                return "dead"
            else:
                run_at = (now_dt + timedelta(seconds=int(base ** new_attempts))).strftime("%Y-%m-%dT%H:%M:%SZ")
                conn.execute("""
                    UPDATE jobs SET state = 'failed', attempts = ?, error_message = ?, run_at = ?, worker_id = NULL, updated_at = ?
                    WHERE id = ?
                """, (new_attempts, error_msg, run_at, now_str, job_id))
                return "failed"

    def re_enqueue_dlq_job(self, job_id: str) -> bool:
        now_str = get_iso_now()
        with self.get_connection() as conn:
            res = conn.execute("""
                UPDATE jobs SET state = 'pending', attempts = 0, error_message = NULL, worker_id = NULL, run_at = NULL, updated_at = ?
                WHERE id = ? AND state = 'dead'
            """, (now_str, job_id))
            return res.rowcount > 0

    def get_jobs_by_state(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            if state:
                rows = conn.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at ASC", (state,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM jobs ORDER BY created_at ASC").fetchall()
            return [dict(r) for r in rows]

    def get_job_counts(self) -> Dict[str, int]:
        counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}
        with self.get_connection() as conn:
            for r in conn.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state").fetchall():
                if r["state"] in counts:
                    counts[r["state"]] = r["count"]
        return counts

    def register_worker(self, worker_id: str, pid: int):
        now = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO workers (worker_id, pid, status, started_at, heartbeat_at)
                VALUES (?, ?, 'active', ?, ?)
            """, (worker_id, pid, now, now))

    def worker_heartbeat(self, worker_id: str):
        now = get_iso_now()
        with self.get_connection() as conn:
            conn.execute("UPDATE workers SET heartbeat_at = ? WHERE worker_id = ?", (now, worker_id))

    def unregister_worker(self, worker_id: str):
        with self.get_connection() as conn:
            conn.execute("DELETE FROM workers WHERE worker_id = ?", (worker_id,))

    def set_workers_stopping(self):
        with self.get_connection() as conn:
            conn.execute("UPDATE workers SET status = 'stopping'")

    def get_active_workers(self) -> List[Dict[str, Any]]:
        with self.get_connection() as conn:
            rows = conn.execute("SELECT * FROM workers WHERE status != 'stopped'").fetchall()
            return [dict(r) for r in rows]
