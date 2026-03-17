"""
Run State Store — persists pipeline run state for the synthesizer and heartbeat.

Local dev: SQLite (file-based, zero-config).
Production: swap backend for DynamoDB by setting WATCH_STATE_STORE=dynamodb.

Schema: run_id (PK), pipeline_id, status, start_time, last_updated, retry_count, expires_at
TTL:    max_job_duration_seconds + 21600 (6 hours). Expired records cleaned on read.
"""
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("watch_obs.state_store")

_DEFAULT_DB_PATH = os.getenv(
    "WATCH_STATE_STORE_PATH",
    str(Path(__file__).parent.parent / "infra" / "run_state.db"),
)
_DEFAULT_MAX_JOB_DURATION_S = int(os.getenv("WATCH_MAX_JOB_DURATION_S", "14400"))  # 4 hours


@dataclass
class RunRecord:
    run_id: str
    pipeline_id: str
    status: str                   # running | success | failed | cancelled
    start_time: float             # unix timestamp
    last_updated: float           # unix timestamp
    retry_count: int = 0
    duration_ms: Optional[int] = None
    expires_at: float = field(init=False)

    def __post_init__(self):
        self.expires_at = self.start_time + _DEFAULT_MAX_JOB_DURATION_S + 21600


class RunStateStore:
    """
    Thread-safe SQLite-backed run state store.
    One instance per process — reuse via module-level singleton `_store`.
    """

    def __init__(self, db_path: str = _DEFAULT_DB_PATH):
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        try:
            conn = self._connect()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id       TEXT PRIMARY KEY,
                    pipeline_id  TEXT NOT NULL,
                    status       TEXT NOT NULL,
                    start_time   REAL NOT NULL,
                    last_updated REAL NOT NULL,
                    retry_count  INTEGER DEFAULT 0,
                    duration_ms  INTEGER,
                    expires_at   REAL NOT NULL
                )
            """)
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning("watch_obs: state store init failed (%s)", exc)

    def upsert(self, record: RunRecord) -> None:
        """Insert or update a run record."""
        try:
            conn = self._connect()
            conn.execute("""
                INSERT INTO pipeline_runs
                    (run_id, pipeline_id, status, start_time, last_updated, retry_count, duration_ms, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status       = excluded.status,
                    last_updated = excluded.last_updated,
                    retry_count  = excluded.retry_count,
                    duration_ms  = excluded.duration_ms
            """, (
                record.run_id, record.pipeline_id, record.status,
                record.start_time, record.last_updated, record.retry_count,
                record.duration_ms, record.expires_at,
            ))
            conn.commit()
            conn.close()
        except Exception as exc:
            logger.warning("watch_obs: state store upsert failed (%s)", exc)

    def get(self, run_id: str) -> Optional[RunRecord]:
        """Fetch a run record. Returns None if not found or expired."""
        try:
            conn = self._connect()
            self._purge_expired(conn)
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE run_id = ?", (run_id,)
            ).fetchone()
            conn.close()
            if row is None:
                return None
            return RunRecord(
                run_id=row["run_id"],
                pipeline_id=row["pipeline_id"],
                status=row["status"],
                start_time=row["start_time"],
                last_updated=row["last_updated"],
                retry_count=row["retry_count"],
                duration_ms=row["duration_ms"],
            )
        except Exception as exc:
            logger.warning("watch_obs: state store get failed (%s)", exc)
            return None

    def _purge_expired(self, conn: sqlite3.Connection) -> None:
        """Delete TTL-expired records."""
        try:
            conn.execute("DELETE FROM pipeline_runs WHERE expires_at < ?", (time.time(),))
            conn.commit()
        except Exception as exc:
            logger.warning("watch_obs: state store purge failed (%s)", exc)


# Module-level singleton — created lazily
_store: Optional[RunStateStore] = None


def get_store(db_path: str = _DEFAULT_DB_PATH) -> RunStateStore:
    global _store
    if _store is None:
        _store = RunStateStore(db_path=db_path)
    return _store
