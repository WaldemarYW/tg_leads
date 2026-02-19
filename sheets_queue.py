import json
import os
import random
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class SheetsEvent:
    id: str
    created_at: float
    event_type: str
    payload: Dict[str, Any]
    attempts: int = 0
    next_attempt_at: float = 0.0
    last_error: str = ""


class SheetsQueueStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._ensure_db()

    def _connect(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_db(self):
        base_dir = os.path.dirname(self.path)
        if base_dir:
            os.makedirs(base_dir, exist_ok=True)
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sheet_events (
                        id TEXT PRIMARY KEY,
                        created_at REAL NOT NULL,
                        event_type TEXT NOT NULL,
                        payload TEXT NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        next_attempt_at REAL NOT NULL,
                        last_error TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_sheet_events_ready ON sheet_events(next_attempt_at, created_at)"
                )
                conn.commit()

    def enqueue(self, event_type: str, payload: Dict[str, Any]) -> str:
        event_id = str(uuid.uuid4())
        now = time.time()
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO sheet_events (id, created_at, event_type, payload, attempts, next_attempt_at, last_error)
                    VALUES (?, ?, ?, ?, 0, ?, '')
                    """,
                    (event_id, now, event_type, json.dumps(payload, ensure_ascii=False), now),
                )
                conn.commit()
        return event_id

    def fetch_batch(self, limit: int, now_ts: Optional[float] = None) -> List[SheetsEvent]:
        now_ts = now_ts if now_ts is not None else time.time()
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, created_at, event_type, payload, attempts, next_attempt_at, last_error
                    FROM sheet_events
                    WHERE next_attempt_at <= ?
                    ORDER BY created_at ASC
                    LIMIT ?
                    """,
                    (now_ts, int(limit)),
                ).fetchall()
        result: List[SheetsEvent] = []
        for row in rows:
            result.append(
                SheetsEvent(
                    id=row["id"],
                    created_at=float(row["created_at"]),
                    event_type=row["event_type"],
                    payload=json.loads(row["payload"] or "{}"),
                    attempts=int(row["attempts"] or 0),
                    next_attempt_at=float(row["next_attempt_at"] or 0),
                    last_error=row["last_error"] or "",
                )
            )
        return result

    def mark_done(self, event_id: str):
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM sheet_events WHERE id = ?", (event_id,))
                conn.commit()

    def mark_retry(self, event_id: str, attempts: int, backoff_sec: float, error: str):
        next_at = time.time() + max(0.0, float(backoff_sec))
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    UPDATE sheet_events
                    SET attempts = ?, next_attempt_at = ?, last_error = ?
                    WHERE id = ?
                    """,
                    (int(attempts), next_at, (error or "")[:1000], event_id),
                )
                conn.commit()

    def stats(self) -> Dict[str, Optional[float]]:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*) AS cnt, MIN(created_at) AS oldest
                    FROM sheet_events
                    """
                ).fetchone()
        pending = int(row["cnt"] or 0)
        oldest = float(row["oldest"]) if row["oldest"] is not None else None
        oldest_age_sec = (time.time() - oldest) if oldest is not None else None
        return {"pending": pending, "oldest_age_sec": oldest_age_sec}


def calculate_backoff_sec(attempts: int, hard_error: bool = False) -> float:
    if hard_error:
        base = 300.0
    else:
        schedule = [1.0, 3.0, 10.0, 30.0, 60.0, 120.0, 300.0]
        idx = min(max(0, attempts - 1), len(schedule) - 1)
        base = schedule[idx]
    jitter = random.uniform(0.0, 1.0)
    return min(300.0, base + jitter)
