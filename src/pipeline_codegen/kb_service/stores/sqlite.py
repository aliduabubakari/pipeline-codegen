"""SQLite metadata store implementation for KB service."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


class SQLiteMetadataStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(str(self._db_path))
        con.row_factory = sqlite3.Row
        return con

    def _init_db(self) -> None:
        with self._connect() as con:
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS kb_packs (
                    pack_id TEXT PRIMARY KEY,
                    target TEXT NOT NULL,
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    object_key TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    source_count INTEGER NOT NULL,
                    validation_errors TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    promoted_at TEXT
                );

                CREATE TABLE IF NOT EXISTS kb_active_packs (
                    target TEXT NOT NULL,
                    version TEXT NOT NULL,
                    pack_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (target, version)
                );

                CREATE TABLE IF NOT EXISTS kb_backfill_jobs (
                    job_id TEXT PRIMARY KEY,
                    target TEXT NOT NULL,
                    version TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error TEXT,
                    pack_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def create_backfill_job(self, job_id: str, target: str, version: str) -> None:
        now = _utc_now()
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO kb_backfill_jobs (job_id, target, version, status, error, pack_id, created_at, updated_at)
                VALUES (?, ?, ?, 'queued', NULL, NULL, ?, ?)
                """,
                (job_id, target, version, now, now),
            )

    def update_backfill_job(
        self,
        job_id: str,
        status: str,
        *,
        error: str | None = None,
        pack_id: str | None = None,
    ) -> None:
        now = _utc_now()
        with self._connect() as con:
            con.execute(
                """
                UPDATE kb_backfill_jobs
                SET status = ?, error = ?, pack_id = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (status, error, pack_id, now, job_id),
            )

    def get_backfill_job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT job_id, target, version, status, error, pack_id, created_at, updated_at
                FROM kb_backfill_jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return dict(row)

    def put_pack_record(self, record: dict[str, Any]) -> None:
        now = _utc_now()
        validation_errors = json.dumps(record.get("validation_errors", []))
        promoted_at = now if record.get("status") == "active" else None
        with self._connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO kb_packs
                (pack_id, target, version, status, object_key, confidence, source_count, validation_errors, created_at, promoted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record["pack_id"],
                    record["target"],
                    record["version"],
                    record["status"],
                    record["object_key"],
                    float(record.get("confidence", 0.0)),
                    int(record.get("source_count", 0)),
                    validation_errors,
                    now,
                    promoted_at,
                ),
            )

    def activate_pack(self, target: str, version: str, pack_id: str) -> None:
        now = _utc_now()
        with self._connect() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO kb_active_packs (target, version, pack_id, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (target, version, pack_id, now),
            )

    def get_active_pack(self, target: str, version: str) -> dict[str, Any] | None:
        with self._connect() as con:
            row = con.execute(
                """
                SELECT p.pack_id, p.target, p.version, p.status, p.object_key, p.confidence, p.source_count, p.created_at
                FROM kb_active_packs ap
                JOIN kb_packs p ON p.pack_id = ap.pack_id
                WHERE ap.target = ? AND ap.version = ?
                """,
                (target, version),
            ).fetchone()
        if row is None:
            return None
        return dict(row)
