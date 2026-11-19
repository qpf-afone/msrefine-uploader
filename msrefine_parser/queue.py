"""Durable local upload tasks; this module performs no network requests.

An experiment is identified by its normalized summary path and original data-file
name within this collector, not by a possibly repeated file name or the parser's
human-readable ``run_id``. Reusing an output directory for another raw file must
not append that experiment's results to the preceding experiment.
Each task stores both its complete JSON payload and the exact original bytes in
one SQLite transaction. Pending tasks survive process restarts and source-file
changes or removal.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


QUEUE_SCHEMA_VERSION = 1
UPLOAD_RETRY_SCHEMA_VERSION = 1

# An optional extension, deliberately separate from the original schema-1
# task identities and columns. Old queues are upgraded only when uploading is
# enabled, and older collectors can continue to read/enqueue their tasks.
_UPLOAD_RETRY_SQL = (
    "CREATE TABLE upload_retry ("
    "task_id TEXT PRIMARY KEY REFERENCES tasks(task_id), "
    "next_attempt_at REAL NOT NULL CHECK(next_attempt_at >= 0), "
    "error_kind TEXT NOT NULL, last_http_status INTEGER, "
    "last_attempt_at TEXT NOT NULL)"
)


class QueueError(RuntimeError):
    """The queue is closed or its database format is unsupported."""


_TABLE_COLUMNS = {
    "queue_metadata": {"key", "value"},
    "experiments": {"source_path", "source_file_name", "experiment_key"},
    "tasks": {
        "task_id", "experiment_key", "idempotency_key", "source_path",
        "source_sha256", "parser_version", "schema_version", "status",
        "created_at", "attempt_count", "last_error", "payload", "source_bytes",
        "receipt", "uploaded_at",
    },
}

_TASK_COLUMNS = (
    "task_id, experiment_key, idempotency_key, source_path, source_sha256, "
    "parser_version, schema_version, status, created_at, attempt_count, "
    "last_error, payload, receipt, uploaded_at"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _json_text(value: dict) -> str:
    # Reject non-JSON data and NaN before entering any write transaction.
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":"))


class UploadQueue:
    """A local SQLite queue with only ``pending`` and ``uploaded`` states.

    Each instance uses one connection on its creating thread. Multiple instances
    or processes may share a database; SQLite serializes enqueue transactions.
    ``mapped`` metric metadata and business alarms are outside this module.
    """

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path).expanduser().resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._closed = False
        self._connection = sqlite3.connect(
            str(self.db_path), timeout=5.0, isolation_level=None,
        )
        self._connection.row_factory = sqlite3.Row
        try:
            self._connection.execute("PRAGMA busy_timeout = 5000")
            # Check before changing journal mode: an unrelated/incompatible DB
            # must not be converted, initialized or silently upgraded.
            self._check_database_format()
            mode = self._connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise QueueError("本地队列数据库无法启用 WAL 日志模式")
            self._connection.execute("PRAGMA synchronous = FULL")
            self._connection.execute("PRAGMA foreign_keys = ON")
            with self._transaction():
                # Recheck under the write lock in case another process created
                # this initially empty database after the first read.
                version = self._check_database_format()
                if version == 0:
                    self._create_schema()
                row = self._connection.execute(
                    "SELECT value FROM queue_metadata WHERE key = 'collector_id'"
                ).fetchone()
                self.collector_id = row[0]
        except BaseException:
            self._connection.close()
            self._closed = True
            raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise QueueError("本地上传队列已关闭")

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._ensure_open()
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise

    def _check_database_format(self) -> int:
        version = self._connection.execute("PRAGMA user_version").fetchone()[0]
        schema_objects = list(self._connection.execute(
            "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ))
        tables = {row[1] for row in schema_objects if row[0] == "table"}
        if version == 0:
            if schema_objects:
                raise QueueError("数据库未标记队列版本且已有结构，拒绝修改")
            return version
        if version != QUEUE_SCHEMA_VERSION:
            raise QueueError(
                f"不支持队列数据库版本 {version}；当前支持 {QUEUE_SCHEMA_VERSION}"
            )
        if not set(_TABLE_COLUMNS) <= tables:
            raise QueueError("队列数据库缺少必要数据表，拒绝修改")
        for table, required_columns in _TABLE_COLUMNS.items():
            # Table names are fixed constants, never input strings.
            columns = {
                row[1] for row in self._connection.execute(f"PRAGMA table_info({table})")
            }
            if columns != required_columns:
                raise QueueError(f"队列数据表 {table} 结构不兼容，拒绝修改")
        row = self._connection.execute(
            "SELECT value FROM queue_metadata WHERE key = 'collector_id'"
        ).fetchone()
        try:
            if row is None:
                raise ValueError("missing collector id")
            uuid.UUID(row[0])
        except (ValueError, TypeError, AttributeError) as exc:
            raise QueueError("队列缺少有效 collector_id，拒绝重新生成身份") from exc
        self._check_upload_retry_format(schema_objects)
        return version

    def _check_upload_retry_format(self, schema_objects=None) -> bool:
        if schema_objects is None:
            schema_objects = list(self._connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ))
        objects = [(row[0], row[1]) for row in schema_objects if row[1] == "upload_retry"]
        marker = self._connection.execute(
            "SELECT value FROM queue_metadata WHERE key = 'upload_retry_schema_version'"
        ).fetchone()
        if not objects and marker is None:
            return False
        if objects != [("table", "upload_retry")] or marker is None or marker[0] != str(UPLOAD_RETRY_SCHEMA_VERSION):
            raise QueueError("上传重试表或版本标记不兼容，拒绝修改")
        sql = self._connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'upload_retry'"
        ).fetchone()[0]
        if re.sub(r"\s+", "", sql).lower() != re.sub(r"\s+", "", _UPLOAD_RETRY_SQL).lower():
            raise QueueError("上传重试表结构不兼容，拒绝修改")
        return True

    def enable_upload_retry(self) -> None:
        """Add the owned upload extension without rewriting schema-1 tasks."""
        with self._transaction():
            if not self._check_upload_retry_format():
                self._connection.execute(_UPLOAD_RETRY_SQL)
                self._connection.execute(
                    "INSERT INTO queue_metadata(key, value) VALUES ('upload_retry_schema_version', ?)",
                    (str(UPLOAD_RETRY_SCHEMA_VERSION),),
                )

    def ready_task_ids(self, *, now: float, limit: int = 100) -> list[str]:
        """Select due pending tasks in SQL, so one bad task cannot starve others."""
        self._ensure_open()
        self._validate_retry_time(now)
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit 必须是正整数")
        if not self._check_upload_retry_format():
            raise QueueError("尚未启用上传重试扩展")
        return [row[0] for row in self._connection.execute(
            "SELECT t.task_id FROM tasks t LEFT JOIN upload_retry r ON r.task_id = t.task_id "
            "WHERE t.status = 'pending' AND (r.task_id IS NULL OR r.next_attempt_at <= ?) "
            "ORDER BY t.created_at, t.rowid LIMIT ?", (now, limit),
        )]

    @staticmethod
    def _validate_retry_time(value: float) -> None:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("重试时间必须是非负有限的 Unix 时间")

    def retry_state(self, task_id: str) -> dict | None:
        self._ensure_open()
        if not self._check_upload_retry_format():
            return None
        row = self._connection.execute("SELECT * FROM upload_retry WHERE task_id = ?", (task_id,)).fetchone()
        return None if row is None else dict(row)

    def upload_auth_retry_at(self) -> float:
        self._ensure_open()
        row = self._connection.execute(
            "SELECT value FROM queue_metadata WHERE key = 'upload_auth_retry_at'"
        ).fetchone()
        try:
            result = 0.0 if row is None else float(row[0])
            self._validate_retry_time(result)
            return result
        except (ValueError, TypeError) as exc:
            raise QueueError("上传认证重试时间损坏，拒绝重置") from exc

    def record_upload_failure(self, task_id: str, message: str, *, error_kind: str,
                              next_attempt_at: float, http_status: int | None = None,
                              authentication_pause: bool = False) -> None:
        """Atomically retain a failure, its backoff, and optional account pause."""
        self._validate_retry_time(next_attempt_at)
        if error_kind not in {"network", "server", "rate_limit", "authentication", "validation", "conflict", "protocol", "local_data"}:
            raise ValueError("未知上传错误类别")
        if not isinstance(message, str) or len(message) > 500:
            raise ValueError("上传错误必须是至多 500 字符的安全说明")
        if http_status is not None and (isinstance(http_status, bool) or not isinstance(http_status, int) or not 100 <= http_status <= 599):
            raise ValueError("无效 HTTP 状态码")
        with self._transaction():
            if not self._check_upload_retry_format():
                raise QueueError("尚未启用上传重试扩展")
            row = self._connection.execute("SELECT status FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row[0] != "pending":
                return
            self._connection.execute(
                "UPDATE tasks SET attempt_count = attempt_count + 1, last_error = ? WHERE task_id = ? AND status = 'pending'",
                (message, task_id),
            )
            self._connection.execute(
                "INSERT INTO upload_retry(task_id, next_attempt_at, error_kind, last_http_status, last_attempt_at) "
                "VALUES (?, ?, ?, ?, ?) ON CONFLICT(task_id) DO UPDATE SET "
                "next_attempt_at = excluded.next_attempt_at, error_kind = excluded.error_kind, "
                "last_http_status = excluded.last_http_status, last_attempt_at = excluded.last_attempt_at",
                (task_id, next_attempt_at, error_kind, http_status, _utc_now()),
            )
            if authentication_pause:
                self._connection.execute(
                    "INSERT INTO queue_metadata(key, value) VALUES ('upload_auth_retry_at', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (str(next_attempt_at),),
                )

    def _create_schema(self) -> None:
        self._connection.execute(
            "CREATE TABLE queue_metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._connection.execute(
            "CREATE TABLE experiments ("
            "source_path TEXT NOT NULL, source_file_name TEXT NOT NULL, "
            "experiment_key TEXT NOT NULL UNIQUE, "
            "PRIMARY KEY(source_path, source_file_name))"
        )
        self._connection.execute(
            "CREATE TABLE tasks ("
            "task_id TEXT PRIMARY KEY, "
            "experiment_key TEXT NOT NULL REFERENCES experiments(experiment_key), "
            "idempotency_key TEXT NOT NULL UNIQUE, source_path TEXT NOT NULL, "
            "source_sha256 TEXT NOT NULL, parser_version TEXT NOT NULL, "
            "schema_version TEXT NOT NULL, "
            "status TEXT NOT NULL CHECK(status IN ('pending', 'uploaded')), "
            "created_at TEXT NOT NULL, "
            "attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0), "
            "last_error TEXT, payload TEXT NOT NULL, source_bytes BLOB NOT NULL, "
            "receipt TEXT, uploaded_at TEXT, "
            "UNIQUE(experiment_key, source_sha256, parser_version, schema_version))"
        )
        self._connection.execute(
            "CREATE INDEX tasks_pending_order ON tasks(status, created_at)"
        )
        self._connection.execute(
            "INSERT INTO queue_metadata(key, value) VALUES ('collector_id', ?)",
            (str(uuid.uuid4()),),
        )
        self._connection.execute(f"PRAGMA user_version = {QUEUE_SCHEMA_VERSION}")

    def __enter__(self) -> UploadQueue:
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            self._connection.close()
            self._closed = True

    def enqueue(self, record: dict, *, source_bytes: bytes) -> dict:
        """Atomically retain a parsed record and its verified source bytes.

        Returns identity keys and ``created``. Repeating the same normalized
        path/data-file-name/hash/parser-version/schema-version preserves the original payload,
        task identity and uploaded status, including after reopening the queue.
        Invalid inputs raise ValueError (JSON serialization may raise TypeError).
        """
        self._ensure_open()
        if not isinstance(record, dict) or not isinstance(record.get("source"), dict):
            raise ValueError("record 必须包含 source 对象")
        source = record["source"]
        source_path = source.get("path")
        source_file_name = record.get("source_file_name")
        expected_sha = source.get("sha256")
        if not isinstance(source_path, str) or not source_path.strip():
            raise ValueError("source.path 必须是非空路径字符串")
        if not isinstance(source_file_name, str) or not source_file_name.strip():
            raise ValueError("source_file_name 必须是非空原始数据文件名字符串")
        if not isinstance(expected_sha, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha):
            raise ValueError("source.sha256 必须是 64 位十六进制 SHA-256")
        if not isinstance(source_bytes, bytes):
            raise ValueError("source_bytes 必须是原始 bytes")
        source_sha256 = hashlib.sha256(source_bytes).hexdigest()
        if source_sha256 != expected_sha.lower():
            raise ValueError("source_bytes 的 SHA-256 与解析记录不符，未入队")
        versions = []
        for field in ("parser_version", "schema_version"):
            value = record.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} 必须是非空字符串")
            versions.append(value)
        parser_version, schema_version = versions
        normalized_path = os.path.normcase(str(Path(source_path).expanduser().resolve()))
        # This may itself be an absolute Windows raw-file path. Preserve it
        # exactly; resolving it as a local path or taking only its basename
        # would merge distinct experiments or change cross-platform meaning.
        payload = _json_text(record)

        with self._transaction():
            self._connection.execute(
                "INSERT OR IGNORE INTO experiments(source_path, source_file_name, experiment_key) "
                "VALUES (?, ?, ?)",
                (normalized_path, source_file_name, str(uuid.uuid4())),
            )
            experiment_key = self._connection.execute(
                "SELECT experiment_key FROM experiments "
                "WHERE source_path = ? AND source_file_name = ?",
                (normalized_path, source_file_name),
            ).fetchone()[0]
            identity = (experiment_key, source_sha256, parser_version, schema_version)
            existing = self._connection.execute(
                "SELECT task_id, experiment_key, idempotency_key FROM tasks "
                "WHERE experiment_key = ? AND source_sha256 = ? "
                "AND parser_version = ? AND schema_version = ?",
                identity,
            ).fetchone()
            if existing is not None:
                return {**dict(existing), "created": False}
            task_id = str(uuid.uuid4())
            key_material = json.dumps(
                [self.collector_id, *identity], ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            idempotency_key = hashlib.sha256(key_material).hexdigest()
            self._connection.execute(
                "INSERT INTO tasks (task_id, experiment_key, idempotency_key, "
                "source_path, source_sha256, parser_version, schema_version, "
                "status, created_at, payload, source_bytes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)",
                (task_id, experiment_key, idempotency_key, normalized_path,
                 source_sha256, parser_version, schema_version, _utc_now(),
                 payload, sqlite3.Binary(source_bytes)),
            )
            return {
                "task_id": task_id, "created": True,
                "experiment_key": experiment_key, "idempotency_key": idempotency_key,
            }

    @staticmethod
    def _task_dict(row: sqlite3.Row) -> dict:
        task = dict(row)
        task["payload"] = json.loads(task["payload"])
        task["receipt"] = None if task["receipt"] is None else json.loads(task["receipt"])
        return task

    def list_tasks(self, status: str | None = "pending", limit: int = 100) -> list[dict]:
        """List tasks without BLOBs; ``status=None`` includes both states."""
        self._ensure_open()
        if status not in {None, "pending", "uploaded"}:
            raise ValueError("status 只支持 pending、uploaded 或 None")
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ValueError("limit 必须是非负整数")
        query = f"SELECT {_TASK_COLUMNS} FROM tasks"
        parameters: list = []
        if status is not None:
            query += " WHERE status = ?"
            parameters.append(status)
        query += " ORDER BY created_at, rowid LIMIT ?"
        parameters.append(limit)
        return [self._task_dict(row) for row in self._connection.execute(query, parameters)]

    def get_task(self, task_id: str) -> dict | None:
        """Retrieve one complete task, including its original ``source_bytes``."""
        self._ensure_open()
        row = self._connection.execute(
            f"SELECT {_TASK_COLUMNS}, source_bytes FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        return None if row is None else self._task_dict(row)

    def counts(self) -> dict:
        self._ensure_open()
        result = {"pending": 0, "uploaded": 0}
        result.update({
            row[0]: row[1] for row in self._connection.execute(
                "SELECT status, COUNT(*) FROM tasks GROUP BY status"
            )
        })
        return result

    def mark_uploaded(self, task_id: str, *, receipt: dict | None = None) -> None:
        """Record a future server acknowledgement; keep the first receipt."""
        if receipt is not None and not isinstance(receipt, dict):
            raise ValueError("receipt 必须是对象或 None")
        receipt_text = None if receipt is None else _json_text(receipt)
        with self._transaction():
            row = self._connection.execute(
                "SELECT status FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row[0] == "pending":
                self._connection.execute(
                    "UPDATE tasks SET status = 'uploaded', receipt = ?, uploaded_at = ? "
                    "WHERE task_id = ? AND status = 'pending'",
                    (receipt_text, _utc_now(), task_id),
                )
                if self._check_upload_retry_format():
                    self._connection.execute("DELETE FROM upload_retry WHERE task_id = ?", (task_id,))

    def record_failure(self, task_id: str, message: str) -> None:
        """Increment attempts for pending tasks; ignore late uploaded failures."""
        if not isinstance(message, str):
            raise ValueError("message 必须是字符串")
        with self._transaction():
            row = self._connection.execute(
                "SELECT status FROM tasks WHERE task_id = ?", (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            self._connection.execute(
                "UPDATE tasks SET attempt_count = attempt_count + 1, last_error = ? "
                "WHERE task_id = ? AND status = 'pending'",
                (message, task_id),
            )
