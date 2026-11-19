"""Standard-library HTTPS uploader for already committed local queue tasks.

Every uploader owns its SQLite connection and its OS lock. HTTP responses never
get interpolated into logs: an upstream error page may contain credentials or
the uploaded scientific snapshot. Only validated committed receipts acknowledge
tasks; timeouts and response loss leave the original bytes and identity intact.
"""

from __future__ import annotations

import http.client
import json
import logging
import math
import os
import random
import re
import socket
import ssl
import stat
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

from msrefine_protocol import (
    RECEIPT_IDENTITY_FIELDS, ProtocolError, build_upload_request, canonical_json,
    validate_receipt,
)

from .collector import _StateLock
from .queue import UploadQueue

MAX_RECEIPT_BYTES = 64 * 1024


@dataclass(frozen=True)
class UploadConfig:
    endpoint: str
    token: str = field(repr=False)
    timeout_seconds: float = 15
    poll_interval: float = 5
    retry_base_seconds: float = 5
    retry_max_seconds: float = 300
    data_retry_seconds: float = 3600
    auth_retry_seconds: float = 300
    batch_size: int = 100
    allow_loopback_http: bool = False
    ca_file: Path | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.endpoint, str):
            raise ValueError("上传 endpoint 必须是 URL 字符串")
        try:
            parsed = urlsplit(self.endpoint)
            port = parsed.port
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("上传 endpoint 不是有效 URL") from exc
        if (not parsed.hostname or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or not parsed.path.startswith("/")
                or any(ord(c) <= 32 or ord(c) == 127 for c in self.endpoint)
                or (port is not None and not 1 <= port <= 65535)):
            raise ValueError("上传 endpoint 必须包含主机及路径，不能携带凭据、查询串或片段")
        if not isinstance(self.allow_loopback_http, bool):
            raise ValueError("allow_loopback_http 必须是布尔值")
        if parsed.scheme != "https":
            if not (parsed.scheme == "http" and self.allow_loopback_http
                    and parsed.hostname in {"localhost", "127.0.0.1", "::1"}):
                raise ValueError("上传必须使用 HTTPS；HTTP 仅可显式用于本机回环地址")
        if not isinstance(self.token, str) or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", self.token) is None:
            raise ValueError("采集凭据必须是非空有效 Bearer token，不能包含空格或控制字符")
        for key in ("timeout_seconds", "poll_interval", "retry_base_seconds", "retry_max_seconds",
                    "data_retry_seconds", "auth_retry_seconds"):
            value = getattr(self, key)
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value) or value <= 0):
                raise ValueError(f"{key} 必须是大于零的有限数")
        if self.timeout_seconds > 120:
            raise ValueError("timeout_seconds 不得超过 120 秒")
        if self.retry_max_seconds < self.retry_base_seconds:
            raise ValueError("retry_max_seconds 不得小于 retry_base_seconds")
        if isinstance(self.batch_size, bool) or not isinstance(self.batch_size, int) or not 1 <= self.batch_size <= 1000:
            raise ValueError("batch_size 必须是 1～1000 的整数")
        if self.ca_file is not None and not isinstance(self.ca_file, Path):
            raise ValueError("ca_file 必须是证书文件路径")


def load_upload_config(path: str | Path) -> UploadConfig:
    """Read an explicit config; inline tokens require private POSIX permissions.

    Windows deployments must restrict this file using the account's NTFS ACL;
    POSIX permission bits do not represent those Windows access controls.
    Prefer token_env so a shareable config never contains the actual token.
    """
    config_path = Path(path).expanduser().resolve()
    try:
        with config_path.open("rb") as handle:
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size > 64 * 1024:
                raise ValueError("上传配置必须是至多 64 KiB 的普通文件")
            raw = handle.read(64 * 1024 + 1)
        values = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("上传配置必须是有效 UTF-8 JSON") from exc
    if not isinstance(values, dict):
        raise ValueError("上传配置必须是 JSON 对象")
    allowed = set(UploadConfig.__dataclass_fields__) | {"token_env"}
    if set(values) - allowed:
        raise ValueError("上传配置包含不支持的字段")
    if "token" in values and "token_env" in values:
        raise ValueError("上传配置只能选择 token 或 token_env")
    if "token" in values:
        if os.name != "nt" and stat.S_IMODE(info.st_mode) & 0o077:
            raise ValueError("包含 token 的配置须限制为当前用户可读（例如 chmod 600）")
    else:
        env_name = values.pop("token_env", "MSREFINE_UPLOAD_TOKEN")
        if not isinstance(env_name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name) is None:
            raise ValueError("token_env 必须是有效环境变量名")
        values["token"] = os.environ.get(env_name)
    if "endpoint" not in values:
        raise ValueError("上传配置缺少 endpoint")
    if values.get("ca_file") is not None:
        if not isinstance(values["ca_file"], str) or not values["ca_file"].strip():
            raise ValueError("ca_file 必须是非空证书路径")
        certificate = Path(values["ca_file"]).expanduser()
        values["ca_file"] = (config_path.parent / certificate).resolve() if not certificate.is_absolute() else certificate.resolve()
    return UploadConfig(**values)


def _unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("重复 JSON 字段")
        result[key] = value
    return result


class _UploadFailure(Exception):
    def __init__(self, kind: str, message: str, *, status: int | None = None,
                 retry_after: float | None = None):
        self.kind, self.message = kind, message
        self.status = status if isinstance(status, int) and 100 <= status <= 599 else None
        self.retry_after = retry_after
        super().__init__(message)


class Uploader:
    """Upload using one thread's connection; pass a stop Event to ``run``.

    ``stop_network`` alone is thread safe and may be called by the main thread
    during shutdown. All queue operations remain on the uploader's own thread.
    """

    def __init__(self, state_dir: str | Path, config: UploadConfig, *,
                 clock: Callable[[], float] = time.time,
                 jitter: Callable[[], float] = random.random):
        self.state_dir = Path(state_dir).expanduser().resolve()
        self.config = config
        self._clock, self._jitter = clock, jitter
        self._closed = False
        self._connection_guard = threading.Lock()
        self._active_connection = None
        self._active_socket = None
        self._request_stopped = threading.Event()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._lock = _StateLock(self.state_dir / "uploader.lock")
        except RuntimeError as exc:
            raise RuntimeError("该状态目录已有上传器运行") from exc
        try:
            if not (self.state_dir / "queue.sqlite3").is_file():
                raise ValueError("本地队列尚未建立；请先启动采集器")
            self.queue = UploadQueue(self.state_dir / "queue.sqlite3")
            self.queue.enable_upload_retry()
            self._tls_context = ssl.create_default_context(
                cafile=None if config.ca_file is None else str(config.ca_file)
            )
            self.logger = logging.getLogger(f"msrefine.uploader.{id(self)}")
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False
            self._handler = RotatingFileHandler(self.state_dir / "uploader.log", maxBytes=5 * 1024 * 1024,
                                               backupCount=3, encoding="utf-8")
            self._handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(self._handler)
        except BaseException:
            if hasattr(self, "queue"):
                self.queue.close()
            self._lock.close()
            raise

    def __enter__(self) -> Uploader:
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    def stop_network(self) -> None:
        self._request_stopped.set()
        with self._connection_guard:
            connection = self._active_connection
            active_socket = self._active_socket
            if active_socket is not None:
                try:
                    active_socket.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            if connection is not None:
                connection.close()

    def close(self) -> None:
        if not self._closed:
            self.stop_network()
            self.queue.close()
            self.logger.removeHandler(self._handler)
            self._handler.close()
            self._lock.close()
            self._closed = True

    def _retry_after(self, value: str | None) -> float | None:
        if value is None:
            return None
        try:
            seconds = float(value)
            if not math.isfinite(seconds):
                return None
        except ValueError:
            try:
                timestamp = parsedate_to_datetime(value)
                if timestamp.tzinfo is None:
                    return None
                seconds = timestamp.timestamp() - self._clock()
            except (ValueError, TypeError, OverflowError):
                return None
        return min(max(seconds, 0), 3600)

    def _post(self, request: dict) -> dict:
        parsed = urlsplit(self.config.endpoint)
        if parsed.scheme == "https":
            connection = http.client.HTTPSConnection(parsed.hostname, parsed.port,
                timeout=self.config.timeout_seconds, context=self._tls_context)
        else:
            connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=self.config.timeout_seconds)
        # A real wall-clock deadline also interrupts a peer that trickles bytes
        # before each socket timeout. It never bypasses certificate checks.
        self._request_stopped.clear()
        with self._connection_guard:
            self._active_connection = connection
        deadline = threading.Timer(self.config.timeout_seconds, self.stop_network)
        deadline.daemon = True
        deadline.start()
        try:
            connection.connect()
            with self._connection_guard:
                self._active_socket = connection.sock
            if self._request_stopped.is_set():
                raise TimeoutError
            connection.request("POST", parsed.path, body=canonical_json(request), headers={
                "Authorization": "Bearer " + self.config.token,
                "Content-Type": "application/json", "Accept": "application/json",
                "Idempotency-Key": request["idempotency_key"],
            })
            response = connection.getresponse()
            status = response.status
            if status in {401, 403}:
                raise _UploadFailure("authentication", "采集凭据或仪器归属验证失败；暂停后重试", status=status)
            if status == 409:
                raise _UploadFailure("conflict", "云端任务身份或内容冲突；请检查原队列和服务端记录", status=status)
            if status == 429:
                raise _UploadFailure("rate_limit", "云端限流；保留待上传并退避重试", status=status,
                                     retry_after=self._retry_after(response.getheader("Retry-After")))
            if 500 <= status <= 599:
                raise _UploadFailure("server", "云端暂时不可用；保留待上传并退避重试", status=status,
                                     retry_after=self._retry_after(response.getheader("Retry-After")))
            if 400 <= status <= 499:
                raise _UploadFailure("validation", "云端拒绝此任务；保留原数据，其他任务继续上传", status=status)
            # http.client does not follow any redirect, so Authorization cannot
            # be forwarded to a different origin (including 307/308 responses).
            if status != 200:
                raise _UploadFailure("protocol", "云端未返回 HTTP 200 已提交回执", status=status)
            if response.getheader("Content-Type", "").split(";", 1)[0].strip().lower() != "application/json":
                raise _UploadFailure("protocol", "云端成功响应不是 JSON 回执", status=status)
            raw = response.read(MAX_RECEIPT_BYTES + 1)
            if len(raw) > MAX_RECEIPT_BYTES:
                raise _UploadFailure("protocol", "云端回执超过 64 KiB 上限", status=status)
            try:
                receipt = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                validate_receipt(receipt, request)
            except (ValueError, UnicodeError, TypeError, OverflowError, RecursionError):
                raise _UploadFailure("protocol", "云端回执缺失、无效或与当前任务不匹配", status=status) from None
            # Never persist unrelated upstream fields, even if a service or
            # proxy accidentally echoes Authorization into the response.
            allowed = set(RECEIPT_IDENTITY_FIELDS) | {
                "receipt_version", "status", "request_sha256", "experiment_id",
                "result_id", "committed_at", "duplicate",
            }
            return {key: value for key, value in receipt.items() if key in allowed}
        except (OSError, http.client.HTTPException, TimeoutError):
            raise _UploadFailure("network", "网络或 TLS 校验失败；保留待上传，请检查地址、证书和网络") from None
        finally:
            deadline.cancel()
            deadline.join()
            with self._connection_guard:
                self._active_connection = None
                self._active_socket = None
            connection.close()

    def _delay(self, kind: str, attempts: int, retry_after: float | None) -> float:
        if kind == "authentication":
            return self.config.auth_retry_seconds
        if kind in {"validation", "conflict", "local_data"}:
            return self.config.data_retry_seconds
        exponent = min(max(attempts, 0), 30)
        base = min(self.config.retry_max_seconds, self.config.retry_base_seconds * (2 ** exponent))
        delay = min(self.config.retry_max_seconds, base * (0.8 + 0.4 * self._jitter()))
        return max(delay, retry_after or 0)

    def run_once(self, stop_event: threading.Event | None = None) -> dict:
        """Try one due batch, preserving delayed tasks across process restarts."""
        if self._closed:
            raise RuntimeError("上传器已关闭")
        stats = {"attempted": 0, "uploaded": 0, "failed": 0, "auth_paused": False}
        if self.queue.upload_auth_retry_at() > self._clock():
            stats["auth_paused"] = True
            return stats
        for task_id in self.queue.ready_task_ids(now=self._clock(), limit=self.config.batch_size):
            if stop_event is not None and stop_event.is_set():
                break
            task = self.queue.get_task(task_id)
            if task is None or task["status"] != "pending":
                continue
            stats["attempted"] += 1
            try:
                try:
                    request = build_upload_request(self.queue.collector_id, task)
                except (ProtocolError, TypeError, ValueError, OverflowError, RecursionError):
                    raise _UploadFailure("local_data", "本地任务未通过上传契约校验；保留原队列，其他任务继续") from None
                receipt = self._post(request)
                self.queue.mark_uploaded(task_id, receipt=receipt)
                stats["uploaded"] += 1
                self.logger.info("上传已确认 task=%s result=%s duplicate=%s", task_id, receipt["result_id"], receipt["duplicate"])
            except _UploadFailure as exc:
                next_at = self._clock() + self._delay(exc.kind, task["attempt_count"], exc.retry_after)
                self.queue.record_upload_failure(task_id, exc.message, error_kind=exc.kind,
                    next_attempt_at=next_at, http_status=exc.status,
                    authentication_pause=exc.kind == "authentication")
                stats["failed"] += 1
                self.logger.warning("上传暂未确认 task=%s kind=%s http=%s retry_at=%s；%s", task_id,
                    exc.kind, exc.status, datetime.fromtimestamp(next_at, timezone.utc).isoformat(), exc.message)
                if exc.kind == "authentication":
                    stats["auth_paused"] = True
                    break
        return stats

    def run(self, stop_event: threading.Event | None = None) -> None:
        event = stop_event if stop_event is not None else threading.Event()
        self.logger.info("启动上传器；仅在校验已提交回执后确认本地任务")
        while not event.is_set():
            self.run_once(event)
            event.wait(self.config.poll_interval)
        self.logger.info("上传器停止；队列=%s", self.queue.counts())
