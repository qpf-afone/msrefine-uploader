"""Versioned, standard-library wire contract shared by uploader and backend.

No database or HTTP dependencies belong here. Parser semantics/version remain
unchanged when transport code changes.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

UPLOAD_SCHEMA_VERSION = "1.0"
RECEIPT_VERSION = "1.0"
CATALOG_VERSION = "msrefine-summary-v1"
SUPPORTED_PARSER_VERSIONS = frozenset({"0.2.0"})
SUPPORTED_SCHEMA_VERSIONS = frozenset({"1.0"})
MAX_SOURCE_BYTES = 16 * 1024 * 1024
MAX_REQUEST_BYTES = 48 * 1024 * 1024

REQUEST_FIELDS = frozenset({
    "upload_schema_version", "catalog_version", "collector_id", "task_id",
    "experiment_key", "idempotency_key", "source_sha256", "parser_version",
    "schema_version", "created_at", "source_bytes_base64", "payload",
})
RECEIPT_IDENTITY_FIELDS = (
    "collector_id", "task_id", "experiment_key", "idempotency_key",
    "source_sha256", "parser_version", "schema_version", "catalog_version",
)


class ProtocolError(ValueError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False,
                          sort_keys=True, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, OverflowError, RecursionError) as exc:
        raise ProtocolError("INVALID_JSON", "请求必须是有限数值组成的有效 UTF-8 JSON") from exc


def request_fingerprint(request: dict) -> str:
    return hashlib.sha256(canonical_json(request)).hexdigest()


def _uuid(value: Any, field: str) -> None:
    try:
        if not isinstance(value, str) or str(uuid.UUID(value)) != value:
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise ProtocolError("INVALID_IDENTITY", f"{field} 必须为规范小写 UUID") from exc


def _sha(value: Any, field: str) -> None:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ProtocolError("INVALID_HASH", f"{field} 必须为 64 位小写十六进制 SHA-256")


def aware_datetime(value: Any, field: str = "created_at") -> datetime:
    try:
        if not isinstance(value, str) or len(value) > 64:
            raise ValueError
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None or result.utcoffset() is None:
            raise ValueError
        return result
    except (ValueError, TypeError, OverflowError) as exc:
        raise ProtocolError("INVALID_TIME", f"{field} 必须为带时区的 ISO 8601 时间") from exc


@dataclass(frozen=True)
class ValidatedUpload:
    request: dict
    source_bytes: bytes
    payload: dict
    request_sha256: str
    created_at: datetime


def build_upload_request(collector_id: str, task: dict) -> dict:
    """Serialize an existing task, preserving its exact original identity."""
    raw = task.get("source_bytes")
    if not isinstance(raw, bytes):
        raise ProtocolError("MISSING_SOURCE_BYTES", "上传必须读取原队列中的 source_bytes")
    if not raw or len(raw) > MAX_SOURCE_BYTES:
        raise ProtocolError("SOURCE_TOO_LARGE", "原文件为空或超过 16 MiB 上限")
    try:
        request = {field: task[field] for field in (
            "task_id", "experiment_key", "idempotency_key", "source_sha256",
            "parser_version", "schema_version", "created_at", "payload",
        )}
    except KeyError as exc:
        raise ProtocolError("INCOMPLETE_TASK", "本地任务缺少必要的身份或快照字段") from exc
    request.update({"upload_schema_version": UPLOAD_SCHEMA_VERSION,
                    "catalog_version": CATALOG_VERSION,
                    "collector_id": collector_id,
                    "source_bytes_base64": base64.b64encode(raw).decode("ascii")})
    # Validate transport identities locally without re-parsing each task. The
    # backend performs the independent scientific snapshot verification.
    _validate_envelope(request)
    if hashlib.sha256(raw).hexdigest() != request["source_sha256"]:
        raise ProtocolError("SOURCE_HASH_MISMATCH", "本地原始字节与任务 SHA-256 不一致")
    if len(canonical_json(request)) > MAX_REQUEST_BYTES:
        raise ProtocolError("REQUEST_TOO_LARGE", "JSON 请求超过 48 MiB 上限")
    return request


def _validate_envelope(request: Any) -> datetime:
    if not isinstance(request, dict) or set(request) != REQUEST_FIELDS:
        raise ProtocolError("INVALID_ENVELOPE", "上传对象存在缺失或不支持的字段")
    if request["upload_schema_version"] != UPLOAD_SCHEMA_VERSION:
        raise ProtocolError("UNSUPPORTED_UPLOAD_VERSION", "不支持的上传协议版本")
    if request["catalog_version"] != CATALOG_VERSION:
        raise ProtocolError("UNSUPPORTED_CATALOG", "不支持的指标目录版本")
    for field in ("collector_id", "task_id", "experiment_key"):
        _uuid(request[field], field)
    for field in ("source_sha256", "idempotency_key"):
        _sha(request[field], field)
    for field, supported in (("parser_version", SUPPORTED_PARSER_VERSIONS),
                              ("schema_version", SUPPORTED_SCHEMA_VERSIONS)):
        if not isinstance(request[field], str) or request[field] not in supported:
            raise ProtocolError("UNSUPPORTED_VERSION", f"不支持的 {field}")
    identity = [request["collector_id"], request["experiment_key"], request["source_sha256"],
                request["parser_version"], request["schema_version"]]
    expected = hashlib.sha256(json.dumps(identity, ensure_ascii=False,
                                        separators=(",", ":")).encode("utf-8")).hexdigest()
    if request["idempotency_key"] != expected:
        raise ProtocolError("IDEMPOTENCY_KEY_MISMATCH", "幂等键与原任务身份不一致")
    return aware_datetime(request["created_at"])


def validate_upload_request(request: Any) -> ValidatedUpload:
    """Verify wire bytes and the complete snapshot with the trusted parser."""
    created = _validate_envelope(request)
    encoded = request["source_bytes_base64"]
    if not isinstance(encoded, str) or len(encoded) > ((MAX_SOURCE_BYTES + 2) // 3) * 4:
        raise ProtocolError("SOURCE_TOO_LARGE", "Base64 原文件无效或超过上限")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError, UnicodeError) as exc:
        raise ProtocolError("INVALID_BASE64", "原文件必须使用有效的标准 Base64") from exc
    if not raw or len(raw) > MAX_SOURCE_BYTES:
        raise ProtocolError("SOURCE_TOO_LARGE", "原文件为空或超过 16 MiB 上限")
    if base64.b64encode(raw).decode("ascii") != encoded:
        raise ProtocolError("INVALID_BASE64", "原文件必须使用规范的标准 Base64")
    if hashlib.sha256(raw).hexdigest() != request["source_sha256"]:
        raise ProtocolError("SOURCE_HASH_MISMATCH", "原文件 SHA-256 与任务不一致")
    supplied = request["payload"]
    if not isinstance(supplied, dict) or not isinstance(supplied.get("source"), dict):
        raise ProtocolError("INVALID_PAYLOAD", "缺少完整解析快照")
    for field in ("parser_version", "schema_version"):
        if supplied.get(field) != request[field]:
            raise ProtocolError("VERSION_MISMATCH", "任务版本与解析快照不一致")
    path, encoding = supplied["source"].get("path"), supplied["source"].get("encoding")
    if not isinstance(path, str) or not path.strip() or "\x00" in path:
        raise ProtocolError("INVALID_SOURCE_PATH", "原文件追溯路径无效")
    if not isinstance(encoding, str) or encoding not in {"utf-8", "utf-8-sig", "gb18030"}:
        raise ProtocolError("UNSUPPORTED_ENCODING", "快照使用不支持的编码")
    from msrefine_parser.parser import ParseError, parse_summary_bytes
    try:
        # A Windows path cannot be resolved on the Linux server. Re-parse bytes
        # with a server-local placeholder, then preserve the client's trace-only
        # path exactly. Never open or resolve a client-supplied filesystem path.
        expected = parse_summary_bytes(raw, source_path=Path.cwd() / "__wire_summary__.txt",
                                       encoding=encoding)
        expected["source"]["path"] = path
    except (ParseError, ValueError, OSError, OverflowError) as exc:
        raise ProtocolError("SOURCE_PARSE_FAILED", "原文件未通过已支持模板的解析校验") from exc
    if len(expected["unnamed_values"]) != 47:
        raise ProtocolError("UNEXPECTED_EXTRA_VALUES", "当前模板要求 47 个无表头值")
    if canonical_json(supplied) != canonical_json(expected):
        raise ProtocolError("PAYLOAD_MISMATCH", "上传快照与原始字节独立解析结果不一致")
    wire = canonical_json(request)
    if len(wire) > MAX_REQUEST_BYTES:
        raise ProtocolError("REQUEST_TOO_LARGE", "JSON 请求超过 48 MiB 上限")
    return ValidatedUpload(request=request, source_bytes=raw, payload=expected,
                           request_sha256=hashlib.sha256(wire).hexdigest(), created_at=created)


def validate_receipt(receipt: Any, request: dict) -> dict:
    """Only an authenticated HTTP 200 with this receipt can acknowledge a task."""
    if not isinstance(receipt, dict):
        raise ProtocolError("INVALID_RECEIPT", "云端回执必须是 JSON 对象")
    if receipt.get("receipt_version") != RECEIPT_VERSION or receipt.get("status") != "committed":
        raise ProtocolError("UNCOMMITTED_RECEIPT", "云端未返回已提交的有效回执")
    for field in RECEIPT_IDENTITY_FIELDS:
        if receipt.get(field) != request[field]:
            raise ProtocolError("RECEIPT_MISMATCH", f"云端回执 {field} 与当前任务不符")
    if receipt.get("request_sha256") != request_fingerprint(request):
        raise ProtocolError("RECEIPT_MISMATCH", "云端回执请求摘要不符")
    for field in ("experiment_id", "result_id"):
        _uuid(receipt.get(field), field)
    aware_datetime(receipt.get("committed_at"), "committed_at")
    if not isinstance(receipt.get("duplicate"), bool):
        raise ProtocolError("INVALID_RECEIPT", "云端回执缺少重复请求标记")
    return receipt
