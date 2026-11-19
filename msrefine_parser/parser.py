"""Parse a single completed MSRefine summary into a traceable JSON record."""

from __future__ import annotations

import codecs
import csv
import hashlib
import io
import math
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path, PureWindowsPath
from typing import Any

from . import __version__
from .metrics import METRIC_DEFINITIONS, PRIMARY_METRIC_CODES

SCHEMA_VERSION = "1.0"


class ParseError(ValueError):
    """An input cannot safely be interpreted as a supported summary."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _read_source(path: Path, encoding: str) -> tuple[bytes, str, str]:
    try:
        before = path.stat()
        raw = path.read_bytes()
        after = path.stat()
    except OSError as exc:
        raise ParseError("READ_ERROR", f"无法读取 {path}: {exc}") from exc
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ParseError("FILE_CHANGED", f"读取过程中发生变化，请在文件生成完成后重试: {path}")
    text, detected = _decode_source(raw, encoding)
    return raw, text, detected


def _decode_source(raw: bytes, encoding: str) -> tuple[str, str]:
    if not raw:
        raise ParseError("EMPTY_FILE", "文件为空")
    if b"\x00" in raw:
        raise ParseError("UNSUPPORTED_ENCODING", "文件含 NUL 字节；当前支持 UTF-8 和 GB18030，不支持直接读取 UTF-16 或二进制文件")

    candidates = [encoding] if encoding != "auto" else ["utf-8-sig", "gb18030"]
    for candidate in candidates:
        try:
            # Accept a UTF-8 BOM even when the caller explicitly selects UTF-8.
            actual = "utf-8-sig" if candidate == "utf-8" and raw.startswith(codecs.BOM_UTF8) else candidate
            text = raw.decode(actual, errors="strict")
            detected = "utf-8" if actual == "utf-8-sig" and not raw.startswith(codecs.BOM_UTF8) else actual
            return text, detected
        except (UnicodeDecodeError, LookupError):
            continue
    raise ParseError("DECODE_ERROR", f"无法用 {', '.join(candidates)} 解码；请检查文件编码")


def _number(raw_value: str, field: str, value_type: str = "number") -> int | float | None:
    token = raw_value.strip()
    if token == "-":
        return None
    if not token:
        raise ParseError("EMPTY_VALUE", f"字段 {field} 为空；已知缺值标记为 '-'，请检查是否写入未完成")
    try:
        decimal = Decimal(token)
    except InvalidOperation as exc:
        raise ParseError("INVALID_NUMBER", f"字段 {field} 不是数值或 '-'：{raw_value!r}") from exc
    if not decimal.is_finite():
        raise ParseError("NON_FINITE_NUMBER", f"字段 {field} 不允许 NaN 或 Infinity")
    if not re.fullmatch(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", token):
        raise ParseError("INVALID_NUMBER", f"字段 {field} 不是标准十进制或科学计数法：{raw_value!r}")
    try:
        value = float(decimal)
    except (OverflowError, ValueError) as exc:
        raise ParseError("NUMBER_OUT_OF_RANGE", f"字段 {field} 超出 JSON 数值范围") from exc
    if not math.isfinite(value):
        raise ParseError("NUMBER_OUT_OF_RANGE", f"字段 {field} 超出 JSON 数值范围")
    if decimal != 0 and value == 0:
        raise ParseError("NUMBER_OUT_OF_RANGE", f"字段 {field} 太小，转换将丢失为零")
    if value_type == "integer" and (decimal != decimal.to_integral_value() or decimal < 0):
        raise ParseError("INVALID_COUNT", f"字段 {field} 应为非负整数：{raw_value!r}")
    if value_type == "ratio" and not Decimal(0) <= decimal <= Decimal(1):
        raise ParseError("INVALID_RATIO", f"字段 {field} 应为 0～1 的比例：{raw_value!r}")
    return int(decimal) if decimal == decimal.to_integral_value() else value


def _display_value(value: int | float | None, raw_value: str, scale: int | float, field: str) -> int | float | None:
    if value is None:
        return None
    if scale == 1:
        return value
    # Percentage rounding affects presentation only; value and raw_value retain the source.
    display = float(Decimal(raw_value.strip()) * Decimal(str(scale)))
    if not math.isfinite(display):
        raise ParseError("NUMBER_OUT_OF_RANGE", f"字段 {field} 的展示换算超出 JSON 数值范围")
    return round(display, 2)


def parse_summary(path: str | Path, *, encoding: str = "auto") -> dict[str, Any]:
    """Parse one INFO_Summary.txt; do not invent metadata, scores or alarm states.

    All 47 known headers are required. Their order can change and additional named
    numeric columns are retained. Extra values without headers remain unnamed.
    This is a one-shot reader; its stat check cannot prove a paused writer is done.
    """
    source_path = Path(path).expanduser().resolve()
    raw, text, detected_encoding = _read_source(source_path, encoding)
    return _parse_summary(raw, text, detected_encoding, source_path)


def parse_summary_bytes(raw: bytes, *, source_path: str | Path, encoding: str = "auto") -> dict[str, Any]:
    """Parse the exact snapshot retained by a collector, without rereading a file."""
    text, detected_encoding = _decode_source(raw, encoding)
    return _parse_summary(raw, text, detected_encoding, Path(source_path).expanduser().resolve())


def _parse_summary(raw: bytes, text: str, detected_encoding: str, source_path: Path) -> dict[str, Any]:
    try:
        rows = [row for row in csv.reader(io.StringIO(text, newline=""), delimiter="\t", strict=True) if row]
    except csv.Error as exc:
        raise ParseError("INVALID_TSV", f"Tab 分隔格式无效：{exc}") from exc
    if len(rows) < 2:
        raise ParseError("MISSING_RECORD", "需要表头和一条完整实验记录")
    if len(rows) > 2:
        raise ParseError("MULTIPLE_RECORDS", "当前版本每份汇总只支持一条实验记录，检测到多条记录")

    headers, values = rows
    if not headers or headers[0] != "File name":
        raise ParseError("INVALID_HEADER", "第一列必须为 'File name'，请传入 INFO_Summary.txt")
    if any(not name.strip() for name in headers):
        raise ParseError("EMPTY_HEADER", "存在空表头，无法可靠映射指标")
    if len(set(headers)) != len(headers):
        raise ParseError("DUPLICATE_HEADER", "存在重复表头，无法可靠映射指标")
    missing_headers = sorted(set(METRIC_DEFINITIONS) - set(headers))
    if missing_headers:
        raise ParseError("MISSING_HEADERS", "缺少已知指标表头，可能是文件未写完或软件版本不同：" + ", ".join(missing_headers))
    if len(values) < len(headers):
        raise ParseError("TRUNCATED_RECORD", f"数据只有 {len(values)} 列，少于 {len(headers)} 列表头")

    source_file_name = values[0].strip()
    base_name = PureWindowsPath(source_file_name).name
    if not base_name or base_name in {".", ".."}:
        raise ParseError("INVALID_FILE_NAME", "实验原始文件名不能为空或目录路径")
    run_id = PureWindowsPath(base_name).stem
    warnings: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    for name, raw_value in zip(headers[1:], values[1:len(headers)]):
        definition = METRIC_DEFINITIONS.get(name)
        if definition is None:
            definition = {
                "code": "unmapped_" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:16],
                "name": name, "group": "unmapped", "unit": None,
                "display_unit": None, "display_scale": 1,
                "status": "unmapped", "value_type": "number",
            }
            warnings.append({"code": "UNKNOWN_METRIC", "message": f"新增字段尚无业务定义，已保留：{name}", "fields": [name]})
        value = _number(raw_value, name, definition.get("value_type", "number"))
        definition_status = definition["status"]
        status = "missing" if value is None else ("available" if definition_status == "mapped" else definition_status)
        metrics.append({
            "code": definition["code"], "name": definition["name"],
            "source_name": name, "raw_value": raw_value, "value": value,
            "group": definition["group"], "unit": definition["unit"],
            "display_unit": definition["display_unit"],
            "display_value": _display_value(value, raw_value, definition["display_scale"], name),
            "status": status, "definition_status": definition_status,
            "is_primary": definition["code"] in PRIMARY_METRIC_CODES,
        })

    review_fields = [metric["source_name"] for metric in metrics if metric["definition_status"] == "needs_review"]
    if review_fields:
        warnings.append({
            "code": "METRIC_DEFINITION_REVIEW",
            "message": "这些字段的含义或映射待确认；保留原值，不交换前体质量偏差均值/标准差，也不生成业务告警",
            "fields": review_fields,
        })
    missing_fields = [metric["source_name"] for metric in metrics if metric["value"] is None]
    if missing_fields:
        warnings.append({"code": "MISSING_METRIC_VALUES", "message": "部分指标为 '-'，已保存为 null", "fields": missing_fields})

    unnamed_values = [
        {"position": i + 1, "raw_value": token, "value": _number(token, f"无表头第 {i + 1} 列")}
        for i, token in enumerate(values[len(headers):], start=len(headers))
    ]
    if unnamed_values:
        warnings.append({
            "code": "UNNAMED_VALUES", "message": f"发现 {len(unnamed_values)} 个无表头数值；未将其解释为指标或报告评分",
            "count": len(unnamed_values),
        })
    if detected_encoding == "gb18030":
        warnings.append({"code": "GB18030_ENCODING", "message": "按 GB18030 解码；请在采集环境确认编码配置"})

    return {
        "schema_version": SCHEMA_VERSION, "parser_version": __version__,
        "run_id": run_id, "source_file_name": source_file_name,
        "source": {
            "path": str(source_path), "sha256": hashlib.sha256(raw).hexdigest(),
            "encoding": detected_encoding, "raw_text": text,
            "header_count": len(headers), "data_field_count": len(values),
        },
        "metrics": metrics, "primary_metric_codes": list(PRIMARY_METRIC_CODES),
        "unnamed_values": unnamed_values,
        "counts": {
            "named_metrics": len(metrics),
            "available_metrics": sum(metric["value"] is not None for metric in metrics),
            "missing_metrics": len(missing_fields),
            "review_metrics": len(review_fields), "unnamed_values": len(unnamed_values),
        },
        "parse_status": "success_with_warnings" if warnings else "success",
        "warnings": warnings,
    }
