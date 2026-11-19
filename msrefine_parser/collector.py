"""Continuously observe completed summaries and transactionally enqueue snapshots."""

from __future__ import annotations

import hashlib
import logging
import math
import os
import sqlite3
import stat
import threading
import time
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import BinaryIO, Callable

from .parser import ParseError, parse_summary_bytes
from .queue import UploadQueue

MAX_SUMMARY_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class CollectorConfig:
    input_path: Path
    state_dir: Path
    poll_interval: float = 5
    stable_seconds: float = 30
    stable_scans: int = 3
    completion_marker: str | None = None
    expected_extra_values: int = 47
    encoding: str = "auto"
    new_experiments_only: bool = False

    def __post_init__(self) -> None:
        if not math.isfinite(self.poll_interval) or self.poll_interval <= 0:
            raise ValueError("扫描间隔必须为大于零的有限数")
        if not math.isfinite(self.stable_seconds) or self.stable_seconds < 0:
            raise ValueError("稳定时长必须为非负有限数")
        if isinstance(self.stable_scans, bool) or not isinstance(self.stable_scans, int) or self.stable_scans < 2:
            raise ValueError("稳定观察次数必须为至少 2 的整数")
        if isinstance(self.expected_extra_values, bool) or not isinstance(self.expected_extra_values, int) or self.expected_extra_values < 0:
            raise ValueError("预期无表头值数量必须为非负整数")
        marker = self.completion_marker
        if marker is not None and (not marker.strip() or marker in {".", ".."} or any(c in marker for c in '/\\:')):
            raise ValueError("完成标志必须是同目录中的单个文件名")
        if marker is not None and marker.casefold() == "info_summary.txt":
            raise ValueError("完成标志必须与汇总文件不同")
        if self.encoding not in {"auto", "utf-8", "utf-8-sig", "gb18030"}:
            raise ValueError("不支持的编码配置")


class _StateLock:
    """OS-held lock is released on exit or process death, including on Windows."""

    def __init__(self, path: Path):
        self.handle = path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                if path.stat().st_size == 0:
                    self.handle.write(b"0")
                    self.handle.flush()
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.handle.close()
            raise RuntimeError(f"该状态目录已有采集器运行：{path.parent}") from exc

    def close(self) -> None:
        if not self.handle.closed:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()


def _open_snapshot(path: Path) -> BinaryIO:
    if os.name != "nt":
        return path.open("rb")
    # Deny sharing while reading. A still-open writer causes a sharing violation.
    import ctypes
    import msvcrt
    from ctypes import wintypes
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
                       wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    windows_path = str(path)
    if not windows_path.startswith("\\\\?\\"):
        windows_path = "\\\\?\\UNC\\" + windows_path[2:] if windows_path.startswith("\\\\") else "\\\\?\\" + windows_path
    handle = create(windows_path, 0x80000000, 0, None, 3, 0x80, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        close(handle)
        raise
    try:
        return os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise


def _stat_key(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _snapshot(path: Path) -> tuple[bytes, tuple[int, ...]]:
    with _open_snapshot(path) as handle:
        before = os.fstat(handle.fileno())
        if not stat.S_ISREG(before.st_mode):
            raise ParseError("NOT_REGULAR_FILE", "汇总路径不是普通文件")
        if before.st_size > MAX_SUMMARY_BYTES:
            raise ParseError("SUMMARY_TOO_LARGE", "汇总文件超过 16 MiB，需检查是否选错文件")
        raw = handle.read(MAX_SUMMARY_BYTES + 1)
        after = os.fstat(handle.fileno())
        if len(raw) > MAX_SUMMARY_BYTES or _stat_key(before) != _stat_key(after):
            raise ParseError("FILE_CHANGED", "读取期间文件发生变化")
    # Detect replacement of the pathname while the original handle was open.
    if _stat_key(after) != _stat_key(path.stat()):
        raise ParseError("FILE_CHANGED", "读取期间源路径被替换或更新")
    if not raw:
        raise ParseError("EMPTY_FILE", "汇总文件为空，等待继续生成")
    return raw, _stat_key(after)


@dataclass
class _Observation:
    signature: tuple
    since: float
    scans: int = 1
    handled: bool = False
    retry_at: float = 0
    last_error: str | None = None


class Collector:
    def __init__(self, config: CollectorConfig, *, clock: Callable[[], float] = time.monotonic):
        self.config = config
        input_path = Path(config.input_path).expanduser()
        if config.new_experiments_only:
            # The configured drive or UNC path may be unavailable when the task starts.
            self.input_path = Path(os.path.abspath(input_path))
        else:
            self.input_path = input_path.resolve()
            if not self.input_path.is_dir():
                raise ValueError(f"监测目录不存在或不是目录：{self.input_path}")
        self.state_dir = Path(config.state_dir).expanduser().resolve()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._clock = clock
        self._observations: dict[Path, _Observation] = {}
        self._read_errors: dict[Path, str] = {}
        self._closed = False
        self._lock = _StateLock(self.state_dir / "collector.lock")
        try:
            self.queue = UploadQueue(self.state_dir / "queue.sqlite3")
            self._experiment_baseline = (
                self.queue.load_experiment_baseline(self.input_path) if config.new_experiments_only else None
            )
            self.logger = logging.getLogger(f"msrefine.collector.{id(self)}")
            self.logger.setLevel(logging.INFO)
            self.logger.propagate = False
            self._handler = RotatingFileHandler(self.state_dir / "collector.log", maxBytes=5 * 1024 * 1024,
                                               backupCount=3, encoding="utf-8")
            self._handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self.logger.addHandler(self._handler)
        except BaseException:
            if hasattr(self, "queue"):
                self.queue.close()
            self._lock.close()
            raise

    def __enter__(self) -> Collector:
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    def close(self) -> None:
        if not self._closed:
            try:
                self.queue.close()
            finally:
                self.logger.removeHandler(self._handler)
                self._handler.close()
                self._lock.close()
                self._closed = True

    def _marker_key(self, path: Path, summary_mtime_ns: int) -> tuple | None:
        name = self.config.completion_marker
        if name is None:
            return ()
        marker = path.parent / name
        try:
            info = marker.stat()
        except FileNotFoundError:
            return None
        if not stat.S_ISREG(info.st_mode) or info.st_mtime_ns < summary_mtime_ns:
            return None
        return _stat_key(info)

    def _new_experiment_summaries(self, onerror: Callable[[OSError], None]) -> set[Path]:
        # Finish enumerating the root before committing the initial baseline.
        with os.scandir(self.input_path) as entries:
            runs = [Path(entry.path) for entry in entries if entry.is_dir(follow_symlinks=False)]
        if self._experiment_baseline is None:
            baseline = frozenset(os.path.normcase(run.name) for run in runs)
            self.queue.store_experiment_baseline(self.input_path, baseline)
            self._experiment_baseline = baseline
            self.logger.info("已建立新增实验基线，记录 %d 个已有目录", len(baseline))
            return set()

        paths = set()
        for run in runs:
            if os.path.normcase(run.name) in self._experiment_baseline:
                continue
            output = run / "MSRefine"
            summary = output / "INFO_Summary.txt"
            try:
                if stat.S_ISDIR(output.lstat().st_mode) and stat.S_ISREG(summary.lstat().st_mode):
                    paths.add(summary)
            except (FileNotFoundError, NotADirectoryError):
                continue  # The new experiment may still be running.
            except OSError as exc:
                onerror(exc)
        return paths

    def scan_once(self) -> dict[str, int]:
        if self._closed:
            raise RuntimeError("采集器已经关闭")
        stats = dict(seen=0, waiting=0, enqueued=0, duplicates=0, errors=0)
        now = self._clock()
        paths: set[Path] = set()

        def traversal_error(exc: OSError) -> None:
            stats["errors"] += 1
            self.logger.error("扫描目录失败 %s：%s", self.input_path, exc)

        if self.config.new_experiments_only:
            try:
                paths = self._new_experiment_summaries(traversal_error)
            except OSError as exc:
                traversal_error(exc)
        elif self.input_path.is_dir():
            for directory, _dirs, names in os.walk(self.input_path, onerror=traversal_error):
                for name in names:
                    if name.casefold() == "info_summary.txt":
                        paths.add((Path(directory) / name).resolve())
        else:
            self.logger.error("监测目录暂不可访问：%s", self.input_path)
            stats["errors"] = 1

        for path in sorted(paths):
            stats["seen"] += 1
            try:
                raw, file_key = _snapshot(path)
                marker_key = self._marker_key(path, file_key[3])
                if marker_key is None:
                    self._observations.pop(path, None)
                    stats["waiting"] += 1
                    continue
                digest = hashlib.sha256(raw).hexdigest()
                signature = (file_key, digest, marker_key)
                self._read_errors.pop(path, None)
            except (OSError, ParseError) as exc:
                self._observations.pop(path, None)
                message = str(exc)
                if self._read_errors.get(path) != message:
                    self.logger.warning("暂不能读取 %s：%s", path, message)
                    self._read_errors[path] = message
                stats["waiting"] += 1
                continue

            observation = self._observations.get(path)
            if observation is None or observation.signature != signature:
                self._observations[path] = _Observation(signature, now)
                stats["waiting"] += 1
                continue
            observation.scans += 1
            if observation.handled:
                stats["duplicates"] += 1
                continue
            if (observation.scans < self.config.stable_scans
                    or now - observation.since < self.config.stable_seconds or now < observation.retry_at):
                stats["waiting"] += 1
                continue
            try:
                record = parse_summary_bytes(raw, source_path=path, encoding=self.config.encoding)
                if len(record["unnamed_values"]) != self.config.expected_extra_values:
                    raise ParseError("UNEXPECTED_EXTRA_VALUES", f"预期 {self.config.expected_extra_values} 个额外值，"
                                     f"实际 {len(record['unnamed_values'])} 个；可能仍在写入或输出模板不同")
                # Parsing occurs on the captured bytes, and the source must still match.
                if _stat_key(path.stat()) != file_key or self._marker_key(path, file_key[3]) != marker_key:
                    self._observations.pop(path, None)
                    stats["waiting"] += 1
                    continue
                result = self.queue.enqueue(record, source_bytes=raw)
                observation.handled = True
                key = "enqueued" if result["created"] else "duplicates"
                stats[key] += 1
                self.logger.info("%s task=%s experiment=%s source=%s sha256=%s",
                                 key, result["task_id"], result["experiment_key"], path, digest)
            except (ParseError, OSError, ValueError, sqlite3.OperationalError) as exc:
                if isinstance(exc, sqlite3.OperationalError):
                    code = getattr(exc, "sqlite_errorcode", None)
                    # BUSY=5, LOCKED=6; extended result codes keep this low byte.
                    transient = (code & 0xff) in {5, 6} if code is not None else "locked" in str(exc).lower()
                    if not transient:
                        self.logger.exception("队列持久化失败，停止采集以免丢失状态：%s", path)
                        raise
                message = str(exc)
                if observation.last_error != message:
                    self.logger.error("解析或入队失败 %s：%s", path, message)
                    observation.last_error = message
                observation.retry_at = now + max(self.config.poll_interval, 30)
                stats["errors"] += 1

        for path in set(self._observations) - paths:
            self._observations.pop(path, None)
        for path in set(self._read_errors) - paths:
            self._read_errors.pop(path, None)
        return stats

    def run(self, stop_event: threading.Event | None = None) -> None:
        stop_event = stop_event if stop_event is not None else threading.Event()
        self.logger.info("开始监测 %s；稳定窗口=%ss，次数=%s，marker=%s；队列=%s",
                         self.input_path, self.config.stable_seconds, self.config.stable_scans,
                         self.config.completion_marker, self.queue.counts())
        while not stop_event.is_set():
            stats = self.scan_once()
            if stats["enqueued"] or stats["errors"]:
                self.logger.info("扫描结果 %s", stats)
            stop_event.wait(self.config.poll_interval)
        self.logger.info("停止监测；队列=%s", self.queue.counts())
