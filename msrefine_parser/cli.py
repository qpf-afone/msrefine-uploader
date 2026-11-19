"""Batch entry point: python -m msrefine_parser INPUT --output OUTPUT."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path
from typing import Any

from . import __version__
from .parser import SCHEMA_VERSION, ParseError, parse_summary


def _atomic_json(path: Path, record: dict[str, Any]) -> None:
    data = (json.dumps(record, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode("utf-8")
    if path.is_file() and path.read_bytes() == data:
        return
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, prefix=".msrefine-", suffix=".tmp", delete=False) as handle:
            temp_name = handle.name
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if temp_name is not None and os.path.exists(temp_name):
            os.unlink(temp_name)


def _discover(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        # File paths are collected only; scan-level detail files are not loaded.
        return sorted(path.rglob("INFO_Summary.txt"))
    raise ParseError("INPUT_NOT_FOUND", f"输入路径不存在：{path}")


def _output_name(record: dict[str, Any]) -> str:
    safe_name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", record["run_id"]).strip(". ")[:100] or "unnamed"
    # Keep distinct source locations traceable even when their bytes are identical.
    source_hash = hashlib.sha256(record["source"]["path"].encode("utf-8")).hexdigest()[:12]
    return f"run-{safe_name}-{record['source']['sha256'][:16]}-{source_hash}.json"


def _watch_main(argv: list[str]) -> int:
    from .collector import Collector, CollectorConfig
    parser = argparse.ArgumentParser(prog="msrefine_parser watch", description="持续扫描 MSRefine 结果，并保存到本地 SQLite 待上传队列")
    parser.add_argument("input", type=Path, help="需要监测的实验根目录")
    parser.add_argument("--state-dir", type=Path, default=Path("采集状态"), help="本机持久化状态目录（默认：采集状态）")
    parser.add_argument("--poll-interval", type=float, default=5, help="扫描间隔，单位秒（默认：5）")
    parser.add_argument("--stable-seconds", type=float, default=30, help="首次观察后连续不变的最短秒数（默认：30）")
    parser.add_argument("--stable-scans", type=int, default=3, help="至少连续观察次数，必须 ≥2（默认：3）")
    parser.add_argument("--completion-marker", help="可选的同目录完成标志文件名，须由上游在生成完成后创建")
    parser.add_argument("--expected-extra-values", type=int, default=47, help="已确认模板的无表头值数量（默认：47）")
    parser.add_argument("--encoding", choices=["auto", "utf-8", "utf-8-sig", "gb18030"], default="auto")
    parser.add_argument("--scan-once", action="store_true", help="仅观察一次后退出，用于诊断；不跳过稳定窗口")
    parser.add_argument("--upload-config", type=Path, help="可选上传配置；用独立线程同步上传本地待上传任务")
    args = parser.parse_args(argv)
    if args.scan_once and args.upload_config is not None:
        parser.error("--scan-once 是单轮采集诊断，不能与 --upload-config 同时使用；单轮上传请使用 upload --once")
    try:
        config = CollectorConfig(input_path=args.input, state_dir=args.state_dir,
                                 poll_interval=args.poll_interval, stable_seconds=args.stable_seconds,
                                 stable_scans=args.stable_scans, completion_marker=args.completion_marker,
                                 expected_extra_values=args.expected_extra_values, encoding=args.encoding)
        upload_config = None
        if args.upload_config is not None:
            from .uploader import load_upload_config
            upload_config = load_upload_config(args.upload_config)
    except ValueError as exc:
        parser.error(str(exc))
    except OSError as exc:
        print(f"无法读取上传配置：{exc}", file=sys.stderr)
        return 1
    try:
        with Collector(config) as collector:
            console = logging.StreamHandler(sys.stderr)
            console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            collector.logger.addHandler(console)
            stop_event = threading.Event()
            worker = None
            ready = threading.Event()
            uploader_holder = []
            upload_errors = []
            try:
                print(f"监测目录：{collector.input_path}\n状态目录：{collector.state_dir}\n"
                      f"已有队列：{collector.queue.counts()}\n按 Ctrl+C 停止；待上传记录会保留。", flush=True)
                if args.scan_once:
                    stats = collector.scan_once()
                    print(json.dumps(stats, ensure_ascii=False), flush=True)
                    return 1 if stats["errors"] else 0
                if upload_config is not None:
                    from .uploader import Uploader

                    def upload_worker() -> None:
                        # Construct the connection in its owning thread. Never
                        # pass collector.queue across the thread boundary.
                        try:
                            with Uploader(collector.state_dir, upload_config) as uploader:
                                uploader_holder.append(uploader)
                                uploader.logger.addHandler(console)
                                ready.set()
                                try:
                                    uploader.run(stop_event)
                                finally:
                                    uploader.logger.removeHandler(console)
                        except BaseException as exc:
                            upload_errors.append(type(exc).__name__)
                            ready.set()
                            stop_event.set()

                    worker = threading.Thread(target=upload_worker, name="msrefine-uploader", daemon=False)
                    worker.start()
                    ready.wait()
                    if upload_errors:
                        raise RuntimeError("上传器初始化失败；请检查上传器锁、配置和本地队列")
                collector.run(stop_event)
            except KeyboardInterrupt:
                collector.logger.info("收到停止请求；已提交队列保持在本地")
            finally:
                stop_event.set()
                if uploader_holder:
                    uploader_holder[0].stop_network()
                if worker is not None:
                    worker.join()
                collector.logger.removeHandler(console)
                console.close()
            if upload_errors:
                raise RuntimeError("上传器停止；已保留本地队列，请检查状态目录和上传配置")
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        print(f"采集器停止：{exc}", file=sys.stderr)
        return 1
    return 0


def _upload_main(argv: list[str]) -> int:
    from .uploader import Uploader, load_upload_config
    parser = argparse.ArgumentParser(prog="msrefine_parser upload", description="上传本地队列中的到期任务；失败后持久化退避并保留原数据")
    parser.add_argument("--state-dir", type=Path, default=Path("采集状态"))
    parser.add_argument("--config", type=Path, required=True, help="JSON 配置路径；token 从受保护文件或环境变量读取")
    parser.add_argument("--once", action="store_true", help="仅尝试一批已到重试时间的任务后退出")
    args = parser.parse_args(argv)
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    try:
        config = load_upload_config(args.config)
        with Uploader(args.state_dir, config) as uploader:
            uploader.logger.addHandler(console)
            try:
                if args.once:
                    stats = uploader.run_once()
                    print(json.dumps({**stats, "counts": uploader.queue.counts()}, ensure_ascii=False), flush=True)
                    return 1 if stats["failed"] or stats["auth_paused"] else 0
                print("上传器已启动；按 Ctrl+C 停止。失败任务和重试时间会保留在原状态目录。", flush=True)
                uploader.run()
            finally:
                uploader.logger.removeHandler(console)
    except KeyboardInterrupt:
        print("上传器已停止；未确认的任务仍保留在本地。", file=sys.stderr)
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        print(f"上传器停止：{exc}", file=sys.stderr)
        return 1
    finally:
        console.close()
    return 0


def _queue_main(argv: list[str]) -> int:
    from .queue import UploadQueue
    parser = argparse.ArgumentParser(prog="msrefine_parser queue", description="查看本地待上传队列，不发送网络请求")
    parser.add_argument("--state-dir", type=Path, default=Path("采集状态"))
    parser.add_argument("--status", choices=["pending", "uploaded", "all"], default="pending")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--include-payload", action="store_true", help="同时显示完整解析数据")
    args = parser.parse_args(argv)
    if args.limit < 1:
        parser.error("limit 必须大于零")
    path = args.state_dir.expanduser().resolve() / "queue.sqlite3"
    if not path.is_file():
        print(f"队列尚未建立：{path}", file=sys.stderr)
        return 1
    try:
        with UploadQueue(path) as queue:
            tasks = queue.list_tasks(status=None if args.status == "all" else args.status, limit=args.limit)
            if not args.include_payload:
                tasks = [{key: value for key, value in task.items() if key not in {"payload", "receipt"}} for task in tasks]
            print(json.dumps({"collector_id": queue.collector_id, "database": str(path),
                              "counts": queue.counts(), "tasks": tasks}, ensure_ascii=False, indent=2, allow_nan=False))
    except (OSError, ValueError, RuntimeError, sqlite3.Error) as exc:
        print(f"无法读取队列：{exc}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "watch":
        return _watch_main(arguments[1:])
    if arguments and arguments[0] == "queue":
        return _queue_main(arguments[1:])
    if arguments and arguments[0] == "upload":
        return _upload_main(arguments[1:])
    return _batch_main(arguments)


def _batch_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="将 MSRefine 汇总 TXT 转为可追溯 JSON（不修改原始文件）")
    parser.add_argument("input", type=Path, help="单份 INFO_Summary.txt，或包含多组实验的目录")
    parser.add_argument("--output", type=Path, default=Path("解析结果"), help="JSON 输出目录，默认：解析结果")
    parser.add_argument("--encoding", choices=["auto", "utf-8", "utf-8-sig", "gb18030"], default="auto")
    parser.add_argument("--version", action="version", version=__version__)
    parser.epilog = "常驻采集：python -m msrefine_parser watch --help；查看队列：python -m msrefine_parser queue --help；上传：python -m msrefine_parser upload --help"
    args = parser.parse_args(argv)
    input_path = args.input.expanduser().resolve()
    output_dir = args.output.expanduser().resolve()
    try:
        paths = _discover(input_path)
        if not paths:
            raise ParseError("NO_SUMMARIES", f"目录中没有 INFO_Summary.txt：{input_path}")
        output_dir.mkdir(parents=True, exist_ok=True)
    except (ParseError, OSError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 1

    results = []
    errors = []
    for path in paths:
        try:
            record = parse_summary(path, encoding=args.encoding)
            filename = _output_name(record)
            _atomic_json(output_dir / filename, record)
        except (ParseError, OSError) as exc:
            errors.append({"source_path": str(path), "code": getattr(exc, "code", "WRITE_ERROR"), "message": str(exc)})
            print(f"失败：{path}：{exc}", file=sys.stderr)
            continue
        results.append({
            "run_id": record["run_id"], "source_path": str(path),
            "source_sha256": record["source"]["sha256"], "output_file": filename,
            "parse_status": record["parse_status"], "counts": record["counts"],
            "warning_codes": [warning["code"] for warning in record["warnings"]],
        })
        print(f"完成：{record['run_id']}，{record['counts']['named_metrics']} 项指标，{len(record['warnings'])} 项解析提示")

    index = {
        "schema_version": SCHEMA_VERSION, "parser_version": __version__,
        "input_path": str(input_path), "total_files": len(paths),
        "success_count": len(results), "error_count": len(errors),
        "results": results, "errors": errors,
    }
    try:
        _atomic_json(output_dir / "index.json", index)
    except OSError as exc:
        print(f"无法保存汇总清单：{exc}", file=sys.stderr)
        return 1
    print(f"成功 {len(results)}/{len(paths)}，失败 {len(errors)}；清单：{output_dir / 'index.json'}")
    return 1 if errors else 0
