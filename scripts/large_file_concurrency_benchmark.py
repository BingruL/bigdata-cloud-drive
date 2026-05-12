#!/usr/bin/env python3
"""
多用户大文件并发上传 / 下载压测脚本

与 scale_benchmark.py 的定位不同：
- scale_benchmark.py 验证 HBase / HDFS / Spark 的数据处理规模；
- 本脚本走真实 Web API，验证网盘服务在多用户并发大文件上传下载下的表现。

示例：
  python scripts/large_file_concurrency_benchmark.py \\
    --base-url http://localhost:5000 \\
    --users benchu1,benchu2,benchu3,benchu4 \\
    --file-size 1GB \\
    --files-per-user 1 \\
    --concurrency 4 \\
    --csv-out reports/large-file-concurrency.csv

预检建议：
  python scripts/large_file_concurrency_benchmark.py \\
    --file-size 100MB --files-per-user 1 --concurrency 2
"""
import argparse
import csv
import json
import os
import re
import statistics
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import requests


SIZE_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?i?b?|bytes?)?\s*$", re.I)
UNIT_FACTORS = {
    "": 1,
    "b": 1,
    "byte": 1,
    "bytes": 1,
    "k": 1024,
    "kb": 1024,
    "kib": 1024,
    "m": 1024 ** 2,
    "mb": 1024 ** 2,
    "mib": 1024 ** 2,
    "g": 1024 ** 3,
    "gb": 1024 ** 3,
    "gib": 1024 ** 3,
    "t": 1024 ** 4,
    "tb": 1024 ** 4,
    "tib": 1024 ** 4,
}


@dataclass
class TaskResult:
    phase: str
    username: str
    filename: str
    file_id: str = ""
    bytes: int = 0
    seconds: float = 0.0
    status_code: int = 0
    ok: bool = False
    error: str = ""

    @property
    def mbps(self):
        if not self.seconds or not self.bytes:
            return 0.0
        return round((self.bytes / 1024 / 1024) / self.seconds, 2)


def parse_size(value):
    match = SIZE_RE.match(str(value))
    if not match:
        raise argparse.ArgumentTypeError(f"非法大小: {value!r}，示例: 1GB, 512MB")
    amount = float(match.group(1))
    unit = (match.group(2) or "").lower()
    if unit not in UNIT_FACTORS:
        raise argparse.ArgumentTypeError(f"不支持的单位: {unit}")
    return int(amount * UNIT_FACTORS[unit])


def format_bytes(num):
    n = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.2f}{unit}" if unit != "B" else f"{int(n)}B"
        n /= 1024


def percentile(values, pct):
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(pct / 100.0 * (len(s) - 1)))))
    return s[k]


def write_payload(path, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    block = (b"bigdata-cloud-drive-large-file-benchmark\n" * 32768)[:1024 * 1024]
    remaining = size
    start = time.perf_counter()
    with path.open("wb") as f:
        while remaining > 0:
            chunk = block[:min(len(block), remaining)]
            f.write(chunk)
            remaining -= len(chunk)
    print(f"payload ready: {path} ({format_bytes(size)}) in {time.perf_counter() - start:.2f}s")


def ensure_payload(payload_path, size, force=False):
    path = Path(payload_path) if payload_path else Path(tempfile.gettempdir()) / "cloud-drive-large-benchmark" / f"payload-{size}.bin"
    if path.exists() and path.stat().st_size == size and not force:
        print(f"reuse payload: {path} ({format_bytes(size)})")
        return path
    write_payload(path, size)
    return path


def api_json(method, url, **kwargs):
    resp = requests.request(method, url, **kwargs)
    try:
        body = resp.json()
    except Exception:
        body = {}
    return resp, body


def ensure_token(base_url, username, password, register=True, timeout=30):
    resp, body = api_json(
        "POST",
        f"{base_url}/api/auth/login",
        json={"username": username, "password": password},
        timeout=timeout,
    )
    if resp.status_code == 200:
        return body["token"]
    if not register:
        raise RuntimeError(f"登录失败 {username}: HTTP {resp.status_code} {body}")

    reg_resp, reg_body = api_json(
        "POST",
        f"{base_url}/api/auth/register",
        json={"username": username, "password": password},
        timeout=timeout,
    )
    if reg_resp.status_code not in (201, 409):
        raise RuntimeError(f"注册失败 {username}: HTTP {reg_resp.status_code} {reg_body}")

    resp, body = api_json(
        "POST",
        f"{base_url}/api/auth/login",
        json={"username": username, "password": password},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"注册后登录失败 {username}: HTTP {resp.status_code} {body}")
    return body["token"]


def upload_one(base_url, token, username, payload_path, remote_name, timeout):
    start = time.perf_counter()
    size = payload_path.stat().st_size
    try:
        with payload_path.open("rb") as f:
            resp = requests.post(
                f"{base_url}/api/files/upload",
                headers={"Authorization": f"Bearer {token}"},
                data={"parent_id": "root"},
                files={"file": (remote_name, f, "application/octet-stream")},
                timeout=timeout,
            )
        elapsed = time.perf_counter() - start
        body = {}
        try:
            body = resp.json()
        except Exception:
            pass
        if resp.status_code == 201:
            return TaskResult(
                phase="upload",
                username=username,
                filename=remote_name,
                file_id=body.get("file", {}).get("file_id", ""),
                bytes=size,
                seconds=elapsed,
                status_code=resp.status_code,
                ok=True,
            )
        return TaskResult(
            phase="upload",
            username=username,
            filename=remote_name,
            bytes=size,
            seconds=elapsed,
            status_code=resp.status_code,
            ok=False,
            error=body.get("error") or resp.text[:200],
        )
    except Exception as e:
        return TaskResult(
            phase="upload",
            username=username,
            filename=remote_name,
            bytes=size,
            seconds=time.perf_counter() - start,
            ok=False,
            error=repr(e),
        )


def download_one(base_url, token, username, file_id, filename, expected_size, timeout, chunk_size):
    start = time.perf_counter()
    bytes_read = 0
    try:
        with requests.get(
            f"{base_url}/api/files/{file_id}/download",
            headers={"Authorization": f"Bearer {token}"},
            stream=True,
            timeout=timeout,
        ) as resp:
            status_code = resp.status_code
            if status_code != 200:
                try:
                    error = resp.json().get("error", "")
                except Exception:
                    error = resp.text[:200]
                return TaskResult(
                    phase="download",
                    username=username,
                    filename=filename,
                    file_id=file_id,
                    bytes=bytes_read,
                    seconds=time.perf_counter() - start,
                    status_code=status_code,
                    ok=False,
                    error=error,
                )
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    bytes_read += len(chunk)
    except Exception as e:
        return TaskResult(
            phase="download",
            username=username,
            filename=filename,
            file_id=file_id,
            bytes=bytes_read,
            seconds=time.perf_counter() - start,
            ok=False,
            error=repr(e),
        )

    ok = bytes_read == expected_size
    return TaskResult(
        phase="download",
        username=username,
        filename=filename,
        file_id=file_id,
        bytes=bytes_read,
        seconds=time.perf_counter() - start,
        status_code=200,
        ok=ok,
        error="" if ok else f"size mismatch: expected {expected_size}, got {bytes_read}",
    )


def summarize(phase, results):
    phase_results = [r for r in results if r.phase == phase]
    if not phase_results:
        return None
    ok_results = [r for r in phase_results if r.ok]
    latencies = [r.seconds for r in ok_results]
    total_bytes = sum(r.bytes for r in ok_results)
    total_wall = max((r.seconds for r in phase_results), default=0.0)
    return {
        "phase": phase,
        "total": len(phase_results),
        "success": len(ok_results),
        "errors": len(phase_results) - len(ok_results),
        "error_rate_pct": round((len(phase_results) - len(ok_results)) / len(phase_results) * 100, 2),
        "bytes": total_bytes,
        "wall_seconds_approx": round(total_wall, 3),
        "aggregate_mbps_approx": round((total_bytes / 1024 / 1024) / total_wall, 2) if total_wall > 0 else 0.0,
        "mean_seconds": round(statistics.mean(latencies), 3) if latencies else 0.0,
        "p50_seconds": round(percentile(latencies, 50), 3),
        "p95_seconds": round(percentile(latencies, 95), 3),
        "p99_seconds": round(percentile(latencies, 99), 3),
    }


def print_results(results, summaries):
    print("\n=== 任务明细 ===")
    print("phase,user,filename,file_id,bytes,seconds,mbps,status_code,ok,error")
    for r in results:
        print(",".join([
            r.phase,
            r.username,
            r.filename,
            r.file_id,
            str(r.bytes),
            f"{r.seconds:.3f}",
            f"{r.mbps:.2f}",
            str(r.status_code),
            str(r.ok).lower(),
            json.dumps(r.error, ensure_ascii=False),
        ]))

    print("\n=== 汇总 ===")
    print("phase,total,success,errors,error_rate_pct,bytes,wall_seconds_approx,aggregate_mbps_approx,mean_seconds,p50_seconds,p95_seconds,p99_seconds")
    for s in summaries:
        if not s:
            continue
        print(",".join(str(s[k]) for k in [
            "phase", "total", "success", "errors", "error_rate_pct", "bytes",
            "wall_seconds_approx", "aggregate_mbps_approx",
            "mean_seconds", "p50_seconds", "p95_seconds", "p99_seconds",
        ]))


def append_csv(path, label, args, summaries):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "label", "phase", "users", "files_per_user", "concurrency", "file_size",
        "total", "success", "errors", "error_rate_pct", "bytes",
        "wall_seconds_approx", "aggregate_mbps_approx",
        "mean_seconds", "p50_seconds", "p95_seconds", "p99_seconds",
    ]
    write_header = not out.exists() or out.stat().st_size == 0
    with out.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for s in summaries:
            if not s:
                continue
            row = {
                "label": label,
                "users": len([u for u in args.users.split(",") if u.strip()]),
                "files_per_user": args.files_per_user,
                "concurrency": args.concurrency,
                "file_size": args.file_size,
                **s,
            }
            writer.writerow(row)


def save_json(path, label, args, results, summaries):
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "label": label,
        "base_url": args.base_url,
        "users": [u.strip() for u in args.users.split(",") if u.strip()],
        "file_size": args.file_size,
        "files_per_user": args.files_per_user,
        "concurrency": args.concurrency,
        "summaries": summaries,
        "results": [r.__dict__ for r in results],
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:5000")
    parser.add_argument("--users", default="benchu1,benchu2,benchu3,benchu4")
    parser.add_argument("--password", default="123456")
    parser.add_argument("--file-size", default="100MB", help="单个上传文件大小，如 100MB/1GB")
    parser.add_argument("--files-per-user", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument("--chunk-size", type=parse_size, default=parse_size("4MB"), help="下载读取块大小")
    parser.add_argument("--payload-path", default=None, help="复用已有本地文件作为上传 payload")
    parser.add_argument("--force-payload", action="store_true", help="重新生成 payload")
    parser.add_argument("--skip-register", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--label", default=None)
    parser.add_argument("--csv-out", default="reports/large-file-concurrency.csv")
    parser.add_argument("--json-out", default=None)
    args = parser.parse_args()

    size = parse_size(args.file_size)
    users = [u.strip() for u in args.users.split(",") if u.strip()]
    if not users:
        raise SystemExit("--users 不能为空")
    label = args.label or time.strftime("http_large_%Y%m%d_%H%M%S")

    print("=== 多用户大文件并发压测计划 ===")
    print(f"label:          {label}")
    print(f"base_url:       {args.base_url}")
    print(f"users:          {', '.join(users)}")
    print(f"file_size:      {format_bytes(size)}")
    print(f"files_per_user: {args.files_per_user}")
    print(f"concurrency:    {args.concurrency}")
    print(f"download:       {'no' if args.skip_download else 'yes'}")

    payload_path = ensure_payload(args.payload_path, size, force=args.force_payload)

    print("\n登录 / 注册用户...")
    tokens = {}
    for username in users:
        tokens[username] = ensure_token(
            args.base_url,
            username,
            args.password,
            register=not args.skip_register,
            timeout=args.timeout,
        )
        print(f"  {username}: ok")

    upload_jobs = []
    for username in users:
        for i in range(args.files_per_user):
            remote_name = f"{label}_{username}_{i:03d}_{Path(payload_path).name}"
            upload_jobs.append((username, remote_name))

    results = []
    print(f"\n开始并发上传：{len(upload_jobs)} 个文件...")
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [
            executor.submit(upload_one, args.base_url, tokens[username], username, payload_path, remote_name, args.timeout)
            for username, remote_name in upload_jobs
        ]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            state = "ok" if result.ok else "fail"
            print(f"  upload {state}: {result.username} {result.filename} {result.seconds:.2f}s {result.error}")

    uploaded = [r for r in results if r.phase == "upload" and r.ok]
    if not args.skip_download and uploaded:
        print(f"\n开始并发下载：{len(uploaded)} 个文件...")
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            futures = [
                executor.submit(
                    download_one,
                    args.base_url,
                    tokens[r.username],
                    r.username,
                    r.file_id,
                    r.filename,
                    r.bytes,
                    args.timeout,
                    args.chunk_size,
                )
                for r in uploaded
            ]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                state = "ok" if result.ok else "fail"
                print(f"  download {state}: {result.username} {result.filename} {result.seconds:.2f}s {result.error}")

    summaries = [summarize("upload", results), summarize("download", results)]
    print_results(results, summaries)
    if args.csv_out:
        append_csv(args.csv_out, label, args, summaries)
        print(f"\n已追加 CSV: {args.csv_out}")
    if args.json_out:
        save_json(args.json_out, label, args, results, summaries)
        print(f"已写入 JSON: {args.json_out}")


if __name__ == "__main__":
    main()
