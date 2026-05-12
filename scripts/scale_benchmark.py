#!/usr/bin/env python3
"""
GB 级规模数据生成与处理压测脚本

这个脚本用于补齐课程评分表里的“数据处理规模与效率”证据：

1. 直接向 HBase 批量写入文件元数据和操作日志，快速构造大规模样本。
2. 按需向 HDFS 写入真实二进制 payload，用于证明存储层能承载 GB 级数据。
3. 可选触发 Spark 批统计和 Spark 标签倒排索引，记录处理耗时。

默认只生成“逻辑规模”数据：HBase 文件元数据中的 size 字段累计达到目标值，
不会真的写入 GB 级文件内容。需要真实写 HDFS 时显式传 --hdfs-bytes。

示例：
  python scripts/scale_benchmark.py --files 100000 --logs 1000000 --logical-bytes 1GB

  python scripts/scale_benchmark.py --files 10000 --logs 100000 \\
      --logical-bytes 1GB --hdfs-bytes 1GB --run-spark-stats --csv-out reports/scale.csv
"""
import argparse
import csv
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.auth.jwt_handler import hash_password
from backend.config import get_config
from backend.services.hdfs_service import HDFSService


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

FILE_TYPES = ["txt", "csv", "pdf", "md", "json", "xml", "py", "jpg", "xlsx", "zip"]
TAG_POOL = [
    "Hadoop", "HBase", "Spark", "Kafka", "MapReduce", "大数据", "数据分析",
    "课程资料", "系统设计", "日志", "统计", "推荐", "可视化", "文档",
]
ACTIONS = ["upload", "download", "preview", "search", "share", "delete"]


@dataclass
class StepMetric:
    step: str
    rows: int = 0
    bytes: int = 0
    seconds: float = 0.0
    status: str = "ok"

    @property
    def rows_per_sec(self):
        return round(self.rows / self.seconds, 2) if self.seconds > 0 and self.rows else 0.0

    @property
    def mb_per_sec(self):
        return round((self.bytes / 1024 / 1024) / self.seconds, 2) if self.seconds > 0 and self.bytes else 0.0

    def as_row(self, label):
        return {
            "label": label,
            "step": self.step,
            "rows": self.rows,
            "bytes": self.bytes,
            "seconds": round(self.seconds, 3),
            "rows_per_sec": self.rows_per_sec,
            "mb_per_sec": self.mb_per_sec,
            "status": self.status,
        }


def parse_size(value):
    if isinstance(value, int):
        return value
    match = SIZE_RE.match(str(value))
    if not match:
        raise argparse.ArgumentTypeError(f"非法大小: {value!r}，示例: 1GB, 512MB, 1048576")
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


def sanitize_label(label):
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", label.strip())
    return cleaned or f"scale_{int(time.time())}"


def distribute_sizes(total_bytes, count, rng, min_size=1024):
    """生成 count 个正整数 size，合计严格等于 total_bytes。"""
    if count <= 0:
        return []
    if total_bytes <= 0:
        return [0] * count
    if total_bytes < count * min_size:
        min_size = max(1, total_bytes // count)

    sizes = [min_size] * count
    remaining = total_bytes - min_size * count
    if remaining <= 0:
        sizes[-1] += total_bytes - sum(sizes)
        return sizes

    weights = [rng.randint(1, 100) for _ in range(count)]
    weight_sum = sum(weights)
    allocated = 0
    for i, weight in enumerate(weights):
        extra = remaining * weight // weight_sum
        sizes[i] += extra
        allocated += extra
    sizes[-1] += remaining - allocated
    rng.shuffle(sizes)
    return sizes


def ensure_hbase_tables(conn, config):
    existing = {t.decode() for t in conn.tables()}
    required = {
        config.HBASE_TABLE_USERS: {"info": dict()},
        config.HBASE_TABLE_FILES: {"meta": dict()},
        config.HBASE_TABLE_LOGS: {"log": dict()},
        config.HBASE_TABLE_STATS: {"data": dict()},
    }
    for table, families in required.items():
        if table not in existing:
            conn.create_table(table, families)


def ensure_users(conn, config, users):
    table = conn.table(config.HBASE_TABLE_USERS)
    password = hash_password("123456")
    now = str(int(time.time() * 1000)).encode()
    with table.batch(batch_size=100) as batch:
        for username in users:
            if table.row(username.encode()):
                continue
            batch.put(username.encode(), {
                b"info:password": password.encode(),
                b"info:role": b"user",
                b"info:created_at": now,
                b"info:status": b"active",
            })


def file_meta_row(label, idx, owner, size, rng, created_at, shared_group):
    ext = FILE_TYPES[idx % len(FILE_TYPES)]
    file_id = f"scale_{label}_{idx:08d}"
    filename = f"{label}_file_{idx:08d}.{ext}"
    tags = rng.sample(TAG_POOL, k=3)
    downloads = rng.randint(0, 200)
    is_shared = shared_group and rng.random() < 0.25
    return file_id, {
        b"meta:filename": filename.encode(),
        b"meta:display_name": filename.encode(),
        b"meta:parent_id": b"root",
        b"meta:size": str(size).encode(),
        b"meta:type": ext.encode(),
        b"meta:owner": owner.encode(),
        b"meta:hdfs_path": f"/cloud-drive/scale-benchmark/{label}/{file_id}.{ext}".encode(),
        b"meta:created_at": str(created_at).encode(),
        b"meta:updated_at": str(created_at).encode(),
        b"meta:downloads": str(downloads).encode(),
        b"meta:summary": b"",
        b"meta:tags": ",".join(tags).encode(),
        b"meta:is_shared": b"1" if is_shared else b"0",
        b"meta:shared_groups": shared_group.encode() if is_shared else b"",
        b"meta:benchmark_label": label.encode(),
    }


def write_file_metadata(conn, config, label, users, file_count, logical_bytes, batch_size, seed):
    rng = random.Random(seed)
    sizes = distribute_sizes(logical_bytes, file_count, rng)
    now = int(time.time() * 1000)
    start = time.perf_counter()
    table = conn.table(config.HBASE_TABLE_FILES)
    shared_group = f"scale_group_{label}"

    with table.batch(batch_size=batch_size) as batch:
        for i in range(file_count):
            owner = users[i % len(users)]
            created_at = now - rng.randint(0, 30 * 24 * 3600 * 1000)
            file_id, row = file_meta_row(label, i, owner, sizes[i], rng, created_at, shared_group)
            batch.put(file_id.encode(), row)

    return StepMetric(
        step="hbase_file_metadata",
        rows=file_count,
        bytes=logical_bytes,
        seconds=time.perf_counter() - start,
    )


def write_logs(conn, config, label, users, file_count, log_count, batch_size, seed):
    rng = random.Random(seed + 1)
    now = int(time.time() * 1000)
    start = time.perf_counter()
    table = conn.table(config.HBASE_TABLE_LOGS)

    with table.batch(batch_size=batch_size) as batch:
        for i in range(log_count):
            ts = now - rng.randint(0, 30 * 24 * 3600 * 1000)
            username = users[i % len(users)]
            action = ACTIONS[rng.randint(0, len(ACTIONS) - 1)]
            detail = f"scale_{label}_{rng.randint(0, max(0, file_count - 1)):08d}"
            row_key = f"{ts}_scale_{label}_{i:09d}"
            batch.put(row_key.encode(), {
                b"log:username": username.encode(),
                b"log:action": action.encode(),
                b"log:detail": detail.encode(),
                b"log:timestamp": str(ts).encode(),
                b"log:benchmark_label": label.encode(),
            })

    return StepMetric(
        step="hbase_operation_logs",
        rows=log_count,
        seconds=time.perf_counter() - start,
    )


def write_local_payload(path, size):
    block = (b"cloud-drive-scale-benchmark\n" * 4096)[:1024 * 1024]
    remaining = size
    with open(path, "wb") as f:
        while remaining > 0:
            chunk = block[:min(len(block), remaining)]
            f.write(chunk)
            remaining -= len(chunk)


def write_hdfs_payloads(config, label, total_bytes, chunk_bytes, keep_local):
    if total_bytes <= 0:
        return StepMetric(step="hdfs_payload", bytes=0, seconds=0.0, status="skipped")
    hdfs = HDFSService(config.HDFS_URL, config.HDFS_USER, config.HDFS_ROOT_DIR)
    target_dir = f"{config.HDFS_ROOT_DIR}/scale-benchmark/{label}"
    hdfs.client.makedirs(target_dir)

    tmp_dir = tempfile.mkdtemp(prefix="cloud-drive-scale-")
    start = time.perf_counter()
    written = 0
    files = 0
    try:
        while written < total_bytes:
            size = min(chunk_bytes, total_bytes - written)
            local_path = os.path.join(tmp_dir, f"payload-{files:05d}.bin")
            write_local_payload(local_path, size)
            hdfs_path = f"{target_dir}/payload-{files:05d}.bin"
            hdfs.client.upload(hdfs_path, local_path, overwrite=True)
            written += size
            files += 1
            print(f"  HDFS payload {files}: {format_bytes(written)} / {format_bytes(total_bytes)}")
    finally:
        if keep_local:
            print(f"  保留本地 payload 目录: {tmp_dir}")
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return StepMetric(
        step="hdfs_payload",
        rows=files,
        bytes=written,
        seconds=time.perf_counter() - start,
    )


def run_command(step, cmd):
    if not shutil.which(cmd[0]):
        return StepMetric(step=step, status=f"skipped: {cmd[0]} not found")

    start = time.perf_counter()
    print(f"\n=== 运行 {step}: {' '.join(cmd)} ===")
    result = subprocess.run(cmd, cwd=ROOT, text=True)
    status = "ok" if result.returncode == 0 else f"failed:{result.returncode}"
    return StepMetric(step=step, seconds=time.perf_counter() - start, status=status)


def write_csv(path, label, metrics):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["label", "step", "rows", "bytes", "seconds", "rows_per_sec", "mb_per_sec", "status"]
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for metric in metrics:
            writer.writerow(metric.as_row(label))


def print_summary(label, metrics):
    print("\n=== 规模实验摘要 ===")
    print("label,step,rows,bytes,seconds,rows_per_sec,mb_per_sec,status")
    for metric in metrics:
        row = metric.as_row(label)
        print(",".join(str(row[k]) for k in (
            "label", "step", "rows", "bytes", "seconds", "rows_per_sec", "mb_per_sec", "status"
        )))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default=None, help="本次数据集标签，默认按时间生成")
    parser.add_argument("--users", default="alice,bob,charlie,diana",
                        help="逗号分隔的生成用户；不存在时自动创建，密码 123456")
    parser.add_argument("--files", type=int, default=100000, help="生成文件元数据行数")
    parser.add_argument("--logs", type=int, default=1000000, help="生成操作日志行数")
    parser.add_argument("--logical-bytes", type=parse_size, default=parse_size("1GB"),
                        help="写入文件元数据 size 字段的总规模，如 1GB/10GB")
    parser.add_argument("--hdfs-bytes", type=parse_size, default=0,
                        help="真实写入 HDFS 的二进制规模，默认 0 表示不写真实大文件")
    parser.add_argument("--hdfs-chunk-bytes", type=parse_size, default=parse_size("64MB"),
                        help="HDFS payload 单文件大小，默认 64MB")
    parser.add_argument("--batch-size", type=int, default=1000, help="HBase batch size")
    parser.add_argument("--seed", type=int, default=20260512, help="随机种子")
    parser.add_argument("--run-spark-stats", action="store_true",
                        help="生成后运行 spark_jobs/file_stats.py 并计时")
    parser.add_argument("--run-spark-tag-index", action="store_true",
                        help="生成后运行 spark_jobs/tag_index_spark.py 并计时")
    parser.add_argument("--csv-out", default=None, help="将摘要追加写入 CSV")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不写入数据")
    parser.add_argument("--keep-local-payload", action="store_true",
                        help="真实写 HDFS 后保留临时本地 payload 文件")
    args = parser.parse_args()

    label = sanitize_label(args.label or time.strftime("scale_%Y%m%d_%H%M%S"))
    users = [u.strip() for u in args.users.split(",") if u.strip()]
    if not users:
        raise SystemExit("--users 不能为空")

    print("=== GB 级规模实验计划 ===")
    print(f"标签:            {label}")
    print(f"用户:            {', '.join(users)}")
    print(f"文件元数据:      {args.files:,} 行")
    print(f"操作日志:        {args.logs:,} 行")
    print(f"逻辑文件规模:    {format_bytes(args.logical_bytes)}")
    print(f"真实 HDFS 写入:  {format_bytes(args.hdfs_bytes)}")
    print(f"HBase batch:     {args.batch_size}")
    print(f"Spark 统计:      {'yes' if args.run_spark_stats else 'no'}")
    print(f"Spark 标签索引:  {'yes' if args.run_spark_tag_index else 'no'}")

    if args.dry_run:
        return

    import happybase
    config = get_config()
    metrics = []
    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT, timeout=30000)
    try:
        ensure_hbase_tables(conn, config)
        ensure_users(conn, config, users)
        metrics.append(write_file_metadata(
            conn, config, label, users, args.files, args.logical_bytes,
            args.batch_size, args.seed,
        ))
        metrics.append(write_logs(
            conn, config, label, users, args.files, args.logs,
            args.batch_size, args.seed,
        ))
    finally:
        conn.close()

    metrics.append(write_hdfs_payloads(
        config, label, args.hdfs_bytes, args.hdfs_chunk_bytes, args.keep_local_payload
    ))

    if args.run_spark_stats:
        metrics.append(run_command(
            "spark_file_stats",
            ["spark-submit", "--master", config.SPARK_MASTER, "spark_jobs/file_stats.py"],
        ))
    if args.run_spark_tag_index:
        metrics.append(run_command(
            "spark_tag_index",
            ["spark-submit", "--master", config.SPARK_MASTER, "spark_jobs/tag_index_spark.py"],
        ))

    print_summary(label, metrics)
    if args.csv_out:
        write_csv(args.csv_out, label, metrics)
        print(f"\n已追加写入 CSV: {args.csv_out}")


if __name__ == "__main__":
    main()
