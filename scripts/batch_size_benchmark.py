#!/usr/bin/env python3
"""
HBase 批量写入 batch_size 对照实验

固定写入行数，扫描多组 batch_size，记录耗时和吞吐，输出 CSV。
用于实验报告里证明"批量 put 相对单条 put 的性能收益"。

示例：
  python scripts/batch_size_benchmark.py \\
    --rows 5000 \\
    --batch-sizes 1,10,100,500,1000,5000 \\
    --csv-out reports/batch-size-benchmark.csv
"""
import argparse
import csv
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from backend.config import get_config

FILE_TYPES = ["txt", "csv", "pdf", "md", "json"]


def write_file_metadata(table, label, rows, batch_size, seed):
    rng = random.Random(seed)
    now = int(time.time() * 1000)
    start = time.perf_counter()
    with table.batch(batch_size=batch_size) as batch:
        for i in range(rows):
            ext = FILE_TYPES[i % len(FILE_TYPES)]
            file_id = f"batchcmp_{label}_b{batch_size:05d}_{i:08d}"
            filename = f"batchcmp_{label}_b{batch_size:05d}_{i:08d}.{ext}"
            size = rng.randint(1024, 1024 * 1024)
            batch.put(file_id.encode(), {
                b"meta:filename": filename.encode(),
                b"meta:display_name": filename.encode(),
                b"meta:parent_id": b"root",
                b"meta:size": str(size).encode(),
                b"meta:type": ext.encode(),
                b"meta:owner": b"alice",
                b"meta:hdfs_path": f"/cloud-drive/batchcmp/{file_id}.{ext}".encode(),
                b"meta:created_at": str(now).encode(),
                b"meta:updated_at": str(now).encode(),
                b"meta:downloads": b"0",
                b"meta:tags": b"benchmark,batch_size",
                b"meta:is_shared": b"0",
                b"meta:shared_groups": b"",
                b"meta:benchmark_label": f"batchcmp_{label}".encode(),
            })
    return time.perf_counter() - start


def cleanup_rows(table, label):
    """Delete rows produced by this benchmark to keep HBase tidy between runs."""
    prefix = f"batchcmp_{label}_".encode()
    deleted = 0
    for row_key, _ in table.scan(row_prefix=prefix, limit=100000):
        table.delete(row_key)
        deleted += 1
    return deleted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5000,
                        help="每个 batch_size 写入的行数（固定值便于横向比较）")
    parser.add_argument("--batch-sizes", default="1,10,100,500,1000,5000",
                        help="逗号分隔的 batch_size 列表")
    parser.add_argument("--seed", type=int, default=20260512)
    parser.add_argument("--label", default=None,
                        help="本次实验标签，默认基于时间戳生成")
    parser.add_argument("--csv-out", default="reports/batch-size-benchmark.csv")
    parser.add_argument("--keep-rows", action="store_true",
                        help="实验结束后不删除写入的测试行")
    args = parser.parse_args()

    label = args.label or time.strftime("batchcmp_%Y%m%d_%H%M%S")
    batch_sizes = [int(x.strip()) for x in args.batch_sizes.split(",") if x.strip()]
    if not batch_sizes:
        raise SystemExit("--batch-sizes 不能为空")

    import happybase
    config = get_config()
    conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT, timeout=60000)
    try:
        existing = {t.decode() for t in conn.tables()}
        if config.HBASE_TABLE_FILES not in existing:
            conn.create_table(config.HBASE_TABLE_FILES, {"meta": dict()})
        table = conn.table(config.HBASE_TABLE_FILES)

        print(f"=== HBase batch_size 对照实验 ===")
        print(f"label:        {label}")
        print(f"rows / group: {args.rows:,}")
        print(f"batch_sizes:  {batch_sizes}")
        print()

        results = []
        for bs in batch_sizes:
            print(f"[batch_size={bs}] 写入 {args.rows:,} 行 ...", flush=True)
            seconds = write_file_metadata(table, label, args.rows, bs, args.seed)
            rows_per_sec = round(args.rows / seconds, 2) if seconds > 0 else 0.0
            speedup_vs_1 = None  # filled later
            results.append({
                "label": label,
                "batch_size": bs,
                "rows": args.rows,
                "seconds": round(seconds, 4),
                "rows_per_sec": rows_per_sec,
            })
            print(f"  -> {seconds:.3f}s   {rows_per_sec:,.2f} rows/s")
            cleanup_rows(table, label)

        # speedup vs batch_size=1
        baseline = next((r for r in results if r["batch_size"] == 1), None)
        if baseline and baseline["seconds"] > 0:
            for r in results:
                r["speedup_vs_b1"] = round(baseline["seconds"] / r["seconds"], 2) if r["seconds"] > 0 else 0.0
        else:
            for r in results:
                r["speedup_vs_b1"] = ""

    finally:
        conn.close()

    csv_path = Path(args.csv_out)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["label", "batch_size", "rows", "seconds", "rows_per_sec", "speedup_vs_b1"]
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for r in results:
            writer.writerow(r)

    print()
    print(f"已写入 {csv_path}")
    print()
    print(f"{'batch_size':>10} {'seconds':>10} {'rows/sec':>12} {'speedup':>10}")
    for r in results:
        print(f"{r['batch_size']:>10} {r['seconds']:>10.3f} {r['rows_per_sec']:>12,.2f} "
              f"{str(r['speedup_vs_b1']):>10}")


if __name__ == "__main__":
    main()
