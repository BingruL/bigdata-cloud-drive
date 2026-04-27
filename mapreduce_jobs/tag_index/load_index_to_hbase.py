#!/usr/bin/env python3
"""
读取 MR 输出 /cloud-drive/mr_output/tag_index/part-* 写入 HBase 表 cloud_drive_tag_index

HBase 表设计：
    RowKey: tag（标签字符串）
    列族 idx:
        files       — JSON 数组 [{"file_id":..., "filename":...}, ...]
        count       — 文件数（便于不解析 JSON 直接做 Top-N 排序）
        updated_at  — 写入时间戳

用法：
    python3 mapreduce_jobs/tag_index/load_index_to_hbase.py
"""
import os
import sys
import json
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.config import get_config
from backend.services.hbase_service import HBaseService
from hdfs import InsecureClient

INDEX_TABLE = "cloud_drive_tag_index"


def ensure_table(hbase, table_name):
    """如果索引表不存在则创建"""
    with hbase._get_connection() as conn:
        existing = [t.decode() for t in conn.tables()]
        if table_name not in existing:
            conn.create_table(table_name, {"idx": dict()})
            print(f"  创建 HBase 表: {table_name}")
        else:
            print(f"  HBase 表已存在: {table_name}")


def main():
    config = get_config()
    print(f"[1/3] 连接 HBase {config.HBASE_HOST}:{config.HBASE_PORT} 与 HDFS {config.HDFS_URL}...")
    hbase = HBaseService(config.HBASE_HOST, config.HBASE_PORT)
    hdfs = InsecureClient(config.HDFS_URL, user=config.HDFS_USER)
    ensure_table(hbase, INDEX_TABLE)

    print("[2/3] 读取 HDFS MR 输出...")
    output_dir = f"{config.HDFS_ROOT_DIR}/mr_output/tag_index"
    parts = []
    try:
        for entry in hdfs.list(output_dir):
            if entry.startswith("part-"):
                parts.append(f"{output_dir}/{entry}")
    except Exception as e:
        print(f"  错误：无法列出 {output_dir}: {e}")
        sys.exit(1)
    if not parts:
        print(f"  错误：{output_dir} 下找不到 part-* 文件，请先跑 MR 作业")
        sys.exit(1)
    print(f"  发现 {len(parts)} 个 part 文件: {[p.split('/')[-1] for p in parts]}")

    print("[3/3] 写入 HBase 倒排索引...")
    ts = str(int(time.time() * 1000)).encode()
    written = 0
    with hbase._get_connection() as conn:
        table = conn.table(INDEX_TABLE)
        with table.batch(batch_size=100) as batch:
            for part in parts:
                with hdfs.read(part, encoding="utf-8") as reader:
                    for line in reader:
                        line = line.rstrip("\n")
                        if not line or "\t" not in line:
                            continue
                        tag, json_value = line.split("\t", 1)
                        try:
                            payload = json.loads(json_value)
                        except json.JSONDecodeError:
                            continue
                        batch.put(tag.encode(), {
                            b"idx:files": json_value.encode("utf-8"),
                            b"idx:count": str(payload.get("count", 0)).encode(),
                            b"idx:updated_at": ts,
                        })
                        written += 1
    print(f"\n完成。共写入 {written} 个 tag 倒排项到 {INDEX_TABLE}")
    print(f"验证：hbase shell 中执行 scan '{INDEX_TABLE}', {{LIMIT => 5}}")


if __name__ == "__main__":
    main()
