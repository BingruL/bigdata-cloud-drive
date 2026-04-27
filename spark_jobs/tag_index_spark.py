"""
Tag 倒排索引 — Spark 对照实现
对应课程"MapReduce vs Spark"对比：同一份业务逻辑，分别用两种范式实现。

与 mapreduce_jobs/tag_index/{mapper,reducer}.py 的区别：
- MR 版需要先把数据导出到 HDFS TSV，再 hadoop streaming 跑 mapper+reducer，最后加载回 HBase
- Spark 版直接 happybase 读 HBase，flatMap+groupByKey 一气呵成，结果回写 HBase
- Spark 用内存计算 + DAG 调度，避免了 MR 的多次磁盘 shuffle，速度快一个数量级

执行方式：
    spark-submit --master local[*] spark_jobs/tag_index_spark.py
"""
import os
import sys
import json
import time

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))
FILES_TABLE = "cloud_drive_files"
INDEX_TABLE = "cloud_drive_tag_index"


def load_files():
    """从 HBase 读取所有未删除文件的 (file_id, filename, tags)"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)
    rows = []
    try:
        table = conn.table(FILES_TABLE)
        for key, data in table.scan():
            file_id = key.decode()
            cols = {k.decode().split(":", 1)[1]: v.decode() for k, v in data.items()}
            if cols.get("deleted") == "1":
                continue
            tags = cols.get("tags", "")
            if not tags:
                continue
            rows.append((file_id, cols.get("filename", ""), tags))
    finally:
        conn.close()
    return rows


def ensure_index_table():
    """如不存在则创建 cloud_drive_tag_index"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)
    try:
        existing = [t.decode() for t in conn.tables()]
        if INDEX_TABLE not in existing:
            conn.create_table(INDEX_TABLE, {"idx": dict()})
            print(f"  创建 HBase 表: {INDEX_TABLE}")
    finally:
        conn.close()


def write_index_partition(records):
    """每个分区单独建 happybase 连接，批量写入索引行"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)
    ts = str(int(time.time() * 1000)).encode()
    written = 0
    try:
        table = conn.table(INDEX_TABLE)
        with table.batch(batch_size=100) as batch:
            for tag, payload in records:
                json_str = json.dumps(payload, ensure_ascii=False)
                batch.put(tag.encode(), {
                    b"idx:files": json_str.encode("utf-8"),
                    b"idx:count": str(payload["count"]).encode(),
                    b"idx:updated_at": ts,
                })
                written += 1
    finally:
        conn.close()
    return [written]


def main():
    spark = (SparkSession.builder
             .appName("CloudDrive-TagIndex-Spark")
             .getOrCreate())
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("=" * 60)
    print("  Tag 倒排索引 — Spark 实现")
    print("=" * 60)

    print("\n[1/4] 从 HBase 加载文件元数据...")
    rows = load_files()
    print(f"  加载 {len(rows)} 条有 tag 的文件")
    if not rows:
        print("  无数据，退出")
        spark.stop()
        return

    rdd = sc.parallelize(rows, numSlices=4)

    # ===== 核心：与 MR 版完全等价的 flatMap + groupByKey =====
    # MR mapper：每条记录拆 tag → 多条 (tag, file)；
    # MR reducer：相同 tag 聚合 → 列表
    print("[2/4] flatMap 拆分 tag 并 groupBy...")
    pairs = rdd.flatMap(lambda r: [
        (tag.strip(), {"file_id": r[0], "filename": r[1]})
        for tag in r[2].split(",") if tag.strip()
    ])
    grouped = pairs.groupByKey().mapValues(lambda files: {
        "count": len(set(f["file_id"] for f in files)),
        "files": list({f["file_id"]: f for f in files}.values()),  # 按 file_id 去重
    })

    total = grouped.count()
    print(f"  生成 {total} 个 tag 倒排项")

    print("[3/4] 确保 HBase 索引表存在...")
    ensure_index_table()

    print("[4/4] 写入 HBase cloud_drive_tag_index（每分区一连接）...")
    counts = grouped.mapPartitions(write_index_partition).collect()
    print(f"  实际写入 {sum(counts)} 项")

    print("\n" + "=" * 60)
    print("  完成。可在前端用 /api/files/by-tag/<tag> 查询。")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
