"""
Spark Structured Streaming 实时统计作业
对应课程第 8 章：Spark 流计算 + Lambda 架构 speed layer

订阅 Kafka topic `cloud_drive_events`，按 2 秒微批计算实时指标，
结果写入 HBase `cloud_drive_stats` 表（rowkey 以 `realtime_` 开头）。
后端 `/api/stats/realtime` 读取这些行渲染前端实时面板。

写入的行：
  realtime_action_counts   —— 最近 60s 各动作计数：{"upload": 3, "download": 8, ...}
  realtime_active_users    —— 最近 5min 活跃用户：{"count": 4, "users": [...]}
  realtime_hot_files       —— 最近 60s 上传/下载次数 Top 5：[{"file_id": ..., "count": N}, ...]
  realtime_event_stream    —— 最新 30 条事件（按时间倒序）

启动方式：
  export KAFKA_ENABLED=1
  export KAFKA_BOOTSTRAP=localhost:9092

  spark-submit \\
    --master local[*] \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
    spark_jobs/streaming_stats.py

依赖：spark-sql-kafka 包（首次运行 Maven 自动下载）；happybase（pip install happybase）
"""
import os
import sys
import json
import time
from collections import deque, Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ===== 配置（与 backend.config 对齐，独立读环境变量避免依赖 Flask） =====
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_EVENTS", "cloud_drive_events")
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))
STATS_TABLE = "cloud_drive_stats"

ACTION_WINDOW_SEC = 60      # 动作计数 / 热门文件窗口
ACTIVE_USER_WINDOW_SEC = 300  # 活跃用户窗口
EVENT_STREAM_KEEP = 30      # 事件流保留条数
TRIGGER_INTERVAL = "2 seconds"

# 事件 schema —— 与 EventBus 产出格式严格一致
EVENT_SCHEMA = StructType([
    StructField("username", StringType(), True),
    StructField("action", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("timestamp", LongType(), True),
])


# ===== 跨 batch 维护的滚动状态 =====
# 注：foreachBatch 在 driver 端执行，这些全局结构体是安全的；
# 不要把状态放在 worker 任务里。
_recent_events = deque(maxlen=500)   # (ts_ms, username, action, detail) 全部保留 5 分钟内
_last_log_time = [0.0]


def _prune(now_ms):
    """裁剪掉超出最大窗口（5 分钟）的事件"""
    cutoff = now_ms - ACTIVE_USER_WINDOW_SEC * 1000
    while _recent_events and _recent_events[0][0] < cutoff:
        _recent_events.popleft()


def _save_stats_to_hbase(rows):
    """把多个 (rowkey, value_dict) 一次性写入 HBase"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT, timeout=10000)
    try:
        table = conn.table(STATS_TABLE)
        ts = str(int(time.time() * 1000)).encode()
        with table.batch() as b:
            for key, value in rows:
                b.put(key.encode(), {
                    b"data:value": json.dumps(value, ensure_ascii=False).encode(),
                    b"data:updated_at": ts,
                })
    finally:
        conn.close()


def write_batch_to_hbase(batch_df, batch_id):
    """每个微批触发一次：合并新事件到滚动状态，重算指标，写 HBase"""
    if batch_df.rdd.isEmpty():
        new_events = []
    else:
        # collect 仅在驱动节点处理少量微批数据，可接受
        new_events = batch_df.collect()

    now_ms = int(time.time() * 1000)
    for r in new_events:
        ts = r["timestamp"] or now_ms
        _recent_events.append((ts, r["username"] or "", r["action"] or "", r["detail"] or ""))
    _prune(now_ms)

    # ===== 指标 1：最近 60s 动作计数 =====
    cutoff_60 = now_ms - ACTION_WINDOW_SEC * 1000
    last60 = [e for e in _recent_events if e[0] >= cutoff_60]
    action_counts = Counter(e[2] for e in last60 if e[2])

    # ===== 指标 2：最近 5min 活跃用户 =====
    active_users = sorted({e[1] for e in _recent_events if e[1]})

    # ===== 指标 3：最近 60s 热门文件（upload / download / preview 类动作） =====
    hot_actions = {"upload", "download", "preview"}
    file_counter = Counter(e[3] for e in last60 if e[2] in hot_actions and e[3])
    hot_files = [{"file_id": fid, "count": c} for fid, c in file_counter.most_common(5)]

    # ===== 指标 4：最近 30 条事件流（按时间倒序） =====
    event_stream = [
        {"timestamp": e[0], "username": e[1], "action": e[2], "detail": e[3]}
        for e in list(_recent_events)[-EVENT_STREAM_KEEP:][::-1]
    ]

    rows = [
        ("realtime_action_counts", dict(action_counts)),
        ("realtime_active_users", {"count": len(active_users), "users": active_users}),
        ("realtime_hot_files", hot_files),
        ("realtime_event_stream", event_stream),
    ]
    try:
        _save_stats_to_hbase(rows)
    except Exception as e:
        print(f"[batch {batch_id}] HBase 写入失败: {e}", file=sys.stderr)
        return

    # 限制 stdout 噪音：每 10 秒打印一次摘要
    now = time.time()
    if now - _last_log_time[0] > 10:
        _last_log_time[0] = now
        print(f"[batch {batch_id}] 新事件={len(new_events)} "
              f"窗口内事件={len(last60)} 活跃用户={len(active_users)} "
              f"热门文件={len(hot_files)}")


def main():
    spark = (SparkSession.builder
             .appName("CloudDrive-StreamingStats")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  智能云盘 - Spark Structured Streaming 实时统计")
    print(f"  Kafka: {KAFKA_BOOTSTRAP}  topic: {KAFKA_TOPIC}")
    print(f"  HBase: {HBASE_HOST}:{HBASE_PORT}  → {STATS_TABLE}")
    print("=" * 60)

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "latest")
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw
              .selectExpr("CAST(value AS STRING) AS json")
              .select(F.from_json("json", EVENT_SCHEMA).alias("e"))
              .select("e.*"))

    query = (parsed.writeStream
             .foreachBatch(write_batch_to_hbase)
             .outputMode("append")
             .trigger(processingTime=TRIGGER_INTERVAL)
             .option("checkpointLocation", "/tmp/cloud-drive-streaming-ckpt")
             .start())

    print("Streaming 已启动，等待事件...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
