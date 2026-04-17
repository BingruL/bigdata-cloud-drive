"""
Spark 统计分析作业
对应课程第 8 章：Spark 内存计算框架

本脚本使用 PySpark 对文件元数据和操作日志进行批量统计分析：
1. 各用户文件数量统计
2. 文件类型分布
3. 每日上传趋势
4. 热门文件排行
5. 用户存储空间统计

运行方式：
  spark-submit --master local[*] spark_jobs/file_stats.py

结果写回 HBase 的 cloud_drive_stats 表
"""
import json
import time
import sys
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 将项目根目录加入 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# HBase 配置
HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))
FILES_TABLE = "cloud_drive_files"
LOGS_TABLE = "cloud_drive_logs"
STATS_TABLE = "cloud_drive_stats"


def get_spark_session():
    """创建 Spark Session"""
    return (SparkSession.builder
            .appName("CloudDrive-FileStatistics")
            .getOrCreate())


def load_files_from_hbase():
    """从 HBase 加载文件元数据"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
    table = conn.table(FILES_TABLE)
    rows = []
    for key, data in table.scan():
        row = {"file_id": key.decode()}
        for k, v in data.items():
            col = k.decode().split(":", 1)[1]
            row[col] = v.decode()
        rows.append(row)
    conn.close()
    return rows


def load_logs_from_hbase():
    """从 HBase 加载操作日志"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
    table = conn.table(LOGS_TABLE)
    rows = []
    for key, data in table.scan():
        row = {"log_id": key.decode()}
        for k, v in data.items():
            col = k.decode().split(":", 1)[1]
            row[col] = v.decode()
        rows.append(row)
    conn.close()
    return rows


def save_stat_to_hbase(stat_key, data):
    """将统计结果写回 HBase"""
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
    table = conn.table(STATS_TABLE)
    table.put(stat_key.encode(), {
        b"data:value": json.dumps(data, ensure_ascii=False).encode(),
        b"data:updated_at": str(int(time.time() * 1000)).encode(),
    })
    conn.close()


def main():
    spark = get_spark_session()
    sc = spark.sparkContext

    print("=" * 60)
    print("  智能云盘 - Spark 统计分析作业")
    print("=" * 60)

    # ===== 1. 加载数据 =====
    print("\n[1/6] 从 HBase 加载数据...")
    files_raw = load_files_from_hbase()
    logs_raw = load_logs_from_hbase()
    print(f"  文件数: {len(files_raw)}, 日志数: {len(logs_raw)}")

    if not files_raw:
        print("  暂无文件数据，退出分析")
        spark.stop()
        return

    # 转为 DataFrame
    files_df = spark.createDataFrame(files_raw)
    files_df = files_df.withColumn("size_long", F.col("size").cast(LongType()))
    files_df = files_df.withColumn("downloads_long", F.col("downloads").cast(LongType()))
    files_df = files_df.withColumn("created_ts", F.col("created_at").cast(LongType()))

    # ===== 2. 各用户文件数量统计 =====
    print("[2/6] 统计各用户文件数量...")
    user_counts = (files_df
                   .groupBy("owner")
                   .agg(
                       F.count("*").alias("file_count"),
                       F.sum("size_long").alias("total_size"),
                   )
                   .orderBy(F.desc("file_count"))
                   .collect())

    user_stats = [{"username": r["owner"], "count": r["file_count"],
                   "total_size": r["total_size"]} for r in user_counts]
    save_stat_to_hbase("user_file_counts", user_stats)
    print(f"  完成，共 {len(user_stats)} 个用户")

    # ===== 3. 文件类型分布 =====
    print("[3/6] 统计文件类型分布...")
    type_dist = (files_df
                 .groupBy("type")
                 .agg(F.count("*").alias("count"))
                 .orderBy(F.desc("count"))
                 .collect())

    type_stats = [{"type": r["type"], "count": r["count"]} for r in type_dist]
    save_stat_to_hbase("file_type_distribution", type_stats)
    print(f"  完成，共 {len(type_stats)} 种类型")

    # ===== 4. 每日上传趋势（最近30天） =====
    print("[4/6] 统计每日上传趋势...")
    files_with_date = files_df.withColumn(
        "upload_date",
        F.from_unixtime(F.col("created_ts") / 1000, "yyyy-MM-dd")
    )
    daily_counts = (files_with_date
                    .groupBy("upload_date")
                    .agg(F.count("*").alias("count"))
                    .orderBy("upload_date")
                    .collect())

    daily_stats = [{"date": r["upload_date"], "count": r["count"]} for r in daily_counts]
    save_stat_to_hbase("daily_upload_trend", daily_stats)
    print(f"  完成，共 {len(daily_stats)} 天数据")

    # ===== 5. 热门文件排行 =====
    print("[5/6] 统计热门文件排行...")
    hot = (files_df
           .orderBy(F.desc("downloads_long"))
           .limit(20)
           .collect())

    hot_stats = [{
        "file_id": r["file_id"],
        "filename": r["filename"],
        "downloads": r["downloads_long"],
        "owner": r["owner"],
        "type": r["type"],
    } for r in hot]
    save_stat_to_hbase("hot_files", hot_stats)
    print(f"  完成，Top {len(hot_stats)} 热门文件")

    # ===== 6. 总体概览 =====
    print("[6/6] 生成总体概览...")
    summary = {
        "total_files": files_df.count(),
        "total_size": files_df.agg(F.sum("size_long")).collect()[0][0] or 0,
        "total_downloads": files_df.agg(F.sum("downloads_long")).collect()[0][0] or 0,
        "total_users": files_df.select("owner").distinct().count(),
        "computed_at": int(time.time() * 1000),
    }
    save_stat_to_hbase("dashboard_summary", summary)
    print(f"  完成: {summary}")

    print("\n" + "=" * 60)
    print("  所有统计任务完成！结果已写入 HBase")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
