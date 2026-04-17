"""
Spark 推荐计算作业
对应课程第 7 章：大数据分析与挖掘（推荐算法）

使用 Spark 计算：
1. 基于用户行为的协同过滤推荐矩阵
2. 文件热度评分
3. 用户相似度矩阵

运行方式：
  spark-submit --master local[*] spark_jobs/recommendation.py
"""
import json
import time
import sys
import os
import math

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))
FILES_TABLE = "cloud_drive_files"
LOGS_TABLE = "cloud_drive_logs"
STATS_TABLE = "cloud_drive_stats"


def load_data(table_name):
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
    table = conn.table(table_name)
    rows = []
    for key, data in table.scan():
        row = {"_key": key.decode()}
        for k, v in data.items():
            col = k.decode().split(":", 1)[1]
            row[col] = v.decode()
        rows.append(row)
    conn.close()
    return rows


def save_stat(key, data):
    import happybase
    conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
    table = conn.table(STATS_TABLE)
    table.put(key.encode(), {
        b"data:value": json.dumps(data, ensure_ascii=False).encode(),
        b"data:updated_at": str(int(time.time() * 1000)).encode(),
    })
    conn.close()


def main():
    spark = SparkSession.builder.appName("CloudDrive-Recommendation").getOrCreate()
    sc = spark.sparkContext

    print("=" * 60)
    print("  智能云盘 - Spark 推荐计算作业")
    print("=" * 60)

    # 加载数据
    logs_raw = load_data(LOGS_TABLE)
    files_raw = load_data(FILES_TABLE)

    download_logs = [l for l in logs_raw if l.get("action") == "download"]
    print(f"下载日志: {len(download_logs)} 条, 文件总数: {len(files_raw)}")

    if not download_logs or not files_raw:
        print("数据不足，跳过推荐计算")
        spark.stop()
        return

    # ===== 1. 构建用户-文件交互矩阵 =====
    print("\n[1/3] 构建用户-文件交互矩阵...")
    interactions = [(l["username"], l.get("detail", "")) for l in download_logs if l.get("detail")]
    inter_df = spark.createDataFrame(interactions, ["username", "file_id"])

    # 计算每个用户对每个文件的交互次数
    user_file_matrix = (inter_df
                        .groupBy("username", "file_id")
                        .agg(F.count("*").alias("interaction_count"))
                        .cache())

    print(f"  交互矩阵: {user_file_matrix.count()} 条记录")

    # ===== 2. 计算用户相似度（基于 Jaccard） =====
    print("[2/3] 计算用户相似度...")
    user_files_set = (user_file_matrix
                      .groupBy("username")
                      .agg(F.collect_set("file_id").alias("files")))

    users = user_files_set.collect()
    similarities = []

    for i in range(len(users)):
        for j in range(i + 1, len(users)):
            u1, f1 = users[i]["username"], set(users[i]["files"])
            u2, f2 = users[j]["username"], set(users[j]["files"])
            intersection = len(f1 & f2)
            union = len(f1 | f2)
            if union > 0:
                jaccard = intersection / union
                if jaccard > 0:
                    similarities.append({
                        "user1": u1, "user2": u2,
                        "similarity": round(jaccard, 4),
                    })

    save_stat("user_similarity_matrix", similarities)
    print(f"  用户对数: {len(similarities)}")

    # ===== 3. 计算文件热度评分 =====
    print("[3/3] 计算文件热度评分...")
    file_map = {f["_key"]: f for f in files_raw}

    # 热度 = 下载次数 * 0.6 + 近 7 天下载次数 * 0.4
    now = int(time.time() * 1000)
    week_ago = now - 7 * 24 * 3600 * 1000

    recent_downloads = {}
    for l in download_logs:
        fid = l.get("detail", "")
        ts = int(l.get("timestamp", 0))
        if ts >= week_ago:
            recent_downloads[fid] = recent_downloads.get(fid, 0) + 1

    file_scores = []
    for f in files_raw:
        fid = f["_key"]
        total_downloads = int(f.get("downloads", 0))
        recent = recent_downloads.get(fid, 0)
        score = total_downloads * 0.6 + recent * 0.4
        file_scores.append({
            "file_id": fid,
            "filename": f.get("filename", ""),
            "owner": f.get("owner", ""),
            "type": f.get("type", ""),
            "total_downloads": total_downloads,
            "recent_downloads": recent,
            "hot_score": round(score, 2),
        })

    file_scores.sort(key=lambda x: x["hot_score"], reverse=True)
    save_stat("file_hot_scores", file_scores[:50])
    print(f"  完成，已评分 {len(file_scores)} 个文件")

    print("\n" + "=" * 60)
    print("  推荐计算完成！")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
