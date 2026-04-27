#!/usr/bin/env bash
# 端到端跑通 Tag 倒排索引 MapReduce 流水线
# 步骤：HBase → HDFS → Hadoop Streaming → HDFS → HBase
#
# 前置条件：
#   - HDFS NameNode 在 9870 端口
#   - HBase Thrift Server 在 9090 端口
#   - $HADOOP_HOME 已设置，hadoop 命令在 PATH 中
#
# 用法：bash mapreduce_jobs/tag_index/run.sh
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
JOB_DIR="$PROJECT_ROOT/mapreduce_jobs/tag_index"

# 默认 HDFS 路径
HDFS_INPUT="${HDFS_INPUT:-/cloud-drive/mr_input/files.tsv}"
HDFS_OUTPUT="${HDFS_OUTPUT:-/cloud-drive/mr_output/tag_index}"

# 自动定位 hadoop-streaming jar
if [[ -z "${HADOOP_STREAMING_JAR:-}" ]]; then
    HADOOP_STREAMING_JAR=$(find "${HADOOP_HOME:-/opt/hadoop}/share/hadoop/tools/lib" \
                          -name "hadoop-streaming-*.jar" 2>/dev/null | head -n 1 || true)
fi
if [[ -z "$HADOOP_STREAMING_JAR" ]]; then
    echo "[错误] 找不到 hadoop-streaming-*.jar，请 export HADOOP_STREAMING_JAR=/path/to/jar"
    exit 1
fi

echo "============================================================"
echo "  Tag 倒排索引 MapReduce 流水线"
echo "  Streaming JAR: $HADOOP_STREAMING_JAR"
echo "  输入:  $HDFS_INPUT"
echo "  输出:  $HDFS_OUTPUT"
echo "============================================================"

echo ""
echo "[STEP 1/4] 从 HBase 导出文件元数据到 HDFS..."
python3 "$JOB_DIR/export_files_to_hdfs.py"

echo ""
echo "[STEP 2/4] 清理旧的 MR 输出目录（如果存在）..."
hadoop fs -rm -r -f -skipTrash "$HDFS_OUTPUT" || true

echo ""
echo "[STEP 3/4] 提交 Hadoop Streaming MR 作业..."
hadoop jar "$HADOOP_STREAMING_JAR" \
    -D mapreduce.job.name="cloud-drive-tag-index" \
    -D mapreduce.job.reduces=2 \
    -files "$JOB_DIR/mapper.py,$JOB_DIR/reducer.py" \
    -input "$HDFS_INPUT" \
    -output "$HDFS_OUTPUT" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"

echo ""
echo "[STEP 4/4] 加载 MR 输出到 HBase cloud_drive_tag_index..."
python3 "$JOB_DIR/load_index_to_hbase.py"

echo ""
echo "============================================================"
echo "  全部完成。可在前端用 GET /api/files/by-tag/<tag> 查询。"
echo "============================================================"
