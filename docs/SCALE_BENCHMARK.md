# GB 级规模与效率实验

本项目提供 `scripts/scale_benchmark.py`，用于补充“数据处理规模与效率”评分项的实验依据。

## 实验思路

- **逻辑规模**：批量写入 HBase 文件元数据，把 `size` 字段累计到 1GB、10GB 或更高，用于测试 Spark/HBase 统计处理能力。
- **日志规模**：批量写入 HBase 操作日志，例如 100 万条下载、预览、搜索、分享事件，用于测试统计聚合和推荐输入规模。
- **真实 HDFS payload**：可选真实写入 GB 级二进制文件，用于测试 HDFS 写入吞吐和存储承载能力。
- **处理计时**：可选运行 Spark 文件统计与 Spark 标签倒排索引，输出耗时、吞吐、CSV 摘要。

## 快速运行

只生成 1GB 逻辑数据，不真实写入大文件：

```bash
python scripts/scale_benchmark.py \
  --files 100000 \
  --logs 1000000 \
  --logical-bytes 1GB \
  --csv-out reports/scale-benchmark.csv
```

生成 1GB 逻辑数据，并真实向 HDFS 写入 1GB payload：

```bash
python scripts/scale_benchmark.py \
  --files 100000 \
  --logs 1000000 \
  --logical-bytes 1GB \
  --hdfs-bytes 1GB \
  --hdfs-chunk-bytes 64MB \
  --csv-out reports/scale-benchmark.csv
```

生成后顺带运行 Spark 批处理统计和标签索引：

```bash
python scripts/scale_benchmark.py \
  --files 100000 \
  --logs 1000000 \
  --logical-bytes 1GB \
  --run-spark-stats \
  --run-spark-tag-index \
  --csv-out reports/scale-benchmark.csv
```

也可以使用 Makefile 的默认实验：

```bash
make scale-benchmark
```

## 报告建议

把 `reports/scale-benchmark.csv` 中的结果整理成表格：

| 实验项 | 数据规模 | 耗时 | 吞吐 |
|---|---:|---:|---:|
| HBase 文件元数据写入 | 100000 行 / 1GB 逻辑规模 | 以实际输出为准 | rows/s |
| HBase 操作日志写入 | 1000000 行 | 以实际输出为准 | rows/s |
| HDFS payload 写入 | 1GB | 以实际输出为准 | MB/s |
| Spark 文件统计 | 100000 文件 + 1000000 日志 | 以实际输出为准 | - |
| Spark 标签倒排索引 | 100000 文件 | 以实际输出为准 | - |

答辩时建议准确表述：

> 项目通过规模实验构造了 GB 级逻辑文件规模和百万级操作日志，并可选真实写入 GB 级 HDFS payload。实验重点验证 HBase 元数据扫描、Spark 批处理、Spark 标签索引和 HDFS 写入吞吐，不把演示环境中的小样本冒充为生产级数据。
