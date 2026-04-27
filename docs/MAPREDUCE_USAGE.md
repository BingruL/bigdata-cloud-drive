# MapReduce 标签倒排索引使用指南

> 本文档介绍 `mapreduce_jobs/tag_index/` 下的 MapReduce 作业：基于 Hadoop Streaming + Python 构建文件标签的倒排索引。
> 同时提供 Spark 对照实现（`spark_jobs/tag_index_spark.py`），方便答辩时直接对比 MR 与 Spark 两种范式。

## 一、为什么做这件事

| 角度 | 说明 |
|---|---|
| **课程要求** | 大数据课程的 MapReduce 章节，倒排索引是经典题型（搜索引擎的核心结构） |
| **业务价值** | 给 `cloud_drive_files.meta:tags` 字段建索引，前端"按标签搜索"无需 scan 全表 |
| **架构对照** | MR 和 Spark 两种范式同写一份逻辑，凸显计算范式的演进 |

## 二、整体流程

```
┌─────────────────────────┐
│ HBase cloud_drive_files │  rowkey: file_id
│   meta:tags = "Hadoop,大数据,文档"
└─────────────┬───────────┘
              │ ① export_files_to_hdfs.py
              ▼
┌─────────────────────────┐
│ HDFS /cloud-drive/mr_input/files.tsv │
│   file_id \t filename \t tags        │
└─────────────┬───────────┘
              │ ② Hadoop Streaming
              │   mapper.py：每条记录拆 tag → 多条 (tag, file_id|filename)
              │   reducer.py：相同 tag 聚合 → JSON 数组
              ▼
┌─────────────────────────┐
│ HDFS /cloud-drive/mr_output/tag_index/part-* │
│   tag \t {"count": N, "files": [...]}        │
└─────────────┬───────────┘
              │ ③ load_index_to_hbase.py
              ▼
┌─────────────────────────┐
│ HBase cloud_drive_tag_index │
│   rowkey: tag             │
│   idx:files = JSON 数组    │
│   idx:count = N            │
│   idx:updated_at = ts      │
└─────────────┬───────────┘
              │ ④ Flask GET /api/files/by-tag/<tag>
              ▼
        前端按标签搜索
```

## 三、运行（一键脚本）

### 3.1 前置条件

- HDFS NameNode 在 9870 端口、HBase Thrift Server 在 9090 端口都已启动
- `$HADOOP_HOME` 已设置，`hadoop` 命令在 PATH 中
- 自动定位 `hadoop-streaming-*.jar`；如失败请 `export HADOOP_STREAMING_JAR=/path/to/jar`
- Python 3 已装 `happybase` 和 `hdfs` 包（`pip install -r backend/requirements.txt` 已包含）

### 3.2 一键执行

```bash
bash mapreduce_jobs/tag_index/run.sh
```

脚本依次完成 4 步：

1. `export_files_to_hdfs.py` — 从 HBase 扫描所有未删除文件 → 写入 HDFS TSV
2. 清理旧 MR 输出目录
3. `hadoop jar hadoop-streaming.jar ... -mapper mapper.py -reducer reducer.py`
4. `load_index_to_hbase.py` — 读 MR 输出 → 写 `cloud_drive_tag_index`

### 3.3 分步调试

```bash
# 仅导出
python3 mapreduce_jobs/tag_index/export_files_to_hdfs.py

# 仅跑 MR
hadoop fs -rm -r -f /cloud-drive/mr_output/tag_index
hadoop jar $HADOOP_STREAMING_JAR \
    -files mapreduce_jobs/tag_index/mapper.py,mapreduce_jobs/tag_index/reducer.py \
    -input /cloud-drive/mr_input/files.tsv \
    -output /cloud-drive/mr_output/tag_index \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"

# 查看 MR 输出
hadoop fs -cat /cloud-drive/mr_output/tag_index/part-* | head

# 仅加载到 HBase
python3 mapreduce_jobs/tag_index/load_index_to_hbase.py
```

### 3.4 本地纯命令行验证（无需 Hadoop）

mapper/reducer 都是标准 stdin/stdout，可以用 shell 模拟一次运行，方便快速验证逻辑：

```bash
printf 'f1\tHadoop教程.pdf\tHadoop,大数据,文档\nf2\t风景.jpg\t摄影\n' \
  | python3 mapreduce_jobs/tag_index/mapper.py \
  | sort \
  | python3 mapreduce_jobs/tag_index/reducer.py
```

输出：
```
Hadoop  {"count": 1, "files": [{"file_id": "f1", "filename": "Hadoop教程.pdf"}]}
大数据  {"count": 1, "files": [{"file_id": "f1", "filename": "Hadoop教程.pdf"}]}
摄影    {"count": 1, "files": [{"file_id": "f2", "filename": "风景.jpg"}]}
文档    {"count": 1, "files": [{"file_id": "f1", "filename": "Hadoop教程.pdf"}]}
```

## 四、查询索引（前端入口）

```bash
# 已登录用户用 token 调用
curl -H "Authorization: Bearer $TOKEN" http://localhost:5000/api/files/by-tag/大数据
```

返回（已应用权限过滤）：

```json
{
  "tag": "大数据",
  "count": 3,
  "files": [
    {"file_id": "...", "filename": "数据分析.csv", "owner": "alice", ...},
    ...
  ],
  "index_updated_at": "1714214400123"
}
```

如索引表尚未生成，会返回 `hint` 字段引导先跑作业：

```json
{
  "tag": "大数据", "files": [], "count": 0,
  "hint": "倒排索引表尚未生成，请先运行 mapreduce_jobs/tag_index/run.sh ..."
}
```

## 五、Spark 对照实现

```bash
spark-submit --master local[*] spark_jobs/tag_index_spark.py
```

写入的目标表完全相同（`cloud_drive_tag_index`），所以前端不感知是哪边产生的索引。

## 六、MR vs Spark 范式对比（答辩用）

| 维度 | MapReduce 版本 | Spark 版本 |
|---|---|---|
| 输入读取 | 必须先把 HBase 数据导出为 HDFS TSV | RDD 直接 `parallelize(load_files())` |
| 拆 tag | mapper.py 中 `for tag in tags.split(",")` | `flatMap(lambda r: [(tag, file) for tag in ...])` |
| 聚合 | shuffle 后 reducer 顺序扫描相同 key | `groupByKey().mapValues(...)` |
| 中间数据 | 落盘到 HDFS（map 输出 + shuffle 输出） | 内存为主，必要时溢出磁盘 |
| 调度 | 单 stage：Map → Shuffle → Reduce | DAG：可链式 transform，多 stage 自动优化 |
| 写回 HBase | 单独脚本 `load_index_to_hbase.py` | `mapPartitions` 内每分区一条 happybase 连接，并行写 |
| 代码量 | 4 个文件（mapper/reducer/export/load） | 1 个文件 |
| 性能 | 每步落盘，约 N×IO 开销 | 内存计算，DAG 优化，比 MR 快约 10× |

**这正是为什么工业界已经基本用 Spark 替代了 MR**——但 MR 的概念（map / shuffle / reduce 三阶段）仍是理解 Spark stage 划分的基础。

## 七、HBase 表设计

```
表名:    cloud_drive_tag_index
RowKey:  tag（标签字符串，UTF-8）
列族 idx:
    idx:files       JSON 数组 [{"file_id":..., "filename":...}, ...]
    idx:count       整数（便于不解析 JSON 直接做 Top-N 排序）
    idx:updated_at  时间戳（毫秒）
```

为什么 RowKey 用 tag 本身而非哈希？

- HBase RowKey 按字典序排列，相邻 tag 物理相邻，未来如果做 "tag 前缀建议" 可直接 `scan(row_prefix='Hadoop')`
- 标签数量级在万级以内，热点风险低，无需打散

## 八、常见问题

### Q1. `hadoop streaming` 提示找不到 python3

容器化或异构环境下 worker 节点可能没有 python3。临时解决：

```bash
hadoop jar $HADOOP_STREAMING_JAR \
    -D mapreduce.map.env="PATH=/usr/local/bin:/usr/bin:/bin" \
    -D mapreduce.reduce.env="PATH=/usr/local/bin:/usr/bin:/bin" \
    ...
```

或在 `-mapper` 里写绝对路径：`-mapper "/usr/bin/python3 mapper.py"`。

### Q2. 输出目录已存在

Hadoop 不允许覆盖输出目录。`run.sh` 已自动 `hadoop fs -rm -r -f` 旧目录；手动跑时记得清理。

### Q3. 中文乱码

mapper/reducer 输出全部走 UTF-8。如出现乱码，确认：
- 文件是 UTF-8 编码（`file mapper.py`）
- HDFS 客户端 `LANG=zh_CN.UTF-8`

### Q4. 想看每个 part 的 reducer 编号

提交命令里 `-D mapreduce.job.reduces=2` 控制 reducer 数（默认 1）。增加可观察 shuffle 把不同 tag 分配到不同 reducer 的过程。

## 九、关键设计说明（答辩可用）

- **典型倒排索引**：mapper 一对多发射，reducer 按 key 聚合 —— 这是搜索引擎索引、文档检索、日志分析的通用范式
- **Streaming 而非 Java**：选 Python 不是为了图省事，而是 Hadoop Streaming 本身就是 MR 的一种官方接口，能演示"任何能读 stdin / 写 stdout 的程序都能成为 mapper / reducer"这一开放性设计
- **索引和源表分离**：源表 `cloud_drive_files` 高频写，索引表 `cloud_drive_tag_index` 低频批量重建。读路径走索引，写路径不受影响 —— 经典的"读写分离 + 离线索引"模式
- **索引滞后是可接受的**：API 端读到索引中的 file_id 后会再次取最新元数据并过滤，避免索引滞后导致用户看到已删除/已变权限的文件
