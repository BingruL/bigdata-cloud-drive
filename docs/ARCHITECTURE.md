# 系统总体架构（Lambda 三层架构）

> 本文档汇总整个项目实际落地的技术栈和数据流向。答辩时可作为开场材料。

## 一、架构总览

本项目以 Hadoop 生态为基础，实现了 **Lambda 架构** 的完整三层：

```
                        ┌────────────────────────────────────┐
                        │          Serving Layer             │
                        │  Flask REST API + Vue 3 SPA        │
                        │  ECharts 数据可视化                 │
                        └──────────────┬─────────────────────┘
                                       │ 读取
        ┌──────────────────────────────┼──────────────────────────────┐
        │                              │                              │
        ▼                              ▼                              ▼
┌──────────────┐            ┌──────────────────┐            ┌─────────────────┐
│  Batch Layer │            │   Speed Layer    │            │  Storage Layer  │
│              │            │                  │            │                 │
│ Spark Batch  │            │ Spark Streaming  │            │  HDFS (文件)     │
│ (历史指标)    │            │ (实时窗口指标)    │            │  HBase (元数据)  │
│              │            │                  │            │                 │
│ MapReduce    │            └────────┬─────────┘            └────────▲────────┘
│ (倒排索引)    │                     │                              │
└──────┬───────┘                     │                              │
       │ 全量扫描                     │ 订阅                         │ CRUD
       │                              ▼                              │
       │                     ┌──────────────────┐                    │
       └────────────────────►│  Kafka (事件总线) │◄───────────────────┘
                             │ cloud_drive_events│       生产事件
                             └──────────────────┘
                                       ▲
                                       │
                              ┌────────┴────────┐
                              │    EventBus     │
                              │ (Flask 路由埋点) │
                              └─────────────────┘
```

## 二、各层职责

### 2.1 存储层（Storage Layer）

| 组件 | 用途 | RowKey / 路径设计 |
|---|---|---|
| **HDFS** | 文件二进制内容 | `/cloud-drive/files/{username}/{file_id}.{ext}` |
| **HBase: cloud_drive_users** | 用户账号 | rowkey = `username` |
| **HBase: cloud_drive_files** | 文件元数据 | rowkey = `file_id`，列族 meta，含 `is_shared`、`shared_groups` 稀疏列 |
| **HBase: cloud_drive_logs** | 操作审计日志 | rowkey = `timestamp_uuid`（时序排列） |
| **HBase: cloud_drive_stats** | 离线 + 实时统计结果 | rowkey 含 `realtime_*` 前缀做命名空间隔离 |
| **HBase: cloud_drive_groups / group_members / user_groups** | 群组的双表反向索引 | `{gid}#{user}` 与 `{user}#{gid}` 各存一份，两个方向都能 O(prefix) 扫 |
| **HBase: cloud_drive_tag_index** | MR/Spark 离线生成的标签倒排索引 | rowkey = `tag` |

### 2.2 批处理层（Batch Layer）

| 作业 | 频率 | 输出 |
|---|---|---|
| `spark_jobs/file_stats.py` | 手动 / cron | 全量统计：用户文件数、类型分布、热门文件、总体概览 |
| `spark_jobs/recommendation.py` | 手动 / cron | 用户 Jaccard 相似度、群组内协同过滤推荐 |
| `mapreduce_jobs/tag_index/` | 手动 | Hadoop Streaming MR 构建 tag 倒排索引 |
| `spark_jobs/tag_index_spark.py` | 手动 | 同上的 Spark 对照实现，写入同一张索引表 |

特点：**全量、容错、高吞吐、分钟级延迟**。

### 2.3 速度层（Speed Layer）

| 作业 | 触发 | 输出 |
|---|---|---|
| `spark_jobs/streaming_stats.py` | Structured Streaming，2 秒微批 | 5 分钟动作计数、10 分钟活跃用户、热门文件、最新 30 条事件流 |

特点：**增量、低延迟（2-4 秒）、近似精确**。

### 2.4 服务层（Serving Layer）

- **Flask REST API**：`/api/auth/*`、`/api/files/*`、`/api/groups/*`、`/api/stats/*`、`/api/ai/*`
- **EventBus**：路由层埋点，统一发到 Kafka topic（或回退直写 HBase）
- **Vue 3 SPA**：登录、文件管理、群组共享、智能推荐、数据看板（含实时面板）、回收站
- **ECharts**：柱状图、饼图、折线图、日历热力图、横向柱状图、关系图谱

### 2.5 事件总线（Kafka）

| 主题 | Producer | Consumer |
|---|---|---|
| `cloud_drive_events` | Flask EventBus | (1) `backend/workers/log_consumer.py` 落库 HBase；(2) `streaming_stats.py` 计算实时指标；(3) 未来可加 Flume sink HDFS 冷归档 |

降级策略：Kafka 不可用时 EventBus 自动回退到 HBase 直写，请求链路永不报错。

## 三、典型数据流

### 3.1 用户上传文件

```
浏览器  ──multipart──►  Flask /api/files/upload
                              │
                              ├─► HDFSService.upload_file        (二进制内容)
                              ├─► HBaseService.save_file_meta    (元数据)
                              ├─► EventBus.log("upload", fid)    (事件)
                              │       │
                              │       ├─► Kafka topic
                              │       │      ├─► log_consumer ──► HBase cloud_drive_logs
                              │       │      └─► streaming_stats ──► HBase realtime_*
                              │       └─(回退)─► 直接写 HBase cloud_drive_logs
                              │
                              └─► AIService.generate_summary     (LLM 异步生成摘要)
                                      └─► HBaseService.update_file_ai
```

### 3.2 仪表盘加载

```
浏览器  ──HTTP──►  Flask /api/stats/dashboard       (从 HBase 实时聚合)
                  Flask /api/stats/realtime         (读 cloud_drive_stats 中 realtime_* 行)
                  Flask /api/stats/user-file-counts (实时 scan + Counter)
                  ...

前端 Dashboard 每 2 秒轮询 /api/stats/realtime，把绿色脉冲点变亮。
```

### 3.3 按标签搜索（走 MR/Spark 倒排索引）

```
浏览器  ──HTTP──►  Flask /api/files/by-tag/Hadoop
                              │
                              ├─► HBase cloud_drive_tag_index.get('Hadoop')
                              │   返回 [{file_id, filename}, ...]
                              ├─► 对每个 file_id 取最新元数据
                              └─► 应用 _can_access 权限过滤
```

## 四、为什么是 Lambda 架构

| 特征 | 本项目对应 |
|---|---|
| **批处理保证最终精确** | `file_stats.py` / `recommendation.py` 全量 scan，结果稳定权威 |
| **流处理保证低延迟** | `streaming_stats.py` 2 秒微批，让大屏几乎实时 |
| **同源数据**（不必两套数据收集） | Kafka 事件流既被 streaming 消费，也被 log_consumer 持久化后供 batch 扫描 |
| **统一服务接口** | 前端只调 Flask，不感知背后是 batch 还是 speed 产出的指标 |
| **降级路径** | Kafka / Streaming 离线时，core 路径仍可用 |

## 五、技术栈总览

| 层次 | 技术 |
|---|---|
| 前端 | Vue 3 + ECharts + Lucide Icons + 原生 Canvas |
| 后端 | Flask 3 + JWT + happybase + WebHDFS REST |
| 文件存储 | Hadoop HDFS |
| 元数据存储 | Apache HBase（含双表反向索引、复合 RowKey、稀疏列三种典型建模） |
| 批处理 | Apache Spark（DataFrame API） |
| 流处理 | Apache Spark Structured Streaming |
| 离线计算（对照） | Hadoop Streaming MapReduce（Python） |
| 消息队列 | Apache Kafka（KRaft 单节点） |
| 日志采集 | Flume（配置示例） |
| 智能 | 兼容 OpenAI 协议的 LLM API（DeepSeek / Ollama / 通义千问） |

## 六、答辩开场陈述（建议）

> "本项目以 Hadoop 生态为基础，落地了一套 Lambda 架构的智能云盘。
> **存储层**用 HDFS 存文件、HBase 存元数据，并刻意演示了双表反向索引、复合 RowKey、稀疏列三种典型 HBase 建模手法。
> **批处理层**用 Spark 跑统计与推荐、用 Hadoop Streaming MR 构建标签倒排索引，并把同一份倒排索引逻辑用 Spark 重写一遍，便于对比 MR 和 Spark 两种范式。
> **速度层**用 Spark Structured Streaming 订阅 Kafka 操作事件流，2 秒微批输出 5 分钟/10 分钟滑动窗口指标，前端实时面板每 2 秒刷新。
> **服务层**用 Flask + Vue 3 提供 RESTful API 与可视化看板，并通过 EventBus 把所有路由埋点解耦到 Kafka，配合优雅降级保证主路径永不阻塞。
> 此外提供了 Docker Compose 一键部署、压测脚本、集成测试套件来佐证工程化质量。"

## 七、后续演进方向（可选答辩反问材料）

- **Kappa 架构演进**：把 batch 也从 Kafka 重放，统一只用流处理
- **HBase 协处理器**：在 put 时自动维护索引，免去离线重建
- **Phoenix / Omid 事务层**：解决双表反向索引的写放大与最终一致性
- **数据湖治理**：Iceberg / Hudi 接管文件存储，提供时间旅行、ACID
