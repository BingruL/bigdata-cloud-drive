# 项目完善方案（答辩前补救计划）

> 面向《大数据技术》期末项目答辩，用于把当前项目从"能跑"升级到"架构完整、工程化、可演示"。
> 按 **补齐架构空缺 → 工程化 → 大数据深度** 三层组织，每项都标注预计工作量和答辩价值。

---

## 背景与现状判断

当前项目核心栈：HDFS（文件存储）+ HBase（元数据）+ Spark（批处理）+ Flask（后端）+ Vue 3（前端）+ ECharts（可视化）+ JWT（认证）+ LLM API（AI）。

存在的架构缺口：

- **MapReduce**：未实现，但 Spark 是其官方升级版，属于**合理替代**——答辩时可正面回答。
- **Kafka**：未实现，且**没有等价替代**——当前操作日志是 Flask 同步写 HBase，缺少消息队列、削峰解耦、流处理这一环。
- **Flume**：仅有 `scripts/flume-log-collector.conf` 配置文件，未在启动流程中 wire up，目前是"挂在墙上的"。

本方案目标：把这三个缺口补齐，并顺势把项目推到 Lambda 架构完整闭环。

---

## 第一层：补齐架构空缺（必做）

### 1.1 Kafka：真正落到事件链路里 ⭐⭐⭐⭐⭐

**目标**：让 Kafka 成为操作日志的中枢，而不是补丁。

**改造后的架构**：

```
Flask 路由 ──► Kafka topic: cloud_drive_events
                    │
                    ├──► Consumer A: 持久化 → HBase (cloud_drive_logs)
                    ├──► Consumer B: Spark Streaming 实时聚合 → HBase (cloud_drive_stats)
                    └──► Consumer C: Flume sink → HDFS 冷归档
```

**落地步骤**：

- `backend/services/kafka_producer.py` — 封装 `produce(topic, key, event_dict)`，所有 `hbase.add_log()` 调用点改为发 Kafka
- `backend/workers/log_consumer.py` — 独立进程，订阅 topic 写 HBase（保留原同步写路径作为 fallback）
- `docker-compose.yml` 拉起单节点 Kafka（KRaft 模式无需 ZK，最简单）
- 在 README 架构图里 Kafka 占据中心位置

**预计工作量**：1-2 天
**答辩价值**：直接把 "Flume/Kafka 日志采集" 这句话坐实，且能讲清"为什么不直接同步写 HBase"——削峰、解耦、多消费者扇出。

---

### 1.2 Spark Streaming：实时大屏 ⭐⭐⭐⭐⭐

**目标**：现在 Dashboard 全是"刷新才更新"的离线统计，加一个**实时面板**。

**指标候选**（任选 3 个即可）：

- 最近 1 分钟上传/下载计数（滑动窗口）
- 当前在线活跃用户数（5 分钟去重）
- 实时热门文件 Top 5（窗口聚合）
- 实时操作流（Server-Sent Events 推送到前端）

**落地步骤**：

- `spark_jobs/streaming_stats.py` — 用 Structured Streaming 订阅 Kafka，输出到 HBase 的 `cloud_drive_stats` 表（rowkey 用 `realtime_*` 前缀）
- 前端 Dashboard 加一个 "实时" 卡片区块，每 2 秒轮询 `/api/stats/realtime`
- 答辩时**现场上传一个文件，看大屏数字立刻跳动**——视觉冲击力极强

**预计工作量**：1-2 天
**答辩价值**：Lambda 架构里的 speed layer 完整闭环，演示效果炸裂。

---

### 1.3 MapReduce：写一个"原生" MR 作业 ⭐⭐⭐

**目标**：让"理解 MR 原理"变成可演示的事，而不是嘴上说说。

**推荐题目**（选一个）：

- **倒排索引（inverted index）**：扫描所有文件的 `tags` 字段，构建 `tag → [file_id, ...]` 索引，结果写回 HBase 一张新表 `cloud_drive_tag_index`，给前端"按标签检索"用
- **文档词频统计**：扫描 HDFS 上所有文本文件内容，做 wordcount，结果给 AI 推荐做 keyword 兜底

**实现方式（推荐 Hadoop Streaming + Python）**：

```bash
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -input /cloud-drive/files \
  -output /cloud-drive/mr_output/tag_index \
  -mapper mapper.py -reducer reducer.py
```

新建 `mapreduce_jobs/tag_index/mapper.py` + `reducer.py`，30 行代码就能写完。

**预计工作量**：半天
**答辩价值**：可以现场演示 "同一份逻辑，MR 版 vs Spark 版" 的代码对比，体现你对计算范式的理解，分量很重。

---

### 1.4 Flume：真正 wire up ⭐⭐

**目标**：让 `scripts/flume-log-collector.conf` 不再是摆设。

**最小改造**：

- Flask 用 `logging.FileHandler` 把所有请求日志写到 `/var/log/cloud-drive/access.log`
- Flume 配置 `exec` 或 `taildir` source 监听该文件 → HDFS sink → `/cloud-drive/audit_logs/yyyy-MM-dd/`
- Spark 离线作业可以直接读这个目录做月度审计报表

**预计工作量**：2-3 小时
**答辩价值**：完成日志采集闭环，且和 Kafka 路径形成对比（应用埋点 vs 文件采集，两种主流模式都展示）。

---

## 第二层：工程化加分项（让项目像"产品"而不是"作业"）

### 2.1 Docker Compose 一键拉起整套环境 ⭐⭐⭐⭐

**核心痛点**：现在助教/老师如果想跑你的项目，需要自己装 Hadoop+HBase+Kafka+Spark，几乎不可能。

**`docker-compose.yml` 包含**：

- `namenode` / `datanode`（HDFS）
- `hbase-master` + `hbase-thrift`
- `kafka`（KRaft 单节点）
- `spark-master`（可选，演示用）
- `app`（Flask 后端）

`docker compose up` 一行命令起整套，README 第一行就放这个命令。

**预计工作量**：1 天（踩镜像兼容坑）
**答辩价值**：超级加分。"我们做了容器化部署，助教任何机器都能复现" 是直接拉档次的话。

---

### 2.2 集成测试 ⭐⭐⭐

当前一个测试都没有。建议加：

- `tests/test_auth.py` — 注册/登录/JWT 校验
- `tests/test_files.py` — 上传/下载/分享/权限
- `tests/test_groups.py` — 群组双表反向索引正确性
- 用 `pytest` + `pytest-flask` + 启动一个 Mock HBase（或 Docker 起真的 HBase 跑集成测试）

**预计工作量**：1-2 天
**答辩价值**：中等。但如果老师问"你们怎么保证质量"，这是唯一硬答案。

---

### 2.3 CI（GitHub Actions）⭐⭐

push 后自动跑 lint + pytest + Docker build。一个 yaml 文件搞定。

**预计工作量**：2 小时
**答辩价值**：体现工程素养。

---

### 2.4 性能压测脚本 ⭐⭐⭐

写一个 `scripts/benchmark.py`：

- 并发上传 1000 个小文件，统计 QPS
- 跑两次：一次直接同步写 HBase，一次走 Kafka 异步链路
- 出一张对比图（柱状图）

**预计工作量**：半天
**答辩价值**：高。这是把"为什么需要 Kafka"从"理论上削峰"变成"我跑了数据"的关键证据。

---

## 第三层：大数据深度加分（拉开和其他作业的差距）

### 3.1 HBase 二级索引 / 协处理器演示 ⭐⭐⭐

现在 `cloud_drive_files` 只能按 `file_id` 查；按 `owner` / `type` / `created_at` 查需要全表扫描。可以：

- **方案 A**：在 RowKey 设计上做二级索引表（仿照已有的 `user_groups` 反向索引模式），如 `cloud_drive_files_by_owner` rowkey = `{owner}#{created_at}#{file_id}`
- **方案 B**：写一个简单的协处理器（observer）演示 put 时自动维护索引

**答辩价值**：高。这是 HBase 课程的高阶知识点，老师肯定会喜欢。

---

### 3.2 Lambda 架构总结图 ⭐⭐⭐⭐

做完上面这些，README/PPT 加一张架构图：

```
                     ┌──── Spark Batch ─────► HBase (历史)
                     │
日志/事件 ──► Kafka ─┤
                     │
                     └──── Spark Streaming ─► HBase (实时)
                                  │
                                  └──► Frontend Dashboard
```

答辩开场就讲："本项目实现了 Lambda 架构的完整三层"——批处理层 + 速度层 + 服务层。这是非常体面的开场。

---

### 3.3 数据治理小演示 ⭐⭐

- HDFS 冷热数据分层：90 天前的文件移到 `/cloud-drive/cold/`
- HBase TTL 设置：操作日志保留 30 天自动过期
- 写一个 `scripts/data_lifecycle.py` 定期跑

**预计工作量**：半天
**答辩价值**：体现你考虑了"数据生命周期"，是大数据课程的核心议题之一。

---

## 推荐执行顺序

### 假设有 2-3 周

| 周 | 任务 | 产出 |
|----|------|------|
| 第 1 周 | 1.1 Kafka + 1.2 Spark Streaming + 1.4 Flume | 架构补完整 |
| 第 2 周 | 1.3 MapReduce + 2.1 Docker Compose + 2.4 压测对比 | 演示就绪 |
| 第 3 周 | 3.1 HBase 二级索引 + 3.2 架构图 + 2.2 测试 + PPT 重做 | 答辩材料 |

### 假设只有 1 周

砍到 ROI 最高的五项组合：**1.1 Kafka + 1.2 Spark Streaming + 1.3 MapReduce + 2.1 Docker Compose + 3.2 架构图**。

---

## 起步建议

**先从 1.1 Kafka 开始**——它是后面所有动作的地基：

- Spark Streaming（1.2）依赖它
- 压测对比（2.4）依赖它
- Lambda 架构图（3.2）依赖它

打通"Flask → Kafka → Consumer → HBase"这条链路后，剩下的扩展都顺水推舟。

---

## 答辩话术储备

完成上述方案后，可以这样自我介绍项目：

> "本项目以 Hadoop 生态为基础，实现了 Lambda 架构的完整三层：HDFS + HBase 作为存储层，Spark 批处理作为 batch layer 处理离线统计与协同过滤，Kafka + Spark Streaming 作为 speed layer 实现实时大屏，Flask + Vue 作为 serving layer 提供 RESTful API 与可视化。在数据建模上刻意演示了 HBase 双表反向索引、稀疏列、复合 RowKey 三种典型手法；在工程化上提供了 Docker Compose 一键部署与压测对比脚本。"

这一段开场，足以撑起一个高质量答辩。
