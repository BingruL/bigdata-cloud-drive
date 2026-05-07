# Spark Streaming 实时面板启用指南

> 本文档介绍如何启用 `spark_jobs/streaming_stats.py`，让前端 Dashboard 顶部的"实时面板"开始接收实时指标。
> 该作业是 Lambda 架构的 **speed layer**，与 Kafka 事件总线（见 `KAFKA_USAGE.md`）配合使用。

## 一、整体链路

```
用户操作 → Flask EventBus → Kafka topic: cloud_drive_events
                                  │
                                  ├─ log_consumer 进程 ─► HBase cloud_drive_logs（持久化）
                                  │
                                  └─ Spark Structured Streaming ──► HBase cloud_drive_stats
                                       (streaming_stats.py)              （rowkey: realtime_*）
                                                                                │
                                                                       Flask /api/stats/realtime
                                                                                │
                                                                       前端 Dashboard 实时面板
                                                                          （每 2 秒轮询）
```

实时面板包含 4 块指标：

| 指标 | 窗口 | HBase RowKey |
|---|---|---|
| 各动作计数 | 最近 5 分钟 | `realtime_action_counts` |
| 活跃用户列表 | 最近 10 分钟 | `realtime_active_users` |
| 热门文件 Top 5 | 最近 5 分钟 | `realtime_hot_files` |
| 事件流 | 最新 30 条 | `realtime_event_stream` |

---

## 二、前置条件

1. **Kafka 已启用**：见 `docs/KAFKA_USAGE.md`，`docker compose -f docker-compose.kafka.yml up -d`
2. **HBase Thrift Server 在 9090 端口运行**
3. **Spark 3.x 已安装**（提供 `spark-submit` 命令）
4. **happybase 已装到 Spark 使用的 Python 环境中**：`pip install happybase`

---

## 三、启动 Streaming 作业

```bash
# 设置环境变量（与后端一致）
export KAFKA_BOOTSTRAP=localhost:9092
export KAFKA_TOPIC_EVENTS=cloud_drive_events
export HBASE_HOST=localhost
export HBASE_PORT=9090

# spark-submit（首次运行会自动从 Maven 下载 spark-sql-kafka 包）
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_jobs/streaming_stats.py
```

成功启动后会看到：

```
============================================================
  智能云盘 - Spark Structured Streaming 实时统计
  Kafka: localhost:9092  topic: cloud_drive_events
  HBase: localhost:9090  → cloud_drive_stats
============================================================
Streaming 已启动，等待事件...
```

每 10 秒会打印一次摘要：

```
[batch 12] 新事件=3 窗口内事件=18 活跃用户=2 热门文件=1
```

---

## 四、验证实时面板

### 4.1 打开前端 Dashboard

浏览器访问 `http://localhost:5000/app`，登录后进入"数据看板"页面，顶部应能看到 **实时面板** 区块。

- **绿色脉冲圆点 + "Spark Streaming 在线"**：作业正在写入 HBase
- **灰色圆点 + "Spark Streaming 离线"**：超过 30 秒未收到更新（作业未启动或异常）

### 4.2 触发事件观察实时变化

- 在文件页上传 / 下载 / 删除文件
- 切换标签页登录/注销
- 创建群组、添加成员

每次操作后约 2-4 秒（micro-batch + 前端轮询周期），实时面板的数字与事件流应跳动。

---

## 五、常用排查

### Q1. 启动报错 `ModuleNotFoundError: No module named 'happybase'`

Spark 使用的 Python 解释器没装 happybase。先确认 Spark 用的是哪个 Python：

```bash
echo $PYSPARK_PYTHON   # 若为空，则用 PATH 中的 python
which python && python -c "import happybase"
```

在该解释器下 `pip install happybase`。

### Q2. 启动报错找不到 Kafka 数据源

确认 `--packages` 参数版本与你的 Spark 大版本匹配：

| Spark | Scala | --packages |
|---|---|---|
| 3.5.x | 2.12 | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0` |
| 3.4.x | 2.12 | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1` |
| 3.3.x | 2.12 | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2` |

### Q3. 前端实时面板始终显示"离线"

排查顺序：

1. Streaming 作业终端是否打印 batch 摘要？没有 → 检查 Kafka 是否在 9092 端口
2. 浏览器 DevTools 看 `/api/stats/realtime` 响应是否为 `streaming_online: false` 但其它字段非空？  
   是 → HBase 写入成功但时间戳过期，说明 batch 间隔过长，触发频率正常应是 2 秒
3. HBase shell 直接查：
   ```
   hbase shell
   > get 'cloud_drive_stats', 'realtime_event_stream'
   ```

### Q4. checkpoint 报错或想重置

```bash
rm -rf /tmp/cloud-drive-streaming-ckpt
```

然后重新 `spark-submit`。该目录由作业自动重建。

### Q5. 想观察 Kafka topic 中的原始事件

```bash
docker exec -it cloud-drive-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cloud_drive_events
```

---

## 六、关键设计说明（答辩可用）

- **Spark Structured Streaming 而非 DStream**：使用 DataFrame API + `readStream/writeStream`，是 Spark 2.0+ 主推的流处理 API
- **micro-batch 模式**：`trigger(processingTime="2 seconds")` 每 2 秒一次微批，平衡延迟与吞吐
- **driver-side 状态**：`foreachBatch` 内维护 `deque` 滚动事件缓冲，避免使用 stateful streaming 的复杂 watermark/state store。代价是 driver 重启会丢失最近 10 分钟数据，对实时面板可接受
- **写 HBase 用 batch put**：`table.batch()` 批量提交 4 个 rowkey，单次连接 4 次 put，开销小
- **读写分离的 Lambda 架构**：speed layer（本作业）只更新 `realtime_*` 行；batch layer（`file_stats.py`）只更新非 `realtime_` 行，互不干扰
- **at-least-once**：`startingOffsets=latest` + checkpoint，重启后从断点继续。重复落库由"覆盖式 put"幂等
- **降级**：作业未启动时前端显示离线状态，主路径功能不受影响

---

## 七、与 batch 作业的对比

| 维度 | `file_stats.py`（batch） | `streaming_stats.py`（streaming） |
|---|---|---|
| 触发 | 手动 / cron | 长驻进程 |
| 输入 | HBase 全表 scan | Kafka topic 订阅 |
| 延迟 | 分钟级 | 2-4 秒 |
| HBase 写入 | `dashboard_summary`、`hot_files` 等 | `realtime_action_counts` 等 |
| 前端 | 数据看板的图表 | 实时面板 |

两者并存即构成完整 Lambda 架构。
