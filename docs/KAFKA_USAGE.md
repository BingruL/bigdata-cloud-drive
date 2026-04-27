# Kafka 事件总线启用指南

> 本项目的 Kafka 事件总线是**可选**的：未启用时所有操作日志直接同步写入 HBase；启用后事件流经 Kafka topic，由独立 consumer 进程落库，并可被 Spark Streaming 等其它消费者订阅。

## 一、整体链路

```
Flask 路由 ──► EventBus.log(username, action, detail)
                    │
                    ├─ KAFKA_ENABLED=0  ──► 直接写 HBase cloud_drive_logs（默认 / 兜底）
                    │
                    └─ KAFKA_ENABLED=1  ──► Kafka topic: cloud_drive_events
                                                  │
                                                  └─► log_consumer 进程 ──► HBase
```

事件 schema（JSON）：

```json
{
  "username":  "alice",
  "action":    "upload",
  "detail":    "fid_abc123",
  "timestamp": 1714214400123
}
```

---

## 二、前置条件

- 已安装 Docker 和 Docker Compose（用于一键拉起 Kafka）
- HBase Thrift Server 已在 9090 端口运行
- 已安装 Python 依赖：`pip install -r backend/requirements.txt`（包含 `kafka-python==2.0.2`）

---

## 三、启用步骤

### 3.1 拉起单节点 Kafka

项目根目录已提供 `docker-compose.kafka.yml`（KRaft 模式，无需 ZooKeeper）：

```bash
# 启动
docker compose -f docker-compose.kafka.yml up -d

# 查看状态
docker compose -f docker-compose.kafka.yml ps

# 查看日志（首次启动需等约 10 秒完成 cluster 初始化）
docker compose -f docker-compose.kafka.yml logs -f kafka
```

容器健康检查通过后，broker 监听在 `localhost:9092`。

### 3.2 设置环境变量

```bash
export KAFKA_ENABLED=1
export KAFKA_BOOTSTRAP=localhost:9092
# 可选：自定义 topic 和 consumer group
# export KAFKA_TOPIC_EVENTS=cloud_drive_events
# export KAFKA_CONSUMER_GROUP=cloud_drive_log_writer
```

### 3.3 启动后端（producer）

```bash
python run.py
```

启动日志中应看到：

```
EventBus: Kafka producer 已连接 localhost:9092
```

### 3.4 启动 Consumer（落库到 HBase）

**另起一个终端**（保持上面的环境变量）：

```bash
python -m backend.workers.log_consumer
```

启动日志：

```
开始消费 topic=cloud_drive_events, group=cloud_drive_log_writer
```

每消费 50 条事件会打印一次进度。

---

## 四、验证链路

### 4.1 触发事件

通过浏览器或 curl 在系统中执行任意操作（登录、上传、下载、分享等），例如：

```bash
curl -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"123456"}'
```

### 4.2 直接订阅 topic 观察消息

```bash
docker exec -it cloud-drive-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cloud_drive_events \
  --from-beginning
```

应能看到 JSON 事件实时打印。

### 4.3 确认事件已落 HBase

```bash
hbase shell
> scan 'cloud_drive_logs', {LIMIT => 5, REVERSED => true}
```

应看到刚才的 login 等动作。

---

## 五、常用运维命令

| 操作 | 命令 |
|---|---|
| 列出所有 topic | `docker exec cloud-drive-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| 查看 topic 详情 | `docker exec cloud-drive-kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic cloud_drive_events` |
| 查看 consumer group 偏移量 | `docker exec cloud-drive-kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group cloud_drive_log_writer` |
| 重置 consumer offset 到最早 | `docker exec cloud-drive-kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group cloud_drive_log_writer --reset-offsets --to-earliest --topic cloud_drive_events --execute` |
| 停止 Kafka | `docker compose -f docker-compose.kafka.yml down` |
| 清空 Kafka 数据 | `docker compose -f docker-compose.kafka.yml down -v` |

---

## 六、关闭 Kafka 链路（回到默认模式）

只需取消环境变量并重启后端：

```bash
unset KAFKA_ENABLED
python run.py
```

此时 `EventBus` 会直接同步写 HBase，consumer 进程可保留也可关停（Ctrl-C）。

---

## 七、关键设计说明（答辩可用）

- **降级而非中断**：producer 初始化失败、send 失败均自动回退到 HBase 直写，请求链路永不报错
- **解耦**：写日志这一动作脱离请求关键路径，前端响应更快；HBase 短暂抖动不影响业务
- **多消费者扇出**：同一份事件可被 log_consumer、Spark Streaming、Flume 等多个消费者订阅，是 Lambda 架构 speed layer 的基础
- **at-least-once 语义**：`auto_offset_reset=earliest` + 自动 commit，简单期末项目场景下保证不丢失，重复落库由 HBase 同 RowKey 的幂等 put 兜底
- **水平扩容**：再起一个 consumer 进程加入同一 group 即可分摊分区

---

## 八、常见问题

### Q1. 启动后端后日志显示 "Kafka 初始化失败，降级为 HBase 直写"

可能原因：
- Kafka 容器未启动或还在初始化（等 10-15 秒后重试）
- `KAFKA_BOOTSTRAP` 端口被占用或写错（确认 `9092` 是否开放）
- WSL 下 Docker 网络异常：尝试 `docker compose -f docker-compose.kafka.yml restart`

### Q2. Consumer 启动后没有任何输出

正常情况——consumer 处于等待消息状态。在系统中执行任意操作触发事件后即可看到日志。

### Q3. HBase 中没有看到新日志

排查顺序：
1. 用 `kafka-console-consumer.sh` 确认事件已发到 topic
2. 检查 consumer 日志是否有"落库失败"错误
3. 确认 `HBASE_HOST` / `HBASE_PORT` 环境变量在 consumer 终端中也正确

### Q4. 想观察"启用 vs 不启用"性能差异

参考 `IMPROVEMENT_PLAN.md` 中第 2.4 节"性能压测脚本"。
