"""
EventBus —— 操作日志 / 业务事件的统一入口

设计目标：
1. 把全站所有 hbase.add_log() 调用收口到一个 log() 方法上
2. 启用 Kafka 时：事件 → Kafka topic → 由 backend.workers.log_consumer 落库 HBase
   （顺带让 Spark Streaming 等其它消费者也能订阅同一份事件流）
3. 未启用 Kafka 时：事件直接同步写 HBase，保证项目无 Kafka 依赖也能跑
4. Kafka 不可用时（producer 初始化失败 / send 失败）自动回退直写，不影响主流程

事件 schema（JSON）:
    {
        "username": "alice",
        "action":   "upload",
        "detail":   "file_id=abc",
        "timestamp": 1714214400123
    }
"""
import json
import time
import logging
import threading

logger = logging.getLogger(__name__)


class EventBus:
    """日志事件总线：Kafka 优先，HBase 直写兜底"""

    def __init__(self, config, hbase_service):
        self.config = config
        self.hbase = hbase_service
        self.log_table = config.HBASE_TABLE_LOGS
        self.topic = config.KAFKA_TOPIC_EVENTS
        self._producer = None
        self._producer_lock = threading.Lock()
        self._kafka_failed = False  # 初始化失败后不再重试，直接走 fallback

        if config.KAFKA_ENABLED:
            self._init_producer()
        else:
            logger.info("EventBus: Kafka 未启用，事件将直接同步写 HBase")

    def _init_producer(self):
        """惰性初始化 KafkaProducer；失败时降级，不抛"""
        try:
            from kafka import KafkaProducer
            self._producer = KafkaProducer(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                retries=2,
                request_timeout_ms=3000,
                max_block_ms=3000,
            )
            logger.info(f"EventBus: Kafka producer 已连接 {self.config.KAFKA_BOOTSTRAP}")
        except Exception as e:
            self._kafka_failed = True
            self._producer = None
            logger.warning(f"EventBus: Kafka 初始化失败，降级为 HBase 直写: {e}")

    def log(self, username, action, detail=""):
        """记录一条操作事件
        - Kafka 启用且健康：异步发送到 topic，由 consumer 入库
        - 否则：直接同步写 HBase
        """
        event = {
            "username": username,
            "action": action,
            "detail": str(detail),
            "timestamp": int(time.time() * 1000),
        }

        if self._producer and not self._kafka_failed:
            try:
                self._producer.send(self.topic, key=username, value=event)
                return event
            except Exception as e:
                logger.warning(f"EventBus: Kafka send 失败，本次回退直写: {e}")
                # 单次发送失败不标记 _kafka_failed，下次仍尝试

        # Fallback: 直接写 HBase
        try:
            self.hbase.add_log(self.log_table, username, action, detail)
        except Exception as e:
            logger.error(f"EventBus: HBase 直写也失败（事件丢弃）: {e}")
        return event

    def close(self):
        """关闭 producer，flush 缓冲事件"""
        if self._producer:
            try:
                self._producer.flush(timeout=3)
                self._producer.close(timeout=3)
            except Exception as e:
                logger.warning(f"EventBus: producer 关闭异常: {e}")
