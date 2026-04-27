"""
日志事件 Consumer —— 独立进程

订阅 Kafka topic `cloud_drive_events`，把事件落库到 HBase `cloud_drive_logs`。

启动方式：
    python -m backend.workers.log_consumer

需要环境变量：
    KAFKA_ENABLED=1
    KAFKA_BOOTSTRAP=localhost:9092
    HBASE_HOST=localhost
    HBASE_PORT=9090

设计要点：
- 与 Flask 进程解耦，消费速率独立扩展（多起几个进程加入同一个 consumer group 即可水平扩容）
- auto_offset_reset=earliest：consumer 首次启动从最早消费，避免漏掉未处理的事件
- enable_auto_commit=True：简单期末项目场景下 at-least-once 即可，不做手动 commit
"""
import os
import sys
import json
import signal
import logging

# 允许直接以脚本方式运行（python backend/workers/log_consumer.py）
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.config import get_config
from backend.services.hbase_service import HBaseService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("log_consumer")

_running = True


def _stop(_signum, _frame):
    global _running
    _running = False
    logger.info("收到停止信号，准备退出...")


def main():
    config = get_config()
    if not config.KAFKA_ENABLED:
        logger.error("KAFKA_ENABLED 未启用，无需启动 consumer。请 export KAFKA_ENABLED=1")
        sys.exit(1)

    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.error("kafka-python 未安装，请先 pip install kafka-python")
        sys.exit(1)

    hbase = HBaseService(config.HBASE_HOST, config.HBASE_PORT)

    consumer = KafkaConsumer(
        config.KAFKA_TOPIC_EVENTS,
        bootstrap_servers=config.KAFKA_BOOTSTRAP.split(","),
        group_id=config.KAFKA_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,  # 让 poll 循环可以周期性检查 _running
    )

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    logger.info(f"开始消费 topic={config.KAFKA_TOPIC_EVENTS}, group={config.KAFKA_CONSUMER_GROUP}")
    count = 0
    while _running:
        try:
            for msg in consumer:
                if not _running:
                    break
                evt = msg.value
                try:
                    hbase.add_log(
                        config.HBASE_TABLE_LOGS,
                        evt.get("username", ""),
                        evt.get("action", ""),
                        evt.get("detail", ""),
                    )
                    count += 1
                    if count % 50 == 0:
                        logger.info(f"已落库 {count} 条事件")
                except Exception as e:
                    logger.error(f"落库失败 evt={evt}: {e}")
        except Exception as e:
            logger.error(f"consumer 异常: {e}")

    consumer.close()
    logger.info(f"退出，本次共落库 {count} 条事件")


if __name__ == "__main__":
    main()
