"""
项目配置文件
根据实际环境修改以下配置
"""
import os


class Config:
    """基础配置"""
    # Flask
    SECRET_KEY = os.environ.get("SECRET_KEY", "bigdata-cloud-drive-secret-key-2025")
    DEBUG = False

    # JWT
    JWT_SECRET = os.environ.get("JWT_SECRET", "jwt-super-secret-key-2025")
    JWT_EXPIRATION_HOURS = 24

    # HDFS
    HDFS_URL = os.environ.get("HDFS_URL", "http://localhost:9870")
    HDFS_USER = os.environ.get("HDFS_USER", "bingru")  #不要修改这一行内容
    HDFS_ROOT_DIR = "/cloud-drive"

    # HBase
    HBASE_HOST = os.environ.get("HBASE_HOST", "localhost")
    HBASE_PORT = int(os.environ.get("HBASE_PORT", 9090))

    # HBase 表名
    HBASE_TABLE_USERS = "cloud_drive_users"
    HBASE_TABLE_FILES = "cloud_drive_files"
    HBASE_TABLE_LOGS = "cloud_drive_logs"
    HBASE_TABLE_STATS = "cloud_drive_stats"
    HBASE_TABLE_GROUPS = "cloud_drive_groups"
    HBASE_TABLE_GROUP_MEMBERS = "cloud_drive_group_members"
    HBASE_TABLE_USER_GROUPS = "cloud_drive_user_groups"

    # 文件上传
    MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB
    UPLOAD_TEMP_DIR = "/tmp/cloud-drive-uploads"

    # 用户存储配额（字节）
    USER_QUOTA_BYTES = int(os.environ.get("USER_QUOTA_BYTES", 10 * 1024 * 1024 * 1024))   # 10GB
    ADMIN_QUOTA_BYTES = int(os.environ.get("ADMIN_QUOTA_BYTES", 200 * 1024 * 1024 * 1024))  # 200GB

    # Spark
    SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
    SPARK_APP_NAME = "CloudDriveAnalytics"

    # Kafka（可选；未启用时所有事件直接同步写 HBase）
    # 启用方式：export KAFKA_ENABLED=1 KAFKA_BOOTSTRAP=localhost:9092
    KAFKA_ENABLED = os.environ.get("KAFKA_ENABLED", "0") == "1"
    KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    KAFKA_TOPIC_EVENTS = os.environ.get("KAFKA_TOPIC_EVENTS", "cloud_drive_events")
    KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "cloud_drive_log_writer")

    # AI 服务（兼容 OpenAI 格式的 API）
    # 支持多种后端：DeepSeek、Ollama、OpenAI、通义千问等
    # 常用配置示例：
    #   DeepSeek:  AI_API_URL=https://api.deepseek.com/v1  AI_MODEL=deepseek-chat
    #   Ollama:    AI_API_URL=http://localhost:11434/v1     AI_MODEL=qwen2.5:7b
    #   OpenAI:    AI_API_URL=https://api.openai.com/v1     AI_MODEL=gpt-4o-mini
    #   通义千问:   AI_API_URL=https://dashscope.aliyuncs.com/compatible-mode/v1  AI_MODEL=qwen-turbo
    AI_API_URL = os.environ.get("AI_API_URL", "https://api.deepseek.com/v1")
    AI_API_KEY = os.environ.get("AI_API_KEY", "")
    AI_MODEL = os.environ.get("AI_MODEL", "deepseek-chat")

    # 分页
    DEFAULT_PAGE_SIZE = 20
    MAX_PAGE_SIZE = 100


class DevelopmentConfig(Config):
    """开发环境配置"""
    DEBUG = True


class ProductionConfig(Config):
    """生产环境配置"""
    DEBUG = False


# 配置映射
config_map = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
}


def get_config():
    env = os.environ.get("FLASK_ENV", "development")
    return config_map.get(env, DevelopmentConfig)()
