"""
基于 HBase/HDFS 的智能云盘系统 - 主入口
大数据技术基础 期末项目
"""
import os
import logging
from flask import Flask, send_from_directory
from flask_cors import CORS

from .config import get_config
from .auth.jwt_handler import JWTHandler
from .services.hbase_service import HBaseService
from .services.hdfs_service import HDFSService
from .services.ai_service import AIService
from .services.stats_service import StatsService
from .routes.auth_routes import auth_bp
from .routes.file_routes import file_bp
from .routes.stats_routes import stats_bp, ai_bp

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_app():
    """Flask 应用工厂"""
    app = Flask(
        __name__,
        static_folder="../frontend",
        static_url_path="",
    )

    config = get_config()
    app.config["APP_CONFIG"] = config
    app.config["SECRET_KEY"] = config.SECRET_KEY
    app.config["MAX_CONTENT_LENGTH"] = config.MAX_CONTENT_LENGTH

    # 允许跨域（开发环境）
    CORS(app)

    # ========== 初始化服务 ==========

    # JWT
    jwt_handler = JWTHandler(config.JWT_SECRET, config.JWT_EXPIRATION_HOURS)
    app.config["JWT_HANDLER"] = jwt_handler

    # HBase
    hbase_service = HBaseService(config.HBASE_HOST, config.HBASE_PORT)
    app.config["HBASE_SERVICE"] = hbase_service

    # HDFS
    hdfs_service = HDFSService(config.HDFS_URL, config.HDFS_USER, config.HDFS_ROOT_DIR)
    app.config["HDFS_SERVICE"] = hdfs_service

    # AI
    ai_service = AIService(config.AI_API_URL, config.AI_API_KEY, config.AI_MODEL)
    app.config["AI_SERVICE"] = ai_service

    # 统计
    stats_service = StatsService(hbase_service, config)
    app.config["STATS_SERVICE"] = stats_service

    # ========== 初始化 HBase 表 ==========

    try:
        table_config = {
            config.HBASE_TABLE_USERS: {"info": dict()},
            config.HBASE_TABLE_FILES: {"meta": dict()},
            config.HBASE_TABLE_LOGS: {"log": dict()},
            config.HBASE_TABLE_STATS: {"data": dict()},
        }
        hbase_service.init_tables(table_config)
        hdfs_service.init_directories()
        logger.info("HBase 表和 HDFS 目录初始化完成")
    except Exception as e:
        logger.warning(f"初始化失败（确保 HBase 和 HDFS 已启动）: {e}")

    # ========== 注册路由 ==========

    app.register_blueprint(auth_bp)
    app.register_blueprint(file_bp)
    app.register_blueprint(stats_bp)
    app.register_blueprint(ai_bp)

    # 前端页面路由
    @app.route("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.route("/<path:path>")
    def serve_static(path):
        file_path = os.path.join(app.static_folder, path)
        if os.path.isfile(file_path):
            return send_from_directory(app.static_folder, path)
        return send_from_directory(app.static_folder, "index.html")

    # 健康检查
    @app.route("/api/health")
    def health_check():
        return {"status": "ok", "service": "BigData Cloud Drive"}

    logger.info("智能云盘系统启动成功!")
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)
