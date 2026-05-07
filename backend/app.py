"""
基于 HBase/HDFS 的智能云盘系统 - 主入口
大数据技术基础 期末项目
"""
import os
import logging
from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import HTTPException

from .config import get_config
from .auth.jwt_handler import JWTHandler
from .services.hbase_service import HBaseService
from .services.hdfs_service import HDFSService
from .services.ai_service import AIService
from .services.stats_service import StatsService
from .services.event_bus import EventBus
from .routes.auth_routes import auth_bp
from .routes.file_routes import file_bp
from .routes.folder_routes import folder_bp
from .routes.stats_routes import stats_bp, ai_bp
from .routes.group_routes import group_bp
from .routes.public_link_routes import public_link_bp

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

    # 事件总线（Kafka 优先 / HBase 直写兜底）
    event_bus = EventBus(config, hbase_service)
    app.config["EVENT_BUS"] = event_bus

    # ========== 初始化 HBase 表 ==========

    try:
        table_config = {
            config.HBASE_TABLE_USERS: {"info": dict()},
            config.HBASE_TABLE_FILES: {"meta": dict()},
            config.HBASE_TABLE_LOGS: {"log": dict()},
            config.HBASE_TABLE_STATS: {"data": dict()},
            config.HBASE_TABLE_GROUPS: {"info": dict()},
            config.HBASE_TABLE_GROUP_MEMBERS: {"info": dict()},
            config.HBASE_TABLE_USER_GROUPS: {"info": dict()},
            config.HBASE_TABLE_FOLDERS: {"meta": dict()},
            config.HBASE_TABLE_PUBLIC_LINKS: {"meta": dict()},
        }
        hbase_service.init_tables(table_config)
        hdfs_service.init_directories()
        logger.info("HBase 表和 HDFS 目录初始化完成")
    except Exception as e:
        logger.warning(f"初始化失败（确保 HBase 和 HDFS 已启动）: {e}")

    # ========== 注册路由 ==========

    app.register_blueprint(auth_bp)
    app.register_blueprint(file_bp)
    app.register_blueprint(folder_bp)
    app.register_blueprint(stats_bp)
    app.register_blueprint(ai_bp)
    app.register_blueprint(group_bp)
    app.register_blueprint(public_link_bp)

    # 前端页面路由
    @app.route("/")
    def landing():
        return send_from_directory(app.static_folder, "landing.html")

    @app.route("/app")
    @app.route("/app/")
    def app_entry():
        return send_from_directory(app.static_folder, "index.html")

    @app.route("/docs")
    @app.route("/docs/")
    def docs_page():
        return send_from_directory(app.static_folder, "docs.html")

    @app.route("/<path:path>")
    def serve_static(path):
        file_path = os.path.join(app.static_folder, path)
        if os.path.isfile(file_path):
            return send_from_directory(app.static_folder, path)
        return send_from_directory(app.static_folder, "index.html")

    # 健康检查（探测 HBase / HDFS 依赖；任一不可用返回 503）
    @app.route("/api/health")
    def health_check():
        deps = {"hbase": "ok", "hdfs": "ok"}

        try:
            hbase_service.ping()
        except Exception as e:
            deps["hbase"] = f"down: {type(e).__name__}"

        try:
            hdfs_service.ping()
        except Exception as e:
            deps["hdfs"] = f"down: {type(e).__name__}"

        healthy = all(v == "ok" for v in deps.values())
        body = {
            "status": "ok" if healthy else "degraded",
            "service": "BigData Cloud Drive",
            "dependencies": deps,
        }
        return (body, 200) if healthy else (body, 503)

    # /api/* 错误统一返回 JSON，避免前端 resp.json() 解析到 Flask 默认的 HTML 错误页
    @app.errorhandler(Exception)
    def handle_api_error(e):
        if not request.path.startswith("/api/"):
            raise e
        if isinstance(e, HTTPException):
            return jsonify({"error": e.description}), e.code
        logger.exception(f"未捕获异常 on {request.path}: {e}")
        return jsonify({"error": "服务器内部错误，请稍后重试"}), 500

    logger.info("智能云盘系统启动成功!")
    return app


if __name__ == "__main__":
    app = create_app()
    app.run(host="0.0.0.0", port=5000, debug=True)
