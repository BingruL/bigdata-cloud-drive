"""
统计分析 & AI 推荐路由
"""
from flask import Blueprint, request, jsonify, g, current_app
from ..auth.jwt_handler import login_required, admin_required

stats_bp = Blueprint("stats", __name__, url_prefix="/api/stats")


@stats_bp.route("/dashboard", methods=["GET"])
@login_required
def dashboard_summary():
    """Dashboard 汇总数据"""
    stats = current_app.config["STATS_SERVICE"]
    return jsonify(stats.get_dashboard_summary())


@stats_bp.route("/user-file-counts", methods=["GET"])
@login_required
def user_file_counts():
    """各用户文件数量"""
    stats = current_app.config["STATS_SERVICE"]
    return jsonify(stats.get_user_file_counts())


@stats_bp.route("/file-type-distribution", methods=["GET"])
@login_required
def file_type_distribution():
    """文件类型分布"""
    stats = current_app.config["STATS_SERVICE"]
    username = None if g.current_role == "admin" else g.current_user
    return jsonify(stats.get_file_type_distribution(username))


@stats_bp.route("/daily-upload-trend", methods=["GET"])
@login_required
def daily_upload_trend():
    """每日上传趋势"""
    stats = current_app.config["STATS_SERVICE"]
    days = int(request.args.get("days", 7))
    username = None if g.current_role == "admin" else g.current_user
    return jsonify(stats.get_daily_upload_trend(days, username))


@stats_bp.route("/storage", methods=["GET"])
@login_required
def storage_stats():
    """存储空间统计"""
    stats = current_app.config["STATS_SERVICE"]
    return jsonify(stats.get_storage_stats())


@stats_bp.route("/hot-files", methods=["GET"])
@login_required
def hot_files():
    """热门文件排行"""
    stats = current_app.config["STATS_SERVICE"]
    top_n = int(request.args.get("top", 10))
    return jsonify(stats.get_hot_files(top_n))


@stats_bp.route("/recent-activity", methods=["GET"])
@login_required
def recent_activity():
    """最近操作动态"""
    stats = current_app.config["STATS_SERVICE"]
    limit = int(request.args.get("limit", 20))
    return jsonify(stats.get_recent_activity(limit))


@stats_bp.route("/hourly-activity", methods=["GET"])
@login_required
def hourly_activity():
    """24小时活跃度"""
    stats = current_app.config["STATS_SERVICE"]
    return jsonify(stats.get_hourly_activity())


@stats_bp.route("/activity-heatmap", methods=["GET"])
@login_required
def activity_heatmap():
    """用户活跃热力图数据（按日聚合操作次数）"""
    stats = current_app.config["STATS_SERVICE"]
    days = int(request.args.get("days", 365))
    username = None if g.current_role == "admin" else g.current_user
    return jsonify(stats.get_activity_heatmap(days, username))


# ========== AI 推荐路由 ==========

ai_bp = Blueprint("ai", __name__, url_prefix="/api/ai")


@ai_bp.route("/recommend/hot", methods=["GET"])
@login_required
def recommend_hot():
    """热门文件推荐"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]

    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    top_n = int(request.args.get("top", 10))
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES)
    result = ai.get_hot_files(all_files, top_n)
    return jsonify(result)


@ai_bp.route("/recommend/personalized", methods=["GET"])
@login_required
def recommend_personalized():
    """个性化推荐"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]

    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    top_n = int(request.args.get("top", 10))
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES)
    all_logs = hbase.get_logs(config.HBASE_TABLE_LOGS, limit=10000)

    result = ai.get_personalized_recommendations(
        all_files, all_logs, g.current_user, top_n
    )
    return jsonify(result)


@ai_bp.route("/recommend/similar-users", methods=["GET"])
@login_required
def recommend_similar_users():
    """基于相似用户的协同过滤推荐"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]

    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    top_n = int(request.args.get("top", 10))
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES)
    all_logs = hbase.get_logs(config.HBASE_TABLE_LOGS, limit=10000)

    result = ai.get_similar_users_recommendations(
        all_files, all_logs, g.current_user, top_n
    )
    return jsonify(result)


# ========== 文件关系图谱路由 ==========

@ai_bp.route("/file-relations", methods=["GET"])
@login_required
def file_relations():
    """获取文件关系图谱数据（节点 + 边）"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]

    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    # 普通用户只看自己的文件关系，管理员看全部
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES)
    if g.current_role != "admin":
        all_files = [f for f in all_files if f.get("owner") == g.current_user]

    threshold = float(request.args.get("threshold", 0.15))
    result = ai.compute_file_relations(all_files, threshold=threshold)
    return jsonify(result)


@ai_bp.route("/related-files/<file_id>", methods=["GET"])
@login_required
def related_files(file_id):
    """获取与指定文件相关的文件列表"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]

    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES)
    if g.current_role != "admin":
        all_files = [f for f in all_files if f.get("owner") == g.current_user]

    top_n = int(request.args.get("top", 10))
    result = ai.get_related_files(all_files, file_id, top_n)
    return jsonify(result)
