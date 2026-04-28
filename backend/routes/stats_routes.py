"""
统计分析 & AI 推荐路由
"""
import time
from flask import Blueprint, request, jsonify, g, current_app
from ..auth.jwt_handler import login_required, admin_required
from ..utils import parse_int_arg, BadArg

stats_bp = Blueprint("stats", __name__, url_prefix="/api/stats")


@stats_bp.errorhandler(BadArg)
def _stats_bad_arg(err):
    return jsonify({"error": str(err)}), 400


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
    days = parse_int_arg("days", default=7, min_value=1, max_value=365)
    username = None if g.current_role == "admin" else g.current_user
    return jsonify(stats.get_daily_upload_trend(days, username))


@stats_bp.route("/storage", methods=["GET"])
@login_required
def storage_stats():
    """存储空间统计"""
    stats = current_app.config["STATS_SERVICE"]
    return jsonify(stats.get_storage_stats())


@stats_bp.route("/my-storage", methods=["GET"])
@login_required
def my_storage():
    """当前用户存储配额与用量"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    # 用户存储占用（含回收站，因为 HDFS 未真正清理）
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=True)
    used = 0
    active_count = 0
    trash_count = 0
    trash_size = 0
    for f in all_files:
        if f.get("owner") != g.current_user:
            continue
        size = int(f.get("size", 0) or 0)
        used += size
        if f.get("deleted") == "1":
            trash_count += 1
            trash_size += size
        else:
            active_count += 1

    quota = config.ADMIN_QUOTA_BYTES if g.current_role == "admin" else config.USER_QUOTA_BYTES
    percent = min(100.0, round(used / quota * 100, 2)) if quota > 0 else 0

    def _fmt(b):
        b = int(b)
        if b < 1024: return f"{b} B"
        if b < 1024**2: return f"{b/1024:.1f} KB"
        if b < 1024**3: return f"{b/1024**2:.1f} MB"
        return f"{b/1024**3:.2f} GB"

    return jsonify({
        "used": used,
        "quota": quota,
        "percent": percent,
        "used_readable": _fmt(used),
        "quota_readable": _fmt(quota),
        "active_count": active_count,
        "trash_count": trash_count,
        "trash_size": trash_size,
        "trash_size_readable": _fmt(trash_size),
    })


@stats_bp.route("/hot-files", methods=["GET"])
@login_required
def hot_files():
    """热门文件排行"""
    stats = current_app.config["STATS_SERVICE"]
    top_n = parse_int_arg("top", default=10, min_value=1, max_value=100)
    return jsonify(stats.get_hot_files(top_n))


@stats_bp.route("/recent-activity", methods=["GET"])
@login_required
def recent_activity():
    """最近操作动态"""
    stats = current_app.config["STATS_SERVICE"]
    limit = parse_int_arg("limit", default=20, min_value=1, max_value=200)
    return jsonify(stats.get_recent_activity(limit))


@stats_bp.route("/hourly-activity", methods=["GET"])
@login_required
def hourly_activity():
    """24小时活跃度"""
    stats = current_app.config["STATS_SERVICE"]
    return jsonify(stats.get_hourly_activity())


@stats_bp.route("/realtime", methods=["GET"])
@login_required
def realtime_stats():
    """实时面板数据：由 spark_jobs/streaming_stats.py 写入 HBase 的 realtime_* 行
    未启用 Streaming 时各项均为 null，前端据此显示"等待中"。
    """
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    keys = ["realtime_action_counts", "realtime_active_users",
            "realtime_hot_files", "realtime_event_stream"]
    result = {}
    latest_update = 0
    for k in keys:
        s = hbase.get_stats(config.HBASE_TABLE_STATS, k)
        if s:
            result[k] = s["value"]
            try:
                latest_update = max(latest_update, int(s["updated_at"]))
            except (TypeError, ValueError):
                pass
        else:
            result[k] = None
    result["updated_at"] = latest_update or None
    # 用更新时间和当前时间差判断 streaming 是否在线（30 秒内有更新视为在线）
    if latest_update:
        result["streaming_online"] = (int(time.time() * 1000) - latest_update) < 30000
    else:
        result["streaming_online"] = False
    return jsonify(result)


@stats_bp.route("/activity-heatmap", methods=["GET"])
@login_required
def activity_heatmap():
    """用户活跃热力图数据（按日聚合操作次数）"""
    stats = current_app.config["STATS_SERVICE"]
    days = parse_int_arg("days", default=365, min_value=1, max_value=365)
    username = None if g.current_role == "admin" else g.current_user
    return jsonify(stats.get_activity_heatmap(days, username))


# ========== AI 推荐路由 ==========

ai_bp = Blueprint("ai", __name__, url_prefix="/api/ai")


@ai_bp.errorhandler(BadArg)
def _ai_bad_arg(err):
    return jsonify({"error": str(err)}), 400


def _group_scoped_corpus(hbase, config):
    """返回 (候选文件池, 语料日志, 元信息)
    候选文件池: is_shared=1 且与我所在群组有交集，并排除我自己的文件
    语料日志: 仅来自我所在群组内成员的日志（含我自己）
    admin 退化为全量
    """
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False)
    all_logs = hbase.get_logs(config.HBASE_TABLE_LOGS, limit=10000)

    if g.current_role == "admin":
        return all_files, all_logs, {"scope": "admin", "groups": [], "members": []}

    my_gids = set(hbase.list_user_group_ids(config.HBASE_TABLE_USER_GROUPS, g.current_user))
    if not my_gids:
        return [], [], {"scope": "no_group", "groups": [], "members": []}

    member_set = {g.current_user}
    for gid in my_gids:
        for m in hbase.list_group_members(config.HBASE_TABLE_GROUP_MEMBERS, gid):
            member_set.add(m["username"])

    candidates = []
    for f in all_files:
        if f.get("owner") == g.current_user:
            continue
        if f.get("is_shared") != "1":
            continue
        shared = {x for x in (f.get("shared_groups") or "").split(",") if x}
        if not (shared & my_gids):
            continue
        candidates.append(f)

    scoped_logs = [l for l in all_logs if l.get("username") in member_set]
    return candidates, scoped_logs, {
        "scope": "group", "groups": sorted(my_gids), "members": sorted(member_set),
    }


@ai_bp.route("/recommend/hot", methods=["GET"])
@login_required
def recommend_hot():
    """群组热门：在我所在群组的共享文件池里按下载量排序"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]
    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    top_n = parse_int_arg("top", default=10, min_value=1, max_value=100)
    files, _logs, meta = _group_scoped_corpus(hbase, config)
    return jsonify({"scope": meta["scope"], "items": ai.get_hot_files(files, top_n)})


@ai_bp.route("/recommend/personalized", methods=["GET"])
@login_required
def recommend_personalized():
    """个性化推荐（语料限定为我所在群组的成员行为）"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]
    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    top_n = parse_int_arg("top", default=10, min_value=1, max_value=100)
    files, logs, meta = _group_scoped_corpus(hbase, config)
    return jsonify({
        "scope": meta["scope"],
        "items": ai.get_personalized_recommendations(files, logs, g.current_user, top_n),
    })


@ai_bp.route("/recommend/similar-users", methods=["GET"])
@login_required
def recommend_similar_users():
    """基于相似用户的协同过滤推荐（限定群组内成员）"""
    config = current_app.config["APP_CONFIG"]
    ai = current_app.config.get("AI_SERVICE")
    hbase = current_app.config["HBASE_SERVICE"]
    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    top_n = parse_int_arg("top", default=10, min_value=1, max_value=100)
    files, logs, meta = _group_scoped_corpus(hbase, config)
    return jsonify({
        "scope": meta["scope"],
        "items": ai.get_similar_users_recommendations(files, logs, g.current_user, top_n),
    })


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
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False)
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

    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False)
    if g.current_role != "admin":
        all_files = [f for f in all_files if f.get("owner") == g.current_user]

    top_n = parse_int_arg("top", default=10, min_value=1, max_value=100)
    result = ai.get_related_files(all_files, file_id, top_n)
    return jsonify(result)
