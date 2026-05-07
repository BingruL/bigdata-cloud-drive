"""
文件管理路由
上传、下载、删除、列表、搜索
"""
import os
import uuid
import time
import io
from flask import Blueprint, request, jsonify, g, current_app, send_file
from ..auth.jwt_handler import login_required
from ..utils import parse_int_arg, BadArg

file_bp = Blueprint("files", __name__, url_prefix="/api/files")

# 可直接按文本读取并送入 LLM 做内容摘要的扩展名；其他类型一律走"仅文件名→标签"分支
TEXT_EXTRACTABLE_TYPES = {"txt", "md", "csv", "json", "xml", "html", "py", "java", "js", "log"}


@file_bp.errorhandler(BadArg)
def _handle_bad_arg(err):
    return jsonify({"error": str(err)}), 400


def _my_group_ids():
    """当前用户所在的群组 id 集合（admin 视为全集，但调用方一般会先看 role）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    return set(hbase.list_user_group_ids(config.HBASE_TABLE_USER_GROUPS, g.current_user))


def _can_access_identity(meta, username, role="user"):
    """按指定身份判断文件读取权限，用于不走 login_required 的预览流。"""
    if role == "admin":
        return True
    if meta.get("owner") == username:
        return True
    if meta.get("is_shared") != "1":
        return False

    shared = {x for x in (meta.get("shared_groups") or "").split(",") if x}
    if not shared:
        return False

    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    user_groups = set(hbase.list_user_group_ids(config.HBASE_TABLE_USER_GROUPS, username))
    return bool(shared & user_groups)


def _can_access(meta):
    """文件读取权限：
    - admin 永远可读
    - 文件主可读
    - 文件 is_shared=1 且 shared_groups 与当前用户所在群组有交集时可读
    """
    return _can_access_identity(meta, g.current_user, g.current_role)


def _now_ms():
    return str(int(time.time() * 1000))


def _json_object():
    body = request.get_json(silent=True)
    if body is None:
        return {}
    if not isinstance(body, dict):
        return None
    return body


def _validate_parent(hbase, config, parent_id):
    if parent_id == "root":
        return True, None
    parent = hbase.get_folder(config.HBASE_TABLE_FOLDERS, parent_id)
    if not parent or parent.get("deleted") == "1":
        return False, ("目标目录不存在", 404)
    if parent.get("owner") != g.current_user:
        return False, ("无权访问目标目录", 403)
    return True, None


def _folder_chain_active(hbase, config, parent_id, owner):
    current_id = parent_id or "root"
    while current_id != "root":
        folder = hbase.get_folder(config.HBASE_TABLE_FOLDERS, current_id)
        if not folder or folder.get("deleted") == "1":
            return False
        if owner and folder.get("owner") != owner:
            return False
        current_id = folder.get("parent_id", "root") or "root"
    return True


def _owned_active_file(hbase, config, file_id):
    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return None, (jsonify({"error": "文件不存在"}), 404)
    if meta.get("owner") != g.current_user:
        return None, (jsonify({"error": "无权操作此文件"}), 403)
    return meta, None


@file_bp.route("/upload", methods=["POST"])
@login_required
def upload_file():
    """文件上传"""
    if "file" not in request.files:
        return jsonify({"error": "没有选择文件"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"error": "文件名为空"}), 400

    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]
    parent_id = (request.form.get("parent_id") or "root").strip() or "root"
    ok, err = _validate_parent(hbase, config, parent_id)
    if not ok:
        msg, code = err
        return jsonify({"error": msg}), code

    # 生成文件 ID
    file_id = uuid.uuid4().hex

    # 保存到临时目录
    os.makedirs(config.UPLOAD_TEMP_DIR, exist_ok=True)
    temp_path = os.path.join(config.UPLOAD_TEMP_DIR, file_id)
    file.save(temp_path)

    # 获取文件信息
    file_size = os.path.getsize(temp_path)
    file_ext = os.path.splitext(file.filename)[1].lstrip(".").lower()
    if not file_ext:
        file_ext = "unknown"

    # 配额检查：统计当前用户已占用空间（含回收站）
    quota = config.ADMIN_QUOTA_BYTES if g.current_role == "admin" else config.USER_QUOTA_BYTES
    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=True)
    used = sum(int(f.get("size", 0) or 0) for f in all_files if f.get("owner") == g.current_user)
    if used + file_size > quota:
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return jsonify({
            "error": f"存储空间不足：已用 {used // (1024*1024)} MB / 配额 {quota // (1024*1024)} MB",
        }), 413

    try:
        display_name = hbase.resolve_available_name(
            config.HBASE_TABLE_FILES,
            config.HBASE_TABLE_FOLDERS,
            g.current_user,
            parent_id,
            file.filename,
        )

        # 上传到 HDFS
        hdfs_path = hdfs.upload_file(
            g.current_user, file_id, temp_path, file.filename
        )

        # 保存元数据到 HBase
        now = _now_ms()
        meta = {
            "filename": file.filename,
            "display_name": display_name,
            "parent_id": parent_id,
            "size": str(file_size),
            "type": file_ext,
            "owner": g.current_user,
            "hdfs_path": hdfs_path,
            "created_at": now,
            "updated_at": now,
            "downloads": "0",
            "summary": "",
            "tags": "",
            "is_shared": "0",
            "shared_groups": "",
        }
        hbase.save_file_meta(config.HBASE_TABLE_FILES, file_id, meta)

        # 记录日志
        current_app.config["EVENT_BUS"].log(g.current_user, "upload", file_id)

        # 文本文件：读内容生成摘要+标签；其他类型：仅根据文件名生成标签
        try:
            ai = current_app.config.get("AI_SERVICE")
            if ai:
                if file_ext in TEXT_EXTRACTABLE_TYPES:
                    content = hdfs.read_text_file(hdfs_path, max_bytes=30000)
                    result = ai.generate_summary(content, file.filename)
                else:
                    result = ai.generate_tags_from_filename(file.filename)
                hbase.update_file_ai(
                    config.HBASE_TABLE_FILES, file_id,
                    summary=result.get("summary", ""),
                    tags=",".join(result.get("tags", [])),
                )
        except Exception as e:
            current_app.logger.warning(f"AI 标签/摘要生成失败: {e}")

        return jsonify({
            "message": "上传成功",
            "file": {
                "file_id": file_id,
                "filename": file.filename,
                "display_name": display_name,
                "parent_id": parent_id,
                "size": file_size,
                "type": file_ext,
                "hdfs_path": hdfs_path,
            },
        }), 201

    except Exception as e:
        current_app.logger.error(f"文件上传失败: {e}")
        return jsonify({"error": f"文件上传失败: {str(e)}"}), 500

    finally:
        # 清理临时文件
        if os.path.exists(temp_path):
            os.remove(temp_path)


@file_bp.route("/list", methods=["GET"])
@login_required
def list_files():
    """文件列表（支持筛选和分页）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    # 查询参数
    owner = request.args.get("owner")
    file_type = request.args.get("type")
    keyword = request.args.get("keyword")
    page = parse_int_arg("page", default=1, min_value=1)
    page_size = parse_int_arg(
        "page_size", default=config.DEFAULT_PAGE_SIZE,
        min_value=1, max_value=config.MAX_PAGE_SIZE,
    )

    # 普通用户只能看自己的文件，管理员可以看所有
    if g.current_role != "admin":
        owner = g.current_user

    result = hbase.list_files(
        config.HBASE_TABLE_FILES,
        owner=owner, file_type=file_type,
        keyword=keyword, page=page, page_size=page_size,
    )
    return jsonify(result)


@file_bp.route("/browse", methods=["GET"])
@login_required
def browse_files():
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    parent_id = request.args.get("parent_id", "root") or "root"
    ok, err = _validate_parent(hbase, config, parent_id)
    if not ok:
        msg, code = err
        return jsonify({"error": msg}), code
    folders = hbase.list_child_folders(config.HBASE_TABLE_FOLDERS, g.current_user, parent_id)
    files = []
    for f in hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False):
        if f.get("owner") != g.current_user:
            continue
        if f.get("parent_id", "root") != parent_id:
            continue
        files.append({**f, "item_type": "file", "display_name": f.get("display_name") or f.get("filename", "")})
    return jsonify({
        "parent_id": parent_id,
        "breadcrumbs": [{"folder_id": "root", "name": "全部文件"}],
        "items": folders + files,
    })


@file_bp.route("/recent", methods=["GET"])
@login_required
def recent_files():
    """
    最近访问：聚合当前用户的 download / preview 日志，取每个文件的最近一次访问时间，按时间倒序返回
    """
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    limit = parse_int_arg("limit", default=30, min_value=1, max_value=500)
    logs = hbase.get_logs(config.HBASE_TABLE_LOGS, username=g.current_user, limit=2000)

    # 聚合每个 file_id 的最近一次 download/preview 时间
    last_access = {}
    access_count = {}
    for log in logs:
        action = log.get("action", "")
        if action not in ("download", "preview"):
            continue
        file_id = log.get("detail", "")
        if not file_id:
            continue
        ts = int(log.get("timestamp", "0") or 0)
        if file_id not in last_access or ts > last_access[file_id]:
            last_access[file_id] = ts
        access_count[file_id] = access_count.get(file_id, 0) + 1

    # 按时间倒序
    sorted_ids = sorted(last_access.items(), key=lambda x: x[1], reverse=True)

    files = []
    for file_id, ts in sorted_ids:
        meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
        if not meta or meta.get("deleted") == "1":
            continue
        meta["last_access"] = str(ts)
        meta["access_count"] = access_count.get(file_id, 0)
        files.append(meta)
        if len(files) >= limit:
            break

    return jsonify({"files": files, "total": len(files)})


@file_bp.route("/trash", methods=["GET"])
@login_required
def list_trash():
    """回收站：已软删除的文件列表"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    page = parse_int_arg("page", default=1, min_value=1)
    page_size = parse_int_arg(
        "page_size", default=config.DEFAULT_PAGE_SIZE,
        min_value=1, max_value=config.MAX_PAGE_SIZE,
    )
    owner = g.current_user if g.current_role != "admin" else None

    result = hbase.list_files(
        config.HBASE_TABLE_FILES,
        owner=owner, page=page, page_size=page_size,
        only_deleted=True,
    )
    return jsonify(result)


@file_bp.route("/<file_id>/restore", methods=["POST"])
@login_required
def restore_file(file_id):
    """从回收站恢复文件"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404
    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权操作此文件"}), 403
    if meta.get("deleted") != "1":
        return jsonify({"error": "该文件未在回收站中"}), 400
    if not _folder_chain_active(hbase, config, meta.get("parent_id", "root"), meta.get("owner")):
        return jsonify({"error": "父目录仍在回收站中，请先恢复父目录"}), 400

    hbase.restore_file(config.HBASE_TABLE_FILES, file_id)
    current_app.config["EVENT_BUS"].log(g.current_user, "restore", file_id)
    return jsonify({"message": "文件已恢复"})


@file_bp.route("/<file_id>", methods=["GET"])
@login_required
def get_file_info(file_id):
    """获取单个文件信息"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return jsonify({"error": "文件不存在"}), 404

    if not _can_access(meta):
        return jsonify({"error": "无权访问此文件"}), 403

    return jsonify(meta)


@file_bp.route("/<file_id>/rename", methods=["PATCH"])
@login_required
def rename_file(file_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须为JSON对象"}), 400
    raw_name = body.get("name")
    if not isinstance(raw_name, str):
        return jsonify({"error": "文件名称必须为字符串"}), 400
    name = raw_name.strip()
    if not name:
        return jsonify({"error": "文件名称不能为空"}), 400

    meta, err = _owned_active_file(hbase, config, file_id)
    if err:
        return err
    parent_id = meta.get("parent_id", "root") or "root"
    display_name = hbase.resolve_available_name(
        config.HBASE_TABLE_FILES,
        config.HBASE_TABLE_FOLDERS,
        g.current_user,
        parent_id,
        name,
        exclude_file_id=file_id,
    )
    hbase.update_file_meta_fields(
        config.HBASE_TABLE_FILES,
        file_id,
        {"display_name": display_name, "updated_at": _now_ms()},
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "rename", file_id)
    updated = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    return jsonify(updated)


@file_bp.route("/<file_id>/move", methods=["PATCH"])
@login_required
def move_file(file_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须为JSON对象"}), 400
    raw_parent_id = body.get("target_parent_id", body.get("parent_id"))
    if raw_parent_id is not None and not isinstance(raw_parent_id, str):
        return jsonify({"error": "目标目录ID必须为字符串"}), 400
    target_parent_id = (raw_parent_id or "root").strip() or "root"

    meta, err = _owned_active_file(hbase, config, file_id)
    if err:
        return err
    ok, parent_err = _validate_parent(hbase, config, target_parent_id)
    if not ok:
        msg, code = parent_err
        return jsonify({"error": msg}), code

    current_name = meta.get("display_name") or meta.get("filename", "")
    display_name = hbase.resolve_available_name(
        config.HBASE_TABLE_FILES,
        config.HBASE_TABLE_FOLDERS,
        g.current_user,
        target_parent_id,
        current_name,
        exclude_file_id=file_id,
    )
    hbase.update_file_meta_fields(
        config.HBASE_TABLE_FILES,
        file_id,
        {"parent_id": target_parent_id, "display_name": display_name, "updated_at": _now_ms()},
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "move", file_id)
    updated = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    return jsonify(updated)


@file_bp.route("/<file_id>/download", methods=["GET"])
@login_required
def download_file(file_id):
    """文件下载"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return jsonify({"error": "文件不存在"}), 404

    if not _can_access(meta):
        return jsonify({"error": "无权下载此文件"}), 403
    hdfs_path = meta.get("hdfs_path")
    if not hdfs_path:
        return jsonify({"error": "文件存储路径缺失"}), 500

    try:
        # 下载到临时目录
        os.makedirs(config.UPLOAD_TEMP_DIR, exist_ok=True)
        temp_path = os.path.join(config.UPLOAD_TEMP_DIR, f"dl_{file_id}")
        hdfs.download_file(hdfs_path, temp_path)

        # 增加下载计数
        hbase.increment_downloads(config.HBASE_TABLE_FILES, file_id)

        # 记录日志
        current_app.config["EVENT_BUS"].log(g.current_user, "download", file_id)

        return send_file(
            temp_path,
            as_attachment=True,
            download_name=meta.get("display_name") or meta.get("filename", "download"),
        )

    except Exception as e:
        current_app.logger.error(f"文件下载失败: {e}")
        return jsonify({"error": f"文件下载失败: {str(e)}"}), 500


@file_bp.route("/<file_id>", methods=["DELETE"])
@login_required
def delete_file(file_id):
    """文件软删除（移至回收站，HDFS 文件保留）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权删除此文件"}), 403

    if meta.get("deleted") == "1":
        return jsonify({"error": "文件已在回收站中"}), 400

    try:
        hbase.soft_delete_file(config.HBASE_TABLE_FILES, file_id)
        current_app.config["EVENT_BUS"].log(g.current_user, "delete", file_id)
        return jsonify({"message": "文件已移至回收站"})
    except Exception as e:
        current_app.logger.error(f"文件软删除失败: {e}")
        return jsonify({"error": f"删除失败: {str(e)}"}), 500


@file_bp.route("/<file_id>/purge", methods=["DELETE"])
@login_required
def purge_file(file_id):
    """彻底删除（从 HDFS + HBase 永久移除，仅能对已在回收站的文件操作）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权操作此文件"}), 403
    if meta.get("deleted") != "1":
        return jsonify({"error": "请先将文件移至回收站"}), 400

    try:
        hdfs_path = meta.get("hdfs_path")
        if hdfs_path:
            try:
                hdfs.delete_file(hdfs_path)
            except Exception as e:
                current_app.logger.warning(f"HDFS 文件删除失败（继续清理元数据）: {e}")
        hbase.disable_public_links_for_file(config.HBASE_TABLE_PUBLIC_LINKS, file_id)
        hbase.delete_file_meta(config.HBASE_TABLE_FILES, file_id)
        current_app.config["EVENT_BUS"].log(g.current_user, "purge", file_id)
        return jsonify({"message": "文件已彻底删除"})
    except Exception as e:
        current_app.logger.error(f"文件彻底删除失败: {e}")
        return jsonify({"error": f"彻底删除失败: {str(e)}"}), 500


@file_bp.route("/search", methods=["GET"])
@login_required
def search_files():
    """文件搜索"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    keyword = request.args.get("keyword", "")
    file_type = request.args.get("type")
    start_date = parse_int_arg("start_date", default=None, min_value=0)
    end_date = parse_int_arg("end_date", default=None, min_value=0)
    page = parse_int_arg("page", default=1, min_value=1)
    page_size = parse_int_arg(
        "page_size", default=20, min_value=1, max_value=config.MAX_PAGE_SIZE,
    )
    if start_date is not None and end_date is not None and start_date > end_date:
        raise BadArg("start_date 不能晚于 end_date")

    # 普通用户只能搜索自己的文件
    owner = None if g.current_role == "admin" else g.current_user

    result = hbase.list_files(
        config.HBASE_TABLE_FILES,
        owner=owner, file_type=file_type,
        keyword=keyword, page=page, page_size=page_size,
        start_date=start_date, end_date=end_date,
    )
    return jsonify(result)


@file_bp.route("/by-tag/<tag>", methods=["GET"])
@login_required
def files_by_tag(tag):
    """按标签查询文件 —— 走 MapReduce/Spark 预计算的倒排索引表 cloud_drive_tag_index

    流程：
      1. 直接 get(tag) 拿到 file_id 列表（O(1)，无需扫描 cloud_drive_files）
      2. 对每个 file_id 取最新元数据 + 应用统一的 _can_access 权限过滤
      3. 如索引表不存在或为空，返回空列表 + 提示，引导用户先跑 MR 作业
    """
    import json as _json
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    index_table = "cloud_drive_tag_index"

    try:
        with hbase._get_connection() as conn:
            existing = [t.decode() for t in conn.tables()]
            if index_table not in existing:
                return jsonify({
                    "tag": tag, "files": [], "count": 0,
                    "hint": "倒排索引表尚未生成，请先运行 mapreduce_jobs/tag_index/run.sh 或 spark_jobs/tag_index_spark.py",
                })
            row = conn.table(index_table).row(tag.encode())
    except Exception as e:
        return jsonify({"error": f"读取索引失败: {e}"}), 500

    if not row:
        return jsonify({"tag": tag, "files": [], "count": 0})

    try:
        payload = _json.loads(row.get(b"idx:files", b"{}").decode())
    except _json.JSONDecodeError:
        payload = {"files": []}

    updated_at = row.get(b"idx:updated_at", b"").decode()
    file_ids = [f["file_id"] for f in payload.get("files", []) if f.get("file_id")]

    # 取最新元数据 + 权限过滤（索引可能滞后于实时元数据）
    files = []
    for fid in file_ids:
        meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, fid)
        if not meta or meta.get("deleted") == "1":
            continue
        if not _can_access(meta):
            continue
        files.append(meta)

    return jsonify({
        "tag": tag,
        "count": len(files),
        "files": files,
        "index_updated_at": updated_at or None,
    })


@file_bp.route("/<file_id>/preview-token", methods=["POST"])
@login_required
def create_pdf_preview_token(file_id):
    """签发短期 PDF 预览 Token，供浏览器 iframe 加载二进制流。"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    jwt_handler = current_app.config["JWT_HANDLER"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return jsonify({"error": "文件不存在"}), 404
    if not _can_access(meta):
        return jsonify({"error": "无权访问此文件"}), 403
    if meta.get("type", "").lower() != "pdf":
        return jsonify({"error": "当前仅支持 PDF 文件在线预览"}), 415

    ttl_seconds = config.PDF_PREVIEW_TOKEN_TTL_SECONDS
    token = jwt_handler.generate_preview_token(
        g.current_user,
        file_id,
        ttl_seconds=ttl_seconds,
        role=g.current_role,
    )
    return jsonify({"token": token, "expires_in": ttl_seconds})


@file_bp.route("/<file_id>/preview-stream", methods=["GET"])
def stream_pdf_preview(file_id):
    """通过短期预览 Token 返回 inline PDF 二进制流。"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]
    jwt_handler = current_app.config["JWT_HANDLER"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return jsonify({"error": "文件不存在"}), 404
    if meta.get("type", "").lower() != "pdf":
        return jsonify({"error": "当前仅支持 PDF 文件在线预览"}), 415

    token = request.args.get("token", "")
    payload = jwt_handler.decode_preview_token(token)
    if not payload:
        return jsonify({"error": "预览链接无效或已过期"}), 401
    if payload.get("file_id") != file_id:
        return jsonify({"error": "预览 Token 与文件不匹配"}), 403

    username = payload.get("username", "")
    role = payload.get("role", "user")
    if not username or not _can_access_identity(meta, username, role):
        return jsonify({"error": "无权预览此文件"}), 403

    hdfs_path = meta.get("hdfs_path")
    if not hdfs_path:
        return jsonify({"error": "文件存储路径缺失"}), 500

    try:
        raw = hdfs.read_file(hdfs_path)
        current_app.config["EVENT_BUS"].log(username, "preview", file_id)
        filename = meta.get("display_name") or meta.get("filename") or "preview.pdf"
        return send_file(
            io.BytesIO(raw),
            mimetype="application/pdf",
            as_attachment=False,
            download_name=filename,
        )
    except Exception as e:
        current_app.logger.error(f"PDF 预览失败: {e}")
        return jsonify({"error": f"PDF 预览失败: {str(e)}"}), 500


@file_bp.route("/<file_id>/preview", methods=["GET"])
@login_required
def preview_file(file_id):
    """
    文件预览
    文本文件返回文本内容，图片文件返回 base64 编码
    """
    import base64

    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return jsonify({"error": "文件不存在"}), 404

    if not _can_access(meta):
        return jsonify({"error": "无权访问此文件"}), 403

    hdfs_path = meta.get("hdfs_path")
    file_type = meta.get("type", "").lower()
    text_types = {"txt", "md", "csv", "json", "xml", "html", "py", "java",
                  "js", "ts", "log", "yaml", "yml", "ini", "conf", "sh", "sql"}
    image_types = {"jpg", "jpeg", "png", "gif", "bmp", "svg", "webp"}

    try:
        if file_type in text_types:
            content = hdfs.read_text_file(hdfs_path, max_bytes=100000)
            current_app.config["EVENT_BUS"].log(g.current_user, "preview", file_id)
            return jsonify({
                "type": "text",
                "filename": meta.get("display_name") or meta.get("filename", ""),
                "content": content,
                "file_type": file_type,
            })
        elif file_type in image_types:
            raw = hdfs.read_file(hdfs_path)
            b64 = base64.b64encode(raw).decode("utf-8")
            mime_map = {
                "jpg": "image/jpeg", "jpeg": "image/jpeg", "png": "image/png",
                "gif": "image/gif", "bmp": "image/bmp", "svg": "image/svg+xml",
                "webp": "image/webp",
            }
            mime = mime_map.get(file_type, "image/png")
            current_app.config["EVENT_BUS"].log(g.current_user, "preview", file_id)
            return jsonify({
                "type": "image",
                "filename": meta.get("display_name") or meta.get("filename", ""),
                "data_url": f"data:{mime};base64,{b64}",
            })
        else:
            return jsonify({
                "type": "unsupported",
                "filename": meta.get("display_name") or meta.get("filename", ""),
                "message": f"不支持预览 {file_type.upper()} 类型的文件",
            })
    except Exception as e:
        current_app.logger.error(f"文件预览失败: {e}")
        return jsonify({"error": f"文件预览失败: {str(e)}"}), 500


@file_bp.route("/<file_id>/summary", methods=["POST"])
@login_required
def generate_file_summary(file_id):
    """手动触发 AI 摘要生成"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]
    ai = current_app.config.get("AI_SERVICE")

    if not ai:
        return jsonify({"error": "AI 服务未配置"}), 503

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权操作此文件"}), 403

    hdfs_path = meta.get("hdfs_path")
    file_ext = meta.get("type", "").lower()
    filename = meta.get("filename", "")

    try:
        if file_ext in TEXT_EXTRACTABLE_TYPES:
            content = hdfs.read_text_file(hdfs_path, max_bytes=30000)
            result = ai.generate_summary(content, filename)
        else:
            result = ai.generate_tags_from_filename(filename)

        summary = result.get("summary", "")
        tags = ",".join(result.get("tags", []))
        hbase.update_file_ai(config.HBASE_TABLE_FILES, file_id, summary=summary, tags=tags)

        return jsonify({
            "message": "AI 分析完成",
            "summary": summary,
            "tags": tags.split(",") if tags else [],
        })

    except Exception as e:
        current_app.logger.error(f"AI 分析失败: {e}")
        return jsonify({"error": f"AI 分析失败: {str(e)}"}), 500


# ========== 群组分享 ==========

def _validate_share_groups(hbase, config, group_ids):
    """要求 group_ids 都是当前用户所在的群组（admin 不限）"""
    if g.current_role == "admin":
        return True, None
    my_gids = set(hbase.list_user_group_ids(config.HBASE_TABLE_USER_GROUPS, g.current_user))
    bad = [gid for gid in group_ids if gid not in my_gids]
    if bad:
        return False, f"无权分享到以下群组: {', '.join(bad)}"
    return True, None


@file_bp.route("/<file_id>/share", methods=["POST"])
@login_required
def share_file(file_id):
    """分享文件到指定群组（覆盖式：传入即为最新分享列表）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404
    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "仅文件所有者可分享"}), 403
    if meta.get("deleted") == "1":
        return jsonify({"error": "回收站中的文件无法分享"}), 400

    body = request.get_json(silent=True) or {}
    group_ids = [str(x).strip() for x in (body.get("groups") or []) if str(x).strip()]
    if not group_ids:
        return jsonify({"error": "至少选择一个群组"}), 400

    ok, err = _validate_share_groups(hbase, config, group_ids)
    if not ok:
        return jsonify({"error": err}), 403

    hbase.update_file_sharing(config.HBASE_TABLE_FILES, file_id, True, group_ids)
    current_app.config["EVENT_BUS"].log(g.current_user, "share", f"{file_id}:{','.join(group_ids)}")
    return jsonify({"message": "已分享", "groups": group_ids})


@file_bp.route("/<file_id>/unshare", methods=["POST"])
@login_required
def unshare_file(file_id):
    """取消文件的全部分享，恢复为私有"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404
    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "仅文件所有者可取消分享"}), 403

    hbase.update_file_sharing(config.HBASE_TABLE_FILES, file_id, False, [])
    current_app.config["EVENT_BUS"].log(g.current_user, "unshare", file_id)
    return jsonify({"message": "已取消分享"})


@file_bp.route("/shared", methods=["GET"])
@login_required
def list_shared_with_me():
    """列出我所在群组里、其他用户分享给我的文件"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    page = parse_int_arg("page", default=1, min_value=1)
    page_size = parse_int_arg(
        "page_size", default=config.DEFAULT_PAGE_SIZE,
        min_value=1, max_value=config.MAX_PAGE_SIZE,
    )

    my_gids = set(hbase.list_user_group_ids(config.HBASE_TABLE_USER_GROUPS, g.current_user))
    if not my_gids and g.current_role != "admin":
        return jsonify({"files": [], "total": 0, "page": page,
                        "page_size": page_size, "total_pages": 0})

    all_files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False)
    matched = []
    for f in all_files:
        if f.get("owner") == g.current_user:
            continue
        if f.get("is_shared") != "1":
            continue
        shared = {x for x in (f.get("shared_groups") or "").split(",") if x}
        if g.current_role == "admin" or (shared & my_gids):
            # 透出 "通过哪些群组共享给我的"，方便前端展示
            via = sorted(shared & my_gids) if g.current_role != "admin" else sorted(shared)
            matched.append({**f, "shared_via": via})

    matched.sort(key=lambda x: x.get("created_at", "0"), reverse=True)
    total = len(matched)
    start = (page - 1) * page_size
    return jsonify({
        "files": matched[start:start + page_size],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    })
