"""
文件管理路由
上传、下载、删除、列表、搜索
"""
import os
import uuid
import time
from flask import Blueprint, request, jsonify, g, current_app, send_file
from ..auth.jwt_handler import login_required

file_bp = Blueprint("files", __name__, url_prefix="/api/files")


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

    try:
        # 上传到 HDFS
        hdfs_path = hdfs.upload_file(
            g.current_user, file_id, temp_path, file.filename
        )

        # 保存元数据到 HBase
        meta = {
            "filename": file.filename,
            "size": str(file_size),
            "type": file_ext,
            "owner": g.current_user,
            "hdfs_path": hdfs_path,
            "created_at": str(int(time.time() * 1000)),
            "downloads": "0",
            "summary": "",
            "tags": "",
        }
        hbase.save_file_meta(config.HBASE_TABLE_FILES, file_id, meta)

        # 记录日志
        hbase.add_log(
            config.HBASE_TABLE_LOGS, g.current_user,
            "upload", file_id
        )

        # 如果是文本文件，异步生成 AI 摘要
        text_types = {"txt", "md", "csv", "json", "xml", "html", "py", "java", "js", "log"}
        if file_ext in text_types:
            try:
                ai = current_app.config.get("AI_SERVICE")
                if ai:
                    content = hdfs.read_text_file(hdfs_path, max_bytes=30000)
                    result = ai.generate_summary(content, file.filename)
                    hbase.update_file_ai(
                        config.HBASE_TABLE_FILES, file_id,
                        summary=result.get("summary", ""),
                        tags=",".join(result.get("tags", [])),
                    )
            except Exception as e:
                current_app.logger.warning(f"AI 摘要生成失败: {e}")

        return jsonify({
            "message": "上传成功",
            "file": {
                "file_id": file_id,
                "filename": file.filename,
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
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", config.DEFAULT_PAGE_SIZE))
    page_size = min(page_size, config.MAX_PAGE_SIZE)

    # 普通用户只能看自己的文件，管理员可以看所有
    if g.current_role != "admin":
        owner = g.current_user

    result = hbase.list_files(
        config.HBASE_TABLE_FILES,
        owner=owner, file_type=file_type,
        keyword=keyword, page=page, page_size=page_size,
    )
    return jsonify(result)


@file_bp.route("/<file_id>", methods=["GET"])
@login_required
def get_file_info(file_id):
    """获取单个文件信息"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    # 权限检查
    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权访问此文件"}), 403

    return jsonify(meta)


@file_bp.route("/<file_id>/download", methods=["GET"])
@login_required
def download_file(file_id):
    """文件下载"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    # 权限检查（允许下载其他用户公开的文件，此处简化为只要登录就可以下载）
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
        hbase.add_log(
            config.HBASE_TABLE_LOGS, g.current_user,
            "download", file_id
        )

        return send_file(
            temp_path,
            as_attachment=True,
            download_name=meta.get("filename", "download"),
        )

    except Exception as e:
        current_app.logger.error(f"文件下载失败: {e}")
        return jsonify({"error": f"文件下载失败: {str(e)}"}), 500


@file_bp.route("/<file_id>", methods=["DELETE"])
@login_required
def delete_file(file_id):
    """文件删除"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]

    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    # 权限检查
    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权删除此文件"}), 403

    try:
        # 1. 删除 HDFS 文件
        hdfs_path = meta.get("hdfs_path")
        if hdfs_path:
            hdfs.delete_file(hdfs_path)

        # 2. 删除 HBase 元数据
        hbase.delete_file_meta(config.HBASE_TABLE_FILES, file_id)

        # 3. 记录日志
        hbase.add_log(
            config.HBASE_TABLE_LOGS, g.current_user,
            "delete", file_id
        )

        return jsonify({"message": "文件已删除"})

    except Exception as e:
        current_app.logger.error(f"文件删除失败: {e}")
        return jsonify({"error": f"文件删除失败: {str(e)}"}), 500


@file_bp.route("/search", methods=["GET"])
@login_required
def search_files():
    """文件搜索"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    keyword = request.args.get("keyword", "")
    file_type = request.args.get("type")
    start_date = request.args.get("start_date")  # 时间戳（毫秒）
    end_date = request.args.get("end_date")
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 20))

    # 普通用户只能搜索自己的文件
    owner = None if g.current_role == "admin" else g.current_user

    result = hbase.list_files(
        config.HBASE_TABLE_FILES,
        owner=owner, file_type=file_type,
        keyword=keyword, page=page, page_size=page_size,
    )

    # 额外按时间范围过滤
    if start_date or end_date:
        filtered = []
        for f in result["files"]:
            ts = int(f.get("created_at", 0))
            if start_date and ts < int(start_date):
                continue
            if end_date and ts > int(end_date):
                continue
            filtered.append(f)
        result["files"] = filtered
        result["total"] = len(filtered)

    return jsonify(result)


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
    if not meta:
        return jsonify({"error": "文件不存在"}), 404

    if g.current_role != "admin" and meta.get("owner") != g.current_user:
        return jsonify({"error": "无权访问此文件"}), 403

    hdfs_path = meta.get("hdfs_path")
    file_type = meta.get("type", "").lower()
    text_types = {"txt", "md", "csv", "json", "xml", "html", "py", "java",
                  "js", "ts", "log", "yaml", "yml", "ini", "conf", "sh", "sql"}
    image_types = {"jpg", "jpeg", "png", "gif", "bmp", "svg", "webp"}

    try:
        if file_type in text_types:
            content = hdfs.read_text_file(hdfs_path, max_bytes=100000)
            return jsonify({
                "type": "text",
                "filename": meta.get("filename", ""),
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
            return jsonify({
                "type": "image",
                "filename": meta.get("filename", ""),
                "data_url": f"data:{mime};base64,{b64}",
            })
        else:
            return jsonify({
                "type": "unsupported",
                "filename": meta.get("filename", ""),
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
    text_types = {"txt", "md", "csv", "json", "xml", "html", "py", "java", "js", "log"}

    if meta.get("type", "").lower() not in text_types:
        return jsonify({"error": "仅支持文本类型文件生成摘要"}), 400

    try:
        content = hdfs.read_text_file(hdfs_path, max_bytes=30000)
        result = ai.generate_summary(content, meta.get("filename", ""))

        summary = result.get("summary", "")
        tags = ",".join(result.get("tags", []))
        hbase.update_file_ai(config.HBASE_TABLE_FILES, file_id, summary=summary, tags=tags)

        return jsonify({
            "message": "摘要生成成功",
            "summary": summary,
            "tags": tags.split(",") if tags else [],
        })

    except Exception as e:
        current_app.logger.error(f"AI 摘要生成失败: {e}")
        return jsonify({"error": f"摘要生成失败: {str(e)}"}), 500
