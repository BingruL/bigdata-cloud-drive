"""
公共链接分享路由
"""
import os
import secrets
import time
from flask import Blueprint, request, jsonify, g, current_app, send_file

from ..auth.jwt_handler import login_required, hash_password, verify_password

public_link_bp = Blueprint("public_links", __name__, url_prefix="/api")


def _now_ms():
    return int(time.time() * 1000)


def _json_object():
    if request.form:
        return request.form.to_dict()
    if not request.data:
        return {}
    try:
        body = request.get_json(silent=False)
    except Exception:
        return None
    if body is None:
        return {}
    if not isinstance(body, dict):
        return None
    return body


def _display_name(meta):
    return meta.get("display_name") or meta.get("filename") or "download"


def _is_owner_or_admin(meta):
    return g.current_role == "admin" or meta.get("owner") == g.current_user


def _get_existing_active_file(hbase, config, file_id):
    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta or meta.get("deleted") == "1":
        return None, (jsonify({"error": "文件不存在"}), 404)
    return meta, None


def _get_existing_file(hbase, config, file_id):
    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_id)
    if not meta:
        return None, (jsonify({"error": "文件不存在"}), 404)
    return meta, None


def _parse_expires_in_days(body):
    raw_days = body.get("expires_in_days", 7)
    if isinstance(raw_days, bool):
        return None
    if isinstance(raw_days, int):
        expires_in_days = raw_days
    elif isinstance(raw_days, str) and raw_days.strip().isdigit():
        expires_in_days = int(raw_days.strip())
    else:
        return None
    if expires_in_days < 1 or expires_in_days > 365:
        return None
    return expires_in_days


def _parse_password(body):
    password = body.get("password", "")
    if password is None:
        return ""
    if not isinstance(password, str):
        return None
    password = password.strip()
    if len(password) > 128:
        return None
    return password


def _link_unavailable(link, meta):
    if not link:
        return "missing"
    if link.get("enabled") != "1":
        return "revoked"
    try:
        expires_at = int(link.get("expires_at", "0") or 0)
    except (TypeError, ValueError):
        expires_at = 0
    if expires_at <= _now_ms():
        return "expired"
    if not meta or meta.get("deleted") == "1":
        return "unavailable"
    return None


def _load_public_link(token):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    link = hbase.get_public_link(config.HBASE_TABLE_PUBLIC_LINKS, token)
    if not link:
        return None, None, "missing"
    meta = hbase.get_file_meta(config.HBASE_TABLE_FILES, link.get("file_id", ""))
    return link, meta, _link_unavailable(link, meta)


@public_link_bp.route("/files/<file_id>/public-links", methods=["POST"])
@login_required
def create_public_link(file_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta, err = _get_existing_active_file(hbase, config, file_id)
    if err:
        return err
    if not _is_owner_or_admin(meta):
        return jsonify({"error": "仅文件所有者可创建公共链接"}), 403

    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须是 JSON 对象"}), 400

    expires_in_days = _parse_expires_in_days(body)
    if expires_in_days is None:
        return jsonify({"error": "expires_in_days 必须是 1 到 365 的整数"}), 400

    password = _parse_password(body)
    if password is None:
        return jsonify({"error": "password 必须是 128 字符以内的字符串"}), 400
    password_hash = hash_password(password) if password else ""
    now = _now_ms()
    token = secrets.token_urlsafe(32)
    link = {
        "token": token,
        "file_id": file_id,
        "owner": meta.get("owner", ""),
        "created_at": str(now),
        "expires_at": str(now + expires_in_days * 24 * 60 * 60 * 1000),
        "password_hash": password_hash,
        "enabled": "1",
        "download_count": "0",
        "last_download_at": "",
    }
    hbase.save_public_link(config.HBASE_TABLE_PUBLIC_LINKS, token, link)
    response = _public_link_response(link)
    return jsonify({"public_link": response, **response}), 201


@public_link_bp.route("/files/<file_id>/public-links", methods=["GET"])
@login_required
def list_public_links(file_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta, err = _get_existing_file(hbase, config, file_id)
    if err:
        return err
    if not _is_owner_or_admin(meta):
        return jsonify({"error": "无权查看公共链接"}), 403

    links = hbase.list_public_links_for_file(config.HBASE_TABLE_PUBLIC_LINKS, file_id)
    response_links = [_public_link_response(link) for link in links]
    return jsonify({"public_links": response_links, "links": response_links})


@public_link_bp.route("/files/<file_id>/public-links/<token>", methods=["DELETE"])
@login_required
def revoke_public_link(file_id, token):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]

    meta, err = _get_existing_file(hbase, config, file_id)
    if err:
        return err
    if not _is_owner_or_admin(meta):
        return jsonify({"error": "无权撤销公共链接"}), 403

    link = hbase.get_public_link(config.HBASE_TABLE_PUBLIC_LINKS, token)
    if not link or link.get("file_id") != file_id:
        return jsonify({"error": "公共链接不存在"}), 404
    hbase.revoke_public_link(config.HBASE_TABLE_PUBLIC_LINKS, token)
    return jsonify({"message": "公共链接已撤销"})


@public_link_bp.route("/public-links/<token>", methods=["GET"])
def get_public_link(token):
    link, meta, state = _load_public_link(token)
    if state == "missing":
        return jsonify({"error": "公共链接不存在"}), 404
    if state:
        return jsonify({"error": "公共链接不可用", "state": state}), 410

    return jsonify({
        "state": "active",
        "filename": _display_name(meta),
        "size": meta.get("size", "0"),
        "type": meta.get("type", ""),
        "requires_password": bool(link.get("password_hash")),
        "expires_at": link.get("expires_at", ""),
    })


@public_link_bp.route("/public-links/<token>/download", methods=["POST"])
def download_public_link(token):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]

    link, meta, state = _load_public_link(token)
    if state == "missing":
        return jsonify({"error": "公共链接不存在"}), 404
    if state:
        return jsonify({"error": "公共链接不可用", "state": state}), 410

    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须是 JSON 对象"}), 400
    password_hash = link.get("password_hash", "")
    if password_hash and not verify_password(str(body.get("password", "")), password_hash):
        return jsonify({"error": "访问密码错误"}), 403

    if request.args.get("probe") == "1":
        return jsonify({"ok": True})

    hdfs_path = meta.get("hdfs_path")
    if not hdfs_path:
        return jsonify({"error": "文件存储路径缺失"}), 410

    try:
        os.makedirs(config.UPLOAD_TEMP_DIR, exist_ok=True)
        temp_path = os.path.join(config.UPLOAD_TEMP_DIR, f"public_dl_{token}")
        hdfs.download_file(hdfs_path, temp_path)
        hbase.increment_public_link_download(config.HBASE_TABLE_PUBLIC_LINKS, token)
        hbase.increment_downloads(config.HBASE_TABLE_FILES, link["file_id"])
        current_app.config["EVENT_BUS"].log(
            "public", "public_download", f"{link['file_id']} via {token}"
        )
        return send_file(temp_path, as_attachment=True, download_name=_display_name(meta))
    except Exception:
        current_app.logger.exception("公共链接下载失败")
        return jsonify({"error": "公共链接下载失败，请稍后重试"}), 500


def _public_link_response(link):
    return {
        "token": link.get("token", ""),
        "file_id": link.get("file_id", ""),
        "owner": link.get("owner", ""),
        "created_at": link.get("created_at", ""),
        "expires_at": link.get("expires_at", ""),
        "enabled": link.get("enabled", "0"),
        "download_count": link.get("download_count", "0"),
        "last_download_at": link.get("last_download_at", ""),
        "requires_password": bool(link.get("password_hash")),
    }
