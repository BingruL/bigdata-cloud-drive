import time
import uuid

from flask import Blueprint, request, jsonify, g, current_app

from ..auth.jwt_handler import login_required

folder_bp = Blueprint("folders", __name__, url_prefix="/api/folders")


def _now_ms():
    return str(int(time.time() * 1000))


def _validate_parent(hbase, config, parent_id):
    if parent_id == "root":
        return True, None
    parent = hbase.get_folder(config.HBASE_TABLE_FOLDERS, parent_id)
    if not parent or parent.get("deleted") == "1":
        return False, ("目标目录不存在", 404)
    if parent.get("owner") != g.current_user:
        return False, ("无权访问目标目录", 403)
    return True, None


@folder_bp.route("", methods=["POST"])
@login_required
def create_folder():
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = request.get_json(silent=True) or {}
    raw_name = body.get("name")
    if raw_name is not None and not isinstance(raw_name, str):
        return jsonify({"error": "文件夹名称必须为字符串"}), 400
    raw_parent_id = body.get("parent_id", "root")
    if raw_parent_id is not None and not isinstance(raw_parent_id, str):
        return jsonify({"error": "父目录ID必须为字符串"}), 400

    name = (raw_name or "").strip()
    parent_id = (raw_parent_id or "root").strip() or "root"
    if not name:
        return jsonify({"error": "文件夹名称不能为空"}), 400
    ok, err = _validate_parent(hbase, config, parent_id)
    if not ok:
        msg, code = err
        return jsonify({"error": msg}), code
    name = hbase.resolve_available_name(
        config.HBASE_TABLE_FILES,
        config.HBASE_TABLE_FOLDERS,
        g.current_user,
        parent_id,
        name,
    )
    folder_id = uuid.uuid4().hex
    now = _now_ms()
    meta = {
        "name": name,
        "owner": g.current_user,
        "parent_id": parent_id,
        "created_at": now,
        "updated_at": now,
    }
    folder = hbase.create_folder(config.HBASE_TABLE_FOLDERS, folder_id, meta)
    current_app.config["EVENT_BUS"].log(g.current_user, "folder_create", folder_id)
    return jsonify(folder), 201
