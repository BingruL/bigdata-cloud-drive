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


def _json_object():
    body = request.get_json(silent=True)
    if body is None:
        return {}
    if not isinstance(body, dict):
        return None
    return body


def _get_mutable_folder(hbase, config, folder_id, allow_deleted=False):
    if folder_id == "root":
        return None, (jsonify({"error": "根目录不能执行该操作"}), 400)
    folder = hbase.get_folder(config.HBASE_TABLE_FOLDERS, folder_id)
    if not folder:
        return None, (jsonify({"error": "文件夹不存在"}), 404)
    if not allow_deleted and folder.get("deleted") == "1":
        return None, (jsonify({"error": "文件夹不存在"}), 404)
    if folder.get("owner") != g.current_user:
        return None, (jsonify({"error": "无权操作此文件夹"}), 403)
    return folder, None


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


@folder_bp.route("/tree", methods=["GET"])
@login_required
def folder_tree():
    """返回当前用户全部活跃文件夹（前端构建移动目录树用）。"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    folders = hbase.list_user_folders(config.HBASE_TABLE_FOLDERS, g.current_user)
    return jsonify({"folders": folders})


@folder_bp.route("/<folder_id>/summary", methods=["GET"])
@login_required
def folder_summary(folder_id):
    """统计文件夹子树规模（删除前确认提示用）。"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    folder, err = _get_mutable_folder(hbase, config, folder_id, allow_deleted=True)
    if err:
        return err
    subtree = hbase.collect_folder_subtree(
        config.HBASE_TABLE_FOLDERS, config.HBASE_TABLE_FILES, folder_id
    )
    folder_count = max(0, len(subtree["folders"]) - 1)
    active_files = [f for f in subtree["files"] if f.get("deleted") != "1"]
    file_count = len(active_files)
    total_size = 0
    for f in active_files:
        try:
            total_size += int(f.get("size", 0) or 0)
        except (TypeError, ValueError):
            pass
    return jsonify({
        "folder_id": folder_id,
        "folder_count": folder_count,
        "file_count": file_count,
        "total_size": total_size,
    })


@folder_bp.route("/trash", methods=["GET"])
@login_required
def list_trashed_folders():
    """回收站：列出当前用户软删除的顶层文件夹（被父级连带删除的子文件夹不展示）。"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    owner = None if g.current_role == "admin" else g.current_user
    folders = hbase.list_trashed_folders(config.HBASE_TABLE_FOLDERS, owner=owner)
    items = [{**f, "item_type": "folder"} for f in folders]
    return jsonify({"folders": items, "total": len(items)})


@folder_bp.route("/<folder_id>", methods=["GET"])
@login_required
def get_folder(folder_id):
    """获取文件夹详情。"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    if folder_id == "root":
        return jsonify(hbase.get_folder(config.HBASE_TABLE_FOLDERS, "root"))
    folder = hbase.get_folder(config.HBASE_TABLE_FOLDERS, folder_id)
    if not folder:
        return jsonify({"error": "文件夹不存在"}), 404
    if g.current_role != "admin" and folder.get("owner") != g.current_user:
        return jsonify({"error": "无权访问此文件夹"}), 403
    return jsonify(folder)


@folder_bp.route("", methods=["POST"])
@login_required
def create_folder():
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须为JSON对象"}), 400
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


@folder_bp.route("/<folder_id>/rename", methods=["PATCH"])
@login_required
def rename_folder(folder_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须为JSON对象"}), 400
    raw_name = body.get("name")
    if not isinstance(raw_name, str):
        return jsonify({"error": "文件夹名称必须为字符串"}), 400
    name = raw_name.strip()
    if not name:
        return jsonify({"error": "文件夹名称不能为空"}), 400

    folder, err = _get_mutable_folder(hbase, config, folder_id)
    if err:
        return err
    name = hbase.resolve_available_name(
        config.HBASE_TABLE_FILES,
        config.HBASE_TABLE_FOLDERS,
        g.current_user,
        folder.get("parent_id", "root") or "root",
        name,
        exclude_folder_id=folder_id,
    )
    hbase.update_folder_fields(
        config.HBASE_TABLE_FOLDERS,
        folder_id,
        {"name": name, "updated_at": _now_ms()},
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "folder_rename", folder_id)
    return jsonify(hbase.get_folder(config.HBASE_TABLE_FOLDERS, folder_id))


@folder_bp.route("/<folder_id>/move", methods=["PATCH"])
@login_required
def move_folder(folder_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = _json_object()
    if body is None:
        return jsonify({"error": "请求体必须为JSON对象"}), 400
    raw_parent_id = body.get("target_parent_id", body.get("parent_id"))
    if raw_parent_id is not None and not isinstance(raw_parent_id, str):
        return jsonify({"error": "目标目录ID必须为字符串"}), 400
    target_parent_id = (raw_parent_id or "root").strip() or "root"

    folder, err = _get_mutable_folder(hbase, config, folder_id)
    if err:
        return err
    if target_parent_id == folder_id:
        return jsonify({"error": "不能移动到自身目录"}), 400
    ok, parent_err = _validate_parent(hbase, config, target_parent_id)
    if not ok:
        msg, code = parent_err
        return jsonify({"error": msg}), code
    if hbase.is_descendant_folder(config.HBASE_TABLE_FOLDERS, folder_id, target_parent_id):
        return jsonify({"error": "不能移动到子目录中"}), 400

    name = hbase.resolve_available_name(
        config.HBASE_TABLE_FILES,
        config.HBASE_TABLE_FOLDERS,
        g.current_user,
        target_parent_id,
        folder.get("name", ""),
        exclude_folder_id=folder_id,
    )
    hbase.update_folder_fields(
        config.HBASE_TABLE_FOLDERS,
        folder_id,
        {"parent_id": target_parent_id, "name": name, "updated_at": _now_ms()},
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "folder_move", folder_id)
    return jsonify(hbase.get_folder(config.HBASE_TABLE_FOLDERS, folder_id))


@folder_bp.route("/<folder_id>", methods=["DELETE"])
@login_required
def delete_folder(folder_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    folder, err = _get_mutable_folder(hbase, config, folder_id)
    if err:
        return err
    hbase.soft_delete_folder_tree(
        config.HBASE_TABLE_FOLDERS,
        config.HBASE_TABLE_FILES,
        folder["folder_id"],
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "folder_delete", folder_id)
    return jsonify({"message": "文件夹已移至回收站"})


@folder_bp.route("/<folder_id>/restore", methods=["POST"])
@login_required
def restore_folder(folder_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    folder, err = _get_mutable_folder(hbase, config, folder_id, allow_deleted=True)
    if err:
        return err
    if folder.get("deleted") != "1":
        return jsonify({"error": "该文件夹未在回收站中"}), 400
    if not _folder_chain_active(hbase, config, folder.get("parent_id", "root"), folder.get("owner")):
        return jsonify({"error": "父目录仍在回收站中，请先恢复父目录"}), 400
    hbase.restore_folder_tree(
        config.HBASE_TABLE_FOLDERS,
        config.HBASE_TABLE_FILES,
        folder["folder_id"],
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "folder_restore", folder_id)
    return jsonify({"message": "文件夹已恢复"})


@folder_bp.route("/<folder_id>/purge", methods=["DELETE"])
@login_required
def purge_folder(folder_id):
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    hdfs = current_app.config["HDFS_SERVICE"]
    folder, err = _get_mutable_folder(hbase, config, folder_id, allow_deleted=True)
    if err:
        return err
    if folder.get("deleted") != "1":
        return jsonify({"error": "请先将文件夹移至回收站"}), 400
    hbase.purge_folder_tree(
        config.HBASE_TABLE_FOLDERS,
        config.HBASE_TABLE_FILES,
        folder["folder_id"],
        hdfs=hdfs,
    )
    current_app.config["EVENT_BUS"].log(g.current_user, "folder_purge", folder_id)
    return jsonify({"message": "文件夹已彻底删除"})
