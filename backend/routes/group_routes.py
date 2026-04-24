"""
群组路由
群组用于在用户之间限定文件分享范围：
- 任何登录用户都可以创建群组（自动成为群主）
- 群主可以增删成员、解散群组
- 普通成员只能查看群组成员、退出群组
- admin 可以查看/管理所有群组
"""
from flask import Blueprint, request, jsonify, g, current_app
from ..auth.jwt_handler import login_required

group_bp = Blueprint("groups", __name__, url_prefix="/api/groups")


def _tables(config):
    return (
        config.HBASE_TABLE_GROUPS,
        config.HBASE_TABLE_GROUP_MEMBERS,
        config.HBASE_TABLE_USER_GROUPS,
    )


def _is_owner(hbase, groups_table, group_id, username, role):
    if role == "admin":
        return True
    g_info = hbase.get_group(groups_table, group_id)
    return g_info is not None and g_info.get("owner") == username


@group_bp.route("", methods=["POST"])
@login_required
def create_group():
    """创建群组（创建者自动成为群主）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    body = request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    description = (body.get("description") or "").strip()
    if not name:
        return jsonify({"error": "群组名称不能为空"}), 400

    groups_t, members_t, ug_t = _tables(config)
    info = hbase.create_group(groups_t, members_t, ug_t, name, g.current_user, description)
    hbase.add_log(config.HBASE_TABLE_LOGS, g.current_user, "group_create", info["group_id"])
    return jsonify(info), 201


@group_bp.route("", methods=["GET"])
@login_required
def list_my_groups():
    """我加入的群组（admin 加 ?all=1 查看全部）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    groups_t, _, ug_t = _tables(config)

    if g.current_role == "admin" and request.args.get("all") == "1":
        return jsonify({"groups": hbase.list_all_groups(groups_t)})
    return jsonify({"groups": hbase.list_user_groups(ug_t, groups_t, g.current_user)})


@group_bp.route("/<group_id>", methods=["GET"])
@login_required
def get_group_detail(group_id):
    """群组详情 + 成员列表（仅成员或 admin 可看）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    groups_t, members_t, ug_t = _tables(config)

    info = hbase.get_group(groups_t, group_id)
    if not info:
        return jsonify({"error": "群组不存在"}), 404

    my_gids = set(hbase.list_user_group_ids(ug_t, g.current_user))
    if g.current_role != "admin" and group_id not in my_gids:
        return jsonify({"error": "无权查看此群组"}), 403

    info["members"] = hbase.list_group_members(members_t, group_id)
    return jsonify(info)


@group_bp.route("/<group_id>", methods=["DELETE"])
@login_required
def delete_group(group_id):
    """解散群组（仅群主或 admin）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    groups_t, members_t, ug_t = _tables(config)

    info = hbase.get_group(groups_t, group_id)
    if not info:
        return jsonify({"error": "群组不存在"}), 404
    if not _is_owner(hbase, groups_t, group_id, g.current_user, g.current_role):
        return jsonify({"error": "仅群主可解散群组"}), 403

    hbase.delete_group(groups_t, members_t, ug_t, group_id)
    hbase.add_log(config.HBASE_TABLE_LOGS, g.current_user, "group_delete", group_id)
    return jsonify({"message": "群组已解散"})


@group_bp.route("/<group_id>/members", methods=["POST"])
@login_required
def add_member(group_id):
    """添加成员（仅群主或 admin）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    groups_t, members_t, ug_t = _tables(config)

    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    if not username:
        return jsonify({"error": "用户名不能为空"}), 400

    if not _is_owner(hbase, groups_t, group_id, g.current_user, g.current_role):
        return jsonify({"error": "仅群主可添加成员"}), 403

    user = hbase.get_user(config.HBASE_TABLE_USERS, username)
    if not user:
        return jsonify({"error": "用户不存在"}), 404

    if not hbase.add_group_member(groups_t, members_t, ug_t, group_id, username):
        return jsonify({"error": "该用户已是群组成员"}), 400

    hbase.add_log(config.HBASE_TABLE_LOGS, g.current_user, "group_add_member", f"{group_id}:{username}")
    return jsonify({"message": "成员已添加"}), 201


@group_bp.route("/<group_id>/members/<username>", methods=["DELETE"])
@login_required
def remove_member(group_id, username):
    """移除成员：群主可移除任何人；普通成员只能移除自己（退出）"""
    config = current_app.config["APP_CONFIG"]
    hbase = current_app.config["HBASE_SERVICE"]
    groups_t, members_t, ug_t = _tables(config)

    info = hbase.get_group(groups_t, group_id)
    if not info:
        return jsonify({"error": "群组不存在"}), 404

    is_owner = _is_owner(hbase, groups_t, group_id, g.current_user, g.current_role)
    is_self = (username == g.current_user)
    if not (is_owner or is_self):
        return jsonify({"error": "无权执行此操作"}), 403
    if username == info.get("owner"):
        return jsonify({"error": "群主不能被移除，请解散群组"}), 400

    if not hbase.remove_group_member(groups_t, members_t, ug_t, group_id, username):
        return jsonify({"error": "该用户不是群组成员"}), 404

    action = "group_leave" if is_self else "group_remove_member"
    hbase.add_log(config.HBASE_TABLE_LOGS, g.current_user, action, f"{group_id}:{username}")
    return jsonify({"message": "已移除成员"})
