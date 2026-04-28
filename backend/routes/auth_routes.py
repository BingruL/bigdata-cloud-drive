"""
认证路由
注册、登录、Token 刷新、用户信息
"""
import re
from flask import Blueprint, request, jsonify, g, current_app
from ..auth.jwt_handler import hash_password, verify_password, login_required

auth_bp = Blueprint("auth", __name__, url_prefix="/api/auth")

# 用户名白名单：英文字母、数字、下划线、短横线，3-20 位
# 用户名会进入 HDFS 路径段和 HBase 复合 RowKey（如 {gid}#{username}），
# 因此禁止 / # 空格 .. 等会破坏路径或 split 解析的字符。
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{3,20}$")


@auth_bp.route("/register", methods=["POST"])
def register():
    """用户注册"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "请求体不能为空"}), 400

    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"error": "用户名和密码不能为空"}), 400
    if not USERNAME_PATTERN.match(username):
        return jsonify({"error": "用户名只能包含字母、数字、下划线和短横线，长度 3-20"}), 400
    if len(password) < 6:
        return jsonify({"error": "密码至少 6 个字符"}), 400

    hbase = current_app.config["HBASE_SERVICE"]
    table = current_app.config["APP_CONFIG"].HBASE_TABLE_USERS

    password_hash = hash_password(password)
    result = hbase.create_user(table, username, password_hash, role="user")

    if not result:
        return jsonify({"error": "用户名已存在"}), 409

    # 记录日志
    current_app.config["EVENT_BUS"].log(username, "register", "新用户注册")

    return jsonify({"message": "注册成功", "user": result}), 201


@auth_bp.route("/login", methods=["POST"])
def login():
    """用户登录"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "请求体不能为空"}), 400

    username = data.get("username", "").strip()
    password = data.get("password", "").strip()

    if not username or not password:
        return jsonify({"error": "用户名和密码不能为空"}), 400

    hbase = current_app.config["HBASE_SERVICE"]
    table = current_app.config["APP_CONFIG"].HBASE_TABLE_USERS

    user = hbase.get_user(table, username)
    if not user:
        return jsonify({"error": "用户不存在"}), 404

    if user["status"] != "active":
        return jsonify({"error": "账户已被禁用"}), 403

    if not verify_password(password, user["password"]):
        return jsonify({"error": "密码错误"}), 401

    # 生成 Token
    jwt_handler = current_app.config["JWT_HANDLER"]
    token = jwt_handler.generate_token(username, user["role"])

    # 记录日志
    current_app.config["EVENT_BUS"].log(username, "login", "用户登录")

    return jsonify({
        "message": "登录成功",
        "token": token,
        "user": {
            "username": user["username"],
            "role": user["role"],
        },
    })


@auth_bp.route("/refresh", methods=["POST"])
@login_required
def refresh_token():
    """刷新 Token"""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header[7:] if auth_header.startswith("Bearer ") else ""

    jwt_handler = current_app.config["JWT_HANDLER"]
    new_token = jwt_handler.refresh_token(token)

    if not new_token:
        return jsonify({"error": "Token 刷新失败"}), 401

    return jsonify({"token": new_token})


@auth_bp.route("/me", methods=["GET"])
@login_required
def get_current_user():
    """获取当前用户信息"""
    hbase = current_app.config["HBASE_SERVICE"]
    table = current_app.config["APP_CONFIG"].HBASE_TABLE_USERS

    user = hbase.get_user(table, g.current_user)
    if not user:
        return jsonify({"error": "用户不存在"}), 404

    return jsonify({
        "username": user["username"],
        "role": user["role"],
        "created_at": user["created_at"],
    })
