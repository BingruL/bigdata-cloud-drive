"""
JWT Token 认证模块
实现 Token 的生成、验证、刷新
对应课程第 10 章：身份认证与访问控制
"""
import jwt
import time
import hashlib
import logging
from functools import wraps
from flask import request, jsonify, g

logger = logging.getLogger(__name__)


class JWTHandler:
    """JWT Token 管理器"""

    def __init__(self, secret, expiration_hours=24):
        self.secret = secret
        self.expiration_hours = expiration_hours

    def generate_token(self, username, role="user"):
        """
        生成 JWT Token
        payload 包含: username, role, iat(签发时间), exp(过期时间)
        """
        now = int(time.time())
        payload = {
            "username": username,
            "role": role,
            "iat": now,
            "exp": now + self.expiration_hours * 3600,
        }
        token = jwt.encode(payload, self.secret, algorithm="HS256")
        return token

    def verify_token(self, token):
        """
        验证 Token 的有效性
        返回 payload 或 None
        """
        try:
            payload = jwt.decode(token, self.secret, algorithms=["HS256"], leeway=5)
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("Token 已过期")
            return None
        except jwt.InvalidTokenError as e:
            logger.warning(f"Token 无效: {e}")
            return None

    def refresh_token(self, token):
        """
        刷新 Token（延长有效期）
        """
        payload = self.verify_token(token)
        if payload:
            return self.generate_token(payload["username"], payload["role"])
        return None


def hash_password(password):
    """
    密码哈希（使用 SHA-256 + 盐）
    生产环境建议使用 bcrypt
    """
    salt = "bigdata-cloud-drive-salt"
    return hashlib.sha256(f"{password}{salt}".encode()).hexdigest()


def verify_password(password, password_hash):
    """验证密码"""
    return hash_password(password) == password_hash


# ========== Flask 装饰器 ==========

def login_required(f):
    """
    登录验证装饰器
    从 Authorization Header 中提取并验证 Token
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth_header = request.headers.get("Authorization", "")

        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

        if not token:
            return jsonify({"error": "缺少认证 Token", "code": 401}), 401

        # 从 app 上下文获取 jwt_handler
        from flask import current_app
        jwt_handler = current_app.config.get("JWT_HANDLER")
        if not jwt_handler:
            return jsonify({"error": "服务器认证配置错误", "code": 500}), 500

        payload = jwt_handler.verify_token(token)
        if not payload:
            return jsonify({"error": "Token 无效或已过期", "code": 401}), 401

        # 将用户信息存入 g 对象
        g.current_user = payload["username"]
        g.current_role = payload.get("role", "user")
        return f(*args, **kwargs)

    return decorated


def admin_required(f):
    """管理员权限装饰器"""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

        if not token:
            return jsonify({"error": "缺少认证 Token", "code": 401}), 401

        from flask import current_app
        jwt_handler = current_app.config.get("JWT_HANDLER")
        payload = jwt_handler.verify_token(token)
        if not payload:
            return jsonify({"error": "Token 无效或已过期", "code": 401}), 401

        if payload.get("role") != "admin":
            return jsonify({"error": "需要管理员权限", "code": 403}), 403

        g.current_user = payload["username"]
        g.current_role = payload["role"]
        return f(*args, **kwargs)

    return decorated
