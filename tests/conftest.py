"""
pytest 配置 —— 通过 monkeypatch 在 create_app 之前替换真实服务为内存版假实现。
"""
import os
import sys
import pytest

# 项目根加入路径，方便 import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.fakes import FakeHBaseService, FakeHDFSService


@pytest.fixture
def fake_hbase():
    return FakeHBaseService()


@pytest.fixture
def fake_hdfs():
    return FakeHDFSService()


@pytest.fixture
def app(monkeypatch, fake_hbase, fake_hdfs):
    """构造一个完全用内存假后端的 Flask app。
    在 create_app 内部 import 之前就 patch 类，使 init_tables / init_directories 不会真连。
    """
    # 强制关闭 Kafka，防止 EventBus 试图连
    monkeypatch.setenv("KAFKA_ENABLED", "0")

    # 替换 backend.app 命名空间内的服务类
    import backend.app as app_module
    monkeypatch.setattr(app_module, "HBaseService", lambda *a, **kw: fake_hbase)
    monkeypatch.setattr(app_module, "HDFSService", lambda *a, **kw: fake_hdfs)

    flask_app = app_module.create_app()
    flask_app.config["TESTING"] = True
    return flask_app


@pytest.fixture
def client(app):
    return app.test_client()


# ===== 辅助：注册并登录用户，返回 token =====

def _register(client, username, password="123456"):
    return client.post("/api/auth/register",
                       json={"username": username, "password": password})


def _login(client, username, password="123456"):
    rv = client.post("/api/auth/login",
                     json={"username": username, "password": password})
    return rv.get_json().get("token") if rv.status_code == 200 else None


@pytest.fixture
def make_user(client):
    """用例工厂：创建一个用户并登录，返回 (username, token, headers)"""
    def _make(username, password="123456"):
        _register(client, username, password)
        token = _login(client, username, password)
        assert token, f"login failed for {username}"
        return username, token, {"Authorization": f"Bearer {token}"}
    return _make


@pytest.fixture
def alice(make_user):
    return make_user("alice")


@pytest.fixture
def bob(make_user):
    return make_user("bob")


@pytest.fixture
def admin(client, app, make_user):
    """admin 账号需要 role=admin —— 直接通过 fake_hbase 注入"""
    fake = app.config["HBASE_SERVICE"]
    config = app.config["APP_CONFIG"]
    from backend.auth.jwt_handler import hash_password
    fake.create_user(config.HBASE_TABLE_USERS, "admin", hash_password("admin123"), role="admin")
    rv = client.post("/api/auth/login", json={"username": "admin", "password": "admin123"})
    token = rv.get_json()["token"]
    return "admin", token, {"Authorization": f"Bearer {token}"}
