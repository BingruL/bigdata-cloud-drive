"""统计接口的最小集成测试 —— 只验证 happy path 和响应形态"""
import io


def _upload(client, headers, filename, content=b"x"):
    return client.post("/api/files/upload", headers=headers,
                       data={"file": (io.BytesIO(content), filename)},
                       content_type="multipart/form-data")


def test_dashboard_summary_basic_shape(client, alice):
    _, _, h = alice
    _upload(client, h, "a.txt")
    _upload(client, h, "b.pdf")

    rv = client.get("/api/stats/dashboard", headers=h)
    assert rv.status_code == 200
    body = rv.get_json()
    # dashboard 包含 total_files / total_size / total_users 等关键字段
    assert "total_files" in body
    assert body["total_files"] >= 2


def test_user_file_counts_endpoint(client, alice):
    _, _, h = alice
    _upload(client, h, "x.txt")
    rv = client.get("/api/stats/user-file-counts", headers=h)
    assert rv.status_code == 200
    rows = rv.get_json()
    assert isinstance(rows, list)
    assert any(r["username"] == "alice" for r in rows)


def test_file_type_distribution(client, alice):
    _, _, h = alice
    _upload(client, h, "a.txt")
    _upload(client, h, "b.txt")
    _upload(client, h, "c.pdf")
    rv = client.get("/api/stats/file-type-distribution", headers=h)
    assert rv.status_code == 200
    by_type = {r["type"]: r["count"] for r in rv.get_json()}
    assert by_type.get("txt") == 2
    assert by_type.get("pdf") == 1


def test_my_storage_quota(client, alice):
    _, _, h = alice
    _upload(client, h, "data.txt", content=b"X" * 1024)

    rv = client.get("/api/stats/my-storage", headers=h)
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["used"] >= 1024
    assert body["quota"] > 0
    assert body["active_count"] == 1


def test_recent_activity(client, alice):
    _, _, h = alice
    _upload(client, h, "tracked.txt")
    rv = client.get("/api/stats/recent-activity?limit=5", headers=h)
    assert rv.status_code == 200
    items = rv.get_json()
    # 至少应该有 register / login / upload 三条
    actions = {it.get("action") for it in items}
    assert "upload" in actions


def test_stats_require_auth(client):
    for path in ["/api/stats/dashboard", "/api/stats/user-file-counts",
                 "/api/stats/my-storage"]:
        assert client.get(path).status_code == 401
