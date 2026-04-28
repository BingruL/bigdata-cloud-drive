"""Regression tests for issues raised in docs/functional_correctness_review.md."""
import io


def _upload(client, headers, filename="hello.txt", content=b"hello world"):
    return client.post(
        "/api/files/upload",
        headers=headers,
        data={"file": (io.BytesIO(content), filename)},
        content_type="multipart/form-data",
    )


def _create_group(client, headers, name="g1"):
    rv = client.post("/api/groups", headers=headers, json={"name": name})
    return rv.get_json()["group_id"]


# ===== Fix 1: 软删除文件不可被详情/下载/预览 =====

def test_trashed_file_info_returns_404(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "doomed.txt", b"x")
    fid = rv.get_json()["file"]["file_id"]
    client.delete(f"/api/files/{fid}", headers=h)

    assert client.get(f"/api/files/{fid}", headers=h).status_code == 404


def test_trashed_file_download_returns_404(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "doomed.txt", b"x")
    fid = rv.get_json()["file"]["file_id"]
    client.delete(f"/api/files/{fid}", headers=h)

    assert client.get(f"/api/files/{fid}/download", headers=h).status_code == 404


def test_trashed_file_preview_returns_404(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "doomed.txt", b"hi")
    fid = rv.get_json()["file"]["file_id"]
    client.delete(f"/api/files/{fid}", headers=h)

    assert client.get(f"/api/files/{fid}/preview", headers=h).status_code == 404


def test_admin_cannot_download_trashed_file(client, alice, admin):
    _, _, ah = alice
    _, _, adh = admin
    rv = _upload(client, ah, "doomed.txt", b"x")
    fid = rv.get_json()["file"]["file_id"]
    client.delete(f"/api/files/{fid}", headers=ah)

    assert client.get(f"/api/files/{fid}/download", headers=adh).status_code == 404


# ===== Fix 2: 预览成功应被记录到最近访问 =====

def test_preview_appears_in_recent(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "note.txt", b"hello")
    fid = rv.get_json()["file"]["file_id"]

    assert client.get(f"/api/files/{fid}/preview", headers=h).status_code == 200

    recent = client.get("/api/files/recent", headers=h).get_json()["files"]
    assert any(f["file_id"] == fid for f in recent)


# ===== Fix 3: 搜索时间范围过滤应在分页前执行 =====

def test_search_date_filter_applied_before_pagination(client, alice):
    _, _, h = alice
    config = client.application.config["APP_CONFIG"]
    hbase = client.application.config["HBASE_SERVICE"]

    # 上传 5 个文件，手动改 created_at 让它们分布在三个时间点
    fids = []
    for i in range(5):
        rv = _upload(client, h, f"f{i}.txt", b"x")
        fids.append(rv.get_json()["file"]["file_id"])

    # 前 3 个时间戳=1000，后 2 个=5000
    table = hbase._t(config.HBASE_TABLE_FILES)
    for fid in fids[:3]:
        table[fid]["created_at"] = "1000"
    for fid in fids[3:]:
        table[fid]["created_at"] = "5000"

    # 用 page_size=2 + start_date=4000：若先分页再过滤，可能返回 0 条
    rv = client.get(
        "/api/files/search?start_date=4000&page=1&page_size=2", headers=h,
    )
    assert rv.status_code == 200
    payload = rv.get_json()
    assert payload["total"] == 2  # 仅 2 个 created_at >= 4000
    assert len(payload["files"]) == 2


def test_search_invalid_int_returns_400(client, alice):
    _, _, h = alice
    rv = client.get("/api/files/search?page=abc", headers=h)
    assert rv.status_code == 400


# ===== Fix 4: 群组推荐候选池应排除自己上传的共享文件 =====

def test_group_hot_excludes_own_files(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob

    gid = _create_group(client, ah, "team")
    client.post(f"/api/groups/{gid}/members",
                headers=ah, json={"username": "bob"})

    # alice 自己上传并分享到群组
    rv = _upload(client, ah, "mine.txt", b"x")
    fid = rv.get_json()["file"]["file_id"]
    client.post(f"/api/files/{fid}/share", headers=ah, json={"groups": [gid]})

    # alice 视角不应在"群组热门"中看到自己上传的文件
    rv = client.get("/api/ai/recommend/hot", headers=ah)
    if rv.status_code == 503:
        return  # AI 服务未配置，跳过
    items = rv.get_json().get("items", [])
    assert all(item.get("file_id") != fid for item in items)


# ===== Fix 5: admin 添加成员到不存在群组应 404，且不写孤儿索引 =====

def test_admin_add_member_to_nonexistent_group_returns_404(client, admin, alice):
    _, _, adh = admin
    # alice 必须存在
    rv = client.post(
        "/api/groups/does-not-exist/members",
        headers=adh, json={"username": "alice"},
    )
    assert rv.status_code == 404

    # 索引表中不应出现孤儿成员关系
    config = client.application.config["APP_CONFIG"]
    hbase = client.application.config["HBASE_SERVICE"]
    members = hbase._t(config.HBASE_TABLE_GROUP_MEMBERS)
    user_groups = hbase._t(config.HBASE_TABLE_USER_GROUPS)
    assert "does-not-exist#alice" not in members
    assert "alice#does-not-exist" not in user_groups


# ===== Fix 6: 非法 int 参数应返回 400 =====

def test_invalid_page_returns_400(client, alice):
    _, _, h = alice
    assert client.get("/api/files/list?page=abc", headers=h).status_code == 400


def test_invalid_top_returns_400(client, alice):
    _, _, h = alice
    assert client.get("/api/stats/hot-files?top=xx", headers=h).status_code == 400


def test_negative_days_returns_400(client, alice):
    _, _, h = alice
    rv = client.get("/api/stats/daily-upload-trend?days=-1", headers=h)
    assert rv.status_code == 400


# ===== Fix 7 (本轮): 用户名白名单 =====

def test_register_rejects_slash_in_username(client):
    rv = client.post("/api/auth/register",
                     json={"username": "alice/bob", "password": "123456"})
    assert rv.status_code == 400


def test_register_rejects_hash_in_username(client):
    rv = client.post("/api/auth/register",
                     json={"username": "alice#bob", "password": "123456"})
    assert rv.status_code == 400


def test_register_rejects_space_in_username(client):
    rv = client.post("/api/auth/register",
                     json={"username": "alice bob", "password": "123456"})
    assert rv.status_code == 400


def test_register_rejects_dotdot_username(client):
    rv = client.post("/api/auth/register",
                     json={"username": "..", "password": "123456"})
    assert rv.status_code == 400


def test_register_accepts_valid_username(client):
    rv = client.post("/api/auth/register",
                     json={"username": "user_1-2", "password": "123456"})
    assert rv.status_code == 201


# ===== Fix 8 (本轮): 看板按用户 scope =====

def test_dashboard_scoped_to_self_for_normal_user(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    _upload(client, ah, "a1.txt", b"x")
    _upload(client, ah, "a2.txt", b"x")
    _upload(client, bh, "b1.txt", b"x")

    rv = client.get("/api/stats/dashboard", headers=ah).get_json()
    assert rv["total_files"] == 2
    assert rv["total_users"] == 1


def test_dashboard_admin_sees_all(client, alice, bob, admin):
    _, _, ah = alice
    _, _, bh = bob
    _, _, adh = admin
    _upload(client, ah, "a.txt", b"x")
    _upload(client, bh, "b.txt", b"x")

    rv = client.get("/api/stats/dashboard", headers=adh).get_json()
    assert rv["total_files"] == 2
    assert rv["total_users"] == 2


def test_hot_files_hides_others_for_normal_user(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    rv = _upload(client, bh, "bobs-secret.txt", b"x")
    bob_fid = rv.get_json()["file"]["file_id"]

    items = client.get("/api/stats/hot-files", headers=ah).get_json()
    assert all(f.get("file_id") != bob_fid for f in items)


def test_recent_activity_hides_others_for_normal_user(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    _upload(client, bh, "b.txt", b"x")  # bob 操作

    items = client.get("/api/stats/recent-activity", headers=ah).get_json()
    assert all(item["username"] == "alice" for item in items)


def test_user_file_counts_hides_others_for_normal_user(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    _upload(client, ah, "a.txt", b"x")
    _upload(client, bh, "b.txt", b"x")

    rows = client.get("/api/stats/user-file-counts", headers=ah).get_json()
    assert len(rows) == 1
    assert rows[0]["username"] == "alice"


# ===== Fix 9 (本轮): 解散群组级联清理 shared_groups =====

def test_delete_group_clears_shared_groups(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob

    gid = _create_group(client, ah, "team")
    client.post(f"/api/groups/{gid}/members",
                headers=ah, json={"username": "bob"})

    rv = _upload(client, ah, "doc.txt", b"x")
    fid = rv.get_json()["file"]["file_id"]
    client.post(f"/api/files/{fid}/share", headers=ah, json={"groups": [gid]})

    # 解散前确认是 shared
    info = client.get(f"/api/files/{fid}", headers=ah).get_json()
    assert info["is_shared"] == "1"
    assert gid in info["shared_groups"]

    # 解散群组
    rv = client.delete(f"/api/groups/{gid}", headers=ah)
    assert rv.status_code == 200
    assert rv.get_json()["files_unshared"] == 1

    # 解散后文件应回落为私有
    info = client.get(f"/api/files/{fid}", headers=ah).get_json()
    assert info["is_shared"] == "0"
    assert gid not in (info.get("shared_groups") or "")


# ===== Fix 10 (本轮): /api/health 探测依赖 =====

def test_health_reports_dependencies(client):
    rv = client.get("/api/health")
    payload = rv.get_json()
    assert "dependencies" in payload
    assert "hbase" in payload["dependencies"]
    assert "hdfs" in payload["dependencies"]


def test_health_returns_503_when_dependency_down(client, app):
    # 模拟 HBase 故障：让 ping 抛异常
    hbase = app.config["HBASE_SERVICE"]
    original = hbase.ping

    def _boom():
        raise RuntimeError("thrift down")

    hbase.ping = _boom
    try:
        rv = client.get("/api/health")
        assert rv.status_code == 503
        payload = rv.get_json()
        assert payload["status"] == "degraded"
        assert payload["dependencies"]["hbase"].startswith("down")
        assert payload["dependencies"]["hdfs"] == "ok"
    finally:
        hbase.ping = original
