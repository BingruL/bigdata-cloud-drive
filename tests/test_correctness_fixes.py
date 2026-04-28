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
