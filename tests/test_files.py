"""文件管理集成测试 —— 上传 / 列表 / 下载 / 软删除 / 恢复 / 彻底删除 / 搜索"""
import io


def _upload(client, headers, filename="hello.txt", content=b"hello world"):
    return client.post(
        "/api/files/upload",
        headers=headers,
        data={"file": (io.BytesIO(content), filename)},
        content_type="multipart/form-data",
    )


# ===== 上传 =====

def test_upload_requires_auth(client):
    rv = client.post("/api/files/upload",
                     data={"file": (io.BytesIO(b"x"), "x.txt")},
                     content_type="multipart/form-data")
    assert rv.status_code == 401


def test_upload_no_file(client, alice):
    _, _, headers = alice
    rv = client.post("/api/files/upload", headers=headers,
                     content_type="multipart/form-data")
    assert rv.status_code == 400


def test_upload_success_returns_file_meta(client, alice):
    _, _, headers = alice
    rv = _upload(client, headers, "report.pdf", b"PDF-DATA-1234")
    assert rv.status_code == 201
    data = rv.get_json()["file"]
    assert data["filename"] == "report.pdf"
    assert data["type"] == "pdf"
    assert data["size"] == len(b"PDF-DATA-1234")


# ===== 列表 / 搜索 =====

def test_list_files_only_returns_own(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    _upload(client, ah, "a1.txt")
    _upload(client, ah, "a2.txt")
    _upload(client, bh, "b1.txt")

    rv = client.get("/api/files/list", headers=ah)
    assert rv.status_code == 200
    files = rv.get_json()["files"]
    names = sorted(f["filename"] for f in files)
    assert names == ["a1.txt", "a2.txt"]


def test_search_by_keyword(client, alice):
    _, _, h = alice
    _upload(client, h, "report-2025.pdf")
    _upload(client, h, "data.csv")

    rv = client.get("/api/files/search?keyword=report", headers=h)
    assert rv.status_code == 200
    files = rv.get_json()["files"]
    assert len(files) == 1
    assert files[0]["filename"] == "report-2025.pdf"


# ===== 下载与权限 =====

def test_owner_can_download(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "data.txt", b"abcdef")
    fid = rv.get_json()["file"]["file_id"]
    rv2 = client.get(f"/api/files/{fid}/download", headers=h)
    assert rv2.status_code == 200
    assert rv2.data == b"abcdef"


def test_other_user_cannot_download_private_file(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    rv = _upload(client, ah, "secret.txt", b"top-secret")
    fid = rv.get_json()["file"]["file_id"]

    rv2 = client.get(f"/api/files/{fid}/download", headers=bh)
    assert rv2.status_code == 403


def test_admin_can_download_anyones_file(client, alice, admin):
    _, _, ah = alice
    _, _, adh = admin
    rv = _upload(client, ah, "alice-file.txt", b"alice-content")
    fid = rv.get_json()["file"]["file_id"]

    rv2 = client.get(f"/api/files/{fid}/download", headers=adh)
    assert rv2.status_code == 200
    assert rv2.data == b"alice-content"


def test_download_nonexistent_file_returns_404(client, alice):
    _, _, h = alice
    rv = client.get("/api/files/nonexistent-id/download", headers=h)
    assert rv.status_code == 404


# ===== 软删除 / 回收站 / 恢复 / 彻底删除 =====

def test_soft_delete_moves_to_trash(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "doomed.txt")
    fid = rv.get_json()["file"]["file_id"]

    rv2 = client.delete(f"/api/files/{fid}", headers=h)
    assert rv2.status_code == 200

    # 主列表里看不到
    rv3 = client.get("/api/files/list", headers=h)
    assert all(f["file_id"] != fid for f in rv3.get_json()["files"])

    # 回收站里能看到
    rv4 = client.get("/api/files/trash", headers=h)
    trash_ids = [f["file_id"] for f in rv4.get_json()["files"]]
    assert fid in trash_ids


def test_restore_file_back_to_list(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "phoenix.txt")
    fid = rv.get_json()["file"]["file_id"]
    client.delete(f"/api/files/{fid}", headers=h)

    rv2 = client.post(f"/api/files/{fid}/restore", headers=h)
    assert rv2.status_code == 200

    # 主列表能看到，回收站看不到
    files = client.get("/api/files/list", headers=h).get_json()["files"]
    assert any(f["file_id"] == fid for f in files)
    trash = client.get("/api/files/trash", headers=h).get_json()["files"]
    assert all(f["file_id"] != fid for f in trash)


def test_purge_requires_being_in_trash(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "alive.txt")
    fid = rv.get_json()["file"]["file_id"]

    # 直接彻底删除一个未在回收站的文件应被拒绝
    rv2 = client.delete(f"/api/files/{fid}/purge", headers=h)
    assert rv2.status_code == 400


def test_purge_after_soft_delete_removes_completely(client, alice):
    _, _, h = alice
    rv = _upload(client, h, "kill-me.txt")
    fid = rv.get_json()["file"]["file_id"]
    client.delete(f"/api/files/{fid}", headers=h)

    rv2 = client.delete(f"/api/files/{fid}/purge", headers=h)
    assert rv2.status_code == 200

    # 文件详情应 404
    rv3 = client.get(f"/api/files/{fid}", headers=h)
    assert rv3.status_code == 404


def test_other_user_cannot_delete(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    rv = _upload(client, ah, "alice.txt")
    fid = rv.get_json()["file"]["file_id"]

    rv2 = client.delete(f"/api/files/{fid}", headers=bh)
    assert rv2.status_code in (403, 404)
