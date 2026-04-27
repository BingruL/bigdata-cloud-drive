"""文件分享集成测试 —— 私有 / 群组共享 / 跨用户访问权限"""
import io


def _upload(client, headers, filename="x.txt", content=b"data"):
    return client.post("/api/files/upload", headers=headers,
                       data={"file": (io.BytesIO(content), filename)},
                       content_type="multipart/form-data")


def _make_group(client, owner_h, member_user, member_h):
    """alice 建群、加 bob、返回 group_id"""
    gid = client.post("/api/groups", headers=owner_h,
                      json={"name": "team"}).get_json()["group_id"]
    client.post(f"/api/groups/{gid}/members", headers=owner_h,
                json={"username": member_user})
    return gid


# ===== 默认私有 =====

def test_uploaded_file_is_private_by_default(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    rv = _upload(client, ah, "private.txt", b"alice-only")
    fid = rv.get_json()["file"]["file_id"]

    # bob 看不到
    rv2 = client.get(f"/api/files/{fid}/download", headers=bh)
    assert rv2.status_code == 403


# ===== 共享后可见 =====

def test_share_to_group_makes_file_visible_to_members(client, alice, bob):
    _, _, ah = alice
    bob_user, _, bh = bob
    gid = _make_group(client, ah, bob_user, bh)

    fid = _upload(client, ah, "shared.txt", b"hello-team").get_json()["file"]["file_id"]
    rv = client.post(f"/api/files/{fid}/share", headers=ah,
                     json={"groups": [gid]})
    assert rv.status_code == 200

    # bob 现在能下载
    rv2 = client.get(f"/api/files/{fid}/download", headers=bh)
    assert rv2.status_code == 200
    assert rv2.data == b"hello-team"

    # /api/files/shared 应能列出该文件
    shared = client.get("/api/files/shared", headers=bh).get_json()["files"]
    assert any(f["file_id"] == fid for f in shared)


def test_share_to_group_user_not_in_returns_403(client, alice, bob, make_user):
    """alice 试图把文件分享到 bob 自建的群（alice 不在其中）"""
    _, _, ah = alice
    bob_user, _, bh = bob
    bob_only_gid = client.post("/api/groups", headers=bh,
                               json={"name": "bob-private"}).get_json()["group_id"]

    fid = _upload(client, ah, "x.txt").get_json()["file"]["file_id"]
    rv = client.post(f"/api/files/{fid}/share", headers=ah,
                     json={"groups": [bob_only_gid]})
    assert rv.status_code == 403


def test_share_with_empty_groups_rejected(client, alice):
    _, _, h = alice
    fid = _upload(client, h).get_json()["file"]["file_id"]
    rv = client.post(f"/api/files/{fid}/share", headers=h, json={"groups": []})
    assert rv.status_code == 400


def test_only_owner_can_share(client, alice, bob):
    """bob 不能分享 alice 的文件"""
    _, _, ah = alice
    bob_user, _, bh = bob
    gid = _make_group(client, ah, bob_user, bh)

    fid = _upload(client, ah).get_json()["file"]["file_id"]
    rv = client.post(f"/api/files/{fid}/share", headers=bh,
                     json={"groups": [gid]})
    assert rv.status_code == 403


# ===== 取消分享 =====

def test_unshare_makes_file_private_again(client, alice, bob):
    _, _, ah = alice
    bob_user, _, bh = bob
    gid = _make_group(client, ah, bob_user, bh)

    fid = _upload(client, ah, "toggle.txt").get_json()["file"]["file_id"]
    client.post(f"/api/files/{fid}/share", headers=ah, json={"groups": [gid]})

    # 取消分享
    rv = client.post(f"/api/files/{fid}/unshare", headers=ah)
    assert rv.status_code == 200

    # bob 又看不到
    rv2 = client.get(f"/api/files/{fid}/download", headers=bh)
    assert rv2.status_code == 403


# ===== /api/files/shared 视角 =====

def test_shared_endpoint_excludes_my_own_files(client, alice, bob):
    _, _, ah = alice
    bob_user, _, bh = bob
    gid = _make_group(client, ah, bob_user, bh)
    fid = _upload(client, ah, "my-own.txt").get_json()["file"]["file_id"]
    client.post(f"/api/files/{fid}/share", headers=ah, json={"groups": [gid]})

    shared = client.get("/api/files/shared", headers=ah).get_json()["files"]
    assert all(f["file_id"] != fid for f in shared), \
        "/api/files/shared 不应包含自己上传的文件"


def test_shared_endpoint_empty_when_no_groups(client, alice):
    _, _, h = alice
    rv = client.get("/api/files/shared", headers=h)
    assert rv.status_code == 200
    assert rv.get_json()["files"] == []
