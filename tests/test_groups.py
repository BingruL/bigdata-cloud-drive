"""群组管理集成测试 —— 创建 / 成员管理 / 解散 / 权限"""


def _create_group(client, headers, name="my-group", description=""):
    return client.post("/api/groups", headers=headers,
                       json={"name": name, "description": description})


# ===== 创建 =====

def test_create_group_requires_auth(client):
    rv = client.post("/api/groups", json={"name": "x"})
    assert rv.status_code == 401


def test_create_group_empty_name_rejected(client, alice):
    _, _, h = alice
    rv = _create_group(client, h, name="")
    assert rv.status_code == 400


def test_create_group_makes_creator_owner(client, alice):
    user, _, h = alice
    rv = _create_group(client, h, name="alice-team")
    assert rv.status_code == 201
    body = rv.get_json()
    assert body["owner"] == user
    assert body["member_count"] == 1


# ===== 成员管理 =====

def test_owner_can_add_member(client, alice, bob):
    _, _, ah = alice
    bob_user, _, _ = bob
    gid = _create_group(client, ah, name="g1").get_json()["group_id"]

    rv = client.post(f"/api/groups/{gid}/members", headers=ah,
                     json={"username": bob_user})
    assert rv.status_code == 201

    detail = client.get(f"/api/groups/{gid}", headers=ah).get_json()
    members = sorted(m["username"] for m in detail["members"])
    assert members == ["alice", "bob"]
    assert detail["member_count"] == 2


def test_non_owner_cannot_add_member(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    gid = _create_group(client, ah, name="closed").get_json()["group_id"]

    rv = client.post(f"/api/groups/{gid}/members", headers=bh,
                     json={"username": "alice"})
    assert rv.status_code == 403


def test_add_unknown_user_returns_404(client, alice):
    _, _, h = alice
    gid = _create_group(client, h).get_json()["group_id"]
    rv = client.post(f"/api/groups/{gid}/members", headers=h,
                     json={"username": "ghost-user"})
    assert rv.status_code == 404


def test_add_duplicate_member_rejected(client, alice, bob):
    _, _, ah = alice
    bob_user, _, _ = bob
    gid = _create_group(client, ah).get_json()["group_id"]
    client.post(f"/api/groups/{gid}/members", headers=ah, json={"username": bob_user})

    rv = client.post(f"/api/groups/{gid}/members", headers=ah, json={"username": bob_user})
    assert rv.status_code == 400


# ===== 列表 / 详情 =====

def test_list_my_groups_only_returns_joined(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    _create_group(client, ah, name="alice-only")
    _create_group(client, bh, name="bob-only")

    a_groups = client.get("/api/groups", headers=ah).get_json()["groups"]
    assert sorted(g["name"] for g in a_groups) == ["alice-only"]


def test_admin_can_see_all_groups_with_query(client, alice, bob, admin):
    _, _, ah = alice
    _, _, bh = bob
    _, _, adh = admin
    _create_group(client, ah, name="g-a")
    _create_group(client, bh, name="g-b")

    rv = client.get("/api/groups?all=1", headers=adh)
    names = sorted(g["name"] for g in rv.get_json()["groups"])
    assert names == ["g-a", "g-b"]


def test_non_member_cannot_see_group_detail(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    gid = _create_group(client, ah, name="private").get_json()["group_id"]

    rv = client.get(f"/api/groups/{gid}", headers=bh)
    assert rv.status_code == 403


# ===== 退出 / 移除 =====

def test_member_can_leave_group(client, alice, bob):
    _, _, ah = alice
    bob_user, _, bh = bob
    gid = _create_group(client, ah).get_json()["group_id"]
    client.post(f"/api/groups/{gid}/members", headers=ah, json={"username": bob_user})

    rv = client.delete(f"/api/groups/{gid}/members/{bob_user}", headers=bh)
    assert rv.status_code == 200

    detail = client.get(f"/api/groups/{gid}", headers=ah).get_json()
    assert all(m["username"] != bob_user for m in detail["members"])


def test_owner_cannot_be_removed(client, alice):
    user, _, h = alice
    gid = _create_group(client, h).get_json()["group_id"]
    rv = client.delete(f"/api/groups/{gid}/members/{user}", headers=h)
    assert rv.status_code == 400


# ===== 解散 =====

def test_owner_can_delete_group(client, alice):
    _, _, h = alice
    gid = _create_group(client, h).get_json()["group_id"]

    rv = client.delete(f"/api/groups/{gid}", headers=h)
    assert rv.status_code == 200

    rv2 = client.get(f"/api/groups/{gid}", headers=h)
    assert rv2.status_code == 404


def test_non_owner_cannot_delete_group(client, alice, bob):
    _, _, ah = alice
    bob_user, _, bh = bob
    gid = _create_group(client, ah).get_json()["group_id"]
    client.post(f"/api/groups/{gid}/members", headers=ah, json={"username": bob_user})

    rv = client.delete(f"/api/groups/{gid}", headers=bh)
    assert rv.status_code == 403
