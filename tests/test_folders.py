import io


def _create_folder(client, headers, name, parent_id="root"):
    rv = client.post("/api/folders", headers=headers, json={"name": name, "parent_id": parent_id})
    assert rv.status_code == 201
    return rv.get_json()


def _upload(client, headers, filename="hello.txt", content=b"hello", parent_id=None):
    data = {"file": (io.BytesIO(content), filename)}
    if parent_id is not None:
        data["parent_id"] = parent_id
    return client.post(
        "/api/files/upload",
        headers=headers,
        data=data,
        content_type="multipart/form-data",
    )


def test_root_browse_starts_empty(client, alice):
    _, _, headers = alice
    rv = client.get("/api/files/browse?parent_id=root", headers=headers)
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["parent_id"] == "root"
    assert body["breadcrumbs"] == [{"folder_id": "root", "name": "全部文件"}]
    assert body["items"] == []


def test_admin_browse_only_lists_own_files(client, app, alice, admin):
    config = app.config["APP_CONFIG"]
    hbase = app.config["HBASE_SERVICE"]
    hbase.save_file_meta(config.HBASE_TABLE_FILES, "alice-file", {
        "filename": "alice.txt",
        "owner": "alice",
        "parent_id": "root",
    })

    _, _, headers = admin
    rv = client.get("/api/files/browse?parent_id=root", headers=headers)

    assert rv.status_code == 200
    assert rv.get_json()["items"] == []


def test_browse_child_file_not_limited_by_first_page(client, app, alice):
    config = app.config["APP_CONFIG"]
    hbase = app.config["HBASE_SERVICE"]
    for i in range(config.MAX_PAGE_SIZE + 1):
        hbase.save_file_meta(config.HBASE_TABLE_FILES, f"root-file-{i}", {
            "filename": f"root-{i}.txt",
            "owner": "alice",
            "parent_id": "root",
            "created_at": str(2000 + i),
        })
    hbase.save_file_meta(config.HBASE_TABLE_FILES, "child-file", {
        "filename": "child.txt",
        "owner": "alice",
        "parent_id": "folder-1",
        "created_at": "1",
    })

    _, _, headers = alice
    rv = client.get("/api/files/browse?parent_id=folder-1", headers=headers)

    assert rv.status_code == 200
    items = rv.get_json()["items"]
    assert [item["file_id"] for item in items] == ["child-file"]


def test_create_folder_and_browse_root(client, alice):
    _, _, headers = alice
    rv = client.post("/api/folders", headers=headers, json={"name": "资料", "parent_id": "root"})
    assert rv.status_code == 201
    folder = rv.get_json()
    assert folder["name"] == "资料"
    root = client.get("/api/files/browse?parent_id=root", headers=headers).get_json()
    assert any(i["item_type"] == "folder" and i["name"] == "资料" for i in root["items"])


def test_create_folder_auto_renames_conflict(client, alice):
    _, _, headers = alice
    client.post("/api/folders", headers=headers, json={"name": "资料", "parent_id": "root"})
    rv = client.post("/api/folders", headers=headers, json={"name": "资料", "parent_id": "root"})
    assert rv.status_code == 201
    assert rv.get_json()["name"] == "资料 (1)"


def test_create_folder_rejects_cross_owner_parent(client, app, alice, bob):
    config = app.config["APP_CONFIG"]
    hbase = app.config["HBASE_SERVICE"]
    hbase.create_folder(config.HBASE_TABLE_FOLDERS, "bob-parent", {
        "name": "bob-parent",
        "owner": "bob",
        "parent_id": "root",
    })

    _, _, headers = alice
    rv = client.post("/api/folders", headers=headers, json={"name": "child", "parent_id": "bob-parent"})

    assert rv.status_code == 403


def test_admin_create_folder_rejects_cross_owner_parent(client, app, admin, bob):
    config = app.config["APP_CONFIG"]
    hbase = app.config["HBASE_SERVICE"]
    hbase.create_folder(config.HBASE_TABLE_FOLDERS, "bob-parent", {
        "name": "bob-parent",
        "owner": "bob",
        "parent_id": "root",
    })

    _, _, headers = admin
    rv = client.post("/api/folders", headers=headers, json={"name": "child", "parent_id": "bob-parent"})

    assert rv.status_code == 403


def test_create_folder_rejects_non_string_name(client, alice):
    _, _, headers = alice
    rv = client.post("/api/folders", headers=headers, json={"name": ["docs"], "parent_id": "root"})

    assert rv.status_code == 400


def test_create_folder_rejects_non_string_parent_id(client, alice):
    _, _, headers = alice
    rv = client.post("/api/folders", headers=headers, json={"name": "docs", "parent_id": ["root"]})

    assert rv.status_code == 400


def test_create_folder_rejects_non_object_json(client, alice):
    _, _, headers = alice
    rv = client.post("/api/folders", headers=headers, json=["docs"])

    assert rv.status_code == 400


def test_upload_to_folder_and_rename_file(client, app, alice):
    _, _, headers = alice
    folder = _create_folder(client, headers, "资料")

    uploaded = _upload(client, headers, "报告.pdf", b"PDF", folder["folder_id"])
    assert uploaded.status_code == 201
    fid = uploaded.get_json()["file"]["file_id"]

    renamed = client.patch(f"/api/files/{fid}/rename", headers=headers, json={"name": "新报告.pdf"})
    assert renamed.status_code == 200
    assert renamed.get_json()["display_name"] == "新报告.pdf"

    config = app.config["APP_CONFIG"]
    meta = app.config["HBASE_SERVICE"].get_file_meta(config.HBASE_TABLE_FILES, fid)
    assert meta["parent_id"] == folder["folder_id"]
    assert meta["display_name"] == "新报告.pdf"
    assert meta["updated_at"]


def test_move_file_between_folders(client, alice):
    _, _, headers = alice
    source = _create_folder(client, headers, "来源")
    target = _create_folder(client, headers, "目标")
    uploaded = _upload(client, headers, "报告.pdf", b"PDF", source["folder_id"])
    fid = uploaded.get_json()["file"]["file_id"]

    moved = client.patch(f"/api/files/{fid}/move", headers=headers, json={"target_parent_id": target["folder_id"]})

    assert moved.status_code == 200
    body = moved.get_json()
    assert body["parent_id"] == target["folder_id"]
    items = client.get(f"/api/files/browse?parent_id={target['folder_id']}", headers=headers).get_json()["items"]
    assert any(item["item_type"] == "file" and item["file_id"] == fid for item in items)


def test_move_folder_tree_and_reject_self_or_descendant(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    child = _create_folder(client, headers, "子", parent["folder_id"])
    target = _create_folder(client, headers, "目标")

    into_self = client.patch(
        f"/api/folders/{parent['folder_id']}/move",
        headers=headers,
        json={"target_parent_id": parent["folder_id"]},
    )
    assert into_self.status_code == 400

    into_child = client.patch(
        f"/api/folders/{parent['folder_id']}/move",
        headers=headers,
        json={"target_parent_id": child["folder_id"]},
    )
    assert into_child.status_code == 400

    moved = client.patch(
        f"/api/folders/{parent['folder_id']}/move",
        headers=headers,
        json={"target_parent_id": target["folder_id"]},
    )
    assert moved.status_code == 200
    assert moved.get_json()["parent_id"] == target["folder_id"]
    items = client.get(f"/api/files/browse?parent_id={parent['folder_id']}", headers=headers).get_json()["items"]
    assert any(item["item_type"] == "folder" and item["folder_id"] == child["folder_id"] for item in items)


def test_delete_restore_purge_folder_tree_including_child_files(client, app, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    child = _create_folder(client, headers, "子", parent["folder_id"])
    uploaded = _upload(client, headers, "报告.txt", b"hello", child["folder_id"])
    fid = uploaded.get_json()["file"]["file_id"]
    hdfs_path = uploaded.get_json()["file"]["hdfs_path"]

    deleted = client.delete(f"/api/folders/{parent['folder_id']}", headers=headers)
    assert deleted.status_code == 200
    assert client.get(f"/api/files/{fid}", headers=headers).status_code == 404

    restored = client.post(f"/api/folders/{parent['folder_id']}/restore", headers=headers)
    assert restored.status_code == 200
    assert client.get(f"/api/files/{fid}", headers=headers).status_code == 200

    assert client.delete(f"/api/folders/{parent['folder_id']}", headers=headers).status_code == 200
    purged = client.delete(f"/api/folders/{parent['folder_id']}/purge", headers=headers)
    assert purged.status_code == 200

    config = app.config["APP_CONFIG"]
    hbase = app.config["HBASE_SERVICE"]
    hdfs = app.config["HDFS_SERVICE"]
    assert hbase.get_folder(config.HBASE_TABLE_FOLDERS, parent["folder_id"]) is None
    assert hbase.get_folder(config.HBASE_TABLE_FOLDERS, child["folder_id"]) is None
    assert hbase.get_file_meta(config.HBASE_TABLE_FILES, fid) is None
    assert not hdfs.file_exists(hdfs_path)


def test_cross_namespace_naming_with_files_and_folders(client, alice):
    _, _, headers = alice
    uploaded = _upload(client, headers, "资料", b"file")
    assert uploaded.status_code == 201

    folder = _create_folder(client, headers, "资料")
    assert folder["name"] == "资料 (1)"

    renamed = client.patch(
        f"/api/folders/{folder['folder_id']}/rename",
        headers=headers,
        json={"name": "资料"},
    )
    assert renamed.status_code == 200
    assert renamed.get_json()["name"] == "资料 (1)"

    folder2 = _create_folder(client, headers, "报告.pdf")
    uploaded2 = _upload(client, headers, "source.txt", b"file")
    fid = uploaded2.get_json()["file"]["file_id"]
    renamed_file = client.patch(f"/api/files/{fid}/rename", headers=headers, json={"name": "报告.pdf"})
    assert renamed_file.status_code == 200
    assert renamed_file.get_json()["display_name"] == "报告 (1).pdf"
    assert folder2["name"] == "报告.pdf"
