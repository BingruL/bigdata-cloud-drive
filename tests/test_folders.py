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
    hbase.create_folder(config.HBASE_TABLE_FOLDERS, "folder-1", {
        "name": "child-folder",
        "owner": "alice",
        "parent_id": "root",
    })
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


def test_restore_folder_preserves_previously_deleted_child_file(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    uploaded = _upload(client, headers, "报告.txt", b"hello", parent["folder_id"])
    fid = uploaded.get_json()["file"]["file_id"]

    assert client.delete(f"/api/files/{fid}", headers=headers).status_code == 200
    assert client.delete(f"/api/folders/{parent['folder_id']}", headers=headers).status_code == 200
    assert client.post(f"/api/folders/{parent['folder_id']}/restore", headers=headers).status_code == 200

    assert client.get(f"/api/files/{fid}", headers=headers).status_code == 404


def test_restore_legacy_deleted_folder_without_delete_source_marker(client, app, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    uploaded = _upload(client, headers, "报告.txt", b"hello", parent["folder_id"])
    fid = uploaded.get_json()["file"]["file_id"]

    config = app.config["APP_CONFIG"]
    hbase = app.config["HBASE_SERVICE"]
    hbase.update_folder_fields(config.HBASE_TABLE_FOLDERS, parent["folder_id"], {
        "deleted": "1",
        "deleted_at": "1",
    })
    hbase.update_file_meta_fields(config.HBASE_TABLE_FILES, fid, {
        "deleted": "1",
        "deleted_at": "1",
    })

    restored = client.post(f"/api/folders/{parent['folder_id']}/restore", headers=headers)

    assert restored.status_code == 200
    assert client.get(f"/api/files/{fid}", headers=headers).status_code == 200


def test_reject_partial_restore_under_deleted_folder(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    child = _create_folder(client, headers, "子", parent["folder_id"])
    uploaded = _upload(client, headers, "报告.txt", b"hello", child["folder_id"])
    fid = uploaded.get_json()["file"]["file_id"]

    assert client.delete(f"/api/folders/{parent['folder_id']}", headers=headers).status_code == 200

    file_restore = client.post(f"/api/files/{fid}/restore", headers=headers)
    folder_restore = client.post(f"/api/folders/{child['folder_id']}/restore", headers=headers)

    assert file_restore.status_code == 400
    assert folder_restore.status_code == 400


def test_browse_validates_parent_folder(client, app, alice, bob):
    _, _, alice_headers = alice
    _, _, bob_headers = bob
    bob_folder = _create_folder(client, bob_headers, "bob-docs")
    own_folder = _create_folder(client, alice_headers, "alice-docs")

    missing = client.get("/api/files/browse?parent_id=missing-folder", headers=alice_headers)
    cross_owner = client.get(f"/api/files/browse?parent_id={bob_folder['folder_id']}", headers=alice_headers)
    assert client.delete(f"/api/folders/{own_folder['folder_id']}", headers=alice_headers).status_code == 200
    deleted = client.get(f"/api/files/browse?parent_id={own_folder['folder_id']}", headers=alice_headers)

    assert missing.status_code == 404
    assert cross_owner.status_code == 403
    assert deleted.status_code == 404


def test_renamed_file_display_name_used_by_search_preview_and_download(client, alice):
    _, _, headers = alice
    uploaded = _upload(client, headers, "old.txt", b"hello")
    fid = uploaded.get_json()["file"]["file_id"]

    renamed = client.patch(f"/api/files/{fid}/rename", headers=headers, json={"name": "new.txt"})
    assert renamed.status_code == 200

    search = client.get("/api/files/search?keyword=new", headers=headers)
    assert search.status_code == 200
    assert any(item["file_id"] == fid for item in search.get_json()["files"])

    preview = client.get(f"/api/files/{fid}/preview", headers=headers)
    assert preview.status_code == 200
    assert preview.get_json()["filename"] == "new.txt"

    download = client.get(f"/api/files/{fid}/download", headers=headers)
    assert download.status_code == 200
    assert "new.txt" in download.headers["Content-Disposition"]


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


def test_browse_returns_real_breadcrumb_chain(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    child = _create_folder(client, headers, "子", parent["folder_id"])

    rv = client.get(f"/api/files/browse?parent_id={child['folder_id']}", headers=headers)
    assert rv.status_code == 200
    crumbs = rv.get_json()["breadcrumbs"]
    assert [c["name"] for c in crumbs] == ["全部文件", "父", "子"]
    assert [c["folder_id"] for c in crumbs] == ["root", parent["folder_id"], child["folder_id"]]


def test_get_single_folder(client, alice, bob):
    _, _, headers = alice
    _, _, bob_headers = bob
    folder = _create_folder(client, headers, "doc")

    own = client.get(f"/api/folders/{folder['folder_id']}", headers=headers)
    cross = client.get(f"/api/folders/{folder['folder_id']}", headers=bob_headers)
    missing = client.get("/api/folders/no-such-folder", headers=headers)
    root = client.get("/api/folders/root", headers=headers)

    assert own.status_code == 200
    assert own.get_json()["name"] == "doc"
    assert cross.status_code == 403
    assert missing.status_code == 404
    assert root.status_code == 200
    assert root.get_json()["folder_id"] == "root"


def test_folder_trash_lists_only_top_level_deletes(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    child = _create_folder(client, headers, "子", parent["folder_id"])
    sibling = _create_folder(client, headers, "邻居")

    assert client.delete(f"/api/folders/{parent['folder_id']}", headers=headers).status_code == 200
    assert client.delete(f"/api/folders/{sibling['folder_id']}", headers=headers).status_code == 200

    rv = client.get("/api/folders/trash", headers=headers)
    assert rv.status_code == 200
    body = rv.get_json()
    folder_ids = {f["folder_id"] for f in body["folders"]}
    assert parent["folder_id"] in folder_ids
    assert sibling["folder_id"] in folder_ids
    assert child["folder_id"] not in folder_ids
    assert all(f.get("item_type") == "folder" for f in body["folders"])


def test_file_trash_hides_files_inside_deleted_folder(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    nested_file = _upload(client, headers, "nested.txt", b"x", parent["folder_id"]).get_json()["file"]
    standalone_file = _upload(client, headers, "loose.txt", b"x").get_json()["file"]
    assert client.delete(f"/api/files/{standalone_file['file_id']}", headers=headers).status_code == 200
    assert client.delete(f"/api/folders/{parent['folder_id']}", headers=headers).status_code == 200

    rv = client.get("/api/files/trash", headers=headers)
    assert rv.status_code == 200
    visible_ids = {f["file_id"] for f in rv.get_json()["files"]}
    assert standalone_file["file_id"] in visible_ids
    assert nested_file["file_id"] not in visible_ids


def test_folder_tree_returns_only_my_active_folders(client, alice, bob):
    _, _, alice_headers = alice
    _, _, bob_headers = bob
    a_root = _create_folder(client, alice_headers, "alice-root")
    a_child = _create_folder(client, alice_headers, "alice-child", a_root["folder_id"])
    a_trashed = _create_folder(client, alice_headers, "alice-trashed")
    _create_folder(client, bob_headers, "bob-root")
    assert client.delete(f"/api/folders/{a_trashed['folder_id']}", headers=alice_headers).status_code == 200

    rv = client.get("/api/folders/tree", headers=alice_headers)
    assert rv.status_code == 200
    folder_ids = {f["folder_id"] for f in rv.get_json()["folders"]}
    assert a_root["folder_id"] in folder_ids
    assert a_child["folder_id"] in folder_ids
    assert a_trashed["folder_id"] not in folder_ids
    for f in rv.get_json()["folders"]:
        assert f.get("owner") == "alice"


def test_folder_summary_counts_subtree(client, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    _create_folder(client, headers, "子A", parent["folder_id"])
    childB = _create_folder(client, headers, "子B", parent["folder_id"])
    _upload(client, headers, "p.txt", b"12345", parent["folder_id"])
    _upload(client, headers, "b.txt", b"abc", childB["folder_id"])

    rv = client.get(f"/api/folders/{parent['folder_id']}/summary", headers=headers)
    assert rv.status_code == 200
    body = rv.get_json()
    assert body["folder_count"] == 2
    assert body["file_count"] == 2
    assert body["total_size"] == 5 + 3


def test_folder_summary_rejects_cross_owner(client, alice, bob):
    _, _, alice_headers = alice
    _, _, bob_headers = bob
    folder = _create_folder(client, alice_headers, "alice-folder")
    rv = client.get(f"/api/folders/{folder['folder_id']}/summary", headers=bob_headers)
    assert rv.status_code == 403


def test_rename_keeps_type_in_sync_with_extension(client, app, alice):
    _, _, headers = alice
    uploaded = _upload(client, headers, "report.pdf", b"%PDF-1.4")
    fid = uploaded.get_json()["file"]["file_id"]

    renamed = client.patch(f"/api/files/{fid}/rename", headers=headers, json={"name": "report.docx"})
    assert renamed.status_code == 200
    assert renamed.get_json()["display_name"] == "report.docx"
    assert renamed.get_json()["type"] == "docx"


def test_rename_without_extension_preserves_type(client, alice):
    _, _, headers = alice
    uploaded = _upload(client, headers, "report.pdf", b"%PDF-1.4")
    fid = uploaded.get_json()["file"]["file_id"]

    renamed = client.patch(f"/api/files/{fid}/rename", headers=headers, json={"name": "report"})
    assert renamed.status_code == 200
    body = renamed.get_json()
    assert body["display_name"] == "report"
    assert body["type"] == "pdf"


def test_folder_trash_restore_round_trip(client, app, alice):
    _, _, headers = alice
    parent = _create_folder(client, headers, "父")
    uploaded = _upload(client, headers, "child.txt", b"x", parent["folder_id"]).get_json()["file"]
    assert client.delete(f"/api/folders/{parent['folder_id']}", headers=headers).status_code == 200

    listed = client.get("/api/folders/trash", headers=headers).get_json()["folders"]
    assert any(f["folder_id"] == parent["folder_id"] for f in listed)

    restored = client.post(f"/api/folders/{parent['folder_id']}/restore", headers=headers)
    assert restored.status_code == 200
    assert client.get(f"/api/files/{uploaded['file_id']}", headers=headers).status_code == 200

    again = client.get("/api/folders/trash", headers=headers).get_json()["folders"]
    assert all(f["folder_id"] != parent["folder_id"] for f in again)
