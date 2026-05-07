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
