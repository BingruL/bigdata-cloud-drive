import io


def _upload(client, headers, filename="hello.txt", content=b"hello world"):
    rv = client.post(
        "/api/files/upload",
        headers=headers,
        data={"file": (io.BytesIO(content), filename)},
        content_type="multipart/form-data",
    )
    assert rv.status_code == 201
    return rv.get_json()["file"]


def _create_link(client, headers, file_id, body=None):
    rv = client.post(f"/api/files/{file_id}/public-links", headers=headers, json=body or {})
    assert rv.status_code == 201
    assert rv.get_json()["token"]
    return rv.get_json()["public_link"]


def test_owner_creates_link_and_public_downloads_exact_bytes(client, app, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "data.txt", b"public-bytes")
    link = _create_link(client, headers, file_info["file_id"])

    meta = client.get(f"/api/public-links/{link['token']}")
    assert meta.status_code == 200
    assert meta.get_json()["filename"] == "data.txt"
    assert meta.get_json()["requires_password"] is False

    rv = client.post(f"/api/public-links/{link['token']}/download", json={})
    assert rv.status_code == 200
    assert rv.data == b"public-bytes"

    hbase = app.config["HBASE_SERVICE"]
    config = app.config["APP_CONFIG"]
    stored_link = hbase.get_public_link(config.HBASE_TABLE_PUBLIC_LINKS, link["token"])
    stored_file = hbase.get_file_meta(config.HBASE_TABLE_FILES, file_info["file_id"])
    logs = hbase.get_logs(config.HBASE_TABLE_LOGS, action="public_download")
    assert stored_link["download_count"] == "1"
    assert stored_link["last_download_at"]
    assert stored_file["downloads"] == "1"
    assert logs and logs[0]["username"] == "public"
    assert logs[0]["detail"].startswith(file_info["file_id"])
    assert link["token"] in logs[0]["detail"]


def test_password_protected_link_rejects_and_accepts_password(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "secret.txt", b"secret")
    link = _create_link(client, headers, file_info["file_id"], {"password": "open"})

    meta = client.get(f"/api/public-links/{link['token']}")
    assert meta.status_code == 200
    assert meta.get_json()["requires_password"] is True

    wrong = client.post(f"/api/public-links/{link['token']}/download", json={"password": "bad"})
    assert wrong.status_code == 403

    ok = client.post(f"/api/public-links/{link['token']}/download", json={"password": "open"})
    assert ok.status_code == 200
    assert ok.data == b"secret"


def test_public_download_probe_and_form_password(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "form-secret.txt", b"secret")
    link = _create_link(client, headers, file_info["file_id"], {"password": "open"})

    bad_probe = client.post(f"/api/public-links/{link['token']}/download?probe=1", json={"password": "bad"})
    ok_probe = client.post(f"/api/public-links/{link['token']}/download?probe=1", json={"password": "open"})
    form_download = client.post(f"/api/public-links/{link['token']}/download", data={"password": "open"})

    assert bad_probe.status_code == 403
    assert ok_probe.status_code == 200
    assert ok_probe.get_json()["ok"] is True
    assert form_download.status_code == 200
    assert form_download.data == b"secret"


def test_non_owner_cannot_create_public_link(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    file_info = _upload(client, ah, "private.txt", b"x")

    rv = client.post(f"/api/files/{file_info['file_id']}/public-links", headers=bh, json={})
    assert rv.status_code == 403


def test_admin_can_create_list_and_revoke_public_link(client, alice, admin):
    _, _, ah = alice
    _, _, admin_headers = admin
    file_info = _upload(client, ah, "admin-visible.txt", b"x")

    created = client.post(f"/api/files/{file_info['file_id']}/public-links", headers=admin_headers, json={})
    assert created.status_code == 201
    token = created.get_json()["token"]

    listed = client.get(f"/api/files/{file_info['file_id']}/public-links", headers=admin_headers)
    assert listed.status_code == 200
    assert any(link["token"] == token for link in listed.get_json()["links"])

    revoked = client.delete(
        f"/api/files/{file_info['file_id']}/public-links/{token}",
        headers=admin_headers,
    )
    assert revoked.status_code == 200


def test_non_owner_cannot_list_or_revoke_public_links(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    file_info = _upload(client, ah, "private-links.txt", b"x")
    link = _create_link(client, ah, file_info["file_id"])

    listed = client.get(f"/api/files/{file_info['file_id']}/public-links", headers=bh)
    revoked = client.delete(
        f"/api/files/{file_info['file_id']}/public-links/{link['token']}",
        headers=bh,
    )

    assert listed.status_code == 403
    assert revoked.status_code == 403


def test_owner_can_list_and_revoke_link_for_trashed_file(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "trashed-link.txt", b"x")
    link = _create_link(client, headers, file_info["file_id"])
    assert client.delete(f"/api/files/{file_info['file_id']}", headers=headers).status_code == 200

    listed = client.get(f"/api/files/{file_info['file_id']}/public-links", headers=headers)
    revoked = client.delete(
        f"/api/files/{file_info['file_id']}/public-links/{link['token']}",
        headers=headers,
    )

    assert listed.status_code == 200
    assert any(item["token"] == link["token"] for item in listed.get_json()["links"])
    assert revoked.status_code == 200


def test_public_link_create_rejects_invalid_input(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "invalid-input.txt", b"x")
    path = f"/api/files/{file_info['file_id']}/public-links"

    malformed = client.post(path, headers={**headers, "Content-Type": "application/json"}, data='{"password":')
    float_days = client.post(path, headers=headers, json={"expires_in_days": 1.9})
    bad_password = client.post(path, headers=headers, json={"password": ["pw"]})

    assert malformed.status_code == 400
    assert float_days.status_code == 400
    assert bad_password.status_code == 400


def test_revoke_prevents_public_download(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "revoke.txt", b"x")
    link = _create_link(client, headers, file_info["file_id"])

    rv = client.delete(
        f"/api/files/{file_info['file_id']}/public-links/{link['token']}",
        headers=headers,
    )
    assert rv.status_code == 200

    blocked = client.post(f"/api/public-links/{link['token']}/download", json={})
    assert blocked.status_code == 410


def test_trashed_file_link_unavailable(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "trash.txt", b"x")
    link = _create_link(client, headers, file_info["file_id"])

    assert client.delete(f"/api/files/{file_info['file_id']}", headers=headers).status_code == 200

    meta = client.get(f"/api/public-links/{link['token']}")
    assert meta.status_code == 410
    blocked = client.post(f"/api/public-links/{link['token']}/download", json={})
    assert blocked.status_code == 410


def test_expired_link_unavailable(client, app, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "expired.txt", b"x")
    link = _create_link(client, headers, file_info["file_id"])

    hbase = app.config["HBASE_SERVICE"]
    config = app.config["APP_CONFIG"]
    hbase.get_public_link(config.HBASE_TABLE_PUBLIC_LINKS, link["token"])
    hbase._t(config.HBASE_TABLE_PUBLIC_LINKS)[link["token"]]["expires_at"] = "1"

    assert client.get(f"/api/public-links/{link['token']}").status_code == 410
    assert client.post(f"/api/public-links/{link['token']}/download", json={}).status_code == 410


def test_listing_public_links(client, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "list.txt", b"x")
    first = _create_link(client, headers, file_info["file_id"])
    second = _create_link(client, headers, file_info["file_id"], {"password": "pw"})

    rv = client.get(f"/api/files/{file_info['file_id']}/public-links", headers=headers)
    assert rv.status_code == 200
    links = rv.get_json()["public_links"]
    tokens = {link["token"] for link in links}
    assert tokens == {first["token"], second["token"]}
    assert any(link["requires_password"] for link in links)


def test_purge_disables_public_link(client, app, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "purge.txt", b"x")
    link = _create_link(client, headers, file_info["file_id"])

    assert client.delete(f"/api/files/{file_info['file_id']}", headers=headers).status_code == 200
    purged = client.delete(f"/api/files/{file_info['file_id']}/purge", headers=headers)
    assert purged.status_code == 200

    hbase = app.config["HBASE_SERVICE"]
    config = app.config["APP_CONFIG"]
    stored_link = hbase.get_public_link(config.HBASE_TABLE_PUBLIC_LINKS, link["token"])
    assert stored_link["enabled"] == "0"
    assert client.post(f"/api/public-links/{link['token']}/download", json={}).status_code == 410


def test_display_name_used_in_public_download_filename(client, app, alice):
    _, _, headers = alice
    file_info = _upload(client, headers, "original.txt", b"x")
    hbase = app.config["HBASE_SERVICE"]
    config = app.config["APP_CONFIG"]
    hbase.update_file_meta_fields(config.HBASE_TABLE_FILES, file_info["file_id"], {
        "display_name": "renamed.txt",
    })
    link = _create_link(client, headers, file_info["file_id"])

    rv = client.post(f"/api/public-links/{link['token']}/download", json={})
    assert rv.status_code == 200
    assert "renamed.txt" in rv.headers["Content-Disposition"]


def test_public_share_page_route_serves_standalone_page(client):
    rv = client.get("/s/example-token")

    assert rv.status_code == 200
    assert b"public-share" in rv.data
