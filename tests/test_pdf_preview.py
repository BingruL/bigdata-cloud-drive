import io


def _upload(client, headers, filename="doc.pdf", content=b"%PDF-1.4\nbody"):
    rv = client.post(
        "/api/files/upload",
        headers=headers,
        data={"file": (io.BytesIO(content), filename)},
        content_type="multipart/form-data",
    )
    assert rv.status_code == 201
    return rv.get_json()["file"]


def test_pdf_preview_token_and_stream(client, alice):
    _, _, headers = alice
    fid = _upload(client, headers)["file_id"]

    token_resp = client.post(f"/api/files/{fid}/preview-token", headers=headers)
    assert token_resp.status_code == 200
    token = token_resp.get_json()["token"]

    stream = client.get(f"/api/files/{fid}/preview-stream?token={token}")
    assert stream.status_code == 200
    assert stream.headers["Content-Type"].startswith("application/pdf")
    assert stream.headers["Content-Disposition"].startswith("inline")
    assert stream.data.startswith(b"%PDF-1.4")


def test_pdf_preview_rejects_token_for_other_file(client, alice):
    _, _, headers = alice
    fid1 = _upload(client, headers, "a.pdf", b"%PDF-1.4 a")["file_id"]
    fid2 = _upload(client, headers, "b.pdf", b"%PDF-1.4 b")["file_id"]
    token = client.post(f"/api/files/{fid1}/preview-token", headers=headers).get_json()["token"]

    assert client.get(f"/api/files/{fid2}/preview-stream?token={token}").status_code == 403


def test_pdf_preview_rejects_non_pdf(client, alice):
    _, _, headers = alice
    fid = _upload(client, headers, "note.txt", b"hello")["file_id"]

    assert client.post(f"/api/files/{fid}/preview-token", headers=headers).status_code == 415


def test_pdf_preview_appears_in_recent(client, alice):
    _, _, headers = alice
    fid = _upload(client, headers, "paper.pdf", b"%PDF-1.4")["file_id"]
    token = client.post(f"/api/files/{fid}/preview-token", headers=headers).get_json()["token"]

    assert client.get(f"/api/files/{fid}/preview-stream?token={token}").status_code == 200
    recent = client.get("/api/files/recent", headers=headers).get_json()["files"]
    assert any(f["file_id"] == fid for f in recent)


def test_pdf_preview_stream_allows_group_shared_reader(client, alice, bob):
    _, _, ah = alice
    _, _, bh = bob
    group = client.post("/api/groups", headers=ah, json={"name": "team"}).get_json()
    gid = group["group_id"]
    assert client.post(f"/api/groups/{gid}/members", headers=ah, json={"username": "bob"}).status_code == 201
    fid = _upload(client, ah, "shared.pdf", b"%PDF-1.4 shared")["file_id"]
    assert client.post(f"/api/files/{fid}/share", headers=ah, json={"groups": [gid]}).status_code == 200

    token = client.post(f"/api/files/{fid}/preview-token", headers=bh).get_json()["token"]
    stream = client.get(f"/api/files/{fid}/preview-stream?token={token}")

    assert stream.status_code == 200
    assert stream.data.startswith(b"%PDF-1.4 shared")
