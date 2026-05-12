import io

from backend.config import parse_size_bytes


def test_parse_size_bytes_supports_gb_and_mb_units():
    assert parse_size_bytes("5GB", 0) == 5 * 1024 ** 3
    assert parse_size_bytes("1.5GB", 0) == int(1.5 * 1024 ** 3)
    assert parse_size_bytes("512MB", 0) == 512 * 1024 ** 2
    assert parse_size_bytes("1048576", 0) == 1048576


def test_upload_too_large_returns_json_error(client, app, alice):
    _, _, headers = alice
    app.config["MAX_CONTENT_LENGTH"] = 8

    rv = client.post(
        "/api/files/upload",
        headers=headers,
        data={"file": (io.BytesIO(b"x" * 64), "too-large.bin")},
        content_type="multipart/form-data",
    )

    assert rv.status_code == 413
    body = rv.get_json()
    assert "单次上传限制" in body["error"]
    assert body["max_upload_bytes"] == 8
