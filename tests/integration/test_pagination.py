"""Keyset pagination of the documents list: a full walk yields every document
once, in order, and a malformed cursor is a 400."""
from __future__ import annotations


def _token(client, email: str) -> str:
    return client.post("/v1/docpipe/auth/login", json={"email": email}).json()["access_token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _upload(client, token: str, name: str) -> str:
    resp = client.post(
        "/v1/docpipe/documents",
        files={"file": (name, b"x", "application/pdf")},
        headers=_auth(token),
    )
    assert resp.status_code == 201
    return resp.json()["id"]


def test_pages_cover_every_document_once(client, seeded, db, fake_publish):
    token = _token(client, "alice@acme.example")
    uploaded = [_upload(client, token, f"doc_{i}.pdf") for i in range(5)]

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        body = client.get("/v1/docpipe/documents", params=params, headers=_auth(token)).json()
        assert len(body["items"]) <= 2
        seen.extend(item["id"] for item in body["items"])
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            break
        assert pages < 10  # guard against a cursor that never terminates

    assert len(seen) == len(set(seen)) == 5          # every doc once, no duplicates/skips
    assert set(seen) == set(uploaded)
    assert pages == 3                                # 2 + 2 + 1


def test_default_page_has_no_next_cursor_when_small(client, seeded, db, fake_publish):
    token = _token(client, "alice@acme.example")
    _upload(client, token, "only.pdf")
    body = client.get("/v1/docpipe/documents", headers=_auth(token)).json()
    assert len(body["items"]) == 1
    assert body["next_cursor"] is None


def test_malformed_cursor_is_400(client, seeded):
    token = _token(client, "alice@acme.example")
    resp = client.get(
        "/v1/docpipe/documents", params={"cursor": "!!!not-valid!!!"}, headers=_auth(token)
    )
    assert resp.status_code == 400
