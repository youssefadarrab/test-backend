"""Full happy path through the API + worker handlers (broker stubbed, steps made
deterministic), plus tenant isolation and auth."""
from __future__ import annotations

import json

import pytest

from app.pipeline.handlers import handle_step
from app.webhook_security import compute_signature

JOB_ID = "j_flow_test_0001"


@pytest.fixture
def deterministic_steps(monkeypatch):
    monkeypatch.setattr("app.pipeline.steps.ocr", lambda: "lorem ipsum...")
    monkeypatch.setattr("app.pipeline.steps.metadata", lambda text: {"doc_type": "fake_type"})
    monkeypatch.setattr("app.pipeline.steps.chunking", lambda text: ["chunk_1", "chunk_2"])
    monkeypatch.setattr("app.pipeline.steps.external_call", lambda *a, **k: JOB_ID)


def _token(client, email: str) -> str:
    resp = client.post("/auth/login", json={"email": email})
    assert resp.status_code == 200
    return resp.json()["access_token"]


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _drive(db, step: str, doc_id) -> str:
    with db.session_scope() as s:
        return handle_step(s, step, {"document_id": str(doc_id)})


def test_full_pipeline_to_ready(client, seeded, db, fake_publish, deterministic_steps):
    token = _token(client, "alice@acme.example")

    # Upload
    resp = client.post(
        "/documents",
        files={"file": ("contract.pdf", b"%PDF-bytes", "application/pdf")},
        headers=_auth(token),
    )
    assert resp.status_code == 201
    doc_id = resp.json()["id"]
    assert resp.json()["status"] == "processing"

    # Simulate workers consuming each published step.
    assert _drive(db, "ocr", doc_id) == "ack"
    assert _drive(db, "metadata", doc_id) == "ack"
    assert _drive(db, "chunking", doc_id) == "ack"
    assert _drive(db, "external_call", doc_id) == "ack"

    # external_call sent -> awaiting the webhook; still processing.
    detail = client.get(f"/documents/{doc_id}", headers=_auth(token)).json()
    assert detail["status"] == "processing"
    ext = next(s for s in detail["steps"] if s["name"] == "external_call")
    assert ext["status"] == "awaiting_callback"
    assert ext["external_job_id"] == JOB_ID
    assert detail["data"] is None

    # Partner webhook -> ready.
    body = json.dumps(
        {"job_id": JOB_ID, "status": "completed", "result": {"indexed_at": "2026-05-21T14:23:11Z"}}
    ).encode()
    wh = client.post(
        "/webhooks/partner",
        content=body,
        headers={"Content-Type": "application/json", "X-Partner-Signature": compute_signature(body)},
    )
    assert wh.status_code == 200

    detail = client.get(f"/documents/{doc_id}", headers=_auth(token)).json()
    assert detail["status"] == "ready"
    assert detail["data"]["ocr_text"] == "lorem ipsum..."
    assert detail["data"]["metadata"] == {"doc_type": "fake_type"}
    assert detail["data"]["chunks"] == ["chunk_1", "chunk_2"]
    assert detail["data"]["partner_result"] == {"indexed_at": "2026-05-21T14:23:11Z"}


def test_tenant_isolation(client, seeded, db, fake_publish, deterministic_steps):
    alice = _token(client, "alice@acme.example")
    youssef = _token(client, "youssef@globex.example")

    resp = client.post(
        "/documents",
        files={"file": ("a.pdf", b"x", "application/pdf")},
        headers=_auth(alice),
    )
    doc_id = resp.json()["id"]

    # Bob (other org) cannot see Alice's document -> 404, not 403 (no existence leak).
    assert client.get(f"/documents/{doc_id}", headers=_auth(youssef)).status_code == 404
    assert client.get("/documents", headers=_auth(youssef)).json() == []
    assert len(client.get("/documents", headers=_auth(alice)).json()) == 1


def test_auth_required(client, seeded):
    assert client.get("/documents").status_code in (401, 403)
