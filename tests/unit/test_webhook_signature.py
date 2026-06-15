from app.webhook_security import compute_signature, verify_signature


def test_sign_verify_roundtrip():
    body = b'{"job_id":"j_1","status":"completed"}'
    assert verify_signature(body, compute_signature(body)) is True


def test_tampered_body_fails():
    body = b'{"job_id":"j_1","status":"completed"}'
    sig = compute_signature(body)
    tampered = b'{"job_id":"j_1","status":"failed"}'
    assert verify_signature(tampered, sig) is False


def test_missing_signature_fails():
    assert verify_signature(b"{}", None) is False
    assert verify_signature(b"{}", "") is False


def test_wrong_signature_fails():
    assert verify_signature(b"{}", "deadbeef") is False
