import uuid
from datetime import datetime, timezone

import pytest

from app.pagination import decode_cursor, encode_cursor


def test_cursor_round_trip():
    created_at = datetime(2026, 6, 17, 9, 30, 0, tzinfo=timezone.utc)
    document_id = uuid.uuid4()
    decoded_at, decoded_id = decode_cursor(encode_cursor(created_at, document_id))
    assert decoded_at == created_at
    assert decoded_id == document_id


@pytest.mark.parametrize("bad", ["", "!!!", "bm90LWEtY3Vyc29y"])  # empty / non-b64 / no separator
def test_malformed_cursor_raises_value_error(bad):
    with pytest.raises(ValueError):
        decode_cursor(bad)
