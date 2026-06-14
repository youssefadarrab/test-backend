"""Secret and config resolution.

Everything is read through one class so the backing store can change per
environment without touching call sites. Today values come from the environment:
a local .env file in dev, and variables injected by the deployment (the compose
env_file, or the orchestrator) elsewhere. In a deployed setup this is where a
managed store (GCP Secret Manager, Vault) would plug in.
"""
from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv(path: str = ".env") -> None:
    """Load a local .env into the environment for dev convenience. Existing
    environment variables win, so injected deployment config is never overridden."""
    f = Path(path)
    if not f.exists():
        return
    for line in f.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


class SecretManager:
    """Reads secrets and config by name. Backed by the environment for now."""

    def __init__(self) -> None:
        _load_dotenv()

    def get(self, name: str, default: str | None = None, *, required: bool = False) -> str | None:
        # An empty value counts as missing, the way a real secret store behaves.
        value = os.environ.get(name) or default
        if required and not value:
            raise KeyError(f"missing required secret: {name}")
        return value

    def get_int(self, name: str, default: int) -> int:
        raw = self.get(name)
        return int(raw) if raw not in (None, "") else default


secrets = SecretManager()
