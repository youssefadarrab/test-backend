from functools import lru_cache

from app.secrets import secrets


class Settings:
    """Resolved configuration. Secrets and config are read through the
    SecretManager so the backing store can change per environment."""

    def __init__(self) -> None:
        self.env = secrets.get("ENV", "local")

        self.database_url = secrets.get(
            "DATABASE_URL", "postgresql+psycopg2://primmo:primmo@localhost:5432/primmo"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
