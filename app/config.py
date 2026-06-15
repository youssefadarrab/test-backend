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
        self.rabbitmq_url = secrets.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

        self.jwt_secret = secrets.get("JWT_SECRET", "dev-jwt-secret-change-me")
        self.jwt_algorithm = secrets.get("JWT_ALGORITHM", "HS256")
        self.jwt_expire_minutes = secrets.get_int("JWT_EXPIRE_MINUTES", 720)

        self.partner_hmac_secret = secrets.get("PARTNER_HMAC_SECRET", "dev-partner-secret")

        # Retry/recovery knobs, shared by the broker setup, the worker and the reaper.
        self.step_max_attempts = secrets.get_int("STEP_MAX_ATTEMPTS", 5)

        self.storage_dir = secrets.get("STORAGE_DIR", "/data/storage")


@lru_cache
def get_settings() -> Settings:
    return Settings()
