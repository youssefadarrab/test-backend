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
        # Tolerate small clock skew between the API, workers and clients on exp/iat/nbf.
        self.jwt_leeway_seconds = secrets.get_int("JWT_LEEWAY_SECONDS", 30)

        self.partner_hmac_secret = secrets.get("PARTNER_HMAC_SECRET", "dev-partner-secret")

        # Retry/recovery knobs, shared by the broker setup, the worker and the reaper.
        self.step_max_attempts = secrets.get_int("STEP_MAX_ATTEMPTS", 5)
        self.step_timeout_seconds = secrets.get_int("STEP_TIMEOUT_SECONDS", 60)
        self.callback_sla_seconds = secrets.get_int("CALLBACK_SLA_SECONDS", 900)
        self.reaper_interval_seconds = secrets.get_int("REAPER_INTERVAL_SECONDS", 15)

        self.storage_dir = secrets.get("STORAGE_DIR", "/data/storage")

    @property
    def is_local(self) -> bool:
        return self.env == "local"

    # LISTEN/NOTIFY goes through psycopg2 directly, which wants a plain libpq DSN.
    @property
    def listen_dsn(self) -> str:
        return self.database_url.replace("postgresql+psycopg2://", "postgresql://")


@lru_cache
def get_settings() -> Settings:
    return Settings()
