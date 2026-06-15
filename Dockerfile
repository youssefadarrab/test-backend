FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.8.3

WORKDIR /app

RUN pip install "poetry==${POETRY_VERSION}" && poetry config virtualenvs.create false

# Dependency layer (cached unless pyproject changes).
COPY pyproject.toml ./
RUN poetry install --no-root --only main

COPY . .

EXPOSE 8000

# Default command runs the API; worker/reaper override `command` in compose.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
