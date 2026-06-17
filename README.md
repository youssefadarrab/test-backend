# Primmo Document Pipeline

A multi-tenant document-ingestion API. A user belonging to an organization uploads a file; it runs
through a 4-stage processing pipeline; the final stage hands off to an external partner that replies
asynchronously via a signed webhook. Users track progress in real time and retrieve the extracted
data once the document is `ready`.

This README covers **how to set up and run the repo**. For the architecture rationale and trade-offs,
see [`docs/DESIGN.md`](./docs/DESIGN.md).

---

## Prerequisites

- **Docker** + **Docker Compose** (the only requirement for the quickstart).
- For running outside Docker: **Python 3.11**, **Poetry**, and a reachable **Postgres** and
  **RabbitMQ**.

---

## Quickstart (Docker)

```bash
cp .env.example .env          # dev defaults; works as-is
docker compose up --build     # db + rabbitmq + init(migrate+seed) + api + worker + reaper
```

Then open the interactive docs: **http://localhost:8000/docs**

What Compose starts:

| Service   | Role |
|-----------|------|
| `db`      | Postgres (data + `LISTEN/NOTIFY` for the progress stream) |
| `rabbitmq`| Message broker for the pipeline steps |
| `init`    | One-shot: applies migrations and seeds, then exits |
| `api`     | FastAPI app serving `/docs` and the REST + SSE endpoints |
| `worker`  | Consumes step messages and runs the pipeline |
| `reaper`  | Periodically recovers stale/stuck steps |

Stop and wipe everything (including volumes):

```bash
make down        # docker compose down -v
```

Scale workers horizontally:

```bash
docker compose up --scale worker=4
```

### Seeded credentials

The `init` service seeds 2 organizations with 1 user each (no password — dev login):

| Organization | User email             |
|--------------|------------------------|
| Acme         | `alice@acme.example`   |
| Globex       | `youssef@globex.example` |

---

## End-to-end walkthrough (~60 seconds in `/docs`)

All routes are under the prefix `/v1/docpipe`.

1. **`POST /v1/docpipe/auth/login`** with a seeded email → copy the JWT → click **Authorize**.
2. **`POST /v1/docpipe/documents`** — upload any file. Note the returned `id` (status `processing`).
3. **`GET /v1/docpipe/documents/{id}`** — watch per-step status. `external_call` reaches
   `awaiting_callback` and exposes an `external_job_id`.
4. **`POST /v1/docpipe/dev/sign-webhook`** with the partner body (using that `job_id`) → copy the
   signature. (This dev helper is only mounted when `ENV=local`.)
5. **`POST /v1/docpipe/webhooks/partner`** with the same body plus header
   `X-Partner-Signature: <signature>` → 200.
6. **`GET /v1/docpipe/documents/{id}`** — status is now `ready`, extracted data present.

### Live progress via SSE

Swagger can't render a live stream, so use `curl`:

```bash
curl -N -H "Authorization: Bearer <JWT>" \
  http://localhost:8000/v1/docpipe/documents/<id>/events
```

### Signing a webhook manually

The signature is `hex HMAC-SHA256(raw_body, PARTNER_HMAC_SECRET)`, computed over the **exact bytes**
you POST:

```python
import hashlib, hmac
secret = b"dev-partner-secret"   # = PARTNER_HMAC_SECRET
raw = b'{"job_id":"j_abc123def4567890","status":"completed","result":{}}'  # the EXACT bytes you POST
signature = hmac.new(secret, raw, hashlib.sha256).hexdigest()
```

---

## Running outside Docker

You still need a reachable Postgres and RabbitMQ; point the env vars at them.

```bash
poetry install
cp .env.example .env            # then edit DATABASE_URL / RABBITMQ_URL to point at your services

make migrate                    # alembic upgrade head
make seed                       # python -m app.seed

# In separate terminals:
poetry run uvicorn app.main:app --reload     # api
poetry run python -m app.worker.main         # worker
poetry run python -m app.worker.reaper       # reaper
```

---

## Tests

```bash
make test        # or: pytest -q
```

- **Unit tests** need no external services (DAG, status derivation, HMAC, logging).
- **Integration tests** need Postgres. They use **testcontainers** by default (requires a running
  Docker daemon). To run them against an already-running Postgres instead, set `TEST_DATABASE_URL`:

  ```bash
  TEST_DATABASE_URL="postgresql+psycopg2://user@host:5432/dbname" pytest -q
  ```

---

## Configuration (env)

All variables have safe dev defaults in `.env.example`.

| Variable | Purpose |
|----------|---------|
| `ENV` | `local` mounts the `/v1/docpipe/dev/sign-webhook` helper; anything else hides it |
| `DATABASE_URL` | Postgres DSN |
| `RABBITMQ_URL` | AMQP broker URL |
| `JWT_SECRET` / `JWT_ALGORITHM` / `JWT_EXPIRE_MINUTES` / `JWT_LEEWAY_SECONDS` | Auth token settings |
| `PARTNER_HMAC_SECRET` | Shared secret for the partner webhook signature |
| `STEP_MAX_ATTEMPTS` | Per-step retry budget (also the broker delivery-limit) |
| `STEP_TIMEOUT_SECONDS` / `CALLBACK_SLA_SECONDS` | Reaper thresholds |
| `REAPER_INTERVAL_SECONDS` | How often the reaper scans |
| `STORAGE_DIR` | Where uploaded bytes are stored |

Config and secrets are read through a small `SecretManager` (`app/secrets.py`): it reads the
environment today, and a deployed setup can point it at a managed store (GCP Secret Manager, Vault)
without changing call sites.

---

## Project layout

```
app/
  config.py db.py models.py schemas.py auth.py webhook_security.py main.py seed.py secrets.py
  api/        auth.py documents.py webhooks.py events.py dev.py  routes_impl/
  pipeline/   steps.py dag.py transition.py handlers.py publisher.py messages.py
  worker/     broker.py main.py reaper.py
  events/     notify.py
migrations/   (alembic)
tests/        unit/  integration/
docs/         DESIGN.md
docker-compose.yml  Dockerfile  Makefile  pyproject.toml  .env.example
```

---

## Troubleshooting

- **`/docs` not reachable:** give `db` and `rabbitmq` a moment to pass their healthchecks; the `init`
  service must finish (migrate + seed) before `api` is useful. Check `make logs`.
- **Webhook returns 401/invalid signature:** you must sign the *exact* bytes you POST. Re-serializing
  the JSON changes the bytes and breaks verification.
- **Integration tests error on collection:** the Docker daemon isn't running (testcontainers), or set
  `TEST_DATABASE_URL` to use an existing Postgres.
- **Reset everything:** `make down` removes containers and volumes; `docker compose up --build`
  re-migrates and re-seeds.
