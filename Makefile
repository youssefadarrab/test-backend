.PHONY: up down logs migrate seed test fmt

up:            ## Boot the full stack
	docker compose up --build

down:          ## Stop and remove containers + volumes
	docker compose down -v

logs:
	docker compose logs -f api worker reaper

migrate:       ## Apply migrations locally (needs DATABASE_URL pointing at a reachable db)
	alembic upgrade head

seed:          ## Seed orgs/users locally
	python -m app.seed

test:          ## Run the test suite (integration tests need Docker for testcontainers)
	pytest -q

fmt:
	isort app tests && python -m autopep8 -r -i app tests
