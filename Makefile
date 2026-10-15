COMPOSE = docker compose --profile core
DATE ?= $(shell date +%F)

.PHONY: up down logs init generate batch test
up:
	$(COMPOSE) up -d --build

down:
	docker compose --profile core --profile streaming down

logs:
	$(COMPOSE) logs -f

init:
	$(COMPOSE) run --rm --no-deps airflow-scheduler python -m retailpulse.common.initialize

generate:
	$(COMPOSE) exec airflow-scheduler python -m retailpulse.generators --days 90 --orders 20000 --defect-rate 0.04

batch:
	$(COMPOSE) exec airflow-scheduler python -m retailpulse.pipeline $(DATE)

test:
	$(COMPOSE) exec airflow-scheduler python -m pytest tests -q
