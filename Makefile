# RetailPulse -- one entry point for everything. Run `make help`.
SHELL := /bin/bash
COMPOSE  := docker compose
PROFILES := --profile core --profile streaming --profile bi
AF       := $(COMPOSE) exec -T airflow-scheduler
# yesterday's date (GNU date on Linux/WSL, BSD date on macOS)
RUN_DATE ?= $(shell date -u -d yesterday +%F 2>/dev/null || date -u -v-1d +%F)
DAYS     ?= 30
ORDERS   ?= $(shell echo $$(( $(DAYS) * 250 )))
# unique microseconds so a manual run never collides with the scheduler's own run for the same date
LOGICAL_TIME := $(shell date -u +%H:%M:%S)
RID      := manual_$(RUN_DATE)_$(shell date +%s)

.DEFAULT_GOAL := help
.PHONY: help env up up-lite down clean ps logs wait init data generate demo batch batch-direct backfill gate-demo recover stream-demo dashboard smoke test test-local

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Create .env with random passwords if it does not exist
	@if [ ! -f .env ]; then \
	  cp .env.example .env; \
	  sed -i.bak "s/replace_with_a_strong_password/$$(openssl rand -hex 8)/; s/replace_with_another_strong_password/$$(openssl rand -hex 8)/; s/replace_with_an_admin_password/$$(openssl rand -hex 6)/; s/replace_with_a_random_session_secret/$$(openssl rand -hex 24)/" .env && rm -f .env.bak; \
	  echo "Created .env (passwords generated). Airflow login: admin / $$(grep AIRFLOW_ADMIN_PASSWORD .env | cut -d= -f2)"; \
	fi
	@mkdir -p data logs dashboards

up: env ## Start the full stack: Postgres, MinIO, Airflow, Redpanda, Metabase
	$(COMPOSE) $(PROFILES) up -d --build
	@echo ""; echo "Starting... give it 2-3 minutes, then run: make wait"
	@echo "  Airflow   http://localhost:8080   (admin / see AIRFLOW_ADMIN_PASSWORD in .env)"
	@echo "  MinIO     http://localhost:9001   (see .env)"
	@echo "  Redpanda  http://localhost:8085"
	@echo "  Metabase  http://localhost:3000"

up-lite: env ## Start only Postgres, MinIO and Airflow (for machines with little RAM)
	$(COMPOSE) --profile core up -d --build

down: ## Stop everything (data volumes are kept)
	$(COMPOSE) $(PROFILES) down

clean: ## Stop everything AND delete all data volumes
	$(COMPOSE) $(PROFILES) down -v
	rm -rf data logs

ps: ## Show service status
	$(COMPOSE) $(PROFILES) ps

logs: ## Follow logs (make logs S=airflow-scheduler)
	$(COMPOSE) $(PROFILES) logs -f --tail=100 $(S)

wait: ## Wait until Airflow is healthy
	@echo -n "Waiting for Airflow"; until curl -sf http://localhost:8080/health >/dev/null 2>&1; do echo -n "."; sleep 5; done; echo " ready"

init: ## (Re)apply database DDL and mart views (idempotent; also needed on existing volumes)
	$(AF) python -m retailpulse.common.initialize

data: ## Generate demo data ending on RUN_DATE (DAYS=30, 3% injected defects)
	$(AF) python -m retailpulse.generators --days $(DAYS) --orders $(ORDERS) --defect-rate 0.03 --end-date $(RUN_DATE)

generate: data

batch: ## Run the daily batch DAG for RUN_DATE (default: yesterday) and wait for the result
	$(AF) airflow dags unpause retail_batch_pipeline
	$(AF) airflow dags trigger retail_batch_pipeline -e "$(RUN_DATE)T$(LOGICAL_TIME)+00:00" --run-id "$(RID)"
	@echo "Triggered. Watch it at http://localhost:8080/dags/retail_batch_pipeline/grid"
	$(AF) python -m retailpulse.smoke wait --dag retail_batch_pipeline --run-id "$(RID)"

batch-direct: ## Run the same batch stages in-process, without the Airflow scheduler
	$(AF) python -m retailpulse.pipeline $(RUN_DATE)

demo: wait init data batch stream-demo dashboard ## Full demo: data -> batch -> streaming -> dashboard
	@echo ""; echo "Demo ready. Open Metabase at http://localhost:3000 and Airflow at http://localhost:8080"

backfill: ## Process the last N days one by one (make backfill DAYS=7)
	@for i in $$(seq $(DAYS) -1 1); do d=$$(date -d "$$i days ago" +%F 2>/dev/null || date -v-$${i}d +%F); \
	  echo "== $$d"; $(AF) airflow dags backfill retail_batch_pipeline -s $$d -e $$d --reset-dagruns -y >/dev/null 2>&1 || echo "failed $$d"; done

gate-demo: ## Show the quality gate: regenerate RUN_DATE with 20% defects and run the batch (it should FAIL)
	$(AF) python -m retailpulse.generators --days 5 --orders 1250 --defect-rate 0.20 --end-date $(RUN_DATE)
	-$(AF) airflow dags test retail_batch_pipeline $(RUN_DATE)
	@echo ""; echo "The DAG failed on purpose. Look at dq.pipeline_events / logs/alerts.log, then run: make recover"

recover: ## Regenerate clean data (3% defects) and re-run the batch (shows recovery)
	$(AF) python -m retailpulse.generators --days $(DAYS) --orders $(ORDERS) --defect-rate 0.03 --end-date $(RUN_DATE)
	$(AF) airflow dags test retail_batch_pipeline $(RUN_DATE)

stream-demo: ## Unpause the streaming DAG (producer runs in compose; consumer every 5 min)
	$(AF) airflow dags unpause retail_stream_microbatch
	$(AF) airflow dags trigger retail_stream_microbatch

dashboard: ## Create the Metabase dashboard (idempotent)
	$(AF) python /opt/airflow/dashboards/provision_metabase.py

smoke: ## End-to-end smoke test (generate -> batch DAG -> assertions -> streaming)
	$(AF) python -m retailpulse.smoke --days 10 --date $(RUN_DATE) --with-stream

test: ## Run unit tests inside the Airflow image (no local Java/Python setup needed)
	$(COMPOSE) --profile core run --rm --no-deps --entrypoint bash airflow-scheduler -c \
	  "cd /opt/airflow && python -m pytest tests -q -p no:cacheprovider"

test-local: ## Run unit tests with your local Python (needs Java 17 for the Spark tests)
	python3 -m pytest -q
