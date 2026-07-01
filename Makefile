# =============================================================================
# Customer Intelligence Platform — Makefile
# -----------------------------------------------------------------------------
# One-command developer workflow for the local Docker Compose stack.
#
#   make setup    Build/pull all images and bootstrap .env from .env.example
#   make run      Start all services in the background
#   make stop     Stop all services (data preserved)
#   make test     Run dbt tests + pytest suite
#   make docs     Generate and serve the dbt documentation site
#   make clean    Stop services AND remove volumes (destructive — prompts first)
#   make logs     Tail logs from all services
#   make lint     Run ruff + sqlfluff linters
#   make trigger  Trigger the master_pipeline DAG via the Airflow REST API
#
# Recipes use POSIX shell syntax. On Windows, run these targets from Git Bash
# or WSL (GNU Make + a POSIX shell), which matches this project's tooling.
# =============================================================================

# Load .env (if present) so secrets like AIRFLOW_ADMIN_PASSWORD are available
# to recipes such as `make trigger`. Missing file is not an error.
ifneq (,$(wildcard .env))
include .env
export
endif

# ---- Configuration ----------------------------------------------------------
DC                 := docker compose
AIRFLOW_HOST       ?= http://127.0.0.1:8080
AIRFLOW_ADMIN_USER ?= admin
MASTER_DAG_ID      ?= master_pipeline

.DEFAULT_GOAL := help
.PHONY: help setup run stop test docs clean logs lint trigger

# ---- help -------------------------------------------------------------------
help: ## Show this help message
	@echo "Customer Intelligence Platform — available targets:"
	@echo "  setup    Build/pull images and create .env from .env.example"
	@echo "  run      Start all services in the background"
	@echo "  stop     Stop all services (volumes preserved)"
	@echo "  test     Run dbt tests + pytest"
	@echo "  docs     Generate and serve dbt documentation"
	@echo "  clean    Stop services and remove volumes (DESTRUCTIVE)"
	@echo "  logs     Tail logs from all services"
	@echo "  lint     Run ruff + sqlfluff linters"
	@echo "  trigger  Trigger the master_pipeline DAG via Airflow REST API"

# ---- setup ------------------------------------------------------------------
setup: ## Build/pull images and bootstrap .env
	$(DC) build
	$(DC) pull
	@test -f .env || (cp .env.example .env && echo 'Copied .env.example to .env — edit secrets before running')

# ---- run --------------------------------------------------------------------
run: ## Start all services in detached mode
	$(DC) up -d

# ---- stop -------------------------------------------------------------------
stop: ## Stop all services (data preserved)
	$(DC) down

# ---- test -------------------------------------------------------------------
test: ## Run dbt tests inside the scheduler container, then pytest
	$(DC) run --rm airflow-scheduler bash -c "cd /opt/airflow && dbt test --profiles-dir dbt/"
	pytest tests/

# ---- docs -------------------------------------------------------------------
docs: ## Generate and serve the dbt documentation site
	$(DC) run --rm airflow-scheduler bash -c "dbt docs generate --profiles-dir dbt/ && dbt docs serve --profiles-dir dbt/"

# ---- clean ------------------------------------------------------------------
clean: ## Stop services and remove volumes (DESTRUCTIVE — prompts for confirmation)
	@echo "WARNING: this removes all named volumes (postgres_data, airflow_logs,"
	@echo "         mlflow_artifacts, metabase_data) — ALL pipeline data will be lost."
	@read -p "Are you sure you want to continue? [y/N] " ans; \
	if [ "$$ans" = "y" ] || [ "$$ans" = "Y" ]; then \
		$(DC) down -v; \
	else \
		echo "Aborted — no volumes removed."; \
	fi

# ---- logs -------------------------------------------------------------------
logs: ## Tail logs from all services
	$(DC) logs -f

# ---- lint -------------------------------------------------------------------
lint: ## Run ruff (Python) and sqlfluff (dbt SQL) linters
	ruff check .
	sqlfluff lint dbt/models/

# ---- trigger ----------------------------------------------------------------
trigger: ## Trigger the master_pipeline DAG via the Airflow REST API
	@test -n "$(AIRFLOW_ADMIN_PASSWORD)" || { echo "AIRFLOW_ADMIN_PASSWORD is not set (create .env from .env.example)"; exit 1; }
	curl -sS -X POST "$(AIRFLOW_HOST)/api/v1/dags/$(MASTER_DAG_ID)/dagRuns" \
		-H "Content-Type: application/json" \
		--user "$(AIRFLOW_ADMIN_USER):$(AIRFLOW_ADMIN_PASSWORD)" \
		-d '{"conf": {}}'
