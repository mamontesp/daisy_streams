VENV_BIN ?= $(shell if [ -d ".venv/bin" ]; then echo ".venv/bin/"; fi)
PYTHON ?= $(VENV_BIN)python
PYTEST ?= $(VENV_BIN)pytest
PIP ?= $(VENV_BIN)pip

.PHONY: help setup up up-adapter up-ui down ps logs logs-adapter logs-garage logs-ui sync simulate test clean

help:
	@echo "Daisy Streams - Command Reference"
	@echo "  make setup         - Copy .env.example to .env and install dependencies"
	@echo "  make up            - Launch all services via Docker Compose"
	@echo "  make up-adapter    - Launch Garage and Snowflake Adapter only"
	@echo "  make up-ui         - Launch Kafka UI service only"
	@echo "  make down          - Stop and clean up Docker Compose containers"
	@echo "  make ps            - Check status of running containers"
	@echo "  make logs          - Follow all container logs"
	@echo "  make logs-adapter  - Follow Snowflake Adapter container logs"
	@echo "  make logs-garage   - Follow Garage container logs"
	@echo "  make logs-ui       - Follow Kafka UI container logs"
	@echo "  make sync          - Run single-pass Snowflake sync job locally"
	@echo "  make simulate      - Run change simulation in dry-run mode"
	@echo "  make test          - Run pytest test suite"
	@echo "  make clean         - Clean up temporary files, caches, and state files"

setup:
	@if [ ! -f .env ]; then cp .env.example .env; echo "Created .env from .env.example"; fi
	$(PIP) install -r requirements.txt

up:
	docker compose up -d garage kafka postgres kafka-ui
	docker compose up -d snowflake-adapter

up-adapter:
	docker compose up -d garage
	docker compose up -d snowflake-adapter

up-ui:
	docker compose up -d kafka-ui

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f

logs-adapter:
	docker compose logs -f snowflake-adapter

logs-garage:
	docker compose logs -f garage

logs-ui:
	docker compose logs -f kafka-ui

sync:
	$(PYTHON) services/snowflake_adapter/scripts/run_adapter.py

simulate:
	$(PYTHON) services/snowflake_adapter/scripts/simulate_changes.py --dry-run

test:
	$(PYTEST) services/snowflake_adapter/tests -v

clean:
	rm -rf .pytest_cache __pycache__ services/*/__pycache__ services/*/*/__pycache__ services/*/tests/__pycache__ .daisy_sync_state.json
