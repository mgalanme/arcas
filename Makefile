.PHONY: help start start-full stop status logs api dashboard ingest-test agent-test test lint push backup demo

COMPOSE=docker compose
PROFILES_DEV=--profile storage --profile messaging
PROFILES_FULL=--profile storage --profile messaging --profile streaming \
              --profile ai --profile governance --profile observability --profile registry

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

start: ## Start development stack (storage + messaging)
	$(COMPOSE) $(PROFILES_DEV) up -d

start-full: ## Start full stack (all profiles - demo only)
	$(COMPOSE) $(PROFILES_FULL) up -d

stop: ## Stop all services
	$(COMPOSE) $(PROFILES_FULL) down

status: ## Show service status and memory usage
	$(COMPOSE) ps
	@echo ""
	docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"

logs: ## Tail logs
	$(COMPOSE) logs -f --tail=50

api: ## Start FastAPI server
	source .venv-langchain/bin/activate && PYTHONPATH=. uvicorn src.arcas_api.main:app --reload --port 8000

dashboard: ## Start Streamlit dashboard
	source .venv-dashboard/bin/activate && PYTHONPATH=. streamlit run src/arcas_dashboard/app.py --server.port 8501

ingest-test: ## Smoke test ingestion (BOE, 1 day)
	source .venv-langchain/bin/activate && PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources boe --days-back 1

agent-test: ## Smoke test agents (synthetic data)
	source .venv-langchain/bin/activate && PYTHONPATH=. python scripts/maintenance/test_agent.py --synthetic --skip-llm

test: ## Run all tests
	source .venv-langchain/bin/activate && PYTHONPATH=. pytest tests/ -v

lint: ## Run ruff linter
	source .venv-langchain/bin/activate && ruff check src/ tests/

push: ## git add -A + commit + push
	git add -A
	git commit -m "$${MSG:-'chore: update'}"
	git push origin main

backup: ## Backup PostgreSQL and Neo4j to MinIO
	source .venv-langchain/bin/activate && PYTHONPATH=. python scripts/maintenance/backup.py

demo: ## Start full stack and launch dashboard
	$(COMPOSE) $(PROFILES_FULL) up -d
	sleep 30
	source .venv-dashboard/bin/activate && PYTHONPATH=. streamlit run src/arcas_dashboard/app.py --server.port 8501
