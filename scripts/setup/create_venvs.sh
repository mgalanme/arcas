#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/setup/create_venvs.sh
# Creates all three Python virtual environments for the project.
#
# LESSON LEARNED (spaCy): spacy==3.7.6 is yanked due to incorrect
# compatibility for transformer models. Use >=3.8.0,<4.0.
#
# LESSON LEARNED (venvs): LangGraph and CrewAI have conflicting dependency
# trees. Two separate venvs are mandatory, not optional.
# =============================================================================

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_ROOT"

echo "============================================================"
echo "ARCAS - Creating Python virtual environments"
echo "Project root: $PROJECT_ROOT"
echo "============================================================"

# Check uv is available
if ! command -v uv &> /dev/null; then
  echo "ERROR: 'uv' is not installed."
  echo "Install with: curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi

# Check Python 3.11 is available
if ! python3.11 --version &> /dev/null; then
  echo "ERROR: Python 3.11 is not installed."
  echo "Install with: sudo apt-get install python3.11 python3.11-venv"
  exit 1
fi

# =============================================================================
# VENV 1: .venv-langchain
# LangGraph orchestration, NLP pipeline, ingestion connectors,
# knowledge graph, vector store, FastAPI, audit service
# =============================================================================
echo ""
echo "------------------------------------------------------------"
echo "[1/3] Creating .venv-langchain..."
echo "------------------------------------------------------------"

uv venv .venv-langchain --python 3.11
source .venv-langchain/bin/activate

uv pip install \
  "langchain==0.3.7" \
  "langgraph==0.2.53" \
  "langchain-groq==0.2.1" \
  "langsmith==0.1.141" \
  "langchain-community==0.3.7" \
  "langchain-core==0.3.19" \
  "spacy>=3.8.0,<4.0" \
  "transformers==4.46.1" \
  "sentence-transformers==3.2.1" \
  "torch==2.5.1" \
  "neo4j==5.25.0" \
  "qdrant-client==1.11.3" \
  "kafka-python==2.0.2" \
  "fastavro==1.9.7" \
  "fastapi==0.115.4" \
  "uvicorn[standard]==0.32.0" \
  "pydantic==2.9.2" \
  "pydantic-settings==2.6.1" \
  "psycopg2-binary==2.9.10" \
  "sqlalchemy==2.0.36" \
  "alembic==1.13.3" \
  "redis==5.2.0" \
  "minio==7.2.10" \
  "pyarrow==18.0.0" \
  "duckdb==1.1.3" \
  "opentelemetry-sdk==1.28.0" \
  "opentelemetry-exporter-otlp==1.28.0" \
  "opentelemetry-instrumentation-fastapi==0.49b0" \
  "opentelemetry-instrumentation-sqlalchemy==0.49b0" \
  "opentelemetry-instrumentation-redis==0.49b0" \
  "cryptography==43.0.3" \
  "python-dotenv==1.0.1" \
  "playwright==1.48.0" \
  "scrapy==2.12.0" \
  "feedparser==6.0.11" \
  "httpx==0.27.2" \
  "pytest==8.3.3" \
  "pytest-asyncio==0.24.0" \
  "pytest-cov==6.0.0" \
  "ruff==0.7.2" \
  "pyvis==0.3.2"

# Download spaCy language models
echo ""
echo "Downloading spaCy language models (this may take a few minutes)..."
python -m spacy download es_core_news_lg
python -m spacy download en_core_web_lg

# Install Playwright browsers (Chromium only to save disk space)
echo ""
echo "Installing Playwright Chromium browser..."
playwright install chromium --with-deps

# Verify key imports
echo ""
echo "Verifying key imports..."
python -c "import langchain; print(f'  langchain {langchain.__version__} OK')"
python -c "import langgraph; print(f'  langgraph {langgraph.__version__} OK')"
python -c "import spacy; print(f'  spaCy {spacy.__version__} OK')"
python -c "import spacy; spacy.load('es_core_news_lg'); print('  es_core_news_lg OK')"
python -c "import spacy; spacy.load('en_core_web_lg'); print('  en_core_web_lg OK')"
python -c "import neo4j; print(f'  neo4j {neo4j.__version__} OK')"
python -c "import qdrant_client; print(f'  qdrant-client {qdrant_client.__version__} OK')"
python -c "import fastapi; print(f'  fastapi {fastapi.__version__} OK')"

deactivate
echo ""
echo ".venv-langchain created successfully."

# =============================================================================
# VENV 2: .venv-crewai
# CrewAI specialist agent crews
# =============================================================================
echo ""
echo "------------------------------------------------------------"
echo "[2/3] Creating .venv-crewai..."
echo "------------------------------------------------------------"

uv venv .venv-crewai --python 3.11
source .venv-crewai/bin/activate

uv pip install \
  "crewai==0.80.0" \
  "crewai-tools==0.14.0" \
  "langchain-groq==0.2.1" \
  "langsmith==0.1.141" \
  "opentelemetry-sdk==1.28.0" \
  "opentelemetry-exporter-otlp==1.28.0" \
  "python-dotenv==1.0.1" \
  "pydantic==2.9.2" \
  "psycopg2-binary==2.9.10" \
  "pytest==8.3.3" \
  "pytest-asyncio==0.24.0"

# Verify
python -c "import crewai; print(f'  crewai {crewai.__version__} OK')"

deactivate
echo ""
echo ".venv-crewai created successfully."

# =============================================================================
# VENV 3: .venv-dashboard
# Streamlit demo application
# =============================================================================
echo ""
echo "------------------------------------------------------------"
echo "[3/3] Creating .venv-dashboard..."
echo "------------------------------------------------------------"

uv venv .venv-dashboard --python 3.11
source .venv-dashboard/bin/activate

uv pip install \
  "streamlit==1.40.0" \
  "plotly==5.24.1" \
  "pyvis==0.3.2" \
  "pandas==2.2.3" \
  "duckdb==1.1.3" \
  "psycopg2-binary==2.9.10" \
  "httpx==0.27.2" \
  "python-dotenv==1.0.1" \
  "fpdf2==2.8.1" \
  "python-multipart==0.0.12" \
  "cryptography==43.0.3"

# Verify
python -c "import streamlit; print(f'  streamlit {streamlit.__version__} OK')"
python -c "import duckdb; print(f'  duckdb {duckdb.__version__} OK')"
python -c "import pyvis; print(f'  pyvis {pyvis.__version__} OK')"

deactivate
echo ""
echo ".venv-dashboard created successfully."

# =============================================================================
# SUMMARY
# =============================================================================
echo ""
echo "============================================================"
echo "All virtual environments created successfully."
echo ""
echo "Activate with:"
echo "  source .venv-langchain/bin/activate    # LangGraph / NLP / API"
echo "  source .venv-crewai/bin/activate       # CrewAI specialist crews"
echo "  source .venv-dashboard/bin/activate    # Streamlit demo"
echo ""
echo "Run tests with:"
echo "  source .venv-langchain/bin/activate"
echo "  PYTHONPATH=. pytest tests/unit/ -v"
echo "============================================================"
