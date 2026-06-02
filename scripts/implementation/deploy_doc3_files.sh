#!/usr/bin/env bash
# =============================================================================
# ARCAS - scripts/implementation/deploy_doc3_files.sh
# Places all Document 3 source files into their correct project paths.
# Run from the project root after extracting the delivery ZIP.
#
# USAGE:
#   cd /home/pruebas/formacion/arcas
#   bash scripts/implementation/deploy_doc3_files.sh <source_dir>
#
# EXAMPLE:
#   bash scripts/implementation/deploy_doc3_files.sh ~/Downloads/arcas-scripts-v2/src
# =============================================================================
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SOURCE_DIR="${1:-./src}"
cd "$PROJECT_ROOT"

echo "===================================="
echo "ARCAS - Deploying Document 3 files"
echo "Project root: $PROJECT_ROOT"
echo "Source:       $SOURCE_DIR"
echo "===================================="

# Create all src directories
mkdir -p src/{arcas_ingest/{connectors/{gazette,procurement},scraper},arcas_nlp,arcas_knowledge_graph,arcas_vault,arcas_agents/{orchestrator,fraud,network,judicial,disinfo,enrichment},arcas_audit,arcas_api/routers,arcas_dashboard/{pages,hitl}}
mkdir -p scripts/implementation

echo ""
echo "Copying source files..."
if [ -d "$SOURCE_DIR" ]; then
    cp -rv "$SOURCE_DIR"/. src/
    echo "Source files deployed."
else
    echo "ERROR: Source directory not found: $SOURCE_DIR"
    exit 1
fi

echo ""
echo "Copying implementation scripts..."
IMPL_SRC="$(dirname "$SOURCE_DIR")/scripts/implementation"
if [ -d "$IMPL_SRC" ]; then
    cp -v "$IMPL_SRC"/*.py  scripts/implementation/ 2>/dev/null || true
    cp -v "$IMPL_SRC"/*.sh  scripts/implementation/ 2>/dev/null || true
fi

# Permissions
chmod +x scripts/implementation/*.sh 2>/dev/null || true

echo ""
echo "Creating __init__.py files..."
find src -type d -exec touch {}/__init__.py \;

echo ""
echo "===================================="
echo "Deployment complete."
echo ""
echo "Next steps:"
echo "  1. source .venv-langchain/bin/activate"
echo "  2. make start"
echo "  3. PYTHONPATH=. python scripts/implementation/run_ingestion.py --sources boe --days-back 1"
echo "  4. PYTHONPATH=. python scripts/implementation/run_agents.py --limit 10"
echo "  5. make dashboard"
echo "===================================="
