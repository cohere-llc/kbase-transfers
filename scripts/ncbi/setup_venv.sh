#!/bin/bash
# Setup script for ncbi-download Python virtual environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VENV_DIR="${SCRIPT_DIR}/venv"

echo "Creating Python virtual environment..."
python3 -m venv "${VENV_DIR}"

echo "Activating virtual environment..."
source "${VENV_DIR}/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip

echo "Installing kbase_transfers package from repository root..."
pip install -e "${REPO_ROOT}"

if [ -f "${SCRIPT_DIR}/requirements.txt" ]; then
    echo "Installing additional script-specific dependencies..."
    pip install -r "${SCRIPT_DIR}/requirements.txt"
fi

echo ""
echo "✓ Virtual environment setup complete!"
echo ""
echo "To activate the environment, run:"
echo "  source ${VENV_DIR}/bin/activate"
echo ""
echo "Available scripts:"
echo "  - download_genomes.py: Download genome files from NCBI"
echo ""
echo "To run tests (from repository root):"
echo "  python -m pytest tests/test_minio_client.py"
echo ""
echo "To deactivate when done:"
echo "  deactivate"
