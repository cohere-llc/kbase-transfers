#!/bin/bash
# Setup script for ncbi-download Python virtual environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/venv"

echo "Creating Python virtual environment..."
python3 -m venv "${VENV_DIR}"

echo "Activating virtual environment..."
source "${VENV_DIR}/bin/activate"

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r "${SCRIPT_DIR}/requirements.txt"

echo ""
echo "✓ Virtual environment setup complete!"
echo ""
echo "To activate the environment, run:"
echo "  source ${VENV_DIR}/bin/activate"
echo ""
echo "Available scripts:"
echo "  - download_genomes.py: Download genome files from NCBI"
echo "  - minio_client.py: MinIO client for object storage"
echo "  - minio_client_test.py: Tests for MinIO client"
echo ""
echo "To run tests:"
echo "  python -m unittest minio_client_test.py"
echo ""
echo "To deactivate when done:"
echo "  deactivate"
