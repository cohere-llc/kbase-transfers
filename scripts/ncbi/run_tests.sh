#!/bin/bash
# Run tests for ncbi-download tools

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/venv"

# Check if virtual environment exists
if [ ! -d "${VENV_DIR}" ]; then
    echo "Virtual environment not found. Please run setup_venv.sh first."
    exit 1
fi

# Activate virtual environment
source "${VENV_DIR}/bin/activate"

echo "Running MinIO client tests..."
echo "Note: Make sure MinIO server is running (default: http://localhost:9000)"
echo ""

# Run tests
python -m unittest minio_client_test.py -v

echo ""
echo "✓ Tests complete!"
