# kbase-transfers

Data transfer scripts for KBase Lakehouse. This repository contains scripts that transfer data from various sources (NCBI, SPIRE, etc.) to a MinIO object store.

## Repository Structure

```
kbase-transfers/
├── kbase_transfers/          # Shared Python package
│   ├── __init__.py
│   └── minio_client.py       # MinIO S3 client for object storage
├── scripts/
│   ├── ncbi/                 # NCBI genome download scripts
│   └── spire/                # SPIRE data transfer scripts  
├── tests/                    # Tests for shared package code
├── pyproject.toml            # Package configuration
├── README.md
└── LICENSE
```

## Setup

### Install uv

This project uses [uv](https://docs.astral.sh/uv/) for fast Python package management:

```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install the Package

Install dependencies and the `kbase_transfers` package:

```bash
# Install all dependencies in a virtual environment
uv sync
```


### Running Scripts

You can run scripts directly with `uv run`:

```bash
uv run python scripts/ncbi/download_genomes.py test_list.txt
```

Scripts can import the shared MinIO client:

```python
from kbase_transfers import MinioClient

client = MinioClient()
# Use the client...
```

### MinIO Configuration

The MinIO client can be configured via environment variables:

- `MINIO_ENDPOINT_URL` - MinIO server URL (default: `http://localhost:9000`)
- `MINIO_ACCESS_KEY` - Access key (default: `minioadmin`)
- `MINIO_SECRET_KEY` - Secret key (default: `minioadmin`)

## Testing with Containerized MinIO

To test scripts locally, set up a containerized MinIO instance:

### 1. Start MinIO Server

Using Docker:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -d docker.io/minio/minio server /data --console-address ":9001"
```

Or using Podman:
```bash
podman run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  -d docker.io/minio/minio server /data --console-address ":9001"
```

### 2. Run Tests

**Note:** The tests automatically create the required MinIO buckets and folder structure, so no manual configuration is needed.

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest tests/ -v

# Or run specific test files
uv run pytest tests/test_minio_client.py -v
uv run pytest tests/test_nayfach_integration.py -v
uv run pytest tests/test_ncbi_integration.py -v
```

## Test Suite

The project includes both unit tests and integration tests:

### Unit Tests

- **test_minio_client.py** - Tests for MinIO client functions:
  - File upload/download
  - JSON object upload
  - Bucket and prefix existence checks
  - Object listing

### Integration Tests

- **test_nayfach_integration.py** - End-to-end test for the Nayfach 2020 dataset:
  - Downloads the Excel file if needed
  - Loads sample records to MinIO
  - Verifies data integrity and structure
  - Cleans up test data

- **test_ncbi_integration.py** - End-to-end test for NCBI genome downloads:
  - Uses test accessions from test assets
  - Downloads genome files from NCBI FTP
  - Verifies upload to MinIO
  - Validates file structure and metadata
  - Cleans up test data

### Test Assets

Test data is stored in `tests/assets/`:
- `ncbi_test_accessions.txt` - Sample NCBI accessions for testing

Run tests with: `uv run pytest tests/ -v`

## Scripts

See individual script directories for specific documentation:

- [scripts/ncbi/](scripts/ncbi/README.md) - NCBI genome downloads
- [scripts/nayfach_2020/](scripts/nayfach_2020/README.md) - Nayfach et al. 2020 metagenome and MAG dataset
- [scripts/spire/](scripts/spire/README.md) - SPIRE data transfers
