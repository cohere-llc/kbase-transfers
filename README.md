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

### 2. Configure MinIO

1. Navigate to http://localhost:9001
2. Log in with username `minioadmin` and password `minioadmin`
3. Create a bucket named `cdm-lake` (or as required by your script)
4. Upload a test file or create the necessary folder structure for your script

### 3. Run Tests

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest tests/

# Or run a specific script
uv run python scripts/ncbi/download_genomes.py test_list.txt
```

## Scripts

See individual script directories for specific documentation:

- [scripts/ncbi/](scripts/ncbi/README.md) - NCBI genome downloads
- [scripts/spire/](scripts/spire/README.md) - SPIRE data transfers
