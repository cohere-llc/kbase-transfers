# SPIRE Data Transfer Scripts

Scripts for transferring data from SPIRE to the KBase Lakehouse MinIO instance.

## Setup

### Install the kbase_transfers package

From the repository root:
```bash
pip install -e .
```

This installs the shared `kbase_transfers` package in editable mode, making the MinIO client available to all scripts.

## MinIO Configuration

The MinIO client can be configured via environment variables:

- `MINIO_ENDPOINT_URL` - MinIO server URL (default: `http://localhost:9000`)
- `MINIO_ACCESS_KEY` - Access key (default: `minioadmin`)
- `MINIO_SECRET_KEY` - Secret key (default: `minioadmin`)

For testing with a containerized MinIO instance, see the [main README](../../README.md#testing-with-containerized-minio).

## Using the MinIO Client

Example script structure:

```python
from kbase_transfers import MinioClient

# Initialize client (uses defaults or environment variables)
client = MinioClient()

# Upload a file
client.upload_file(
    bucket_name="cdm-lake",
    object_name="path/in/bucket/file.txt",
    file_path="/local/path/to/file.txt"
)

# List objects
objects = client.list_objects("cdm-lake", prefix="path/in/bucket/")
print(f"Found {len(objects)} objects")

# Download a file
client.download_file(
    bucket_name="cdm-lake",
    object_name="path/in/bucket/file.txt",
    file_path="/local/path/to/download/file.txt"
)
```

## Testing

Run the shared MinIO client tests from the repository root:

```bash
python -m pytest tests/test_minio_client.py -v
```
