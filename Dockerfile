FROM python:3.12-slim

# CTS requires /bin/bash; git needed for dtspy dependency
RUN apt-get update && apt-get install -y --no-install-recommends bash git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project files
COPY pyproject.toml ./
COPY kbase_transfers/ kbase_transfers/
COPY scripts/ncbi/ scripts/ncbi/

# Install the package and dependencies
RUN uv pip install --system --no-cache .

ENTRYPOINT ["python", "scripts/ncbi/sync_genomes.py"]
