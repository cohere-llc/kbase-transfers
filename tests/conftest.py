"""Shared pytest configuration and fixtures for the test suite."""

import sys
from pathlib import Path

import pytest

# ── Make scripts importable without per-file sys.path hacks ──────────────
_repo_root = Path(__file__).parent.parent
_scripts_ncbi = _repo_root / "scripts" / "ncbi"

for p in (_repo_root, _scripts_ncbi):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)


# ── Integration-test marker with auto-skip ───────────────────────────────

def _minio_reachable():
    """Return True if the MinIO endpoint accepts connections."""
    try:
        from kbase_transfers import MinioClient
        client = MinioClient()
        client.list_buckets()
        return True
    except Exception:
        return False


_minio_available = None  # lazy singleton


def pytest_collection_modifyitems(config, items):
    """Auto-skip tests marked ``integration`` when MinIO is not reachable."""
    global _minio_available
    if _minio_available is None:
        _minio_available = _minio_reachable()

    if _minio_available:
        return

    skip_marker = pytest.mark.skip(reason="MinIO not reachable")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_marker)
