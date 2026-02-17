"""
kbase_transfers - Shared utilities for KBase data transfer scripts
"""

__version__ = "0.1.0"

from .minio_client import MinioClient

__all__ = ["MinioClient"]
