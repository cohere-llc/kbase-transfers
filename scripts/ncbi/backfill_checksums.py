#!/usr/bin/env python3
"""
Backfill CRC64/NVME checksums on existing NCBI objects in S3/MinIO.

Downloads each object to a temp file, computes CRC64/NVME locally, and
re-uploads with the checksum.  Existing user metadata (e.g. MD5) is
preserved.  MinIO does not support computing CRC64/NVME during
server-side copies, so a full round-trip is required.

Requires MinIO >= 2025-02-07T23-21-09Z for CRC64/NVME support.

Usage:
    python backfill_checksums.py [--prefix PREFIX] [--dry-run] [--limit N]
"""

import argparse
import base64
import logging
import os
import sys
import tempfile

from awscrt.checksums import crc64nvme as _crc64nvme

from kbase_transfers import MinioClient

BUCKET = "cdm-lake"
DEFAULT_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"

logger = logging.getLogger(__name__)


def _compute_crc64nvme(file_path):
    """Compute CRC64/NVME checksum of a file, return base64-encoded string."""
    crc = 0
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            crc = _crc64nvme(chunk, crc)
    return base64.b64encode(crc.to_bytes(8, byteorder="big")).decode()


def backfill(prefix, dry_run=False, limit=None, bucket=None, client=None):
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET
    s3 = client.s3

    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    skipped = 0
    failed = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # HEAD the object to check for existing CRC64NVME and get metadata
            try:
                head = s3.head_object(
                    Bucket=bucket, Key=key, ChecksumMode="ENABLED"
                )
            except Exception as e:
                logger.error(f"Failed to HEAD {key}: {e}")
                failed += 1
                if limit and (count + skipped + failed) >= limit:
                    break
                continue

            if head.get("ChecksumCRC64NVME"):
                skipped += 1
                continue

            if dry_run:
                logger.info(f"[dry-run] would backfill: {key}")
                count += 1
            else:
                try:
                    existing_metadata = head.get("Metadata", {})
                    content_type = head.get("ContentType", "application/octet-stream")

                    # Download to temp file, compute checksum, re-upload
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        tmp_path = tmp.name
                    try:
                        s3.download_file(bucket, key, tmp_path)
                        cksum = _compute_crc64nvme(tmp_path)
                        s3.upload_file(
                            tmp_path,
                            bucket,
                            key,
                            ExtraArgs={
                                "Metadata": existing_metadata,
                                "ContentType": content_type,
                                "ChecksumAlgorithm": "CRC64NVME",
                                "ChecksumCRC64NVME": cksum,
                            },
                        )
                    finally:
                        os.unlink(tmp_path)

                    count += 1
                    if count % 100 == 0:
                        logger.info(f"Backfilled {count} objects so far...")
                except Exception as e:
                    logger.error(f"Failed to backfill {key}: {e}")
                    failed += 1

            if limit and (count + skipped + failed) >= limit:
                break
        if limit and (count + skipped + failed) >= limit:
            break

    logger.info(f"Done. Backfilled: {count}, Skipped (already had checksum): {skipped}, Failed: {failed}")
    return {"backfilled": count, "skipped": skipped, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description="Backfill CRC64/NVME checksums on existing S3 objects")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help=f"S3 prefix to scan (default: {DEFAULT_PREFIX})")
    parser.add_argument("--dry-run", action="store_true", help="List objects that would be backfilled without making changes")
    parser.add_argument("--limit", type=int, help="Stop after processing this many objects")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    backfill(args.prefix, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
