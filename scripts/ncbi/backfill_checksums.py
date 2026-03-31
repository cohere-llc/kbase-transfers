#!/usr/bin/env python3
"""
Backfill CRC64/NVME checksums on existing NCBI objects in S3/MinIO.

Uses server-side copy to add CRC64/NVME checksums without downloading
or re-uploading data.  The S3 server reads the object internally and
computes the checksum during the copy.

Requires MinIO >= 2025-02-07T23-21-09Z for CRC64/NVME support.

Usage:
    python backfill_checksums.py [--prefix PREFIX] [--dry-run] [--limit N]
"""

import argparse
import logging
import sys

from kbase_transfers import MinioClient

BUCKET = "cdm-lake"
DEFAULT_PREFIX = "tenant-general-warehouse/kbase/datasets/ncbi/"

logger = logging.getLogger(__name__)


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
                    # Use REPLACE so S3 treats this as a real change.
                    # Re-supply existing user metadata to preserve it.
                    existing_metadata = head.get("Metadata", {})
                    s3.copy_object(
                        Bucket=bucket,
                        Key=key,
                        CopySource={"Bucket": bucket, "Key": key},
                        ChecksumAlgorithm="CRC64NVME",
                        MetadataDirective="REPLACE",
                        Metadata=existing_metadata,
                    )
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
