#!/usr/bin/env python3
"""
Promote staged files from CTS output to the final Lakehouse paths in MinIO.

Phase 3 of the automated NCBI transfer pipeline.

After the CTS container job completes, the downloaded files are in the CTS
output prefix in S3.  This script copies them to the final Lakehouse path,
attaching CRC64/NVME checksums (computed by boto3 during upload) and storing
NCBI MD5 checksums as user metadata.  It also archives replaced/suppressed
assemblies.

Usage:
    python promote_staged_files.py \\
        --staging-prefix tenant-general-warehouse/kbase/datasets/ncbi/staging/output/ \\
        --removed-manifest removed_manifest_20260415.txt \\
        --ncbi-release 229

    # Or with a local staging directory (for testing without CTS)
    python promote_staged_files.py \\
        --staging-dir ./test_output/ \\
        --removed-manifest removed_manifest_20260415.txt
"""

import argparse
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from kbase_transfers import MinioClient

BUCKET = os.environ.get("MINIO_BUCKET", "cdm-lake")
PATH_PREFIX = os.environ.get(
    "MINIO_PATH_PREFIX", "tenant-general-warehouse/kbase/datasets/ncbi/"
)

logger = logging.getLogger(__name__)


# ── Promote from local staging directory ─────────────────────────────────


def promote_from_local(
    staging_dir,
    removed_manifest=None,
    ncbi_release=None,
    dry_run=False,
    client=None,
    bucket=None,
):
    """Promote files from a local staging directory to MinIO.

    This is used when:
    - Testing locally (files downloaded by container_download.py)
    - CTS has been bypassed and files are on local disk

    Walks the staging directory for raw_data/ structure, reads MD5
    sidecar files for metadata, and uploads to the final Lakehouse
    path with CRC64/NVME checksums (computed by boto3 during upload).
    """
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET

    staging = Path(staging_dir)
    raw_data = staging / "raw_data"

    if not raw_data.exists():
        logger.error("No raw_data/ directory found in %s", staging_dir)
        return {"promoted": 0, "archived": 0, "failed": 0}

    promoted = 0
    failed = 0

    # Walk all assembly directories
    for assembly_dir_path in _find_assembly_dirs(raw_data):
        rel = assembly_dir_path.relative_to(staging)
        s3_prefix = PATH_PREFIX + str(rel) + "/"

        for file_path in assembly_dir_path.iterdir():
            if file_path.is_dir():
                continue
            # Skip sidecar files — they're metadata, not data files
            if file_path.suffix in (".crc64nvme", ".md5"):
                continue

            s3_key = PATH_PREFIX + str(file_path.relative_to(staging))
            md5_sidecar = file_path.parent / f"{file_path.name}.md5"

            metadata = {}
            if md5_sidecar.exists():
                metadata["md5"] = md5_sidecar.read_text().strip()

            if dry_run:
                logger.info("[dry-run] would promote: %s -> %s", file_path, s3_key)
                promoted += 1
                continue

            try:
                extra = {"checksum_algorithm": "CRC64NVME"}
                if metadata:
                    extra["metadata"] = metadata

                client.upload_file(
                    bucket, s3_key, str(file_path), **extra
                )
                promoted += 1
                logger.debug("  Promoted: %s", s3_key)
            except Exception as e:
                logger.error("Failed to promote %s: %s", s3_key, e)
                failed += 1

    # Handle removed assemblies
    archived = 0
    if removed_manifest and os.path.isfile(removed_manifest):
        archived = _archive_removed(
            removed_manifest,
            ncbi_release=ncbi_release,
            dry_run=dry_run,
            client=client,
            bucket=bucket,
        )

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "promoted": promoted,
        "archived": archived,
        "failed": failed,
        "dry_run": dry_run,
    }

    logger.info(
        "PROMOTE SUMMARY: %d promoted, %d archived, %d failed%s",
        promoted,
        archived,
        failed,
        " (dry-run)" if dry_run else "",
    )
    return report


# ── Promote from S3 staging prefix ──────────────────────────────────────


def promote_from_s3(
    staging_prefix,
    removed_manifest=None,
    ncbi_release=None,
    dry_run=False,
    client=None,
    bucket=None,
):
    """Promote files from an S3 staging prefix to the final Lakehouse path.

    Downloads each file to a temp location and re-uploads to the final
    path with CRC64/NVME checksum (computed by boto3) and MD5 metadata
    from sidecar files.
    """
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET

    s3 = client.s3
    paginator = s3.get_paginator("list_objects_v2")

    promoted = 0
    failed = 0

    # Collect all objects under the staging prefix
    staged_objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=staging_prefix):
        for obj in page.get("Contents", []):
            staged_objects.append(obj["Key"])

    # Separate data files from sidecars
    sidecars = {k for k in staged_objects if k.endswith((".crc64nvme", ".md5"))}
    data_files = [k for k in staged_objects if k not in sidecars]

    logger.info(
        "Found %d data files and %d sidecars in staging",
        len(data_files),
        len(sidecars),
    )

    for staged_key in data_files:
        # Skip the download report
        if staged_key.endswith("download_report.json"):
            continue

        # Compute final key by replacing the staging prefix with the lakehouse prefix
        rel_path = staged_key[len(staging_prefix):]
        if not rel_path.startswith("raw_data/"):
            continue
        final_key = PATH_PREFIX + rel_path

        md5_key = staged_key + ".md5"

        if dry_run:
            logger.info("[dry-run] would promote: %s -> %s", staged_key, final_key)
            promoted += 1
            continue

        try:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_path = tmp.name
            try:
                s3.download_file(bucket, staged_key, tmp_path)

                # Read MD5 sidecar for user metadata
                metadata = {}
                if md5_key in sidecars:
                    md5_obj = s3.get_object(Bucket=bucket, Key=md5_key)
                    metadata["md5"] = md5_obj["Body"].read().decode().strip()

                extra = {"checksum_algorithm": "CRC64NVME"}
                if metadata:
                    extra["metadata"] = metadata

                client.upload_file(bucket, final_key, tmp_path, **extra)
                promoted += 1
            finally:
                os.unlink(tmp_path)
        except Exception as e:
            logger.error("Failed to promote %s: %s", staged_key, e)
            failed += 1

    # Handle removed assemblies
    archived = 0
    if removed_manifest and os.path.isfile(removed_manifest):
        archived = _archive_removed(
            removed_manifest,
            ncbi_release=ncbi_release,
            dry_run=dry_run,
            client=client,
            bucket=bucket,
        )

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "promoted": promoted,
        "archived": archived,
        "failed": failed,
        "dry_run": dry_run,
    }

    logger.info(
        "PROMOTE SUMMARY: %d promoted, %d archived, %d failed%s",
        promoted,
        archived,
        failed,
        " (dry-run)" if dry_run else "",
    )
    return report


# ── Archive replaced/suppressed assemblies ──────────────────────────────


def _archive_removed(
    removed_manifest, ncbi_release=None, dry_run=False, client=None, bucket=None
):
    """Move replaced/suppressed assemblies to archive/ with version tags."""
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET

    release_tag = ncbi_release or "unknown"
    datestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    archived = 0

    with open(removed_manifest) as f:
        accessions = [line.strip() for line in f if line.strip()]

    for accession in accessions:
        # Build the source prefix from accession
        m = re.match(r"(GC[AF])_(\d{3})(\d{3})(\d{3})\.\d+", accession)
        if not m:
            logger.warning("Cannot parse accession for archival: %s", accession)
            continue

        db = m.group(1)
        p1, p2, p3 = m.group(2), m.group(3), m.group(4)
        source_prefix = f"{PATH_PREFIX}raw_data/{db}/{p1}/{p2}/{p3}/"

        # Find assembly directories matching this accession
        paginator = client.s3.get_paginator("list_objects_v2")
        objects_to_move = []
        for page in paginator.paginate(Bucket=bucket, Prefix=source_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if accession in key:
                    objects_to_move.append(key)

        if not objects_to_move:
            logger.debug("No objects found for %s, skipping archive", accession)
            continue

        for source_key in objects_to_move:
            # Build archive key: archive/<release>/raw_data/...
            rel = source_key[len(PATH_PREFIX):]
            archive_key = f"{PATH_PREFIX}archive/{release_tag}/{rel}"

            if dry_run:
                logger.info(
                    "[dry-run] would archive: %s -> %s", source_key, archive_key
                )
                archived += 1
                continue

            try:
                # Server-side copy to archive path with metadata tags
                client.s3.copy_object(
                    Bucket=bucket,
                    Key=archive_key,
                    CopySource={"Bucket": bucket, "Key": source_key},
                    Metadata={
                        "ncbi_last_release": release_tag,
                        "archive_reason": "replaced_or_suppressed",
                        "archive_date": datestamp,
                    },
                    MetadataDirective="REPLACE",
                )
                # Delete the original
                client.s3.delete_object(Bucket=bucket, Key=source_key)
                archived += 1
                logger.debug("  Archived: %s -> %s", source_key, archive_key)
            except Exception as e:
                logger.error("Failed to archive %s: %s", source_key, e)

    logger.info("Archived %d objects for %d accessions", archived, len(accessions))
    return archived


# ── Helpers ──────────────────────────────────────────────────────────────


def _find_assembly_dirs(raw_data_path):
    """Walk a raw_data/ directory tree and yield assembly directory paths."""
    pattern = re.compile(r"^GC[AF]_\d{9}\.\d+_")
    for root, dirs, files in os.walk(raw_data_path):
        dirname = os.path.basename(root)
        if pattern.match(dirname):
            yield Path(root)
            dirs.clear()  # Don't descend further


# ── CLI ──────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Promote staged NCBI files to the final Lakehouse path in MinIO",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Promote from S3 staging (after CTS job)
  python promote_staged_files.py \\
      --staging-prefix tenant-general-warehouse/kbase/datasets/ncbi/staging/output/ \\
      --removed-manifest removed_manifest_20260415.txt \\
      --ncbi-release 229

  # Promote from local directory (testing without CTS)
  python promote_staged_files.py \\
      --staging-dir ./test_output/ \\
      --removed-manifest removed_manifest_20260415.txt

  # Dry run
  python promote_staged_files.py \\
      --staging-dir ./test_output/ \\
      --dry-run
        """,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--staging-prefix",
        metavar="PREFIX",
        help="S3 prefix where CTS output was written",
    )
    group.add_argument(
        "--staging-dir",
        metavar="DIR",
        help="Local directory with downloaded files (for testing)",
    )
    parser.add_argument(
        "--removed-manifest",
        metavar="PATH",
        help="Path to the removed_manifest file (replaced/suppressed accessions)",
    )
    parser.add_argument(
        "--ncbi-release",
        metavar="VER",
        help="NCBI release version to tag archived assemblies with (e.g., 229)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be promoted/archived without making changes",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if args.staging_dir:
        report = promote_from_local(
            staging_dir=args.staging_dir,
            removed_manifest=args.removed_manifest,
            ncbi_release=args.ncbi_release,
            dry_run=args.dry_run,
        )
    else:
        report = promote_from_s3(
            staging_prefix=args.staging_prefix,
            removed_manifest=args.removed_manifest,
            ncbi_release=args.ncbi_release,
            dry_run=args.dry_run,
        )

    # Write report
    report_path = "promote_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Report written to: %s", report_path)


if __name__ == "__main__":
    main()
