#!/usr/bin/env python3
"""
Sync NCBI RefSeq genome assemblies to S3/MinIO using assembly_summary_refseq.txt.

Instead of traversing millions of FTP directories, this script downloads the
NCBI assembly summary report to build the list of current assemblies, then
uses the existing download_genome_files() logic to sync each assembly.
download_genome_files() compares md5checksums.txt values against md5 metadata
stored on S3 objects, so unchanged files are skipped automatically.

Designed to run as a containerised CTS (CDM Task Service) job.

Usage:
    python sync_genomes.py [--dry-run] [--limit N] [--threads N]
"""

import argparse
import csv
import logging
import os
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from ftplib import FTP, error_temp
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from kbase_transfers import MinioClient

# Import shared helpers from the download script
sys.path.insert(0, str(Path(__file__).parent))
from download_genomes import (
    download_genome_files,
    minio_bucket,
    setup_logging,
    _set_ftp_keepalive,
)

SUMMARY_URL_FTP = "/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt"
FTP_HOST = "ftp.ncbi.nlm.nih.gov"

logger = logging.getLogger(__name__)


# ── Assembly summary parsing ───────────────────────────────────────────────

def download_assembly_summary(ftp_host=FTP_HOST):
    """Download assembly_summary_refseq.txt from NCBI FTP.

    Returns the raw text content as a string.
    """
    logger.info("Downloading assembly_summary_refseq.txt from NCBI FTP ...")
    ftp = FTP(ftp_host)
    ftp.login()
    _set_ftp_keepalive(ftp)

    lines = []
    ftp.retrlines(f"RETR {SUMMARY_URL_FTP}", lambda line: lines.append(line))
    ftp.quit()

    content = "\n".join(lines)
    logger.info(f"Downloaded {len(lines)} lines from assembly summary")
    return content


def parse_assembly_summary(content):
    """Parse assembly_summary_refseq.txt into a list of dicts.

    Columns of interest (0-indexed):
      0:  assembly_accession      (e.g. GCF_000001215.4)
      10: version_status          ("latest", "replaced", "suppressed")
      19: ftp_path                (full FTP URL or "na")

    Returns:
        dict mapping accession -> {status, ftp_path, assembly_dir}
    """
    assemblies = {}
    reader = csv.reader(
        (line for line in content.splitlines() if not line.startswith("#")),
        delimiter="\t",
    )
    for row in reader:
        if len(row) < 20:
            continue
        accession = row[0]
        status = row[10]               # latest / replaced / suppressed
        ftp_path = row[19]

        if ftp_path == "na":
            continue

        # Derive the assembly directory name from the FTP path
        # e.g. .../GCF_000001215.4_Release_6_plus_ISO1_MT
        assembly_dir = ftp_path.rstrip("/").split("/")[-1]

        assemblies[accession] = {
            "status": status,
            "ftp_path": ftp_path,
            "assembly_dir": assembly_dir,
        }

    logger.info(f"Parsed {len(assemblies)} assemblies from summary")
    return assemblies


def get_latest_assembly_paths(ncbi_assemblies, ftp_host=FTP_HOST):
    """Extract FTP paths for all 'latest' assemblies.

    Converts FTP URLs from the assembly summary into FTP paths suitable
    for download_genome_files().

    Returns:
        list of (accession, ftp_path) tuples
    """
    paths = []
    for accession, info in ncbi_assemblies.items():
        if info["status"] != "latest":
            continue
        ftp_url = info["ftp_path"]
        # Convert URL to path: https://ftp.ncbi.nlm.nih.gov/genomes/... -> /genomes/...
        if ftp_url.startswith("https://"):
            ftp_path = ftp_url.replace("https://ftp.ncbi.nlm.nih.gov", "")
        elif ftp_url.startswith("ftp://"):
            ftp_path = ftp_url.replace(f"ftp://{ftp_host}", "")
        else:
            ftp_path = ftp_url
        paths.append((accession, ftp_path.rstrip("/") + "/"))
    return paths


# ── Sync execution ────────────────────────────────────────────────────────

def sync(
    dry_run=False,
    limit=None,
    threads=4,
    ftp_host=FTP_HOST,
):
    """Run a full sync cycle.

    1. Download assembly_summary_refseq.txt to get the list of assemblies
    2. For each assembly, call download_genome_files() which compares
       NCBI md5checksums.txt against md5 metadata on S3 objects,
       skipping files that are already up to date
    """
    # 1. Download & parse assembly summary
    summary_content = download_assembly_summary(ftp_host=ftp_host)
    ncbi_assemblies = parse_assembly_summary(summary_content)

    # 2. Get FTP paths for latest assemblies
    assembly_paths = get_latest_assembly_paths(ncbi_assemblies, ftp_host=ftp_host)
    logger.info(f"Found {len(assembly_paths)} latest assemblies in NCBI summary")

    # Apply limit
    if limit:
        assembly_paths = assembly_paths[:limit]
        logger.info(f"Limiting to {len(assembly_paths)} assemblies")

    if dry_run:
        logger.info("=== DRY RUN ===")
        logger.info(f"Would sync {len(assembly_paths)} assemblies")
        for acc, path in assembly_paths[:20]:
            logger.info(f"  {acc}: {path}")
        if len(assembly_paths) > 20:
            logger.info(f"  ... and {len(assembly_paths) - 20} more")
        return

    # 3. Verify bucket exists
    s3_client = MinioClient()
    if not s3_client.bucket_exists(minio_bucket):
        logger.error(f"Bucket {minio_bucket} does not exist")
        sys.exit(1)

    # 4. Download assemblies using existing MD5-based sync logic
    if assembly_paths:
        logger.info(f"Syncing {len(assembly_paths)} assemblies ...")
        _sync_batch(assembly_paths, threads=threads, ftp_host=ftp_host)


def _sync_batch(assembly_paths, threads=4, ftp_host=FTP_HOST):
    """Sync a batch of assemblies using a thread pool.

    Each assembly is processed by download_genome_files(), which handles
    MD5 comparison against S3 metadata and skips unchanged files.
    """
    lock = threading.Lock()
    success_count = 0
    failed = []
    failed_transfers = []
    no_checksum_files = []

    def _download_one(accession, assembly_path):
        tmp_dir = tempfile.mkdtemp()
        thread_s3 = MinioClient()
        last_error = None
        try:
            for attempt in range(1, 4):
                try:
                    download_genome_files(
                        accession,
                        thread_s3,
                        tmp_dir,
                        failed_transfers,
                        no_checksum_files,
                        ftp_host=ftp_host,
                        assembly_path=assembly_path,
                    )
                    return accession, None
                except error_temp as e:
                    last_error = e
                    if attempt < 3:
                        logger.warning(
                            f"Transient FTP error for {accession}, "
                            f"retry {attempt}/3: {e}"
                        )
                        time.sleep(5)
                except Exception as e:
                    return accession, e
            return accession, last_error
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(_download_one, acc, path): acc
            for acc, path in assembly_paths
        }
        for future in as_completed(futures):
            accession, error = future.result()
            with lock:
                if error:
                    logger.error(f"FAILED: {accession}: {error}")
                    failed.append((accession, str(error)))
                else:
                    success_count += 1

    total = success_count + len(failed)
    logger.info(f"\n{'='*60}")
    logger.info(f"SYNC SUMMARY:")
    logger.info(f"  Total attempted: {total}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed: {len(failed)}")
    logger.info(f"  Failed file transfers: {len(failed_transfers)}")
    logger.info(f"  Files without checksums: {len(no_checksum_files)}")

    if failed:
        logger.error("Failed accessions:")
        for acc, err in failed:
            logger.error(f"  - {acc}: {err}")


def main():
    parser = argparse.ArgumentParser(
        description="Sync NCBI RefSeq genomes to S3 using assembly summary",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run — show what would be synced
  python sync_genomes.py --dry-run

  # Sync up to 100 assemblies
  python sync_genomes.py --limit 100 --threads 4

  # Full sync
  python sync_genomes.py --threads 8
        """,
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be synced without making changes")
    parser.add_argument("--limit", type=int, metavar="N", help="Limit to first N assemblies")
    parser.add_argument("--threads", type=int, default=4, metavar="N", help="Parallel download threads (default: 4)")
    parser.add_argument("--ftp-host", default=FTP_HOST, help=f"FTP host (default: {FTP_HOST})")
    args = parser.parse_args()

    log_file = setup_logging(module_name=__name__)
    logger.info(f"Logging to: {log_file}")

    sync(
        dry_run=args.dry_run,
        limit=args.limit,
        threads=args.threads,
        ftp_host=args.ftp_host,
    )


if __name__ == "__main__":
    main()
