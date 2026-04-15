#!/usr/bin/env python3
"""
Download NCBI genome assemblies listed in a transfer manifest to a local
output directory.  Designed to run inside a CTS container where inputs are
on a local filesystem mount and outputs are written to another local mount.

Phase 2 of the automated NCBI transfer pipeline.

The container does NOT talk to S3/MinIO.  CTS copies the manifest into the
input directory before launch and copies the output directory to S3 after
the job finishes.

Usage:
    python container_download.py \\
        --manifest /job_input_dir/transfer_manifest.txt \\
        --output-dir /job_output_dir/ \\
        --threads 4
"""

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from ftplib import FTP, error_temp
from pathlib import Path

logger = logging.getLogger(__name__)

FTP_HOST = "ftp.ncbi.nlm.nih.gov"

# Same file filters as download_genomes.py
FILE_FILTERS = [
    "_gene_ontology.gaf.gz",
    "_genomic.fna.gz",
    "_genomic.gff.gz",
    "_protein.faa.gz",
    "_ani_contam_ranges.tsv",
    "_assembly_regions.txt",
    "_assembly_report.txt",
    "_assembly_stats.txt",
    "_gene_expression_counts.txt.gz",
    "_normalized_gene_expression_counts.txt.gz",
]


# ── Helpers ──────────────────────────────────────────────────────────────


def _set_ftp_keepalive(ftp, idle=30, interval=10, count=3):
    sock = ftp.sock
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)


def compute_md5(file_path):
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def parse_md5checksums(content):
    checksums = {}
    for line in content.strip().split("\n"):
        if line.strip():
            parts = line.split()
            if len(parts) >= 2:
                checksums[parts[1].lstrip("./")] = parts[0]
    return checksums


def build_accession_path(assembly_dir):
    """Build the S3-style relative path for an assembly directory."""
    m = re.match(r"GC[AF]_(\d{3})(\d{3})(\d{3})\.\d+.*", assembly_dir)
    if not m:
        raise ValueError(f"Cannot parse accession: {assembly_dir}")
    p1, p2, p3 = m.groups()
    return f"raw_data/{assembly_dir[:3]}/{p1}/{p2}/{p3}/{assembly_dir}/"


def parse_assembly_path(assembly_path):
    """Extract database, assembly_dir, accession from an FTP assembly path."""
    m = re.search(
        r"/(GC[AF])/\d{3}/\d{3}/\d{3}/((GC[AF]_\d{9}\.\d+)_[^/]+)/?$",
        assembly_path.rstrip("/"),
    )
    if not m:
        raise ValueError(f"Cannot parse assembly path: {assembly_path}")
    return m.group(1), m.group(2), m.group(3)


# ── Single assembly download ────────────────────────────────────────────


def download_assembly_to_local(
    assembly_path, output_dir, ftp_host=FTP_HOST
):
    """Download one assembly from NCBI FTP to a local directory.

    Creates the directory structure under output_dir matching the S3 layout
    (raw_data/GCF/000/001/215/GCF_000001215.4_Release_6.../), downloads
    filtered files, verifies MD5 checksums, and writes MD5 sidecar files
    for downstream metadata.  CTS computes CRC64/NVME checksums when it
    uploads the output files back to S3.

    Returns a dict with download stats, or raises on unrecoverable errors.
    """
    database, assembly_dir, accession = parse_assembly_path(assembly_path)
    rel_path = build_accession_path(assembly_dir)
    dest_dir = Path(output_dir) / rel_path
    dest_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Downloading %s -> %s", accession, dest_dir)

    ftp = FTP(ftp_host)
    ftp.login()
    _set_ftp_keepalive(ftp)

    stats = {
        "accession": accession,
        "assembly_dir": assembly_dir,
        "files_downloaded": 0,
        "files_skipped_checksum_mismatch": 0,
        "files_without_checksum": 0,
    }

    try:
        ftp.cwd(assembly_path.rstrip("/"))

        # List available files
        files = []
        ftp.retrlines("NLST", lambda x: files.append(x))

        # Download and parse md5checksums.txt
        md5_checksums = {}
        if "md5checksums.txt" in files:
            md5_content = []
            ftp.retrlines(
                "RETR md5checksums.txt", lambda x: md5_content.append(x)
            )
            md5_text = "\n".join(md5_content)
            md5_checksums = parse_md5checksums(md5_text)

            # Save md5checksums.txt
            md5_file = dest_dir / "md5checksums.txt"
            md5_file.write_text(md5_text)
            stats["files_downloaded"] += 1

        # Filter target files
        target_files = [
            f for f in files if any(f.endswith(s) for s in FILE_FILTERS)
        ]

        last_ftp_activity = time.monotonic()

        for filename in target_files:
            if time.monotonic() - last_ftp_activity > 25:
                try:
                    ftp.sendcmd("NOOP")
                    last_ftp_activity = time.monotonic()
                except Exception:
                    pass

            local_file = dest_dir / filename
            expected_md5 = md5_checksums.get(filename)

            # Download with retry
            success = False
            for attempt in range(1, 4):
                logger.debug(
                    "  Downloading %s (attempt %d/3)", filename, attempt
                )
                with open(local_file, "wb") as f:
                    ftp.retrbinary(f"RETR {filename}", f.write)
                last_ftp_activity = time.monotonic()

                if expected_md5:
                    actual_md5 = compute_md5(str(local_file))
                    if actual_md5 != expected_md5:
                        logger.warning(
                            "  MD5 mismatch for %s: expected %s, got %s",
                            filename,
                            expected_md5,
                            actual_md5,
                        )
                        if attempt < 3:
                            continue
                        stats["files_skipped_checksum_mismatch"] += 1
                        local_file.unlink(missing_ok=True)
                        break
                    else:
                        logger.debug("  MD5 verified: %s", filename)
                else:
                    stats["files_without_checksum"] += 1

                # Write MD5 sidecar (for promote step to store as S3 metadata)
                if expected_md5:
                    (dest_dir / f"{filename}.md5").write_text(expected_md5)

                stats["files_downloaded"] += 1
                success = True
                break

        logger.info(
            "  %s: %d files downloaded", accession, stats["files_downloaded"]
        )

    finally:
        try:
            ftp.quit()
        except Exception:
            pass

    return stats


# ── Batch download ───────────────────────────────────────────────────────


def download_batch(
    manifest_path, output_dir, threads=4, ftp_host=FTP_HOST, limit=None
):
    """Download all assemblies listed in the manifest.

    Returns a report dict with overall stats.
    """
    with open(manifest_path) as f:
        assembly_paths = [
            line.strip() for line in f if line.strip() and not line.startswith("#")
        ]

    if limit:
        assembly_paths = assembly_paths[:limit]

    logger.info(
        "Starting download of %d assemblies with %d threads",
        len(assembly_paths),
        threads,
    )

    lock = threading.Lock()
    success_count = 0
    failed = []
    all_stats = []

    def _download_one(path):
        nonlocal success_count
        last_error = None
        for attempt in range(1, 4):
            try:
                stats = download_assembly_to_local(
                    path, output_dir, ftp_host=ftp_host
                )
                with lock:
                    success_count += 1
                    all_stats.append(stats)
                return path, None
            except error_temp as e:
                last_error = e
                if attempt < 3:
                    logger.warning(
                        "Transient FTP error for %s, retry %d/3: %s",
                        path,
                        attempt,
                        e,
                    )
                    time.sleep(5)
            except Exception as e:
                return path, e
        return path, last_error

    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {
            executor.submit(_download_one, p): p for p in assembly_paths
        }
        for future in as_completed(futures):
            path, error = future.result()
            if error:
                logger.error("FAILED: %s: %s", path, error)
                with lock:
                    failed.append({"path": path, "error": str(error)})

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_attempted": len(assembly_paths),
        "succeeded": success_count,
        "failed": len(failed),
        "failures": failed,
        "assembly_stats": all_stats,
    }

    # Write report to output dir
    report_path = Path(output_dir) / "download_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Download report written to: %s", report_path)

    logger.info(
        "SUMMARY: %d attempted, %d succeeded, %d failed",
        len(assembly_paths),
        success_count,
        len(failed),
    )

    return report


# ── CLI ──────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Download NCBI assemblies from a manifest to a local directory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Designed to run inside a CTS container.  The container receives its
manifest via the CTS input mount and writes downloaded files to the
CTS output mount.  No S3/MinIO access is required.

Examples:
  # CTS container invocation
  python container_download.py \\
      --manifest /job_input_dir/transfer_manifest.txt \\
      --output-dir /job_output_dir/ \\
      --threads 4

  # Local testing
  python container_download.py \\
      --manifest transfer_manifest_20260415.txt \\
      --output-dir ./test_output/ \\
      --limit 2
        """,
    )
    parser.add_argument(
        "--manifest",
        default="/job_input_dir/transfer_manifest.txt",
        help="Path to the transfer manifest file (default: /job_input_dir/transfer_manifest.txt)",
    )
    parser.add_argument(
        "--output-dir",
        default="/job_output_dir/",
        help="Output directory for downloaded files (default: /job_output_dir/)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        metavar="N",
        help="Parallel download threads (default: 4)",
    )
    parser.add_argument(
        "--ftp-host",
        default=FTP_HOST,
        help=f"FTP host (default: {FTP_HOST})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="Limit to first N assemblies (for testing)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if not os.path.isfile(args.manifest):
        logger.error("Manifest not found: %s", args.manifest)
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    report = download_batch(
        manifest_path=args.manifest,
        output_dir=args.output_dir,
        threads=args.threads,
        ftp_host=args.ftp_host,
        limit=args.limit,
    )

    sys.exit(1 if report["failed"] > 0 else 0)


if __name__ == "__main__":
    main()
