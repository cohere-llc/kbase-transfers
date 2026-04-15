#!/usr/bin/env python3
"""
Generate a transfer manifest by diffing the current NCBI assembly summary
against a previously stored version (or the current state of objects in the
MinIO store).

Phase 1 of the automated NCBI transfer pipeline.

Usage:
    # Normal mode (diff against stored previous summary)
    python generate_transfer_manifest.py --database refseq

    # Fallback mode (no previous summary; reconstruct from store)
    python generate_transfer_manifest.py --database refseq --scan-store

    # Limit to a prefix range (for staging size control)
    python generate_transfer_manifest.py --database refseq --prefix-from 000 --prefix-to 003
"""

import argparse
import csv
import gzip
import io
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from ftplib import FTP
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from kbase_transfers import MinioClient

BUCKET = os.environ.get("MINIO_BUCKET", "cdm-lake")
PATH_PREFIX = os.environ.get(
    "MINIO_PATH_PREFIX", "tenant-general-warehouse/kbase/datasets/ncbi/"
)
FTP_HOST = "ftp.ncbi.nlm.nih.gov"

SUMMARY_FTP_PATHS = {
    "refseq": "/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt",
    "genbank": "/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt",
}

logger = logging.getLogger(__name__)


# ── Assembly summary download & parsing ──────────────────────────────────


def _set_ftp_keepalive(ftp, idle=30, interval=10, count=3):
    """Enable TCP keepalive on the FTP control connection."""
    import socket

    sock = ftp.sock
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)


def download_assembly_summary(database="refseq", ftp_host=FTP_HOST):
    """Download assembly_summary file from NCBI FTP to a temp file.

    Returns the path to the temp file. Caller is responsible for cleanup.
    """
    ftp_path = SUMMARY_FTP_PATHS.get(database)
    if not ftp_path:
        raise ValueError(f"Unknown database: {database}")

    logger.info(
        "Downloading assembly_summary_%s.txt from NCBI FTP ...", database
    )
    ftp = FTP(ftp_host)
    ftp.login()
    _set_ftp_keepalive(ftp)

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".tsv", prefix="assembly_summary_", delete=False
    )
    line_count = 0

    def _write_line(line):
        nonlocal line_count
        tmp.write(line + "\n")
        line_count += 1

    try:
        ftp.retrlines(f"RETR {ftp_path}", _write_line)
    finally:
        tmp.close()
        ftp.quit()

    logger.info("Downloaded %d lines from assembly summary", line_count)
    return tmp.name


def parse_assembly_summary(source):
    """Parse assembly_summary file into a dict of assemblies.

    Args:
        source: file path (str/Path) or iterable of lines.

    Columns of interest (0-indexed):
        0: assembly_accession (e.g. GCF_000001215.4)
        10: version_status ("latest", "replaced", "suppressed")
        14: seq_rel_date
        19: ftp_path (full FTP URL or "na")

    Returns:
        dict mapping accession -> {status, seq_rel_date, ftp_path, assembly_dir}
    """
    assemblies = {}

    def _parse_lines(lines):
        reader = csv.reader(
            (line.rstrip("\n") for line in lines if not line.startswith("#")),
            delimiter="\t",
        )
        for row in reader:
            if len(row) < 20:
                continue
            accession = row[0]
            status = row[10]
            seq_rel_date = row[14] if len(row) > 14 else ""
            ftp_path = row[19]

            if ftp_path == "na":
                continue

            assembly_dir = ftp_path.rstrip("/").split("/")[-1]

            assemblies[accession] = {
                "status": status,
                "seq_rel_date": seq_rel_date,
                "ftp_path": ftp_path,
                "assembly_dir": assembly_dir,
            }

    if isinstance(source, (str, Path)) and os.path.isfile(str(source)):
        with open(source) as f:
            _parse_lines(f)
    else:
        if isinstance(source, str):
            source = source.splitlines(keepends=True)
        _parse_lines(source)

    logger.info("Parsed %d assemblies from summary", len(assemblies))
    return assemblies


def get_latest_assembly_paths(ncbi_assemblies, ftp_host=FTP_HOST):
    """Extract FTP paths for all 'latest' assemblies.

    Returns list of (accession, ftp_dir_path) tuples where ftp_dir_path
    is an absolute FTP path ending with '/'.
    """
    paths = []
    for accession, info in ncbi_assemblies.items():
        if info["status"] != "latest":
            continue
        ftp_url = info["ftp_path"]
        if ftp_url.startswith("https://"):
            ftp_path = ftp_url.replace("https://ftp.ncbi.nlm.nih.gov", "")
        elif ftp_url.startswith("ftp://"):
            ftp_path = ftp_url.replace(f"ftp://{ftp_host}", "")
        else:
            ftp_path = ftp_url
        paths.append((accession, ftp_path.rstrip("/") + "/"))
    return paths


# ── Prefix filtering ────────────────────────────────────────────────────


def _accession_prefix(accession):
    """Extract the 3-digit prefix from an accession (e.g. GCF_000005845.2 -> '000')."""
    m = re.match(r"GC[AF]_(\d{3})\d{6}\.\d+", accession)
    return m.group(1) if m else None


def filter_by_prefix_range(accessions, prefix_from=None, prefix_to=None):
    """Filter a dict of assemblies to those whose 3-digit prefix is in range.

    Both bounds are inclusive. If neither is set, returns all accessions.
    """
    if prefix_from is None and prefix_to is None:
        return accessions
    filtered = {}
    for acc, info in accessions.items():
        pfx = _accession_prefix(acc)
        if pfx is None:
            continue
        if prefix_from is not None and pfx < prefix_from:
            continue
        if prefix_to is not None and pfx > prefix_to:
            continue
        filtered[acc] = info
    return filtered


# ── Store scanning (fallback when no previous summary) ──────────────────


def scan_store_accessions(client=None, bucket=None, prefix=None):
    """List accessions currently in the MinIO store under raw_data/.

    Returns a set of accession strings (e.g. {'GCF_000001215.4', ...}).
    """
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET
    if prefix is None:
        prefix = PATH_PREFIX + "raw_data/"

    accession_pattern = re.compile(r"(GC[AF]_\d{9}\.\d+)_[^/]+/$")
    accessions = set()

    paginator = client.s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    ):
        # Walk through all common prefixes recursively
        for cp in page.get("CommonPrefixes", []):
            sub = cp["Prefix"]
            accessions.update(
                _scan_prefix_recursive(client.s3, bucket, sub, accession_pattern)
            )

    logger.info(
        "Found %d accessions in store under %s", len(accessions), prefix
    )
    return accessions


def _scan_prefix_recursive(s3, bucket, prefix, pattern):
    """Recursively walk S3 prefixes to find assembly directories."""
    accessions = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            sub = cp["Prefix"]
            m = pattern.search(sub)
            if m:
                accessions.add(m.group(1))
            else:
                accessions.update(
                    _scan_prefix_recursive(s3, bucket, sub, pattern)
                )
    return accessions


# ── Previous summary loading ────────────────────────────────────────────


def load_previous_summary(database="refseq", client=None, bucket=None):
    """Load the previously stored assembly summary from MinIO.

    Tries the gzipped current-reference copy first, then the uncompressed
    version. Returns a parsed assemblies dict, or None if not found.
    """
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET

    base_key = f"{PATH_PREFIX}metadata/assembly_summary_{database}"

    # Try gzipped first, then plain
    for suffix in (".txt.gz", ".txt"):
        key = base_key + suffix
        info = client.stat_object(bucket, key)
        if info is None:
            continue
        logger.info("Loading previous summary from %s/%s", bucket, key)
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=suffix
        ) as tmp:
            tmp_path = tmp.name
        try:
            client.download_file(bucket, key, tmp_path)
            if suffix.endswith(".gz"):
                with gzip.open(tmp_path, "rt") as f:
                    return parse_assembly_summary(f.readlines())
            else:
                return parse_assembly_summary(tmp_path)
        finally:
            os.unlink(tmp_path)

    logger.info("No previous assembly summary found in store")
    return None


# ── Diff computation ────────────────────────────────────────────────────


def compute_diff(current, previous_assemblies=None, previous_accessions=None):
    """Compute the diff between current and previous assembly state.

    Args:
        current: dict from parse_assembly_summary (the new NCBI summary)
        previous_assemblies: dict from parse_assembly_summary (stored summary),
            or None if using store-scan fallback
        previous_accessions: set of accession strings from scan_store_accessions,
            used when previous_assemblies is None

    Returns:
        dict with keys: new, updated, replaced, suppressed
        Each is a list of accession strings.
    """
    new = []
    updated = []
    replaced = []
    suppressed = []

    # Build the set of known accessions
    if previous_assemblies is not None:
        known = set(previous_assemblies.keys())
    elif previous_accessions is not None:
        known = previous_accessions
    else:
        known = set()

    for acc, info in current.items():
        if info["status"] == "replaced":
            if acc in known:
                replaced.append(acc)
            continue
        if info["status"] == "suppressed":
            if acc in known:
                suppressed.append(acc)
            continue
        if info["status"] != "latest":
            continue

        if acc not in known:
            new.append(acc)
        elif previous_assemblies is not None:
            prev = previous_assemblies.get(acc, {})
            # Detect updates: seq_rel_date changed or assembly_dir changed
            # (assembly_dir changes when the version suffix is updated)
            if (
                info.get("seq_rel_date") != prev.get("seq_rel_date")
                or info.get("assembly_dir") != prev.get("assembly_dir")
            ):
                updated.append(acc)

    # Check for accessions that were in previous but are entirely absent from
    # current (withdrawn, not just status-changed)
    current_accs = set(current.keys())
    for acc in known:
        if acc not in current_accs:
            # Treat as suppressed (removed from summary entirely)
            if acc not in suppressed:
                suppressed.append(acc)

    return {
        "new": sorted(new),
        "updated": sorted(updated),
        "replaced": sorted(replaced),
        "suppressed": sorted(suppressed),
    }


# ── Manifest writing ────────────────────────────────────────────────────


def write_transfer_manifest(diff, current_assemblies, output_path, ftp_host=FTP_HOST):
    """Write the transfer manifest (new + updated assemblies).

    Each line is an FTP directory path suitable for download_genome_files().
    """
    to_transfer = diff["new"] + diff["updated"]
    paths = []
    for acc in sorted(to_transfer):
        info = current_assemblies.get(acc)
        if not info:
            continue
        ftp_url = info["ftp_path"]
        if ftp_url.startswith("https://"):
            ftp_path = ftp_url.replace("https://ftp.ncbi.nlm.nih.gov", "")
        elif ftp_url.startswith("ftp://"):
            ftp_path = ftp_url.replace(f"ftp://{ftp_host}", "")
        else:
            ftp_path = ftp_url
        paths.append(ftp_path.rstrip("/") + "/")

    with open(output_path, "w") as f:
        for p in paths:
            f.write(p + "\n")

    logger.info("Wrote %d entries to transfer manifest: %s", len(paths), output_path)
    return paths


def write_removed_manifest(diff, output_path):
    """Write the removed manifest (replaced + suppressed accessions)."""
    removed = sorted(diff["replaced"] + diff["suppressed"])
    with open(output_path, "w") as f:
        for acc in removed:
            f.write(acc + "\n")
    logger.info("Wrote %d entries to removed manifest: %s", len(removed), output_path)
    return removed


def write_diff_summary(diff, output_path, database, prefix_from=None, prefix_to=None):
    """Write the JSON diff summary."""
    summary = {
        "database": database,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "prefix_range": {
            "from": prefix_from,
            "to": prefix_to,
        },
        "counts": {
            "new": len(diff["new"]),
            "updated": len(diff["updated"]),
            "replaced": len(diff["replaced"]),
            "suppressed": len(diff["suppressed"]),
            "total_to_transfer": len(diff["new"]) + len(diff["updated"]),
            "total_to_remove": len(diff["replaced"]) + len(diff["suppressed"]),
        },
        "accessions": diff,
    }
    with open(output_path, "w") as f:
        json.dump(summary, f, indent=2)
    logger.info("Wrote diff summary to: %s", output_path)
    return summary


# ── Upload new summary to store ─────────────────────────────────────────


def upload_summary_to_store(
    summary_file, database="refseq", client=None, bucket=None
):
    """Upload the new assembly summary to MinIO (gzipped, versioned + current)."""
    if client is None:
        client = MinioClient()
    if bucket is None:
        bucket = BUCKET

    datestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    base = f"{PATH_PREFIX}metadata/assembly_summary_{database}"

    # Compress
    gz_path = summary_file + ".gz"
    with open(summary_file, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        while True:
            chunk = f_in.read(1 << 20)
            if not chunk:
                break
            f_out.write(chunk)

    try:
        # Versioned copy
        versioned_key = f"{base}_{datestamp}.txt.gz"
        client.upload_file(
            bucket, versioned_key, gz_path, checksum_algorithm="CRC64NVME"
        )
        logger.info("Uploaded versioned summary: %s", versioned_key)

        # Current reference copy
        current_key = f"{base}.txt.gz"
        client.upload_file(
            bucket, current_key, gz_path, checksum_algorithm="CRC64NVME"
        )
        logger.info("Uploaded current summary: %s", current_key)
    finally:
        os.unlink(gz_path)


# ── Main entry point ────────────────────────────────────────────────────


def generate_manifest(
    database="refseq",
    output_manifest=None,
    output_removed=None,
    output_summary=None,
    prefix_from=None,
    prefix_to=None,
    scan_store=False,
    upload_summary=True,
    dry_run=False,
    ftp_host=FTP_HOST,
    client=None,
):
    """Run the full manifest generation workflow.

    Returns (manifest_path, diff_summary_dict).
    """
    datestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    if output_manifest is None:
        output_manifest = f"transfer_manifest_{datestamp}.txt"
    if output_removed is None:
        output_removed = f"removed_manifest_{datestamp}.txt"
    if output_summary is None:
        output_summary = f"diff_summary_{datestamp}.json"

    if client is None:
        client = MinioClient()

    # 1. Download current assembly summary
    summary_file = download_assembly_summary(database=database, ftp_host=ftp_host)
    try:
        current = parse_assembly_summary(summary_file)

        # 2. Apply prefix filter
        current_filtered = filter_by_prefix_range(current, prefix_from, prefix_to)
        if prefix_from or prefix_to:
            logger.info(
                "Prefix filter [%s–%s]: %d -> %d assemblies",
                prefix_from or "...",
                prefix_to or "...",
                len(current),
                len(current_filtered),
            )

        # 3. Load previous state
        if scan_store:
            logger.info("Scanning store for existing accessions (fallback mode)")
            store_accessions = scan_store_accessions(client=client)
            if prefix_from or prefix_to:
                store_accessions = {
                    acc
                    for acc in store_accessions
                    if (prefix_from is None or (_accession_prefix(acc) or "") >= prefix_from)
                    and (prefix_to is None or (_accession_prefix(acc) or "") <= prefix_to)
                }
            previous_assemblies = None
            previous_accessions = store_accessions
        else:
            previous_assemblies = load_previous_summary(
                database=database, client=client
            )
            if previous_assemblies is not None and (prefix_from or prefix_to):
                previous_assemblies = filter_by_prefix_range(
                    previous_assemblies, prefix_from, prefix_to
                )
            previous_accessions = None

        # 4. Compute diff
        diff = compute_diff(
            current_filtered,
            previous_assemblies=previous_assemblies,
            previous_accessions=previous_accessions,
        )

        logger.info(
            "Diff: %d new, %d updated, %d replaced, %d suppressed",
            len(diff["new"]),
            len(diff["updated"]),
            len(diff["replaced"]),
            len(diff["suppressed"]),
        )

        # 5. Write outputs
        paths = write_transfer_manifest(
            diff, current_filtered, output_manifest, ftp_host=ftp_host
        )
        removed = write_removed_manifest(diff, output_removed)
        summary = write_diff_summary(
            diff, output_summary, database, prefix_from, prefix_to
        )

        # 6. Upload new summary to store
        if upload_summary and not dry_run:
            upload_summary_to_store(
                summary_file, database=database, client=client
            )

        return output_manifest, summary

    finally:
        os.unlink(summary_file)


def main():
    parser = argparse.ArgumentParser(
        description="Generate NCBI transfer manifest by diffing assembly summaries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Normal diff against stored previous summary
  python generate_transfer_manifest.py --database refseq

  # Fallback: reconstruct "previous" from current store state
  python generate_transfer_manifest.py --database refseq --scan-store

  # Limit to prefix range 000-003
  python generate_transfer_manifest.py --database refseq --prefix-from 000 --prefix-to 003
        """,
    )
    parser.add_argument(
        "--database",
        default="refseq",
        choices=["refseq", "genbank"],
        help="NCBI database (default: refseq)",
    )
    parser.add_argument(
        "--output-manifest",
        metavar="PATH",
        help="Output transfer manifest file",
    )
    parser.add_argument(
        "--output-removed",
        metavar="PATH",
        help="Output removed (replaced/suppressed) manifest file",
    )
    parser.add_argument(
        "--output-summary",
        metavar="PATH",
        help="Output JSON diff summary file",
    )
    parser.add_argument(
        "--prefix-from",
        metavar="NNN",
        help="Only include accessions with 3-digit prefix >= NNN",
    )
    parser.add_argument(
        "--prefix-to",
        metavar="NNN",
        help="Only include accessions with 3-digit prefix <= NNN",
    )
    parser.add_argument(
        "--scan-store",
        action="store_true",
        help="Reconstruct previous state from objects in MinIO (use when no previous summary exists)",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Do not upload the new assembly summary to the store",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute diff and write local files, but do not upload to store",
    )
    parser.add_argument(
        "--ftp-host",
        default=FTP_HOST,
        help=f"FTP host (default: {FTP_HOST})",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    manifest_path, summary = generate_manifest(
        database=args.database,
        output_manifest=args.output_manifest,
        output_removed=args.output_removed,
        output_summary=args.output_summary,
        prefix_from=args.prefix_from,
        prefix_to=args.prefix_to,
        scan_store=args.scan_store,
        upload_summary=not args.no_upload,
        dry_run=args.dry_run,
        ftp_host=args.ftp_host,
    )

    counts = summary["counts"]
    print(f"\nManifest: {manifest_path}")
    print(f"  To transfer: {counts['total_to_transfer']} ({counts['new']} new, {counts['updated']} updated)")
    print(f"  To remove:   {counts['total_to_remove']} ({counts['replaced']} replaced, {counts['suppressed']} suppressed)")


if __name__ == "__main__":
    main()
