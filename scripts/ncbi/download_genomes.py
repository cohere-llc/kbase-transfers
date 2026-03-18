#!/usr/bin/env python3
"""
Download genome files (.fna.gz and .gff.gz) from NCBI for accessions in a list.
"""

import os
import sys
import re
import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from ftplib import FTP
from pathlib import Path
import tempfile
import hashlib
import logging
import argparse
import socket
import time
from datetime import datetime

from frictionless import Package

from kbase_transfers import MinioClient

minio_bucket = "cdm-lake"
minio_path_prefix = "tenant-general-warehouse/kbase/datasets/ncbi/"

# Set up logging
logger = logging.getLogger(__name__)

def setup_logging(log_file=None, module_name=None):
    """
    Set up logging to file and console.
    
    Args:
        log_file: Path to log file. If None, creates timestamped log file.
        module_name: Logger name. If None, configures root logger (good for notebooks).
    """
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"download_genomes_{timestamp}.log"
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Configure logger
    # If no module_name specified, configure root logger (works for notebooks)
    if module_name is None:
        target_logger = logging.getLogger()
    else:
        target_logger = logging.getLogger(module_name)
    
    target_logger.setLevel(logging.DEBUG)
    
    # Clear any existing handlers to avoid duplicates
    target_logger.handlers.clear()
    
    target_logger.addHandler(file_handler)
    target_logger.addHandler(console_handler)
    
    return log_file


def _set_ftp_keepalive(ftp, idle=30, interval=10, count=3):
    """
    Enable TCP keepalive on the FTP control connection socket.

    Prevents NCBI's '421 No transfer timeout' when the control connection sits
    idle while large files are transferred on the data channel, or while MinIO
    checksum verification is running.  The OS sends keepalive probes after
    ``idle`` seconds of inactivity, repeating every ``interval`` seconds up to
    ``count`` times before declaring the connection dead.
    """
    sock = ftp.sock
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
    if hasattr(socket, 'TCP_KEEPINTVL'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
    if hasattr(socket, 'TCP_KEEPCNT'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)


def get_minio_client():
    """
    Initialize and return MinioClient.
    """
    client = MinioClient()

    # Ensure bucket exists
    buckets = client.list_buckets()
    if minio_bucket not in buckets:
        raise Exception(f"MinIO bucket '{minio_bucket}' does not exist.")
    
    # Ensure path prefix exists (not strictly necessary for MinIO/S3, but for sanity check)
    objects = client.list_objects(minio_bucket, prefix=minio_path_prefix)
    if not any(objects):
        raise Exception(f"MinIO path prefix '{minio_path_prefix}' does not exist in bucket '{minio_bucket}'.")

    return client



def parse_accession(entry):
    """
    Parse entry like 'GB_GCA_000195005.1' or 'RS_GCF_000006825.1' or 'GCA_000195005.1'
    Returns: (prefix, database, accession_full)
    e.g., ('GB', 'GCA', 'GCA_000195005.1')
    """
    # Try full format first (GB_GCA_000195005.1)
    match = re.match(r'(GB|RS)_(GC[AF])_([\d.]+)', entry.strip())
    if match:
        prefix = match.group(1)
        database = match.group(2)  # GCA or GCF
        accession_num = match.group(3)
        accession_full = f"{database}_{accession_num}"
        return prefix, database, accession_full
    
    # Try accession-only format (GCA_000195005.1)
    match = re.match(r'(GC[AF])_([\d.]+)', entry.strip())
    if match:
        database = match.group(1)
        accession_num = match.group(2)
        accession_full = f"{database}_{accession_num}"
        return None, database, accession_full
    
    raise ValueError(f"Invalid entry format: {entry}")


def build_ftp_path(database, accession_full):
    """
    Build FTP path from accession.
    e.g., GCA_000195005.1 -> /genomes/all/GCA/000/195/005/
    """
    # Extract numeric parts: GCA_000195005.1 -> ['000', '195', '005']
    match = re.match(r'GC[AF]_(\d{3})(\d{3})(\d{3})\.\d+', accession_full)
    if not match:
        raise ValueError(f"Cannot parse accession: {accession_full}")
    
    part1, part2, part3 = match.groups()
    path = f"/genomes/all/{database}/{part1}/{part2}/{part3}/"
    
    return path


def build_accession_path(assembly_dir):
    """
    Build the NCBI path for a given assembly directory.
    e.g., GCA_000195005.1_MyRecordDescription -> GCA/000/195/005/GCA_000195005.1_MyRecordDescription/
    """
    match = re.match(r'GC[AF]_(\d{3})(\d{3})(\d{3})\.\d+.*', assembly_dir)
    if not match:
        raise ValueError(f"Cannot parse accession: {assembly_dir}")
    
    part1, part2, part3 = match.groups()
    path = f"raw_data/{assembly_dir[0:3]}/{part1}/{part2}/{part3}/{assembly_dir}/"
    
    return path


def find_assembly_dir(ftp, base_path, accession_full):
    """
    Find the actual assembly directory (with assembly name suffix).
    e.g., GCA_000195005.1_ASM19500v1
    """
    try:
        ftp.cwd(base_path)
        dirs = []
        ftp.retrlines('LIST', lambda x: dirs.append(x))
        
        # Find directory that starts with our accession
        for line in dirs:
            parts = line.split()
            if len(parts) >= 9:
                name = parts[-1]
                if name.startswith(accession_full):
                    return name
        
        raise FileNotFoundError(f"No assembly directory found for {accession_full} in {base_path}")
    
    except Exception as e:
        raise Exception(f"Error finding assembly directory: {e}")


file_filters = [
    '_gene_ontology.gaf.gz',
    '_genomic.fna.gz',
    '_genomic.gff.gz',
    '_protein.faa.gz',
    '_ani_contam_ranges.tsv',
    '_assembly_regions.txt',
    '_assembly_report.txt',
    '_assembly_stats.txt',
    '_gene_expression_counts.txt.gz',
    '_normalized_gene_expression_counts.txt.gz',
]

def compute_md5(file_path):
    """
    Compute MD5 checksum of a file.

    Uses 1 MiB read chunks to minimise the number of Python-level loop
    iterations (and GIL acquisitions) for large files.
    """
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def parse_md5checksums(content):
    """
    Parse md5checksums.txt content.
    Returns dict of {filename: checksum}
    """
    checksums = {}
    for line in content.strip().split('\n'):
        if line.strip():
            parts = line.split()
            if len(parts) >= 2:
                checksum = parts[0]
                filename = parts[1].lstrip('./')
                checksums[filename] = checksum
    return checksums


def create_frictionless_descriptor(assembly_dir, accession_full, downloaded_files):
    """
    Create a frictionless data package descriptor for the downloaded files.
    
    Args:
        assembly_dir: Assembly directory name (e.g., GCA_000195005.1_ASM19500v1)
        accession_full: Full accession (e.g., GCA_000195005.1)
        downloaded_files: List of dicts with keys: name, path, format, bytes, hash
    
    Returns:
        dict: KBase credit metadata descriptor
    """
    descriptor = {
        "identifier": f"NCBI:{accession_full}",
        "resource_type": "dataset",
        "version": accession_full.split('_')[-1],
        "titles": [
            {
                "title": f"NCBI Genome Assembly {assembly_dir}"
            }
        ],
        "descriptions": [
            {
                "description_text": f"Genome assembly files for {accession_full} downloaded from NCBI Datasets"
            }
        ],
        "url": f"https://www.ncbi.nlm.nih.gov/datasets/genome/{accession_full}/",
        "contributors": [
            {
                "contributor_type": "Organization",
                "name": "National Center for Biotechnology Information",
                "contributor_id": "ROR:02meqm098",
                "contributor_roles": "DataCurator"
            }
        ],
        "publisher": {
            "organization_name": "National Center for Biotechnology Information",
            "organization_id": "ROR:02meqm098"
        },
        "license": { },
        "meta": {
            "credit_metadata_schema_version": "1.0",
            "credit_metadata_source": [
                {
                    "source_name": "NCBI Genomes FTP",
                    "source_url": "ftp.ncbi.nlm.nih.gov/genomes/all/",
                    "access_timestamp": int(datetime.now().timestamp())
                }
            ],
            "saved_by": "kbase-transfers-ncbi-downloader",
            "timestamp": int(datetime.now().timestamp())
        },
        "resources": downloaded_files
    }

    # Normalise resources: frictionless requires name to be lowercase and
    # does not accept null values for hash.
    for resource in descriptor["resources"]:
        resource["name"] = resource["name"].lower()
        if resource.get("hash") is None:
            resource.pop("hash", None)

    # Validate descriptor structure with frictionless (does not load data sources)
    logger.debug(f"Validating frictionless data package descriptor for {accession_full}")
    package = Package(descriptor)
    errors = list(package.metadata_validate(descriptor))
    if errors:
        messages = [str(e) for e in errors]
        error_details = "\n  - ".join(messages)
        raise ValueError(
            f"Frictionless validation failed for {accession_full} "
            f"({len(messages)} errors):\n  - {error_details}"
        )

    return descriptor


def find_assembly_directories_in_prefix(ftp, prefix_path, start_from=None, limit=None):
    """
    Recursively find all assembly directories under a given prefix.
    Returns list of full paths to assembly directories.
    e.g., /genomes/all/GCF/000/001/215/GCF_000001215.2_Release_5/

    Args:
        ftp: Connected FTP instance.
        prefix_path: Base FTP path to search under.
        start_from: If set, skip top-level subdirectories whose names sort
                    before this value (alphanumeric comparison). Only applied
                    at the first level of subdirectories under prefix_path.
        limit: If set, stop collecting once this many assembly dirs are found.
    """
    assembly_pattern = re.compile(r'^GC[AF]_\d{9}\.\d+_.*')
    assembly_dirs = []

    def traverse_directory(path, skip_before=None):
        if limit and len(assembly_dirs) >= limit:
            return
        logger.debug(f"Traversing: {path}")
        try:
            ftp.cwd(path)
            items = []
            ftp.retrlines('LIST', lambda x: items.append(x))

            for line in items:
                if limit and len(assembly_dirs) >= limit:
                    break

                parts = line.split()
                if len(parts) < 9:
                    continue

                name = parts[-1]
                is_dir = line.startswith('d')

                if not is_dir:
                    continue

                # At the top level, skip subdirectories that sort before start_from
                if skip_before is not None and name < skip_before:
                    logger.debug(f"  Skipping {name} (before start_from={skip_before})")
                    continue

                # Check if this is an assembly directory
                if assembly_pattern.match(name):
                    full_path = f"{path}{name}/"
                    assembly_dirs.append(full_path)
                    logger.debug(f"  Found assembly: {full_path}")
                else:
                    # Recurse into subdirectory (no skip_before for deeper levels)
                    traverse_directory(f"{path}{name}/")

        except Exception as e:
            logger.warning(f"Error traversing {path}: {e}")

    traverse_directory(prefix_path, skip_before=start_from)
    return assembly_dirs


def list_ftp_subdirectories(ftp, path, start_from=None):
    """
    List direct subdirectories of an FTP path, optionally filtering to those
    that sort >= start_from.

    Returns a sorted list of subdirectory names (not full paths).
    """
    try:
        ftp.cwd(path)
        items = []
        ftp.retrlines('LIST', lambda x: items.append(x))
        subdirs = []
        for line in items:
            parts = line.split()
            if len(parts) < 9:
                continue
            if not line.startswith('d'):
                continue
            name = parts[-1]
            if start_from is not None and name < start_from:
                continue
            subdirs.append(name)
        return sorted(subdirs)
    except Exception as e:
        logger.error(f"Error listing subdirectories of {path}: {e}")
        return []


def download_genome_files(entry, s3_client, local_dir, failed_transfers, no_checksum_files, ftp_host='ftp.ncbi.nlm.nih.gov', assembly_path=None):
    """
    Download files according to file_filters for a given accession.
    If assembly_path is provided, use it directly instead of building from entry.
    """
    if assembly_path:
        # Extract database and accession from path
        # e.g., /genomes/all/GCF/000/001/215/GCF_000001215.2_Release_5/
        match = re.search(r'/(GC[AF])/\d{3}/\d{3}/\d{3}/((GC[AF]_\d{9}\.\d+)_[^/]+)/', assembly_path)
        if not match:
            raise ValueError(f"Cannot parse assembly path: {assembly_path}")
        database = match.group(1)
        assembly_dir = match.group(2)
        accession_full = match.group(3)
        base_path = assembly_path.rsplit('/', 2)[0] + '/'
        entry = accession_full  # Use accession as entry for logging
    else:
        _, database, accession_full = parse_accession(entry)
    
    # Ensure local_dir is a Path object
    local_dir = Path(local_dir)
    
    logger.info(f"\nProcessing: {entry}")
    logger.info(f"  Accession: {accession_full}")
    logger.debug(f"  Local temporary dir: {local_dir}")
    
    # Connect to FTP
    ftp = FTP(ftp_host)
    ftp.login()
    _set_ftp_keepalive(ftp)  # Prevent '421 No transfer timeout' on long transfers
    
    try:
        # Build path and find assembly directory (if not already provided)
        if not assembly_path:
            base_path = build_ftp_path(database, accession_full)
            logger.info(f"  Base path: {base_path}")
            
            assembly_dir = find_assembly_dir(ftp, base_path, accession_full)
            logger.info(f"  Assembly dir: {assembly_dir}")
        else:
            logger.info(f"  Using provided path: {assembly_path}")

        s3_path = minio_path_prefix + build_accession_path(assembly_dir)
        logger.info(f"  S3 path: {s3_path}")
        
        # Path for metadata (stored separately from raw data)
        metadata_path = minio_path_prefix + "metadata/"
        metadata_filename = f"{assembly_dir}_datapackage.json"
        
        full_path = base_path + assembly_dir
        ftp.cwd(full_path)
        
        # List files
        files = []
        ftp.retrlines('NLST', lambda x: files.append(x))
        
        # Download and parse md5checksums.txt first
        md5_checksums = {}
        if 'md5checksums.txt' in files:
            logger.info(f"  Downloading md5checksums.txt")
            md5_content = []
            ftp.retrlines('RETR md5checksums.txt', lambda x: md5_content.append(x))
            md5_checksums = parse_md5checksums('\n'.join(md5_content))
            logger.info(f"  Found {len(md5_checksums)} checksums")
            
            # Upload md5checksums.txt to MinIO
            md5_local_file = local_dir / 'md5checksums.txt'
            with open(md5_local_file, 'w') as f:
                f.write('\n'.join(md5_content))
            s3_client.upload_file(
                minio_bucket,
                s3_path + 'md5checksums.txt',
                str(md5_local_file)
            )
            logger.info(f"  Uploaded md5checksums.txt to MinIO: {s3_path}md5checksums.txt")
        else:
            logger.warning(f"  WARNING: md5checksums.txt not found")
        
        # Filter for files based on file_filters
        target_files = [f for f in files if any(f.endswith(suffix) for suffix in file_filters)]
        
        if not target_files:
            logger.warning(f"  WARNING: No files matching filters found")
            return
        
        # Download files and track successful downloads
        downloaded_resources = []
        # Track last FTP protocol activity to send NOOP before long MinIO operations.
        # Cloud NAT gateways kill idle TCP connections after ~60 s even with TCP keepalive;
        # an FTP NOOP is application-layer traffic that resets every NAT/firewall timer.
        last_ftp_activity = time.monotonic()
        
        for filename in target_files:
            # If the FTP control connection has been idle for too long, ping it with NOOP
            # before performing a potentially slow MinIO HEAD request.
            if time.monotonic() - last_ftp_activity > 25:
                try:
                    ftp.sendcmd('NOOP')
                    last_ftp_activity = time.monotonic()
                    logger.debug("  Sent FTP NOOP to keep control connection alive")
                except Exception as noop_err:
                    logger.warning(f"  FTP NOOP failed: {noop_err}")
            # Single HEAD request: tells us whether the file exists AND its stored md5
            obj_info = s3_client.stat_object(minio_bucket, s3_path + filename)
            file_exists = obj_info is not None

            local_file = local_dir / filename
            expected_checksum = md5_checksums.get(filename)
            
            # If file exists in MinIO and we have a checksum, verify it first
            if file_exists and expected_checksum:
                logger.info(f"  File exists in MinIO, verifying: {filename}")
                # Fast path: check stored metadata checksum (no download needed)
                if obj_info and obj_info.get('md5') == expected_checksum:
                    logger.info(f"    ✓ Checksum verified via metadata, skipping download: {expected_checksum}")
                    resource = {
                        "name": filename,
                        "path": s3_path + filename,
                        "format": filename.split('.')[-1] if '.' in filename else "unknown",
                        "bytes": obj_info.get('size'),
                        "hash": expected_checksum
                    }
                    downloaded_resources.append(resource)
                    continue
                # Slow path: download and compute MD5 locally
                s3_client.download_file(
                    minio_bucket,
                    s3_path + filename,
                    str(local_file)
                )
                actual_checksum = compute_md5(local_file)
                if actual_checksum == expected_checksum:
                    logger.info(f"    ✓ Checksum verified, skipping download: {actual_checksum}")
                    # Backfill metadata so future runs use the fast path.
                    # Use a server-side copy (no data transfer) to update metadata only.
                    backfilled = s3_client.update_metadata(
                        minio_bucket,
                        s3_path + filename,
                        {'md5': actual_checksum}
                    )
                    if backfilled:
                        logger.debug(f"    Backfilled md5 metadata for: {filename}")
                    else:
                        logger.warning(f"    Could not backfill md5 metadata for: {filename}")
                    file_size = local_file.stat().st_size
                    resource = {
                        "name": filename,
                        "path": s3_path + filename,
                        "format": filename.split('.')[-1] if '.' in filename else "unknown",
                        "bytes": file_size,
                        "hash": actual_checksum
                    }
                    downloaded_resources.append(resource)
                    continue
                else:
                    logger.warning(f"    ✗ Checksum mismatch in MinIO: expected {expected_checksum}, got {actual_checksum}")
                    logger.info(f"    Will re-download from NCBI")
            elif file_exists:
                logger.info(f"  File exists in MinIO (no checksum available): {filename}")
                # obj_info already contains size from the HEAD request above
                file_size = obj_info.get('size') if obj_info else None
                resource = {
                    "name": filename,
                    "path": s3_path + filename,
                    "format": filename.split('.')[-1] if '.' in filename else "unknown",
                    "bytes": file_size,
                    "hash": None
                }
                downloaded_resources.append(resource)
                no_checksum_files.append({
                    'entry': entry,
                    'filename': filename,
                    'status': 'exists_in_minio'
                })
                continue
            
            # Try up to 3 times
            transfer_success = False
            verified_checksum = False
            for attempt in range(1, 4):
                logger.info(f"  Downloading: {filename} (attempt {attempt}/3)")
                
                # Download from FTP
                with open(local_file, 'wb') as f:
                    ftp.retrbinary(f'RETR {filename}', f.write)
                last_ftp_activity = time.monotonic()
                
                # Verify checksum if available
                if expected_checksum:
                    actual_checksum = compute_md5(local_file)
                    if actual_checksum != expected_checksum:
                        logger.warning(f"    ✗ Checksum mismatch: expected {expected_checksum}, got {actual_checksum}")
                        if attempt < 3:
                            logger.info(f"    Retrying...")
                            continue
                        else:
                            logger.error(f"    Failed after 3 attempts")
                            failed_transfers.append({
                                'entry': entry,
                                'filename': filename,
                                'reason': f'Checksum mismatch after 3 attempts (expected: {expected_checksum}, got: {actual_checksum})'
                            })
                            break
                    else:
                        logger.info(f"    ✓ Checksum verified: {actual_checksum}")
                        verified_checksum = True
                    
                    # Upload to MinIO (store MD5 as metadata for fast future verification)
                    s3_client.upload_file(
                        minio_bucket,
                        s3_path + filename,
                        str(local_file),
                        metadata={'md5': actual_checksum}
                    )
                    logger.info(f"    Uploaded to MinIO: {s3_path + filename}")
                    transfer_success = True
                    
                    # Add to downloaded resources
                    file_size = local_file.stat().st_size
                    resource = {
                        "name": filename,
                        "path": s3_path + filename,
                        "format": filename.split('.')[-1] if '.' in filename else "unknown",
                        "bytes": file_size,
                        "hash": actual_checksum
                    }
                    downloaded_resources.append(resource)
                    break
                else:
                    # No checksum available - upload without verification
                    logger.warning(f"    WARNING: No checksum available for verification")
                    s3_client.upload_file(
                        minio_bucket,
                        s3_path + filename,
                        str(local_file)
                    )
                    logger.info(f"    Uploaded to MinIO: {s3_path + filename}")
                    no_checksum_files.append({
                        'entry': entry,
                        'filename': filename,
                        'status': 'newly_uploaded'
                    })
                    
                    # Add to downloaded resources (without hash)
                    file_size = local_file.stat().st_size
                    resource = {
                        "name": filename,
                        "path": s3_path + filename,
                        "format": filename.split('.')[-1] if '.' in filename else "unknown",
                        "bytes": file_size,
                        "hash": None
                    }
                    downloaded_resources.append(resource)
                    transfer_success = True
                    break
        
        logger.info(f"  ✓ Downloaded {len(target_files)} files")
        
        # Create and upload frictionless data package descriptor
        if downloaded_resources:
            logger.info(f"  Creating frictionless data package descriptor")
            descriptor = create_frictionless_descriptor(
                assembly_dir,
                accession_full,
                downloaded_resources
            )
            
            # Write descriptor to local file
            descriptor_file = local_dir / 'datapackage.json'
            with open(descriptor_file, 'w') as f:
                json.dump(descriptor, f, indent=2)
            
            # Upload to MinIO
            s3_client.upload_file(
                minio_bucket,
                metadata_path + metadata_filename,
                str(descriptor_file)
            )
            logger.info(f"  ✓ Uploaded {metadata_filename} to MinIO: {metadata_path}{metadata_filename}")
    
    except Exception as e:
        logger.error(f"  ✗ ERROR: {e}")
        raise
    
    finally:
        try:
            ftp.quit()
        except Exception:
            # The control connection may have timed out after all work was
            # completed (421 during QUIT).  All files and metadata have
            # already been uploaded at this point, so silently ignore.
            pass


def run(
    input_file=None,
    prefix=None,
    start_from=None,
    output_list=None,
    ftp_host='ftp.ncbi.nlm.nih.gov',
    threads=1,
    limit=None,
):
    """
    Execute the genome download workflow.

    Can be called directly from Python as well as from the CLI via main().

    Example usage:

        # Download from an accession list file
        run(input_file='list_of_accessions.txt')

        # Download all assemblies under a prefix, limiting to 100
        run(prefix='GCF', limit=100, threads=4)

        # Resume a prefix run from a specific top-level subdirectory,
        # saving discovered accessions to a file
        run(prefix='GCF', start_from='003', output_list='gcf_accessions.txt', threads=8)

    Args:
        input_file:  Path to a file containing one accession per line
                     (mutually exclusive with ``prefix``).
        prefix:      FTP sub-path under /genomes/all/ to scan recursively,
                     e.g. ``'GCF'`` or ``'GCF/000/001'``
                     (mutually exclusive with ``input_file``).
        start_from:  When using ``prefix``, skip top-level subdirectories
                     that sort before this value, e.g. ``'003'``.  Useful for
                     resuming an interrupted run.
        output_list: Path to a file where discovered accession IDs will be
                     written incrementally (only valid with ``prefix``).
        ftp_host:    NCBI FTP hostname (default: ``'ftp.ncbi.nlm.nih.gov'``).
        threads:     Number of parallel download threads (default: ``1``).
        limit:       Stop after this many assemblies have been attempted
                     (handy for smoke-testing).
    """
    # Set up logging
    log_file = setup_logging(module_name=__name__)
    logger.info(f"Logging to: {log_file}")

    # Initialize MinIO client and temp dir (needed for all modes)
    s3 = get_minio_client()
    temp_dir = tempfile.TemporaryDirectory()

    # Shared counters / state — protected by a lock when accessed from threads
    lock = threading.Lock()
    success_count = 0
    failed = []
    failed_transfers = []  # Track individual file transfer failures
    no_checksum_files = []  # Track files without checksums

    def _download_one(entry, is_assembly_path=False):
        """Download a single assembly in its own temp subdir and MinIO client. Returns (entry, error|None)."""
        assembly_tmp = tempfile.mkdtemp(dir=temp_dir.name)
        # MinioClient() gives each thread its own boto3 session/connection pool.
        # Bucket/prefix validation was already done by get_minio_client() at startup.
        thread_s3 = MinioClient()
        try:
            download_genome_files(
                entry, thread_s3, assembly_tmp, failed_transfers, no_checksum_files,
                ftp_host=ftp_host,
                assembly_path=entry if is_assembly_path else None,
            )
            return entry, None
        except Exception as e:
            return entry, e
        finally:
            shutil.rmtree(assembly_tmp, ignore_errors=True)

    def _process_batch(entries, is_assembly_path=False):
        """Submit a batch of entries to the thread pool.

        Returns True if the overall limit was reached after this batch.
        """
        nonlocal success_count
        limit_reached = False
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(_download_one, e, is_assembly_path): e for e in entries}
            for future in as_completed(futures):
                entry, error = future.result()
                with lock:
                    if error:
                        logger.error(f"  ✗ FAILED: {entry}")
                        failed.append((entry, str(error)))
                    else:
                        success_count += 1
                    if limit and (success_count + len(failed)) >= limit:
                        limit_reached = True
        return limit_reached

    if input_file:
        # ── File-based mode ────────────────────────────────────────────────
        if not os.path.exists(input_file):
            print(f"Error: File not found: {input_file}")
            sys.exit(1)

        with open(input_file, 'r') as f:
            accessions = [line.strip() for line in f if line.strip()]

        logger.info(f"Mode: File-based")
        logger.info(f"Found {len(accessions)} accessions to process")

        if limit is not None:
            accessions = accessions[:limit]
            logger.info(f"Limiting to first {limit} accessions")

        _process_batch(accessions, is_assembly_path=False)

    else:
        # ── Prefix-based mode ──────────────────────────────────────────────
        prefix_path = prefix.strip('/')
        ftp_path = f"/genomes/all/{prefix_path}/"

        logger.info(f"Mode: Prefix-based")
        logger.info(f"Searching for assemblies under: {ftp_path}")
        if start_from:
            logger.info(f"Starting from subdirectory: {start_from}")

        output_list_fh = open(output_list, 'w') if output_list else None

        def _write_output_list(paths):
            """Append accessions extracted from assembly paths to the output list file."""
            if output_list_fh:
                for path in paths:
                    m = re.search(r'/(GC[AF]_\d{9}\.\d+)_[^/]+/', path)
                    if m:
                        output_list_fh.write(m.group(1) + '\n')
                output_list_fh.flush()

        try:
            if start_from:
                # Iterative mode: list top-level subdirs, then discover + process
                # one subdir at a time to avoid building a huge list upfront.
                ftp = FTP(ftp_host)
                ftp.login()
                try:
                    top_subdirs = list_ftp_subdirectories(ftp, ftp_path, start_from=start_from)
                finally:
                    ftp.quit()

                logger.info(f"Found {len(top_subdirs)} top-level subdirectories >= '{start_from}'")

                for subdir in top_subdirs:
                    subdir_path = f"{ftp_path}{subdir}/"
                    remaining = (limit - success_count - len(failed)) if limit else None

                    logger.info(f"Scanning subdir: {subdir_path}")
                    ftp = FTP(ftp_host)
                    ftp.login()
                    try:
                        subdir_paths = find_assembly_directories_in_prefix(
                            ftp, subdir_path, limit=remaining
                        )
                    finally:
                        ftp.quit()

                    if not subdir_paths:
                        logger.debug(f"No assemblies found under {subdir_path}")
                        continue

                    logger.info(f"  Found {len(subdir_paths)} assemblies in {subdir}")
                    _write_output_list(subdir_paths)

                    success_before = success_count
                    failed_before = len(failed)

                    limit_reached = _process_batch(subdir_paths, is_assembly_path=True)

                    # Per-subdirectory summary
                    sub_success = success_count - success_before
                    sub_failed_entries = failed[failed_before:]
                    logger.info(
                        f"  Subdir {subdir} summary: "
                        f"{len(subdir_paths)} attempted, "
                        f"{sub_success} succeeded, "
                        f"{len(sub_failed_entries)} failed"
                    )
                    if sub_failed_entries:
                        for entry, error in sub_failed_entries:
                            logger.info(f"    - {entry}: {error}")

                    if limit_reached:
                        break

            else:
                # Non-iterative mode: build the full list first, then process.
                ftp = FTP(ftp_host)
                ftp.login()
                try:
                    assembly_paths = find_assembly_directories_in_prefix(
                        ftp, ftp_path, limit=limit
                    )
                finally:
                    ftp.quit()

                logger.info(f"Found {len(assembly_paths)} assembly directories")

                if not assembly_paths:
                    logger.error(f"No assembly directories found under {ftp_path}")
                    sys.exit(1)

                _write_output_list(assembly_paths)
                if output_list:
                    logger.info(f"Saved {len(assembly_paths)} accessions to {output_list}")

                _process_batch(assembly_paths, is_assembly_path=True)

        finally:
            if output_list_fh:
                output_list_fh.close()

    # ── Summary ────────────────────────────────────────────────────────────
    total = success_count + len(failed)
    logger.info("\n" + "="*60)
    logger.info(f"SUMMARY:")
    logger.info(f"  Total attempted: {total}")
    logger.info(f"  Success: {success_count}")
    logger.info(f"  Failed: {len(failed)}")
    logger.info(f"  Failed file transfers: {len(failed_transfers)}")
    logger.info(f"  Files without checksums: {len(no_checksum_files)}")

    if failed:
        logger.error("\nFailed accessions:")
        for entry, error in failed:
            logger.error(f"  - {entry}: {error}")

    if failed_transfers:
        logger.warning("\nFailed file transfers (checksum verification):")
        for transfer in failed_transfers:
            logger.warning(f"  - {transfer['entry']} / {transfer['filename']}")
            logger.warning(f"    Reason: {transfer['reason']}")

    if no_checksum_files:
        logger.warning("\nFiles without checksums:")
        for file_info in no_checksum_files:
            status = "(existing)" if file_info['status'] == 'exists_in_minio' else "(newly uploaded)"
            logger.warning(f"  - {file_info['entry']} / {file_info['filename']} {status}")


def main():
    parser = argparse.ArgumentParser(
        description='Download genome files from NCBI to MinIO',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download from accession list file
  python download_genomes.py list_of_accessions.txt
  
  # Download all genomes under a prefix
  python download_genomes.py --prefix GCF
  python download_genomes.py --prefix GCF/000/001
  python download_genomes.py --prefix GCA/000

  # Resume from a specific subdirectory
  python download_genomes.py --prefix GCF --start-from 003
  python download_genomes.py --prefix GCA --start-from 001
        """
    )

    parser.add_argument('input_file', nargs='?', help='File with list of accessions')
    parser.add_argument('--prefix', help='FTP prefix to download all genomes from (e.g., GCF, GCF/000/001)')
    parser.add_argument('--start-from', metavar='SUBDIR',
                        help='Skip top-level subdirectories under --prefix that sort before SUBDIR '
                             '(e.g., --prefix GCF --start-from 003 processes GCF/003/, GCF/004/, ...)')
    parser.add_argument('--output-list', help='Output file to save list of assemblies found (use with --prefix)')
    parser.add_argument('--ftp-host', default='ftp.ncbi.nlm.nih.gov', help='FTP host (default: ftp.ncbi.nlm.nih.gov)')
    parser.add_argument('--threads', type=int, default=1, metavar='N',
                        help='Number of parallel download threads (default: 1)')
    parser.add_argument('--limit', type=int, metavar='N', help='Limit processing to first N accessions (for testing)')

    args = parser.parse_args()

    # Validate arguments
    if not args.input_file and not args.prefix:
        parser.error('Either provide an input file or use --prefix')

    if args.input_file and args.prefix:
        parser.error('Cannot use both input file and --prefix at the same time')

    if args.output_list and not args.prefix:
        parser.error('--output-list can only be used with --prefix')

    if args.start_from and not args.prefix:
        parser.error('--start-from can only be used with --prefix')

    run(
        input_file=args.input_file,
        prefix=args.prefix,
        start_from=args.start_from,
        output_list=args.output_list,
        ftp_host=args.ftp_host,
        threads=args.threads,
        limit=args.limit,
    )


if __name__ == '__main__':
    main()
