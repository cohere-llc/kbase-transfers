#!/usr/bin/env python3
"""
Download genome files (.fna.gz and .gff.gz) from NCBI for accessions in a list.
"""

import os
import sys
import re
import json
from ftplib import FTP
from pathlib import Path
import time
import tempfile
import hashlib
import logging
import argparse
from datetime import datetime

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
    """
    md5_hash = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
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
        dict: Frictionless data package descriptor
    """
    descriptor = {
        "profile": "tabular-data-package",
        "name": assembly_dir.lower().replace('.', '-').replace('_', '-'),
        "title": f"NCBI Genome Assembly {assembly_dir}",
        "description": f"Genome assembly files for {accession_full} downloaded from NCBI Datasets",
        "homepage": f"https://www.ncbi.nlm.nih.gov/datasets/genome/{accession_full}/",
        "version": accession_full.split('_')[-1],  # e.g., "1" from GCA_000195005.1
        "created": datetime.now().isoformat(),
        "licenses": [
        ],
        "sources": [
            {
                "title": "NCBI Genomes FTP",
                "path": "ftp.ncbi.nlm.nih.gov/genomes/all/"
            }
        ],
        "contributors": [
            {
                "title": "NCBI Datasets Team",
                "role": "author",
                "organization": "National Center for Biotechnology Information"
            }
        ],
        "citations": [
            {
                "title": "Exploring and retrieving sequence and metadata for species across the tree of life with NCBI Datasets",
                "authors": "O'Leary NA, Cox E, Holmes JB, Anderson WR, Falk R, Hem V, Tsuchiya MTN, Schuler GD, Zhang X, Torcivia J, Ketter A, Breen L, Cothran J, Bajwa H, Tinne J, Meric PA, Hlavina W, Schneider VA",
                "journal": "Sci Data",
                "year": "2024",
                "volume": "11",
                "issue": "1",
                "pages": "732",
                "doi": "10.1038/s41597-024-03571-y",
                "pmid": "38969627",
                "pmcid": "PMC11226681"
            }
        ],
        "resources": downloaded_files
    }
    
    return descriptor


def find_assembly_directories_in_prefix(ftp, prefix_path):
    """
    Recursively find all assembly directories under a given prefix.
    Returns list of full paths to assembly directories.
    e.g., /genomes/all/GCF/000/001/215/GCF_000001215.2_Release_5/
    """
    assembly_pattern = re.compile(r'^GC[AF]_\d{9}\.\d+_.*')
    assembly_dirs = []
    
    def traverse_directory(path):
        logger.debug(f"Traversing: {path}")
        try:
            ftp.cwd(path)
            items = []
            ftp.retrlines('LIST', lambda x: items.append(x))
            
            for line in items:
                parts = line.split()
                if len(parts) < 9:
                    continue
                    
                name = parts[-1]
                is_dir = line.startswith('d')
                
                if not is_dir:
                    continue
                
                # Check if this is an assembly directory
                if assembly_pattern.match(name):
                    full_path = f"{path}{name}/"
                    assembly_dirs.append(full_path)
                    logger.debug(f"  Found assembly: {full_path}")
                else:
                    # Recurse into subdirectory
                    traverse_directory(f"{path}{name}/")
        
        except Exception as e:
            logger.warning(f"Error traversing {path}: {e}")
    
    traverse_directory(prefix_path)
    return assembly_dirs


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
        
        for filename in target_files:
            # Check if file exists in MinIO
            existing_objects = s3_client.list_objects(minio_bucket, prefix=s3_path + filename)
            file_exists = bool(existing_objects)
            
            local_file = local_dir / filename
            expected_checksum = md5_checksums.get(filename)
            
            # If file exists in MinIO and we have a checksum, verify it first
            if file_exists and expected_checksum:
                logger.info(f"  File exists in MinIO, verifying: {filename}")
                # Download from MinIO to verify
                s3_client.download_file(
                    minio_bucket,
                    s3_path + filename,
                    str(local_file)
                )
                actual_checksum = compute_md5(local_file)
                if actual_checksum == expected_checksum:
                    logger.info(f"    ✓ Checksum verified, skipping: {actual_checksum}")
                    continue
                else:
                    logger.warning(f"    ✗ Checksum mismatch in MinIO: expected {expected_checksum}, got {actual_checksum}")
                    logger.info(f"    Will re-download from NCBI")
            elif file_exists:
                logger.info(f"  Skipping existing file in MinIO (no checksum available): {filename}")
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
                    
                    # Upload to MinIO
                    s3_client.upload_file(
                        minio_bucket,
                        s3_path + filename,
                        str(local_file)
                    )
                    logger.info(f"    Uploaded to MinIO: {s3_path + filename}")
                    transfer_success = True
                    
                    # Add to downloaded resources
                    file_size = local_file.stat().st_size
                    resource = {
                        "name": filename,
                        "path": filename,
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
                        "path": filename,
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
                s3_path + 'datapackage.json',
                str(descriptor_file)
            )
            logger.info(f"  ✓ Uploaded datapackage.json to MinIO: {s3_path}datapackage.json")
    
    except Exception as e:
        logger.error(f"  ✗ ERROR: {e}")
        raise
    
    finally:
        ftp.quit()


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
        """
    )
    
    parser.add_argument('input_file', nargs='?', help='File with list of accessions')
    parser.add_argument('--prefix', help='FTP prefix to download all genomes from (e.g., GCF, GCF/000/001)')
    parser.add_argument('--output-list', help='Output file to save list of assemblies found (use with --prefix)')
    parser.add_argument('--ftp-host', default='ftp.ncbi.nlm.nih.gov', help='FTP host (default: ftp.ncbi.nlm.nih.gov)')
    parser.add_argument('--limit', type=int, metavar='N', help='Limit processing to first N accessions (for testing)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.input_file and not args.prefix:
        parser.error('Either provide an input file or use --prefix')
    
    if args.input_file and args.prefix:
        parser.error('Cannot use both input file and --prefix at the same time')
    
    if args.output_list and not args.prefix:
        parser.error('--output-list can only be used with --prefix')
    
    # Set up logging
    log_file = setup_logging(module_name=__name__)
    logger.info(f"Logging to: {log_file}")
    
    # Determine mode and get list of items to process
    accessions = []
    assembly_paths = []
    
    if args.input_file:
        # File-based mode
        if not os.path.exists(args.input_file):
            print(f"Error: File not found: {args.input_file}")
            sys.exit(1)
        
        with open(args.input_file, 'r') as f:
            accessions = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Mode: File-based")
        logger.info(f"Found {len(accessions)} accessions to process")
        
        # Apply limit if specified
        if args.limit is not None:
            accessions = accessions[:args.limit]
            logger.info(f"Limiting to first {args.limit} accessions for testing")
    
    else:
        # Prefix-based mode
        prefix = args.prefix.strip('/')
        ftp_path = f"/genomes/all/{prefix}/"
        
        logger.info(f"Mode: Prefix-based")
        logger.info(f"Searching for assemblies under: {ftp_path}")
        
        # Connect to FTP and find all assembly directories
        ftp = FTP(args.ftp_host)
        ftp.login()
        
        try:
            assembly_paths = find_assembly_directories_in_prefix(ftp, ftp_path)
            logger.info(f"Found {len(assembly_paths)} assembly directories")
        finally:
            ftp.quit()
        
        if not assembly_paths:
            logger.error(f"No assembly directories found under {ftp_path}")
            sys.exit(1)
        
        # Apply limit if specified
        if args.limit is not None:
            assembly_paths = assembly_paths[:args.limit]
            logger.info(f"Limiting to first {args.limit} assemblies for testing")
        
        # Save assembly list to file if requested
        if args.output_list:
            logger.info(f"Saving assembly list to: {args.output_list}")
            with open(args.output_list, 'w') as f:
                for path in assembly_paths:
                    # Extract accession from path
                    # e.g., /genomes/all/GCF/000/001/215/GCF_000001215.2_Release_5/ -> GCF_000001215.2
                    match = re.search(r'/(GC[AF]_\d{9}\.\d+)_[^/]+/', path)
                    if match:
                        accession = match.group(1)
                        f.write(accession + '\n')
            logger.info(f"Saved {len(assembly_paths)} accessions to {args.output_list}")

    # Initialize MinIO client and check bucket/path
    s3 = get_minio_client()

    # Create a temporary folder for downloads
    temp_dir = tempfile.TemporaryDirectory()
    
    # Process each accession or assembly path
    success_count = 0
    failed = []
    failed_transfers = []  # Track individual file transfer failures
    no_checksum_files = []  # Track files without checksums
    
    items_to_process = accessions if accessions else assembly_paths
    
    for i, entry in enumerate(items_to_process, 1):
        try:
            logger.info(f"\n[{i}/{len(items_to_process)}] {entry}")
            
            if assembly_paths:
                # Prefix mode: entry is an assembly path
                download_genome_files(entry, s3, temp_dir.name, failed_transfers, no_checksum_files, 
                                     ftp_host=args.ftp_host, assembly_path=entry)
            else:
                # File mode: entry is an accession from the file
                download_genome_files(entry, s3, temp_dir.name, failed_transfers, no_checksum_files,
                                     ftp_host=args.ftp_host)
            
            success_count += 1
            time.sleep(0.5)  # Be nice to NCBI servers
        
        except Exception as e:
            logger.error(f"  ✗ FAILED: {entry}")
            failed.append((entry, str(e)))
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info(f"SUMMARY:")
    logger.info(f"  Total: {len(items_to_process)}")
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


if __name__ == '__main__':
    main()
