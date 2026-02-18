#!/usr/bin/env python3
"""
Download and load Nayfach 2020 dataset into MinIO.

This script downloads the supplementary data from the Nayfach et al. 2020 Nature Biotechnology paper
and loads it into the KBase Lakehouse Object Store (MinIO).

Paper: https://doi.org/10.1038/s41587-020-0718-6
Supplementary Data: https://static-content.springer.com/esm/art%3A10.1038%2Fs41587-020-0718-6/MediaObjects/41587_2020_718_MOESM3_ESM.xlsx
"""

import argparse
import os
import sys
import requests
import pandas as pd
from pathlib import Path

# Add parent directory to path to import kbase_transfers
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from kbase_transfers.minio_client import MinioClient
import dts


XLSX_URL = "https://static-content.springer.com/esm/art%3A10.1038%2Fs41587-020-0718-6/MediaObjects/41587_2020_718_MOESM3_ESM.xlsx"
XLSX_FILENAME = "41587_2020_718_MOESM3_ESM.xlsx"

# MinIO bucket and path configuration
BUCKET_NAME = "cdm-lake"
BASE_PATH = "tenant-general-warehouse/kbase/datasets/jgi"
RAW_DATA_PATH = f"{BASE_PATH}/raw_data"
METAGENOMES_PATH = f"{RAW_DATA_PATH}/metagenomes"
MAGS_PATH = f"{RAW_DATA_PATH}/mags"


def download_xlsx(data_dir, force=False):
    """Download the Excel file if it doesn't exist."""
    xlsx_path = data_dir / XLSX_FILENAME
    
    if xlsx_path.exists() and not force:
        print(f"Excel file already exists at {xlsx_path}")
        return xlsx_path
    
    print(f"Downloading Excel file from {XLSX_URL}...")
    response = requests.get(XLSX_URL, stream=True)
    response.raise_for_status()
    
    with open(xlsx_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Downloaded to {xlsx_path}")
    return xlsx_path


def check_parent_paths_exist(client):
    """Check that the parent paths exist in MinIO, error if not."""
    if not client.bucket_exists(BUCKET_NAME):
        raise RuntimeError(f"Bucket '{BUCKET_NAME}' does not exist. Please create it first.")
    
    if not client.prefix_exists(BUCKET_NAME, BASE_PATH + "/"):
        raise RuntimeError(
            f"Path '{BASE_PATH}/' does not exist in bucket '{BUCKET_NAME}'. "
            "Please create the required parent folders first."
        )
    
    print(f"✓ Verified parent path exists: {BUCKET_NAME}:{BASE_PATH}/")


def query_dts_resources(img_taxon_id, dts_client, orcid, verbose=False):
    """
    Query DTS for file resources associated with an IMG_TAXON_ID.
    
    Returns:
        list: List of dicts with 'id', 'path', and 'bytes' keys, or empty list if no results
    """
    if dts_client is None:
        return []
    
    try:
        results = dts_client.search(
            database='jdp',
            orcid=orcid,
            query=img_taxon_id
        )
        
        resources = []
        for entry in results:
            entry_dict = entry.to_dict()
            resources.append({
                'id': entry_dict['id'],
                'path': entry_dict['path'],
                'bytes': entry_dict['bytes']
            })
        
        if verbose:
            if resources:
                print(f"    Found {len(resources)} resource(s) for IMG_TAXON_ID {img_taxon_id}")
            else:
                print(f"    No resources found for IMG_TAXON_ID {img_taxon_id}")
        
        return resources
    
    except Exception as e:
        print(f"  Warning: Failed to query DTS for IMG_TAXON_ID {img_taxon_id}: {e}")
        return []


def load_metagenomes(client, xlsx_path, dry_run=False, limit=None, dts_client=None, orcid=None, verbose=False):
    """Load metagenome data from S1 sheet into MinIO."""
    print("\nLoading metagenomes from S1 sheet...")
    
    if dts_client:
        print("  DTS integration enabled - will query for file resources")
    
    # Read the S1 sheet
    df = pd.read_excel(xlsx_path, sheet_name='S1')
    print(f"Found {len(df)} metagenomes in S1 sheet")

    # Apply limit if specified
    if limit is not None:
        df = df.head(limit)
        print(f"Limiting to first {limit} metagenomes for testing")
    
    # Track DTS statistics
    dts_queries = 0
    dts_resources_found = 0
    dts_resources_total = 0
    
    # Process each row
    for idx, row in df.iterrows():
        # Check if IMG_TAXON_ID column exists
        if 'IMG_TAXON_ID' not in df.columns:
            if idx == 0:  # Only warn once
                print("  Warning: IMG_TAXON_ID column not found in S1 sheet. Available columns:", list(df.columns))
                print("  Skipping DTS queries for metagenomes.")
            img_taxon_id = None
        else:
            img_taxon_id = str(int(row['IMG_TAXON_ID'])) if pd.notna(row['IMG_TAXON_ID']) else None
        
        # Convert row to dictionary, handling NaN values
        record = {}
        for col in df.columns:
            value = row[col]
            # Convert pandas types to Python native types
            if pd.isna(value):
                record[col] = None
            elif isinstance(value, (pd.Int64Dtype, int)):
                record[col] = int(value)
            elif isinstance(value, float):
                record[col] = float(value)
            else:
                record[col] = str(value)
        
        # Create the object path
        object_path = f"{METAGENOMES_PATH}/{img_taxon_id}/metagenome.json"
        
        if dry_run:
            print(f"  [DRY RUN] Would upload: {object_path}")
        else:
            client.put_json_object(BUCKET_NAME, object_path, record)
            if (idx + 1) % 100 == 0:
                print(f"  Uploaded {idx + 1}/{len(df)} metagenomes...")
        
        # Query DTS for file resources if DTS client is available
        # Always create resources.json, even if no files found
        if dts_client and orcid and img_taxon_id:
            dts_queries += 1
            resources = query_dts_resources(img_taxon_id, dts_client, orcid, verbose=verbose)
            if resources:
                dts_resources_found += 1
                dts_resources_total += len(resources)
            
            # Structure resources.json with IMG_TAXON_ID and associated_files
            # Filter for .fna and .gff files
            filtered_files = [
                r for r in resources 
                if '.fna' in r['path'].lower() or '.gff' in r['path'].lower()
            ]
            
            resources_data = {
                "IMG_TAXON_ID": int(img_taxon_id),
                "associated_files": resources,
                "filtered_files": filtered_files
            }
            
            resources_path = f"{METAGENOMES_PATH}/{img_taxon_id}/resources.json"
            if dry_run:
                print(f"  [DRY RUN] Would upload resources.json ({len(resources)} files) to: {resources_path}")
            else:
                client.put_json_object(BUCKET_NAME, resources_path, resources_data)
    
    if not dry_run:
        print(f"✓ Successfully uploaded {len(df)} metagenomes to {BUCKET_NAME}:{METAGENOMES_PATH}/")
        if dts_client and orcid:
            print(f"  DTS: Queried {dts_queries} metagenomes, found resources for {dts_resources_found} ({dts_resources_total} total files)")


def load_mags(client, xlsx_path, dry_run=False, limit=None, dts_client=None, orcid=None, verbose=False):
    """Load MAG data from S2 sheet into MinIO."""
    print("\nLoading MAGs from S2 sheet...")
    
    if dts_client:
        print("  DTS integration enabled - will query for file resources")
    
    # Read the S2 sheet
    df = pd.read_excel(xlsx_path, sheet_name='S2')
    print(f"Found {len(df)} MAGs in S2 sheet")
    
    # Apply limit if specified
    if limit is not None:
        df = df.head(limit)
        print(f"Limiting to first {limit} MAGs for testing")
    
    # Track DTS statistics
    dts_queries = 0
    dts_resources_found = 0
    dts_resources_total = 0
    
    # Process each row
    for idx, row in df.iterrows():
        genome_id = str(row['genome_id'])
        
        # Convert row to dictionary, handling NaN values
        record = {}
        for col in df.columns:
            value = row[col]
            # Convert pandas types to Python native types
            if pd.isna(value):
                record[col] = None
            elif isinstance(value, (pd.Int64Dtype, int)):
                record[col] = int(value)
            elif isinstance(value, float):
                record[col] = float(value)
            else:
                record[col] = str(value)
        
        # For table S2 it seems like the img_taxon_id is just the genome id without the trailing `_123`
        if 'img_taxon_id' in df.columns:
            img_taxon_id = str(int(row['img_taxon_id'])) if pd.notna(row['img_taxon_id']) else None
        else:
            img_taxon_id = None
        
        # Create the object path
        object_path = f"{MAGS_PATH}/{genome_id}/mag.json"
        
        if dry_run:
            print(f"  [DRY RUN] Would upload: {object_path}")
        else:
            client.put_json_object(BUCKET_NAME, object_path, record)
            if (idx + 1) % 500 == 0:
                print(f"  Uploaded {idx + 1}/{len(df)} MAGs...")
        
        # Query DTS for file resources if DTS client is available and IMG_TAXON_ID exists
        # Always create resources.json, even if no files found
        if dts_client and orcid and img_taxon_id:
            dts_queries += 1
            resources = query_dts_resources(img_taxon_id, dts_client, orcid, verbose=verbose)
            if resources:
                dts_resources_found += 1
                dts_resources_total += len(resources)
            
            # Structure resources.json with IMG_TAXON_ID and associated_files
            # Filter for .fna and .gff files
            filtered_files = [
                r for r in resources 
                if '.fna' in r['path'].lower() or '.gff' in r['path'].lower()
            ]
            
            resources_data = {
                "IMG_TAXON_ID": int(img_taxon_id),
                "associated_files": resources,
                "filtered_files": filtered_files
            }
            
            resources_path = f"{MAGS_PATH}/{genome_id}/resources.json"
            if dry_run:
                print(f"  [DRY RUN] Would upload resources.json ({len(resources)} files) to: {resources_path}")
            else:
                client.put_json_object(BUCKET_NAME, resources_path, resources_data)
    
    if not dry_run:
        print(f"✓ Successfully uploaded {len(df)} MAGs to {BUCKET_NAME}:{MAGS_PATH}/")
        if dts_client and orcid:
            print(f"  DTS: Queried {dts_queries} MAGs, found resources for {dts_resources_found} ({dts_resources_total} total files)")


def main():
    parser = argparse.ArgumentParser(
        description="Download and load Nayfach 2020 dataset into MinIO"
    )
    parser.add_argument(
        '--data-dir',
        type=Path,
        default=Path(__file__).parent / 'data',
        help='Directory to store downloaded Excel file (default: ./data)'
    )
    parser.add_argument(
        '--force-download',
        action='store_true',
        help='Force re-download of Excel file even if it exists'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be uploaded without actually uploading'
    )
    parser.add_argument(
        '--skip-download',
        action='store_true',
        help='Skip downloading the Excel file (use existing file)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        metavar='N',
        help='Limit processing to first N records from each sheet (for testing)'
    )
    parser.add_argument(
        '--dts-token',
        type=str,
        default=os.getenv('DTS_KBASE_DEV_TOKEN'),
        help='DTS API token (or set DTS_KBASE_DEV_TOKEN environment variable)'
    )
    parser.add_argument(
        '--dts-orcid',
        type=str,
        default=os.getenv('DTS_KBASE_TEST_ORCID'),
        help='DTS ORCID ID (or set DTS_KBASE_TEST_ORCID environment variable)'
    )
    parser.add_argument(
        '--skip-dts',
        action='store_true',
        help='Skip DTS queries for file resources (only upload metadata)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging for DTS queries'
    )
    
    args = parser.parse_args()
    
    # Ensure data directory exists
    args.data_dir.mkdir(parents=True, exist_ok=True)
    
    # Download Excel file
    if not args.skip_download:
        xlsx_path = download_xlsx(args.data_dir, force=args.force_download)
    else:
        xlsx_path = args.data_dir / XLSX_FILENAME
        if not xlsx_path.exists():
            print(f"Error: Excel file not found at {xlsx_path}")
            print("Run without --skip-download to download it first.")
            sys.exit(1)
    
    # Initialize MinIO client
    print("\nConnecting to MinIO...")
    client = MinioClient()
    
    # Check that parent paths exist
    check_parent_paths_exist(client)
    
    # Check/create raw_data folder (this is done implicitly by uploading objects with that prefix)
    if not args.dry_run:
        if not client.prefix_exists(BUCKET_NAME, RAW_DATA_PATH + "/"):
            print(f"Creating raw_data folder at {BUCKET_NAME}:{RAW_DATA_PATH}/")
        else:
            print(f"✓ raw_data folder exists at {BUCKET_NAME}:{RAW_DATA_PATH}/")
    
    # Initialize DTS client if credentials provided and not skipped
    dts_client = None
    if not args.skip_dts:
        if args.dts_token and args.dts_orcid:
            print("\nInitializing DTS client...")
            try:
                dts_client = dts.Client(
                    api_key=args.dts_token,
                    server="https://dts.kbase.us"
                )
                print("✓ DTS client initialized")
            except Exception as e:
                print(f"Warning: Failed to initialize DTS client: {e}")
                print("Continuing without DTS integration...")
        else:
            print("\nWarning: DTS credentials not provided (--dts-token and --dts-orcid)")
            print("Skipping DTS queries. Set DTS_KBASE_DEV_TOKEN and DTS_KBASE_TEST_ORCID environment variables or use command-line args.")
    else:
        print("\nSkipping DTS integration (--skip-dts flag set)")
    
    # Load data
    load_metagenomes(client, xlsx_path, dry_run=args.dry_run, limit=args.limit, 
                    dts_client=dts_client, orcid=args.dts_orcid, verbose=args.verbose)
    load_mags(client, xlsx_path, dry_run=args.dry_run, limit=args.limit,
             dts_client=dts_client, orcid=args.dts_orcid, verbose=args.verbose)
    
    print("\n✓ All done!")


if __name__ == '__main__':
    main()
