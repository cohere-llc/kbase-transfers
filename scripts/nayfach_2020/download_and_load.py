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


def load_metagenomes(client, xlsx_path, dry_run=False, limit=None):
    """Load metagenome data from S1 sheet into MinIO."""
    print("\nLoading metagenomes from S1 sheet...")
    
    # Read the S1 sheet
    df = pd.read_excel(xlsx_path, sheet_name='S1')
    print(f"Found {len(df)} metagenomes in S1 sheet")
    
    # Apply limit if specified
    if limit is not None:
        df = df.head(limit)
        print(f"Limiting to first {limit} metagenomes for testing")
    
    # Process each row
    for idx, row in df.iterrows():
        img_taxon_id = str(int(row['IMG_TAXON_ID']))
        
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
    
    if not dry_run:
        print(f"✓ Successfully uploaded {len(df)} metagenomes to {BUCKET_NAME}:{METAGENOMES_PATH}/")


def load_mags(client, xlsx_path, dry_run=False, limit=None):
    """Load MAG data from S2 sheet into MinIO."""
    print("\nLoading MAGs from S2 sheet...")
    
    # Read the S2 sheet
    df = pd.read_excel(xlsx_path, sheet_name='S2')
    print(f"Found {len(df)} MAGs in S2 sheet")
    
    # Apply limit if specified
    if limit is not None:
        df = df.head(limit)
        print(f"Limiting to first {limit} MAGs for testing")
    
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
        
        # Create the object path
        object_path = f"{MAGS_PATH}/{genome_id}/mag.json"
        
        if dry_run:
            print(f"  [DRY RUN] Would upload: {object_path}")
        else:
            client.put_json_object(BUCKET_NAME, object_path, record)
            if (idx + 1) % 500 == 0:
                print(f"  Uploaded {idx + 1}/{len(df)} MAGs...")
    
    if not dry_run:
        print(f"✓ Successfully uploaded {len(df)} MAGs to {BUCKET_NAME}:{MAGS_PATH}/")


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
    
    # Load data
    load_metagenomes(client, xlsx_path, dry_run=args.dry_run, limit=args.limit)
    load_mags(client, xlsx_path, dry_run=args.dry_run, limit=args.limit)
    
    print("\n✓ All done!")


if __name__ == '__main__':
    main()
