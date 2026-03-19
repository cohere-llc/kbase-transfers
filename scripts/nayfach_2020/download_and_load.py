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

# ---------------------------------------------------------------------------
# File transfer filter rules.
# Each entry controls which DTS files are included in the transfer and how
# they are named at the destination.  Edit this list to change criteria.
#
# Fields:
#   label     - human-readable name used as a column header in stats output.
#   suffix    - include files whose path ends with this string (case-insensitive).
#   filename  - include files whose basename exactly matches this string.
#   rename_to - destination filename template; {img_taxon_id} is substituted.
#               Omit (or set to None) to keep the original basename.
# ---------------------------------------------------------------------------
FILE_FILTERS = [
    {"label": ".fna files",              "suffix": ".fna"},
    {"label": ".faa files",              "suffix": ".faa"},
    {"label": ".gff files",              "suffix": ".gff"},
    {"label": "final.contigs.fasta",     "filename": "final.contigs.fasta",
     "rename_to": "{img_taxon_id}.final.contigs.fasta"},
]


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


def _filter_resources(resources, img_taxon_id):
    """Apply FILE_FILTERS to a list of DTS resource dicts.

    Returns a new list of dicts, each containing the original keys plus:
      'dest_name'    - filename to use at the destination.
      'filter_label' - the label of the matching FILE_FILTERS rule.
    Files that match no rule are excluded.
    """
    filtered = []
    for resource in resources:
        path_lower = resource['path'].lower()
        basename = resource['path'].rsplit('/', 1)[-1]
        for rule in FILE_FILTERS:
            matched = False
            if 'suffix' in rule and path_lower.endswith(rule['suffix']):
                matched = True
            elif 'filename' in rule and basename == rule['filename']:
                matched = True
            if matched:
                rename_to = rule.get('rename_to')
                dest_name = (
                    rename_to.format(img_taxon_id=img_taxon_id)
                    if rename_to
                    else basename
                )
                filtered.append({**resource, 'dest_name': dest_name, 'filter_label': rule['label']})
                break  # a file matches at most one rule
    return filtered


def _to_folder_id(value):
    """Return a clean string id from a pandas cell value.

    Floats are converted via int() to strip the Excel-added decimal
    (e.g. 3300001234.0 → '3300001234').  Values that are already strings
    (e.g. '3300028580_9') are returned as-is so underscores are preserved.
    None/NaN yields None.
    """
    if not pd.notna(value):
        return None
    if isinstance(value, float):
        return str(int(value))
    return str(value)


def collect_sheet_stats(xlsx_path, sheet_name, folder_col, dts_id_col, dts_client, orcid, limit=None, verbose=False):
    """Query DTS for every row in a sheet and return a stats DataFrame.

    Columns: id, dts_id, total_files, <one column per FILE_FILTERS label>.
    """
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    if limit is not None:
        df = df.head(limit)

    filter_labels = [rule['label'] for rule in FILE_FILTERS]
    rows = []

    for i, (_, row) in enumerate(df.iterrows(), 1):
        folder_id = _to_folder_id(row[folder_col])
        dts_id = _to_folder_id(row[dts_id_col])

        resources = query_dts_resources(dts_id, dts_client, orcid, verbose=verbose) if dts_id else []
        filtered = _filter_resources(resources, dts_id) if dts_id else []

        counts = {lbl: 0 for lbl in filter_labels}
        for r in filtered:
            counts[r['filter_label']] += 1

        rows.append({'id': folder_id, 'dts_id': dts_id, 'total_files': len(resources), **counts})

        if i % 100 == 0:
            print(f"  Queried {i}/{len(df)} rows...")

    return pd.DataFrame(rows)


def write_stats_xlsx(output_path, metagenome_stats, mag_stats):
    """Write per-row stats and a summary sheet to an Excel file."""
    filter_labels = [rule['label'] for rule in FILE_FILTERS]

    def _summary_block(stats_df, sheet_label):
        """Return a list of (metric, value) pairs for one sheet."""
        total = len(stats_df)
        no_files = (stats_df['total_files'] == 0).sum()
        has_files = total - no_files
        no_filtered = has_files - (stats_df[filter_labels].sum(axis=1) > 0).sum()
        pairs = [
            (f"[{sheet_label}] Total records", total),
            (f"[{sheet_label}] Records with DTS files", int(has_files)),
            (f"[{sheet_label}] Records with no DTS files", int(no_files)),
            (f"[{sheet_label}] Records with DTS files but no filtered matches", int(no_filtered)),
            (f"[{sheet_label}] Total DTS files found", int(stats_df['total_files'].sum())),
        ]
        for lbl in filter_labels:
            pairs.append((f"[{sheet_label}] Records with 0 '{lbl}'", int((stats_df[lbl] == 0).sum())))
            pairs.append((f"[{sheet_label}] Total '{lbl}' across all records", int(stats_df[lbl].sum())))
        return pairs

    summary_rows = (
        _summary_block(metagenome_stats, 'S1 metagenomes')
        + [('', '')]  # blank separator row
        + _summary_block(mag_stats, 'S2 MAGs')
    )
    summary_df = pd.DataFrame(summary_rows, columns=['Metric', 'Value'])

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        metagenome_stats.to_excel(writer, sheet_name='S1 metagenomes', index=False)
        mag_stats.to_excel(writer, sheet_name='S2 MAGs', index=False)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    print(f"\n✓ Stats written to {output_path}")


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
                'id': entry_dict.get('id'),
                'path': entry_dict.get('path', ''),
                'bytes': entry_dict.get('bytes'),
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


def load_sheet(
    client,
    xlsx_path,
    sheet_name,
    label,
    dest_path,
    folder_col,
    dts_id_col,
    dry_run=False,
    limit=None,
    dts_client=None,
    orcid=None,
    verbose=False,
):
    """Load one sheet from the Excel file into MinIO.

    Args:
        sheet_name:  Excel sheet to read (e.g. 'S1', 'S2').
        label:       Human-readable name used in progress messages (e.g. 'metagenomes').
        dest_path:   MinIO path prefix for this dataset (e.g. METAGENOMES_PATH).
        folder_col:  Column whose value becomes the per-record folder name.
        dts_id_col:  Column whose value is used as the IMG_TAXON_ID for DTS queries.
    """
    print(f"\nLoading {label} from {sheet_name} sheet...")

    if dts_client:
        print("  DTS integration enabled - will query for file resources")

    df = pd.read_excel(xlsx_path, sheet_name=sheet_name)
    print(f"Found {len(df)} {label} in {sheet_name} sheet")

    if limit is not None:
        df = df.head(limit)
        print(f"Limiting to first {limit} {label} for testing")

    dts_queries = 0
    dts_resources_found = 0
    dts_resources_total = 0

    for idx, row in df.iterrows():
        folder_id = _to_folder_id(row[folder_col])
        dts_id = _to_folder_id(row[dts_id_col])

        # Convert row to a plain Python dict, handling NaN values
        record = {}
        for col in df.columns:
            value = row[col]
            if pd.isna(value):
                record[col] = None
            elif isinstance(value, (pd.Int64Dtype, int)):
                record[col] = int(value)
            elif isinstance(value, float):
                record[col] = float(value)
            else:
                record[col] = str(value)

        object_path = f"{dest_path}/{folder_id}/gems_info.json"

        if dry_run:
            print(f"  [DRY RUN] Would upload: {object_path}")
        else:
            client.put_json_object(BUCKET_NAME, object_path, record)
            if (idx + 1) % 100 == 0:
                print(f"  Uploaded {idx + 1}/{len(df)} {label}...")

        if dts_client and orcid and dts_id:
            dts_queries += 1
            resources = query_dts_resources(dts_id, dts_client, orcid, verbose=verbose)
            if resources:
                dts_resources_found += 1
                dts_resources_total += len(resources)

            filtered_files = _filter_resources(resources, dts_id)

            resources_data = {
                "IMG_TAXON_ID": int(dts_id),
                "associated_files": resources,
                "filtered_files": filtered_files,
            }

            resources_path = f"{dest_path}/{folder_id}/resources.json"
            if dry_run:
                print(f"  [DRY RUN] Would upload resources.json ({len(resources)} files) to: {resources_path}")
            else:
                client.put_json_object(BUCKET_NAME, resources_path, resources_data)

    if not dry_run:
        print(f"✓ Successfully uploaded {len(df)} {label} to {BUCKET_NAME}:{dest_path}/")
        if dts_client and orcid:
            print(f"  DTS: Queried {dts_queries} {label}, found resources for {dts_resources_found} ({dts_resources_total} total files)")


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
    parser.add_argument(
        '--stats-output',
        type=str,
        metavar='FILE',
        help='Collect DTS availability stats and write to this Excel file (e.g. stats.xlsx); '
             'skips the normal MinIO upload'
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
    
    # Stats-only mode
    if args.stats_output:
        if not (dts_client and args.dts_orcid):
            print("Error: --stats-output requires DTS credentials (--dts-token and --dts-orcid).")
            sys.exit(1)
        print("\nCollecting stats for S1 metagenomes...")
        metagenome_stats = collect_sheet_stats(
            xlsx_path, sheet_name='S1', folder_col='IMG_TAXON_ID', dts_id_col='IMG_TAXON_ID',
            dts_client=dts_client, orcid=args.dts_orcid, limit=args.limit, verbose=args.verbose,
        )
        print("\nCollecting stats for S2 MAGs...")
        mag_stats = collect_sheet_stats(
            xlsx_path, sheet_name='S2', folder_col='genome_id', dts_id_col='img_taxon_id',
            dts_client=dts_client, orcid=args.dts_orcid, limit=args.limit, verbose=args.verbose,
        )
        write_stats_xlsx(args.stats_output, metagenome_stats, mag_stats)
        print("\n✓ All done!")
        return

    # Load data
    shared = dict(dry_run=args.dry_run, limit=args.limit,
                  dts_client=dts_client, orcid=args.dts_orcid, verbose=args.verbose)
    load_sheet(client, xlsx_path, sheet_name='S1', label='metagenomes',
               dest_path=METAGENOMES_PATH, folder_col='IMG_TAXON_ID', dts_id_col='IMG_TAXON_ID', **shared)
    load_sheet(client, xlsx_path, sheet_name='S2', label='MAGs',
               dest_path=MAGS_PATH, folder_col='genome_id', dts_id_col='img_taxon_id', **shared)

    print("\n✓ All done!")


if __name__ == '__main__':
    main()
