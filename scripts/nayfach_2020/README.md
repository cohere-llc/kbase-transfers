# Nayfach et al. 2020 Dataset Transfer

This script downloads and loads the supplementary data from the Nayfach et al. 2020 Nature Biotechnology paper into the KBase Lakehouse Object Store (MinIO).

**Paper:** [A genomic catalog of Earth's microbiomes](https://doi.org/10.1038/s41587-020-0718-6)  
**Supplementary Data:** [Excel file (41587_2020_718_MOESM3_ESM.xlsx)](https://static-content.springer.com/esm/art%3A10.1038%2Fs41587-020-0718-6/MediaObjects/41587_2020_718_MOESM3_ESM.xlsx)

## Data Structure

The script processes two sheets from the Excel file:

- **S1 (Metagenomes):** 10,450 metagenome records → `raw_data/metagenomes/{IMG_TAXON_ID}/metagenome.json`
- **S2 (MAGs):** 52,515 MAG (Metagenome-Assembled Genome) records → `raw_data/mags/{genome_id}/mag.json`

All data is stored under `cdm-lake:tenant-general-warehouse/kbase/datasets/jgi/raw_data/`.

## Usage

### Prerequisites

1. Start MinIO server (see main [README](../../README.md#testing-with-containerized-minio))
2. Create the required bucket and folder structure:

```bash
# Using the MinIO console (http://localhost:9001) or via Python:
source .venv/bin/activate
python -c "
from kbase_transfers.minio_client import MinioClient
client = MinioClient()
client.s3.create_bucket(Bucket='cdm-lake')
client.s3.put_object(
    Bucket='cdm-lake',
    Key='tenant-general-warehouse/kbase/datasets/jgi/.placeholder',
    Body=b''
)
"
```

### Running the Script

Basic usage (downloads Excel file and uploads all data):

```bash
uv run python scripts/nayfach_2020/download_and_load.py
```

### Command-Line Options

- `--data-dir DIR` - Directory to store Excel file (default: `./data`)
- `--force-download` - Re-download Excel file even if it exists
- `--skip-download` - Use existing Excel file without downloading
- `--dry-run` - Show what would be uploaded without uploading
- `--limit N` - Process only first N records from each sheet (for testing)

### Examples

```bash
# Test with dry-run (no upload to MinIO) and limited records
uv run python scripts/nayfach_2020/download_and_load.py --dry-run --limit 10

# Upload only first 100 records of each type (for testing)
uv run python scripts/nayfach_2020/download_and_load.py --limit 100

# Use existing file and upload all data
uv run python scripts/nayfach_2020/download_and_load.py --skip-download

# Force re-download and upload all data
uv run python scripts/nayfach_2020/download_and_load.py --force-download
```

## Output Structure

The script creates the following structure in MinIO:

```
cdm-lake/
└── tenant-general-warehouse/
    └── kbase/
        └── datasets/
            └── jgi/
                └── raw_data/
                    ├── metagenomes/
                    │   ├── 2001200001/
                    │   │   └── metagenome.json
                    │   ├── 2001200002/
                    │   │   └── metagenome.json
                    │   └── ...
                    └── mags/
                        ├── 3300028580_9/
                        │   └── mag.json
                        ├── 3300028580_5/
                        │   └── mag.json
                        └── ...
```

Each JSON file contains all the metadata columns from the corresponding Excel sheet row.

## Error Handling

The script will error if:
- The `cdm-lake` bucket doesn't exist
- The parent path `tenant-general-warehouse/kbase/datasets/jgi/` doesn't exist

The script will automatically create the `raw_data/` subfolder if it doesn't exist.
