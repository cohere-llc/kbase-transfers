# SPIRE Data Transfer Scripts

[SPIRE](https://spire.embl.de/) (Sequence-based Planetary-scale mIcrobiome REcovery) is a
large-scale collection of 1.16 million metagenome-assembled genomes (MAGs) from 739 studies
across diverse global environments (Fullam et al., 2023 — [doi:10.1093/nar/gkad943](https://doi.org/10.1093/nar/gkad943)).

This document walks through the complete workflow to stage the SPIRE v01 dataset locally:
downloading all data and metadata, collecting lat/lon coordinates, and creating the data
package descriptor. Manual upload to MinIO follows once staging is complete.

---

## Prerequisites

- Python ≥ 3.12 with the project virtualenv activated (`source .venv/bin/activate`)
- `wget` available on `$PATH`
- ~175 GB free disk space (81 GB tar archive + 82 GB extracted genomes + ~200 MB metadata)

---

## Quick test-run

Before committing to the full dataset, verify the workflow end-to-end with a small subset.
This uses only 5 studies and skips downloading the large genome archive.

```bash
# 1. Download only the metadata files (fast — a few hundred MB total)
cd scripts/spire/data
wget https://swifter.embl.de/~fullam/spire/metadata/spire_v1_genome_metadata.tsv.gz
wget https://swifter.embl.de/~fullam/spire/metadata/spire_v1_cluster_metadata.tsv.gz
wget https://swifter.embl.de/~fullam/spire/metadata/spire_v1_microntology.tsv.gz
wget https://swifter.embl.de/~fullam/spire/spire_checkm2.tsv.gz
wget https://swifter.embl.de/~fullam/spire/representatives/spire_v1_representatives.tsv.gz
cd -

# 2. Collect coordinates for the first 5 studies only
.venv/bin/python scripts/spire/build_mag_coordinates.py --limit 5

# (Descriptor creation requires the extracted genomes — skip for a quick test-run)
```

The `--limit 5` flag caps how many studies are processed and returns in seconds.
Increase or remove it for the full run.

---

## Full workflow

### Step 1 — Download data and metadata

All files are hosted by EMBL. Download them into `scripts/spire/data/`:

```bash
cd scripts/spire/data

# Representative genome FASTA sequences (~81 GB)
wget https://swifter.embl.de/~fullam/spire/representatives/spire_representative_genomes.tar

# Metadata (~200 MB total)
wget https://swifter.embl.de/~fullam/spire/metadata/spire_v1_genome_metadata.tsv.gz
wget https://swifter.embl.de/~fullam/spire/metadata/spire_v1_cluster_metadata.tsv.gz
wget https://swifter.embl.de/~fullam/spire/metadata/spire_v1_microntology.tsv.gz
wget https://swifter.embl.de/~fullam/spire/spire_checkm2.tsv.gz
wget https://swifter.embl.de/~fullam/spire/representatives/spire_v1_representatives.tsv.gz

cd -
```

Then extract the genome archive (~82 GB extracted):

```bash
cd scripts/spire/data
tar -xf spire_representative_genomes.tar
cd -
```

This produces `scripts/spire/data/spire_representative_genomes/` containing one
gzip-compressed FASTA file (`.fa.gz`) per representative MAG (~1.16 million files).

**What each file contains:**

| File | Contents |
|---|---|
| `spire_representative_genomes.tar` | One `.fa.gz` FASTA per representative MAG |
| `spire_v1_genome_metadata.tsv.gz` | Per-MAG quality, taxonomy, and sample provenance (1.16 M rows) |
| `spire_v1_cluster_metadata.tsv.gz` | Per-cluster taxonomy and size stats |
| `spire_v1_representatives.tsv.gz` | Representative genome per cluster |
| `spire_v1_microntology.tsv.gz` | Microbial ontology annotations per sample |
| `spire_checkm2.tsv.gz` | CheckM2 quality assessment scores |

---

### Step 2 — Collect lat/lon coordinates

`build_mag_coordinates.py` creates `spire_v01_mag_coordinates.tsv` — one row per MAG with
its sample-level geographic coordinates.

**How it works:**

1. Reads the local `spire_v1_genome_metadata.tsv.gz` to build a `derived_from_sample` →
   `[genome_id, ...]` index (covers all 1.16 M MAGs and 73,675 samples — no API calls).
2. Fetches the SPIRE downloads page once to get the full list of study names and caches the result.
3. Fetches the study-level TSV for each study (one API call per study, ~hundreds total) to
   get `(sample_id, lat, lon)`.
4. For each sample present in the local metadata, emits one output row per `genome_id`.

Output is restricted to MAGs in your local download, keeping coordinates consistent with the
rest of the dataset. The SPIRE sample API is never called.

**About missing coordinates:** ~15% of output rows will have empty `latitude`/`longitude`
because SPIRE does not record collection coordinates for all samples. Metalog enrichment
(see below) fills in many of these gaps.

```bash
# Full run — auto-detects spire_v1_genome_metadata.tsv.gz under scripts/spire/data/
.venv/bin/python scripts/spire/build_mag_coordinates.py

# Resume after an interrupted run:
.venv/bin/python scripts/spire/build_mag_coordinates.py --resume

# Limit to the first N studies (for testing):
.venv/bin/python scripts/spire/build_mag_coordinates.py --limit 5

# Process specific studies by name:
.venv/bin/python scripts/spire/build_mag_coordinates.py \
  --study-name Coelho_2018_dog \
  --study-name HMP

# Enrich with Metalog (recommended — fills missing coordinates, adds granularity flags):
.venv/bin/python scripts/spire/build_mag_coordinates.py \
  --metalog-public-preset wide-extended-all \
  --metalog-download-mapping
```

#### Metalog enrichment

Metalog ([metalog.embl.de](https://metalog.embl.de)) provides manually curated sample
metadata including:
- Corrected and more precise coordinates than SPIRE (e.g. swapped lat/lon fixes)
- Coordinate granularity flags — whether coordinates are exact, inferred from city/region, etc.
- Coverage for many samples that have empty lat/lon in SPIRE

The `--metalog-public-preset wide-extended-all` option downloads four pre-built bundles
covering human, animal, ocean, and environmental samples. Adding `--metalog-download-mapping`
also downloads the sequencing DB mapping file to handle cases where SPIRE and Metalog use
different accession formats for the same sample.

Metalog coordinates take precedence over SPIRE coordinates when both are available. Both sets
of values are retained in the output for provenance (`latitude_spire`, `latitude_metalog`).

| Option | Description |
|---|---|
| `--metalog-public-preset wide-extended-all` | Download all four public Metalog metadata bundles |
| `--metalog-download-mapping` | Download the Metalog sequencing DB mapping for ID expansion |
| `--metalog-tsv <path>` | Use a local Metalog TSV export instead of downloading |
| `--metalog-url <url>` | Download a specific Metalog TSV URL (repeatable) |
| `--metalog-no-auth` | Skip the Authorization header (for public endpoints) |

**Outputs** (written to `scripts/spire/data/` by default):

| File | Contents |
|---|---|
| `spire_v01_mag_coordinates.tsv` | MAG-to-sample coordinate table (one row per MAG) |
| `spire_v01_mag_coordinates_summary.json` | Coverage statistics |
| `spire_v01_mag_coordinates_checkpoint.json` | Completed studies (for `--resume`) |

**Cache** (written to `scripts/spire/data/.cache/` — safe to delete and regenerate):

| File | Contents |
|---|---|
| `spire_study_names.json` | Study list scraped from the SPIRE downloads page |

#### Key options

| Option | Default | Description |
|---|---|---|
| `--data-dir` | `scripts/spire/data/` | Directory for outputs and auto-detection of input files |
| `--genome-metadata` | auto-detected | Explicit path to `spire_v1_genome_metadata.tsv.gz` |
| `--limit N` | *(no limit)* | Process only the first N studies — useful for testing |
| `--study-name` | *(all studies)* | Process only named studies (repeatable) |
| `--refetch-studies` | false | Re-fetch the study list from the downloads page, ignoring the cache |
| `--resume` | false | Skip studies already in the checkpoint file |
| `--timeout` | 60 | Per-request timeout in seconds |
| `--max-retries` | 2 | Max HTTP retries per request |
| `--sleep-seconds` | 0.1 | Delay between study API calls |

#### Output schema (`spire_v01_mag_coordinates.tsv`)

| Column | Description |
|---|---|
| `mag_id` | SPIRE MAG identifier (`spire_mag_XXXXXXXX`) — joins to `genome_id` in genome metadata |
| `sample_id` | ENA/SRA biosample accession — joins to `derived_from_sample` in genome metadata |
| `study_name` | SPIRE study name |
| `latitude` | Canonical latitude (Metalog if available, otherwise SPIRE) |
| `longitude` | Canonical longitude (Metalog if available, otherwise SPIRE) |
| `latitude_spire` | Raw latitude from SPIRE study API |
| `longitude_spire` | Raw longitude from SPIRE study API |
| `latitude_metalog` | Latitude from Metalog enrichment (empty if not enriched) |
| `longitude_metalog` | Longitude from Metalog enrichment (empty if not enriched) |
| `coordinate_source` | `spire` or `metalog` |
| `coordinate_granularity` | Granularity flag from Metalog (empty if not enriched) |
| `metalog_is_corrected` | Correction flag from Metalog (empty if not enriched) |
| `source_version` | Dataset version string (`spire-v1`) |
| `fetched_at_unix` | Unix timestamp of the API fetch |

---

### Step 3 — Create the data package descriptor

`create_descriptor.py` generates `spire_v01_datapackage.json` — a
[frictionless data package](https://specs.frictionlessdata.io/data-package/) descriptor
covering all downloaded files, including the coordinates TSV if it exists.

```bash
.venv/bin/python scripts/spire/create_descriptor.py
```

The script auto-detects:
- `scripts/spire/data/spire_representative_genomes/` — all `.fa.gz` genome files
- `scripts/spire/data/spire_v01_mag_coordinates.tsv` — added as a resource if present
- `scripts/spire/data/spire_v01_mag_coordinates_summary.json` — added if present

Output: `scripts/spire/data/spire_v01_datapackage.json`

Run `create_descriptor.py` **after** Step 2 so the coordinates artifacts are included.

---

### Step 4 — Verify staged files

After completing Steps 1–3, the staging directory should contain:

```
scripts/spire/data/
├── spire_representative_genomes/        # ~82 GB, ~1.16M .fa.gz files
├── spire_representative_genomes.tar     # ~81 GB original archive
├── spire_v1_genome_metadata.tsv.gz      # ~92 MB
├── spire_v1_cluster_metadata.tsv.gz     # ~2 MB
├── spire_v1_representatives.tsv.gz      # ~700 KB
├── spire_v1_microntology.tsv.gz
├── spire_checkm2.tsv.gz                 # ~106 MB
├── spire_v01_mag_coordinates.tsv        # generated by Step 2
├── spire_v01_mag_coordinates_summary.json
├── spire_v01_datapackage.json           # generated by Step 3
└── .cache/
    └── spire_study_names.json
```

---

### Step 5 — Upload to MinIO

Upload is manual using the `mc` CLI. Set your MinIO alias first (`mc alias set ...`),
then copy each file:

```bash
TARGET=myminio/cdm-lake/tenant-general-warehouse/kbase/datasets/spire

mc cp scripts/spire/data/spire_representative_genomes.tar      $TARGET/
mc cp scripts/spire/data/spire_v1_genome_metadata.tsv.gz       $TARGET/
mc cp scripts/spire/data/spire_v1_cluster_metadata.tsv.gz      $TARGET/
mc cp scripts/spire/data/spire_v1_representatives.tsv.gz       $TARGET/
mc cp scripts/spire/data/spire_v1_microntology.tsv.gz          $TARGET/
mc cp scripts/spire/data/spire_checkm2.tsv.gz                  $TARGET/
mc cp scripts/spire/data/spire_v01_mag_coordinates.tsv         $TARGET/
mc cp scripts/spire/data/spire_v01_mag_coordinates_summary.json $TARGET/
mc cp scripts/spire/data/spire_v01_datapackage.json            $TARGET/

# Verify
mc ls $TARGET/
```

---

## Reference

### SPIRE API column reference (confirmed May 2026)

| Endpoint | Column names |
|---|---|
| `/spire/api/study/{name}?format=tsv` | `sample_id`, `mags`, `lat`, `lon`, `microntology` |
| `/spire/api/sample/{sample_id}?format=tsv` | `spire_id`, `sample_id`, `genome_size`, `num_contigs`, `n50`, `completeness`, `contamination`, `gunc_css`, `gunc_rrs`, `gene_count`, `spire_cluster` |

### Identifier join keys

The coordinates TSV joins directly to the genome metadata on two keys:

- `coordinates.mag_id` = `genome_metadata.genome_id` (both `spire_mag_XXXXXXXX`)
- `coordinates.sample_id` = `genome_metadata.derived_from_sample` (ENA/SRA biosample accession)

No ID translation is needed — both the API and the metadata downloads use the same identifiers.


