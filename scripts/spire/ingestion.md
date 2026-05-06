# SPIRE Reference Data Schema Documentation

**Database**: `refdata_spire`
**Location**: On-prem Delta Lakehouse (BERDL)
**Tenant**: refdata
**Last Updated**: 2026-05-06
**Verified**: Spark SQL row counts and sample queries

---

## Overview

The `refdata_spire` namespace holds data from the **SPIRE (Sequence-based
Planetary-scale mIcrobiome REcovery) MAG catalog** (v01, September 2023),
published by EMBL and available at https://spire.embl.de/. SPIRE is a global
marine and environmental MAG catalog covering 2.8M MAGs from 95K samples.

Key scale:

| What | Count |
|------|-------|
| Total MAGs (CheckM2 table) | 2,796,940 |
| Medium-to-high quality MAGs (genome_metadata) | 1,158,553 |
| Representative genomes / clusters | 107,008 |
| Samples with environment annotations | 95,305 |
| MAGs with geographic coordinates | 960,425 (83.7%) |

All tables are version **v01** (September 2023), ingested March–May 2026.

---

## Table Summary

| Table | Rows | Description |
|-------|------|-------------|
| `checkm2` | 2,796,940 | CheckM2 quality for all MAGs (including low quality) |
| `genome_metadata` | 1,158,553 | Per-MAG quality, taxonomy, cluster assignment (medium-high quality) |
| `representative_genome_index` | 107,008 | One row per representative genome (specI_v4 + spire_v1 clusters) |
| `sample_microntology` | 510,867 | Sample-to-environment-term annotations (microntology) |
| `environment_envo_map` | 90 | Lookup: microntology term → ENVO label/ID/biome |
| `mag_coordinates` | 1,148,021 | Per-MAG geographic coordinates (lat/lon) |

---

## Table Details

### `checkm2`

CheckM2 quality assessment for all 2,796,940 SPIRE MAGs, including those below the
medium-quality threshold. Use `genome_metadata` for filtered, annotated MAGs.

**Source:** `spire_checkm2.tsv` (v01)

| Column | Type | Description |
|--------|------|-------------|
| `derived_from_sample` | string | BioSample ID (SAMN...) |
| `mag_name` | string | SPIRE MAG identifier (e.g. `spire_mag_01162595`) |
| `completeness` | double | Estimated genome completeness (%) |
| `contamination` | double | Estimated contamination (%) |
| `completeness_model_used` | string | CheckM2 model: `Gradient Boost (General Model)` or `Neural Network (Specific Model)` |
| `translation_table_used` | bigint | Genetic code table |
| `additional_notes` | string | CheckM2 notes (usually NULL) |
| `total_length` | bigint | Total assembly length in bp |
| `number` | bigint | Number of contigs |
| `mean_length` | double | Mean contig length in bp |
| `longest` | bigint | Longest contig in bp |
| `shortest` | bigint | Shortest contig in bp |
| `n_count` | bigint | Total N bases |
| `gaps` | bigint | Number of gaps |
| `n50` | bigint | N50 length in bp |
| `n50n` | bigint | N50 contig count |
| `n70` | bigint | N70 length in bp |
| `n70n` | bigint | N70 contig count |
| `n90` | bigint | N90 length in bp |
| `n90n` | bigint | N90 contig count |

**Key queries:**
```sql
-- High-quality MAGs (completeness ≥ 90, contamination ≤ 5)
SELECT mag_name, completeness, contamination, total_length
FROM refdata_spire.checkm2
WHERE completeness >= 90 AND contamination <= 5
ORDER BY completeness DESC

-- Assembly stats for a specific sample
SELECT mag_name, completeness, contamination, n50, total_length
FROM refdata_spire.checkm2
WHERE derived_from_sample = 'SAMN08778200'
```

---

### `genome_metadata`

Per-MAG metadata for the 1,158,553 medium-to-high quality SPIRE MAGs that passed
quality filtering. Includes GTDB-Tk taxonomy, cluster assignment, GUNC chimera scores,
and genome assembly stats.

**Source:** `spire_v1_genome_metadata.tsv` (v01)

| Column | Type | Description |
|--------|------|-------------|
| `genome_id` | string | SPIRE MAG ID (e.g. `spire_mag_02710429`) |
| `spire_cluster` | string | SPIRE cluster ID or proGenomes3 specI ID |
| `spire_cluster_assignment` | string | How the MAG was assigned to a cluster |
| `genome_size` | double | Assembly size in bp |
| `genome_size_est` | double | Estimated true genome size in bp |
| `gs_est_ratio` | double | Ratio of assembly size to estimated size |
| `n_contigs` | double | Number of contigs |
| `n50` | double | N50 length in bp |
| `max_contig_length` | double | Longest contig in bp |
| `translation_table` | double | Genetic code table |
| `completeness` | double | CheckM2 completeness (%) |
| `contamination` | double | CheckM2 contamination (%) |
| `drep` | double | dRep cluster representative score |
| `n_genes` | bigint | Total predicted genes |
| `gunc_taxlevel` | string | Taxonomic level of GUNC detection |
| `clade_separation_score` | double | GUNC clade separation score |
| `gunc_contamination` | double | GUNC contamination score |
| `reference_representation_score` | double | GUNC reference representation score |
| `gunc_pass` | double | GUNC pass threshold (numeric) |
| `gunc_pass_5` | boolean | Whether MAG passes GUNC at 5% threshold |
| `classification` | string | Full GTDB-Tk taxonomy string (d__;p__;c__;o__;f__;g__;s__) |
| `domain` | string | GTDB domain |
| `phylum` | string | GTDB phylum |
| `class` | string | GTDB class |
| `order` | string | GTDB order |
| `family` | string | GTDB family |
| `genus` | string | GTDB genus |
| `species` | string | GTDB species |
| `red_value` | double | Relative evolutionary divergence |
| `derived_from_sample` | string | BioSample ID (SAMN...) |

**Key queries:**
```sql
-- All Proteobacteria MAGs with high quality
SELECT genome_id, species, completeness, contamination
FROM refdata_spire.genome_metadata
WHERE phylum = 'Proteobacteria'
  AND completeness >= 90 AND contamination <= 5

-- Sample-to-genome lookup
SELECT genome_id, classification, completeness
FROM refdata_spire.genome_metadata
WHERE derived_from_sample = 'SAMN15294818'
```

**Pitfalls:**
- `n_contigs`, `n50`, and other assembly stats are DOUBLE despite being integer-valued — cast with `CAST(n_contigs AS BIGINT)` if needed
- `gunc_pass` and `gunc_pass_5` are separate: use `gunc_pass_5 = true` for the boolean threshold filter
- `classification` is the full semicolon-delimited GTDB string; use individual `domain`/`phylum`/... columns for filtering

---

### `representative_genome_index`

One row per representative genome (107,008 total). Covers two cluster types:
- **`specI_v4`** — proGenomes3 reference clusters (species-level)
- **`spire_v1`** — de novo SPIRE 95% ANI clusters

Derived by joining the SPIRE datapackage genome index with cluster taxonomy, cluster
size stats, and per-MAG quality metrics.

**Source:** derived from `spire_v01_datapackage.json`, `spire_v1_cluster_metadata.tsv`,
`spire_v1_representatives.tsv`, `spire_v1_genome_metadata.tsv` (v01)

| Column | Type | Description |
|--------|------|-------------|
| `genome_id` | string | Representative genome ID |
| `name` | string | Genome file name |
| `path` | string | Path to genome file in SPIRE data package |
| `cluster_type` | string | `specI_v4` or `spire_v1_095` |
| `mediatype` | string | File format (e.g. `application/x-gzip`) |
| `bytes_size` | bigint | File size in bytes |
| `cluster_size_spire` | double | Cluster size from SPIRE |
| `cluster_size_pg3` | double | Cluster size from proGenomes3 |
| `cluster_size_combined` | double | Combined cluster size |
| `cluster_domain` | string | GTDB domain for the cluster |
| `cluster_phylum` | string | GTDB phylum for the cluster |
| `cluster_class` | string | GTDB class |
| `cluster_order` | string | GTDB order |
| `cluster_family` | string | GTDB family |
| `cluster_genus` | string | GTDB genus |
| `cluster_species` | string | GTDB species name |
| `cluster_id` | string | Cluster identifier |
| `completeness` | double | CheckM2 completeness of the representative |
| `contamination` | double | CheckM2 contamination of the representative |
| `drep` | double | dRep score |
| `n_genes` | double | Predicted gene count |
| `n_contigs` | double | Contig count |
| `genome_size` | double | Assembly size in bp |
| `genome_size_est` | double | Estimated true genome size |
| `gunc_pass` | double | GUNC pass score |
| `gunc_pass_5` | boolean | GUNC pass at 5% threshold |
| `gunc_contamination` | double | GUNC contamination |
| `translation_table` | double | Genetic code table |
| `derived_from_sample` | string | BioSample ID |
| `spire_cluster` | string | SPIRE cluster ID |
| `spire_cluster_assignment` | string | Cluster assignment method |

**Key queries:**
```sql
-- All representative genomes for a phylum
SELECT genome_id, cluster_species, cluster_size_combined, completeness
FROM refdata_spire.representative_genome_index
WHERE cluster_phylum = 'Proteobacteria'
ORDER BY cluster_size_combined DESC

-- Cluster type breakdown
SELECT cluster_type, COUNT(*) AS n FROM refdata_spire.representative_genome_index GROUP BY 1
```

---

### `sample_microntology`

Sample-to-environment-term annotations using the SPIRE microntology vocabulary (90
standardised terms). One sample can have multiple environment terms (multiple rows).

**Source:** `spire_v1_microntology.tsv` (v01)

| Column | Type | Description |
|--------|------|-------------|
| `sample_id` | string | BioSample ID (SAMN...) |
| `project_id` | string | BioProject ID (PRJNA...) |
| `environment_term` | string | Standardised environment term (e.g. `terrestrial:soil`) |

**Key queries:**
```sql
-- All environment terms for a sample
SELECT environment_term FROM refdata_spire.sample_microntology
WHERE sample_id = 'SAMN15803493'

-- Count samples per environment
SELECT environment_term, COUNT(DISTINCT sample_id) AS n_samples
FROM refdata_spire.sample_microntology
GROUP BY environment_term ORDER BY n_samples DESC

-- Join with ENVO map for biome labels
SELECT s.sample_id, s.environment_term, e.envo_biome, e.envo_label
FROM refdata_spire.sample_microntology s
JOIN refdata_spire.environment_envo_map e USING (environment_term)
WHERE e.is_environment = true
```

**Pitfalls:**
- One sample has multiple rows (one per environment term) — always `COUNT(DISTINCT sample_id)` not `COUNT(*)`

---

### `environment_envo_map`

Lookup table mapping the 90 SPIRE microntology environment terms to ENVO ontology
labels, IDs, and biome classifications. 90 rows total.

| Column | Type | Description |
|--------|------|-------------|
| `environment_term` | string | SPIRE microntology term (joins to `sample_microntology`) |
| `envo_label` | string | ENVO ontology label (NULL for non-ENVO terms) |
| `envo_id` | string | ENVO ontology ID (NULL for non-ENVO terms) |
| `envo_biome` | string | Broad biome classification (NULL for non-environment terms) |
| `is_environment` | boolean | Whether this term represents a true environment (vs. metadata like age group) |

---

### `mag_coordinates`

Per-MAG geographic coordinates for 1,148,021 SPIRE MAGs. Latitude/longitude sourced
from SPIRE original metadata and MetaLog corrections. 83.7% (960,425) of MAGs have
coordinates; 187,596 are missing (NULL lat/lon).

**Source:** `spire_v01_mag_coordinates.tsv` (v01, fetched 2026-05)
**Note:** Source file had Windows line endings (`\r\n`) — a clean copy was uploaded to
bronze before ingest (`spire_v01_mag_coordinates_clean.tsv`).

| Column | Type | Description |
|--------|------|-------------|
| `mag_id` | string | SPIRE MAG ID (e.g. `spire_mag_00445168`) |
| `sample_id` | string | BioSample ID |
| `study_name` | string | Study/project name |
| `latitude` | double | Best-available latitude (NULL if unavailable) |
| `longitude` | double | Best-available longitude (NULL if unavailable) |
| `latitude_spire` | double | Original SPIRE latitude |
| `longitude_spire` | double | Original SPIRE longitude |
| `latitude_metalog` | double | MetaLog-corrected latitude |
| `longitude_metalog` | double | MetaLog-corrected longitude |
| `coordinate_source` | string | Which source provided the best-available coords: `metalog` (960,425 rows — Metalog had both lat and lon) or `spire` (187,596 rows — Metalog lacked coordinates, falling back to SPIRE's lat/lon) |
| `coordinate_granularity` | string | Precision descriptor (currently NULL for all rows) |
| `metalog_is_corrected` | string | Whether MetaLog applied a correction (empty for all rows in v01 — not populated by the wide-extended-all bundle) |
| `source_version` | string | Data version string |
| `fetched_at_unix` | bigint | Unix timestamp when coordinates were fetched |

**Coverage:**
- Total MAGs: 1,148,021
- With coordinates: 960,425 (83.7%) — `coordinate_source = 'metalog'`
- Without coordinates: 187,596 (16.3%) — `coordinate_source = 'spire'`, Metalog had no coords; SPIRE lat/lon also empty for nearly all of these (19 rows have lat but no lon due to incomplete SPIRE metadata)
- Latitude range: −78.1° to 85.0°
- Longitude range: −179.3° to 179.6°

**Key queries:**
```sql
-- MAGs with valid coordinates in a bounding box (e.g. North Atlantic)
SELECT mag_id, latitude, longitude
FROM refdata_spire.mag_coordinates
WHERE latitude BETWEEN 30 AND 70
  AND longitude BETWEEN -80 AND 0
  AND latitude IS NOT NULL

-- Join coordinates to taxonomy
SELECT c.mag_id, c.latitude, c.longitude, m.phylum, m.species
FROM refdata_spire.mag_coordinates c
JOIN refdata_spire.genome_metadata m ON c.mag_id = m.genome_id
WHERE c.latitude IS NOT NULL
  AND m.phylum = 'Proteobacteria'
```

**Pitfalls:**
- Filter `WHERE latitude IS NOT NULL` — 16.3% of rows have NULL lat/lon; `coordinate_source = 'spire'` does NOT imply valid coordinates, it only means Metalog had no data and SPIRE was the fallback (which is also usually empty)
- `coordinate_granularity` is NULL for all current rows — not yet populated in v01
- `metalog_is_corrected` is NULL for all current rows — the Metalog wide-extended-all bundle does not populate this field in v01

---

## Cross-Reference Patterns

```sql
-- All MAGs from a sample with quality and coordinates
SELECT g.genome_id, g.classification, g.completeness, c.latitude, c.longitude
FROM refdata_spire.genome_metadata g
JOIN refdata_spire.mag_coordinates c ON g.genome_id = c.mag_id
WHERE g.derived_from_sample = 'SAMN09837424'
  AND c.latitude IS NOT NULL

-- Samples with soil environment and their MAG counts
SELECT s.sample_id, COUNT(DISTINCT g.genome_id) AS n_mags
FROM refdata_spire.sample_microntology s
JOIN refdata_spire.genome_metadata g ON s.sample_id = g.derived_from_sample
WHERE s.environment_term LIKE 'terrestrial:soil%'
GROUP BY s.sample_id ORDER BY n_mags DESC

-- Cluster representatives for a genus with coordinates
SELECT r.genome_id, r.cluster_species, c.latitude, c.longitude
FROM refdata_spire.representative_genome_index r
JOIN refdata_spire.mag_coordinates c ON r.genome_id = c.mag_id
WHERE r.cluster_genus = 'g__Prochlorococcus_A'
  AND c.latitude IS NOT NULL
```

## Bronze Paths

| What | Path |
|------|------|
| Raw data (v01) | `s3a://cdm-lake/tenant-general-warehouse/refdata/datasets/spire/raw_data/v01/` |
| Metadata YAMLs | `s3a://cdm-lake/tenant-general-warehouse/refdata/datasets/spire/metadata/` |
| Datapackage | `s3a://cdm-lake/tenant-general-warehouse/refdata/datasets/spire/metadata/v01/spire_v01_datapackage.json` |
