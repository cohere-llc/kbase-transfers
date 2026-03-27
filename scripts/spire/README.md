# SPIRE Data Transfer Scripts

Scripts for the [SPIRE](https://spire.embl.de/) (Sequence-based Planetary-scale mIcrobiome REcovery) dataset. SPIRE v01 is a large-scale collection of metagenome-assembled genomes (MAGs) from globally diverse environments.

## Downloading SPIRE Data

All dataset files are available from the [SPIRE downloads page](https://spire.embl.de/downloads).

Download the following files into `scripts/spire/data/`:

```bash
cd scripts/spire/data

# Representative genome FASTA sequences (~large)
wget https://spire.embl.de/api/download?filename=spire_representative_genomes.tar \
  -O spire_representative_genomes.tar

# Metadata files
wget https://spire.embl.de/api/download?filename=spire_v1_genome_metadata.tsv.gz \
  -O spire_v1_genome_metadata.tsv.gz
wget https://spire.embl.de/api/download?filename=spire_v1_cluster_metadata.tsv.gz \
  -O spire_v1_cluster_metadata.tsv.gz
wget https://spire.embl.de/api/download?filename=spire_v1_representatives.tsv.gz \
  -O spire_v1_representatives.tsv.gz
wget https://spire.embl.de/api/download?filename=spire_checkm2.tsv.gz \
  -O spire_checkm2.tsv.gz
```

Extract the representative genome archive:

```bash
tar -xf spire_representative_genomes.tar
```

This produces a `spire_representative_genomes/` directory containing one gzip-compressed FASTA file (`.fa.gz`) per representative MAG.

## Creating the Data Package Descriptor

The `create_descriptor.py` script generates a [frictionless data package](https://specs.frictionlessdata.io/data-package/) descriptor (`spire_v01_datapackage.json`) for the dataset. The descriptor includes standard frictionless resource metadata and a `credit` key following the [KBase credit metadata schema](https://kbase.github.io/credit_engine/).

```bash
# From the repository root:
uv run python scripts/spire/create_descriptor.py

# Or specify a custom data directory / output name:
uv run python scripts/spire/create_descriptor.py \
  --data-dir scripts/spire/data/spire_representative_genomes \
  --output spire_v01_datapackage.json
```

The descriptor is written to `data/spire_v01_datapackage.json`.

## Uploading to MinIO

File upload is performed manually. Use the MinIO console or `mc` CLI to upload the data files and descriptor to the target bucket:

```bash
# Example using mc (MinIO Client)
mc cp scripts/spire/data/spire_representative_genomes.tar \
   myminio/cdm-lake/tenant-general-warehouse/kbase/datasets/spire/

mc cp scripts/spire/data/spire_v1_genome_metadata.tsv.gz \
   myminio/cdm-lake/tenant-general-warehouse/kbase/datasets/spire/

mc cp scripts/spire/data/spire_v01_datapackage.json \
   myminio/cdm-lake/tenant-general-warehouse/kbase/datasets/spire/
```

