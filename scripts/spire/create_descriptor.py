#!/usr/bin/env python3
"""
Create a frictionless data package descriptor for the SPIRE dataset.

SPIRE (Sequence-based Planetary scale mIcrobiome REcovery) v01 is a large-scale
collection of metagenome-assembled genomes (MAGs) from diverse environments.

Dataset: https://spire.embl.de/
Downloads: https://spire.embl.de/downloads
Reference: Fullam et al. (2023)

Usage:
    uv run python scripts/spire/create_descriptor.py [--data-dir DATA_DIR] [--output OUTPUT]

The descriptor covers the contents of the spire_representative_genomes/ folder
(one .fa.gz file per representative MAG) and is written as JSON.

The output file defaults to spire_v01_datapackage.json, written alongside the
data directory (i.e. scripts/spire/data/spire_v01_datapackage.json).
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from kbase_transfers import validate_descriptor


# ---------------------------------------------------------------------------
# Dataset constants
# ---------------------------------------------------------------------------

DATASET_VERSION = "v01"
DATASET_DATE = "2023-09"




# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def file_sha256(path: Path) -> str:
    """Compute SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def build_genome_resources(genome_dir: Path) -> list[dict]:
    """
    Build frictionless resource descriptors for all .fa.gz MAG FASTA files
    found directly inside genome_dir (i.e. spire_representative_genomes/).
    """
    fa_files = sorted(genome_dir.glob("*.fa.gz"))
    print(f"  Building resource entries for {len(fa_files)} genome FASTA files...")
    resources = []
    for fa_path in fa_files:
        mag_id = fa_path.stem.replace(".fa", "")  # strip .fa from <id>.fa.gz
        resources.append(
            {
                "name": fa_path.name.lower().replace(".", "-").replace("_", "-"),
                "path": fa_path.name,
                "description": f"Representative MAG genome sequence for {mag_id} (gzip-compressed FASTA).",
                "mediatype": "application/gzip",
                "bytes": fa_path.stat().st_size,
            }
        )
    return resources


def build_credit(timestamp: int) -> dict:
    """
    Build KBase credit metadata for the SPIRE v01 dataset following the
    credit_engine schema: https://kbase.github.io/credit_engine/
    """
    return {
        "identifier": "https://spire.embl.de/",
        "resource_type": "dataset",
        "version": DATASET_VERSION,
        "titles": [
            {
                "title": "SPIRE: Sequence-based Planetary-scale mIcrobiome REcovery"
            }
        ],
        "descriptions": [
            {
                "description_text": (
                    "Meta’omic data on microbial diversity and function accrue exponentially "
                    "in public repositories, but derived information is often siloed according "
                    "to data type, study or sampled microbial environment. Here we present SPIRE, "
                    "a Searchable Planetary-scale mIcrobiome REsource that integrates various "
                    "consistently processed metagenome-derived microbial data modalities across "
                    "habitats, geography and phylogeny. SPIRE encompasses 99 146 metagenomic samples "
                    "from 739 studies covering a wide array of microbial environments and augmented "
                    "with manually-curated contextual data. Across a total metagenomic assembly of "
                    "16 Tbp, SPIRE comprises 35 billion predicted protein sequences and 1.16 million "
                    "newly constructed metagenome-assembled genomes (MAGs) of medium or high quality. "
                    "Beyond mapping to the high-quality genome reference provided by proGenomes3 "
                    "(http://progenomes.embl.de), these novel MAGs form 92 134 novel species-level "
                    "clusters, the majority of which are unclassified at species level using current "
                    "tools. SPIRE enables taxonomic profiling of these species clusters via an updated, "
                    "custom mOTUs database (https://motu-tool.org/) and includes several layers of "
                    "functional annotation, as well as crosslinks to several (micro-)biological "
                    "databases. The resource is accessible, searchable and browsable via "
                    "http://spire.embl.de"
                ),
                "description_type": "abstract",
            }
        ],
        "url": "https://spire.embl.de/",
        "dates": [
            {
                "date": DATASET_DATE,
                "event": "issued",
            }
        ],
        "contributors": [
            {
                "contributor_type": "Person",
                "given_name":"Thomas SB",
                "family_name": "Schmidt",
                "contributor_roles": ["contact_person"],
            },
            {
                "contributor_type": "Person",
                "given_name": "Anthony",
                "family_name": "Fullam",
            },
            {
                "contributor_type": "Person",
                "given_name": "Pamela",
                "family_name": "Ferretti",
            },
            {
                "contributor_type": "Person",
                "given_name": "Askarbek",
                "family_name": "Orakov",
            },
            {
                "contributor_type": "Person",
                "given_name": "Oleksandr M",
                "family_name": "Maistrenko",
            },
            {
                "contributor_type": "Person",
                "given_name": "Hans-Joachim",
                "family_name": "Ruscheweyh",
            },
            {
                "contributor_type": "Person",
                "given_name": "Ivica",
                "family_name": "Letunic",
            },
            {
                "contributor_type": "Person",
                "given_name": "Yiqian",
                "family_name": "Duan",
            },
            {
                "contributor_type": "Person",
                "given_name": "Thea",
                "family_name": "Van Rossum",
            },
            {
                "contributor_type": "Person",
                "given_name": "Shinichi",
                "family_name": "Sunagawa",
            },
            {
                "contributor_type": "Person",
                "given_name": "Daniel R",
                "family_name": "Mende",
            },
            {
                "contributor_type": "Person",
                "given_name": "Robert D",
                "family_name": "Finn",
            },
            {
                "contributor_type": "Person",
                "given_name": "Michael",
                "family_name": "Kuhn",
            },
            {
                "contributor_type": "Person",
                "given_name": "Louis Pedro",
                "family_name": "Coelho",
            },
            {
                "contributor_type": "Person",
                "given_name": "Peer",
                "family_name": "Bork",
                "contributor_roles": ["contact_person"],
            },
            {
                "contributor_type": "Organization",
                "name": "European Molecular Biology Laboratory (EMBL)",
                "contributor_id": "ROR:01yr73893",
                "contributor_roles": ["hosting_institution"],
            },
        ],
        "publisher": {
            "organization_name": "European Molecular Biology Laboratory (EMBL)",
            "organization_id": "ROR:01yr73893",
        },
        "related_identifiers": [
            {
                "id": "https://github.com/grp-bork/spire_contribute",
                "relationship_type": "is_supplemented_by",
            },
            {
                "id": "https://doi.org/10.1093/nar/gkad943",
                "relationship_type": "is_described_by",
            },
        ],
        "license": {
            "url": "https://creativecommons.org/licenses/by/4.0/",
        },
        "meta": {
            "credit_metadata_schema_version": "1.0",
            "credit_metadata_source": [
                {
                    "source_name": "SPIRE Downloads",
                    "source_url": "https://spire.embl.de/downloads",
                    "access_timestamp": timestamp,
                }
            ],
            "saved_by": "kbase-transfers-spire",
            "timestamp": timestamp,
        },
    }


# ---------------------------------------------------------------------------
# Main descriptor builder
# ---------------------------------------------------------------------------

def create_descriptor(genome_dir: Path) -> dict:
    """Build and validate the full frictionless data package descriptor."""
    timestamp = int(datetime.now().timestamp())

    print("Building genome FASTA resources...")
    resources = build_genome_resources(genome_dir)
    if not resources:
        raise ValueError(f"No .fa.gz files found in {genome_dir}")

    print("Building credit metadata...")
    credit = build_credit(timestamp)

    descriptor = {
        "name": "spire-v01-representative-genomes",
        "title": "SPIRE v01 — Representative MAG Genome Sequences",
        "description": (
            "Gzip-compressed FASTA files for all representative MAGs from the SPIRE v01 "
            "dataset (September 2023). One genome per 95% ANI cluster, covering "
            "diverse global environments."
        ),
        "version": DATASET_VERSION,
        "homepage": "https://spire.embl.de/",
        "licenses": [{"name": "CC-BY-4.0", "path": "https://creativecommons.org/licenses/by/4.0/"}],
        "credit": credit,
        "resources": resources,
    }

    print("Validating descriptor...")
    result = validate_descriptor(descriptor)
    if not result.is_valid:
        raise ValueError(f"Validation failed:\n{result.summary()}")
    print("  Validation passed.")

    return descriptor



# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Create a frictionless data package descriptor for the SPIRE dataset."
    )
    default_genome_dir = str(Path(__file__).parent / "data" / "spire_representative_genomes")
    parser.add_argument(
        "--data-dir",
        default=default_genome_dir,
        help=(
            "Path to the spire_representative_genomes/ directory containing .fa.gz files "
            f"(default: {default_genome_dir})"
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Output path for the descriptor JSON. "
            "Defaults to spire_v01_datapackage.json in the parent of --data-dir."
        ),
    )
    args = parser.parse_args()

    genome_dir = Path(args.data_dir)
    if not genome_dir.is_dir():
        print(f"Error: directory not found: {genome_dir}", file=sys.stderr)
        sys.exit(1)

    output_path = (
        Path(args.output)
        if args.output
        else genome_dir.parent / "spire_v01_datapackage.json"
    )

    print(f"Creating SPIRE v01 data package descriptor...")
    print(f"  Genome directory : {genome_dir}")
    print(f"  Output file      : {output_path}")
    print()

    descriptor = create_descriptor(genome_dir)

    with open(output_path, "w") as f:
        json.dump(descriptor, f, indent=2)
        f.write("\n")

    n_resources = len(descriptor["resources"])
    print(f"\nDescriptor written to {output_path} ({n_resources} resources).")


if __name__ == "__main__":
    main()
