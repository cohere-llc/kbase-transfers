#!/usr/bin/env python3
"""
Build MAG-to-sample coordinate mappings for SPIRE v01.

This script follows SPIRE guidance for deriving MAG coordinates:
1. Fetch study-level TSV rows to obtain sample-level coordinates.
2. For each sample, fetch sample-level TSV rows and extract MAG IDs.
3. Emit one row per MAG-sample pair.

Optionally enrich coordinates from a Metalog TSV export keyed by sample ID.
When Metalog coordinates are available they become the canonical latitude/
longitude in the output, while SPIRE values are retained for provenance.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SPIRE_BASE_URL = "https://spire.embl.de/spire/api"
SOURCE_VERSION = "spire-v1"
DEFAULT_OUTPUT_NAME = "spire_v01_mag_coordinates.tsv"
DEFAULT_SUMMARY_NAME = "spire_v01_mag_coordinates_summary.json"
DEFAULT_CHECKPOINT_NAME = "spire_v01_mag_coordinates_checkpoint.json"
DEFAULT_STUDY_CACHE_NAME = "spire_study_names.json"

# Confirmed real column names from the SPIRE API (May 2026):
#   study endpoint:  sample_id, mags, lat, lon, microntology
#   sample endpoint: spire_id, sample_id, genome_size, num_contigs, n50,
#                    completeness, contamination, gunc_css, gunc_rrs,
#                    gene_count, spire_cluster
STUDY_TSV_SAMPLE_COL = "sample_id"
STUDY_TSV_LAT_COL = "lat"
STUDY_TSV_LON_COL = "lon"
SAMPLE_TSV_MAG_COL = "spire_id"

# Confirmed column names from the SPIRE bulk metadata download
# (spire_v1_genome_metadata.tsv.gz, May 2026).
GENOME_METADATA_FILENAME = "spire_v1_genome_metadata.tsv.gz"
GENOME_METADATA_ID_COL = "genome_id"
GENOME_METADATA_SAMPLE_COL = "derived_from_sample"

METALOG_PUBLIC_URLS = {
    "wide-extended-all": [
        "https://metalog.embl.de/static/download/metadata/human_extended_wide_latest.tsv.gz",
        "https://metalog.embl.de/static/download/metadata/animal_extended_wide_latest.tsv.gz",
        "https://metalog.embl.de/static/download/metadata/ocean_extended_wide_latest.tsv.gz",
        "https://metalog.embl.de/static/download/metadata/environmental_extended_wide_latest.tsv.gz",
    ],
}

METALOG_MAPPING_URL = (
    "https://metalog.embl.de/static/download/sequencing_db_mapping_latest.tsv.gz"
)

SPIRE_DOWNLOADS_URL = "https://spire.embl.de/downloads"
SPIRE_STUDIES_URL = "https://spire.embl.de/studies"

# Study names appear as top-level JS object keys in the downloads page:
#   "StudyName": { assemblies_url: ..., mags_url: ..., ... }
_STUDY_KEY_RE = re.compile(r'"([A-Za-z][A-Za-z0-9_-]+)"\s*:\s*\{')

# Study names also appear in the /studies page table data:
#   { study: "StudyName", samples: N }
_STUDY_TABLE_RE = re.compile(r'\{\s*study:\s*"([^"]+)",\s*samples:\s*\d+')

METALOG_SAMPLE_COLUMN_CANDIDATES = [
    "spire_sample_name",  # Metalog's dedicated column for SPIRE/ENA/SRA biosample accessions
    "sample_id",
    "sample_alias",
    "sample",
    "sample_accession",
    "biosample",
    "biosample_id",
    "derived_from_sample",
]

EXTERNAL_SAMPLE_COLUMN_CANDIDATES = [
    "external_id",
    "sample_accession",
    "biosample",
    "biosample_id",
    "biosample_accession",
    "external_sample_accession",
    "ena_sample_accession",
    "sra_sample_accession",
    "insdc_sample_accession",
    "sample",
]

KIND_COLUMN_CANDIDATES = [
    "kind",
    "entity_kind",
    "record_kind",
]

# Metalog-specific column candidates (Metalog uses different names from SPIRE)
METALOG_LAT_COLUMN_CANDIDATES = [
    "latitude",
    "lat",
    "sample_latitude",
    "geo_latitude",
    "lat_dd",
]

METALOG_LON_COLUMN_CANDIDATES = [
    "longitude",
    "lon",
    "long",
    "sample_longitude",
    "geo_longitude",
    "lng",
    "lon_dd",
]

METALOG_GRANULARITY_CANDIDATES = [
    "coordinate_granularity",
    "granularity",
    "latlon_granularity",
]

METALOG_SOURCE_CANDIDATES = [
    "coordinate_source",
    "source",
    "latlon_source",
]

METALOG_CORRECTED_CANDIDATES = [
    "is_corrected",
    "coordinates_corrected",
    "corrected",
]


@dataclass
class CoordinateRecord:
    mag_id: str
    sample_id: str
    study_name: str
    latitude: str
    longitude: str
    latitude_spire: str
    longitude_spire: str
    latitude_metalog: str
    longitude_metalog: str
    coordinate_source: str
    coordinate_granularity: str
    metalog_is_corrected: str
    source_version: str
    fetched_at_unix: int


def normalize_field_name(name: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")


def parse_tsv_rows(content: str) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    rows: list[dict[str, str]] = []
    for row in reader:
        clean = {
            (k or "").strip(): (v.strip() if isinstance(v, str) else "")
            for k, v in row.items()
            if k is not None
        }
        if any(clean.values()):
            rows.append(clean)
    return rows


def find_column_name(fieldnames: Iterable[str], candidates: list[str]) -> str | None:
    normalized = {normalize_field_name(name): name for name in fieldnames}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def first_present_value(row: dict[str, str], candidates: list[str]) -> str:
    field = find_column_name(row.keys(), candidates)
    if not field:
        return ""
    return (row.get(field) or "").strip()


def make_session(max_retries: int = 2, backoff_factor: float = 0.5) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET"]),
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def fetch_tsv(session: requests.Session, url: str, timeout: float) -> list[dict[str, str]]:
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return parse_tsv_rows(resp.text)


def _scrape_study_names_from_downloads_page(
    session: requests.Session,
    timeout: float,
) -> list[str]:
    """Fetch both SPIRE pages that list studies and return the union of names found.

    Two complementary sources are scraped and merged:
    - ``/downloads``: study names as JS object keys  ``"StudyName": { ... }``
    - ``/studies``: explicit table entries  ``{ study: "StudyName", samples: N }``

    Neither page is guaranteed to be complete on its own (e.g. MetaHIT is absent
    from ``/downloads`` but present on ``/studies``), so the union is used.
    Two HTTP requests; no pagination or iterative probing required.
    """
    names: set[str] = set()

    resp = session.get(SPIRE_DOWNLOADS_URL, timeout=timeout)
    resp.raise_for_status()
    names.update(_STUDY_KEY_RE.findall(resp.text))

    resp2 = session.get(SPIRE_STUDIES_URL, timeout=timeout)
    resp2.raise_for_status()
    names.update(_STUDY_TABLE_RE.findall(resp2.text))

    return sorted(names)


def enumerate_all_studies(
    cache_path: Path | None,
    *,
    timeout: float = 30.0,
    max_retries: int = 2,
    retry_backoff: float = 0.5,
    refetch: bool = False,
) -> list[str]:
    """Return the full list of SPIRE study names.

    Scrapes both the ``/downloads`` and ``/studies`` pages and returns the
    union of study names found (two HTTP requests, no pagination).  Using
    both sources guards against studies that appear on one page but not the
    other (e.g. MetaHIT is absent from ``/downloads``).

    Results are cached to *cache_path* (if provided) as a JSON file so that
    subsequent runs skip the network fetch.  Pass ``refetch=True`` to ignore
    any cached file.
    """
    if cache_path and cache_path.exists() and not refetch:
        with open(cache_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            studies = data.get("studies", [])
            if studies:
                print(f"Loaded {len(studies)} study names from cache: {cache_path}")
                return studies

    session = make_session(max_retries=max_retries, backoff_factor=retry_backoff)
    studies = _scrape_study_names_from_downloads_page(session, timeout)
    print(f"Found {len(studies)} SPIRE studies (union of /downloads and /studies pages).")

    if cache_path:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump({"studies": studies}, fh, indent=2)
            fh.write("\n")
        print(f"Cached study list to {cache_path}")

    return studies


def split_possible_ids(raw_value: str) -> list[str]:
    raw_value = (raw_value or "").strip()
    if not raw_value:
        return []

    # SPIRE rows are usually one MAG per row, but this handles packed lists safely.
    separators = [",", ";", "|"]
    values = [raw_value]
    for sep in separators:
        if sep in raw_value:
            values = [part.strip() for part in raw_value.split(sep)]
            break
    return [v for v in values if v]


def load_metalog_map(path: Path) -> dict[str, dict[str, str]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"No header found in Metalog TSV: {path}")

        sample_col = find_column_name(reader.fieldnames, METALOG_SAMPLE_COLUMN_CANDIDATES)
        lat_col = find_column_name(reader.fieldnames, METALOG_LAT_COLUMN_CANDIDATES)
        lon_col = find_column_name(reader.fieldnames, METALOG_LON_COLUMN_CANDIDATES)

        if not sample_col or not lat_col or not lon_col:
            raise ValueError(
                "Metalog TSV must include sample-id, latitude, and longitude columns."
            )

        gran_col = find_column_name(reader.fieldnames, METALOG_GRANULARITY_CANDIDATES)
        source_col = find_column_name(reader.fieldnames, METALOG_SOURCE_CANDIDATES)
        corrected_col = find_column_name(reader.fieldnames, METALOG_CORRECTED_CANDIDATES)

        # Detect a secondary alias column so entries are reachable by both keys.
        # The bundle TSVs use spire_sample_name (SAM accession or internal SPIRE ID) as
        # the primary join key, but the sequencing_db_mapping file only knows the
        # sample_alias composite key — so we register entries under both.
        alias_col: str | None = None
        if normalize_field_name(sample_col) == "spire_sample_name":
            alias_col = find_column_name(
                reader.fieldnames,
                [c for c in METALOG_SAMPLE_COLUMN_CANDIDATES if c != "spire_sample_name"],
            )

        mapping: dict[str, dict[str, str]] = {}
        for row in reader:
            sample_id = (row.get(sample_col) or "").strip()
            if not sample_id:
                continue
            entry = {
                "latitude": (row.get(lat_col) or "").strip(),
                "longitude": (row.get(lon_col) or "").strip(),
                "coordinate_granularity": (row.get(gran_col) or "").strip() if gran_col else "",
                "coordinate_source": (row.get(source_col) or "").strip() if source_col else "metalog",
                "is_corrected": (row.get(corrected_col) or "").strip() if corrected_col else "",
            }
            mapping[sample_id] = entry
            # Also register under sample_alias so the mapping-file expansion can find this entry.
            if alias_col:
                alias_id = (row.get(alias_col) or "").strip()
                if alias_id:
                    mapping.setdefault(alias_id, entry)
        return mapping


def load_genome_metadata_map(path: Path) -> dict[str, list[str]]:
    """Build sample_id -> [genome_id, ...] mapping from the SPIRE genome metadata download.

    Uses the local spire_v1_genome_metadata.tsv.gz file so that the
    coordinate builder never needs to call the per-sample SPIRE API.
    """
    opener = gzip.open if path.suffix == ".gz" else open
    sample_to_mags: dict[str, list[str]] = {}
    with opener(path, "rt", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"No header found in genome metadata file: {path}")
        missing = [
            col for col in (GENOME_METADATA_ID_COL, GENOME_METADATA_SAMPLE_COL)
            if col not in reader.fieldnames
        ]
        if missing:
            raise ValueError(
                f"Expected column(s) {missing} not found in {path}. "
                f"Available: {', '.join(reader.fieldnames)}"
            )
        for row in reader:
            genome_id = (row.get(GENOME_METADATA_ID_COL) or "").strip()
            sample_id = (row.get(GENOME_METADATA_SAMPLE_COL) or "").strip()
            if genome_id and sample_id:
                sample_to_mags.setdefault(sample_id, []).append(genome_id)
    return sample_to_mags


def build_external_to_metalog_index(mapping_path: Path) -> dict[str, str]:
    """Build external sample accession -> Metalog sample id index from mapping TSV."""
    opener = gzip.open if mapping_path.suffix == ".gz" else open
    with opener(mapping_path, "rt", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"No header found in mapping file: {mapping_path}")

        # The mapping file's join key back to metalog_map is sample_alias, not the
        # numeric sample_id or spire_sample_name (which the mapping file lacks).
        metalog_id_col = find_column_name(reader.fieldnames, ["sample_alias"] + METALOG_SAMPLE_COLUMN_CANDIDATES)
        external_col = find_column_name(reader.fieldnames, EXTERNAL_SAMPLE_COLUMN_CANDIDATES)
        kind_col = find_column_name(reader.fieldnames, KIND_COLUMN_CANDIDATES)
        if not metalog_id_col or not external_col:
            available = ", ".join(reader.fieldnames)
            raise ValueError(
                "Mapping file missing required columns for external->Metalog lookup. "
                f"Available columns: {available}"
            )

        index: dict[str, str] = {}
        for row in reader:
            if kind_col:
                kind = (row.get(kind_col) or "").strip().lower()
                if kind and "sample" not in kind:
                    continue
            metalog_id = (row.get(metalog_id_col) or "").strip()
            raw_external = (row.get(external_col) or "").strip()
            if not metalog_id or not raw_external:
                continue
            for ext in split_possible_ids(raw_external):
                index[ext] = metalog_id
        return index


def expand_metalog_map_with_external_ids(
    metalog_map: dict[str, dict[str, str]], external_to_metalog: dict[str, str]
) -> dict[str, dict[str, str]]:
    """Create additional keys for external sample accessions using mapping index."""
    expanded = dict(metalog_map)
    for external_id, metalog_id in external_to_metalog.items():
        values = metalog_map.get(metalog_id)
        if values:
            expanded.setdefault(external_id, values)
    return expanded


def download_metalog_tsv(
    *,
    url: str,
    output_path: Path,
    timeout: float,
    max_retries: int,
    retry_backoff: float,
    token_env: str,
    no_auth: bool,
) -> Path:
    """Download Metalog TSV/TSV.GZ to a local path for enrichment."""
    session = make_session(max_retries=max_retries, backoff_factor=retry_backoff)
    headers: dict[str, str] = {}
    if not no_auth:
        token = os.environ.get(token_env, "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"

    response = session.get(url, timeout=timeout, headers=headers, stream=True)
    response.raise_for_status()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as handle:
        for chunk in response.iter_content(chunk_size=65536):
            if chunk:
                handle.write(chunk)
    return output_path


def resolve_metalog_urls(preset: str | None, explicit_urls: list[str]) -> list[str]:
    urls: list[str] = []
    if preset:
        if preset not in METALOG_PUBLIC_URLS:
            valid = ", ".join(sorted(METALOG_PUBLIC_URLS))
            raise ValueError(f"Unknown --metalog-public-preset '{preset}'. Valid presets: {valid}")
        urls.extend(METALOG_PUBLIC_URLS[preset])
    urls.extend(explicit_urls)
    # Preserve order while removing duplicates.
    return list(dict.fromkeys(urls))


def load_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {"completed_studies": []}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_checkpoint(path: Path, completed_studies: set[str]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump({"completed_studies": sorted(completed_studies)}, handle, indent=2)
        handle.write("\n")


def build_coordinate_rows(
    studies: list[str],
    base_url: str,
    timeout: float,
    sleep_seconds: float,
    checkpoint_path: Path | None,
    resume: bool,
    metalog_by_sample: dict[str, dict[str, str]],
    max_retries: int,
    retry_backoff: float,
    genome_metadata_map: dict[str, list[str]] | None = None,
) -> tuple[list[CoordinateRecord], dict[str, int]]:
    session = make_session(max_retries=max_retries, backoff_factor=retry_backoff)
    fetched_at = int(time.time())

    completed_studies: set[str] = set()
    if checkpoint_path:
        checkpoint = load_checkpoint(checkpoint_path)
        completed_studies = set(checkpoint.get("completed_studies", [])) if resume else set()

    rows: list[CoordinateRecord] = []
    stats = {
        "studies_total": len(studies),
        "studies_processed": 0,
        "samples_seen": 0,
        "samples_with_coordinates": 0,
        "samples_skipped_not_in_metadata": 0,
        "sample_api_calls": 0,
        "mag_sample_rows": 0,
        "metalog_enriched_rows": 0,
    }
    using_local_metadata = genome_metadata_map is not None

    for idx, study_name in enumerate(studies, start=1):
        if study_name in completed_studies:
            print(f"[{idx}/{len(studies)}] Skipping completed study: {study_name}")
            continue

        print(f"[{idx}/{len(studies)}] Fetching study rows: {study_name}")
        study_url = f"{base_url}/study/{study_name}?format=tsv"
        try:
            study_rows = fetch_tsv(session, study_url, timeout)
        except requests.RequestException as exc:
            print(f"  Warning: failed to fetch study '{study_name}': {exc}")
            continue

        sample_to_coords: dict[str, tuple[str, str]] = {}
        for row in study_rows:
            sample_id = (row.get(STUDY_TSV_SAMPLE_COL) or "").strip()
            if not sample_id:
                continue
            lat = (row.get(STUDY_TSV_LAT_COL) or "").strip()
            lon = (row.get(STUDY_TSV_LON_COL) or "").strip()
            sample_to_coords[sample_id] = (lat, lon)

        stats["samples_seen"] += len(sample_to_coords)
        stats["samples_with_coordinates"] += sum(
            1 for lat, lon in sample_to_coords.values() if lat and lon
        )

        # When using local metadata, restrict to samples present in our downloaded dataset.
        if using_local_metadata:
            assert genome_metadata_map is not None
            in_map = {
                sid: coords for sid, coords in sample_to_coords.items()
                if sid in genome_metadata_map
            }
            skipped = len(sample_to_coords) - len(in_map)
            if skipped:
                stats["samples_skipped_not_in_metadata"] += skipped
            sample_to_coords = in_map

        for sample_id, (spire_lat, spire_lon) in sample_to_coords.items():
            if using_local_metadata:
                assert genome_metadata_map is not None
                mag_ids: set[str] = set(genome_metadata_map[sample_id])
            else:
                sample_url = f"{base_url}/sample/{sample_id}?format=tsv"
                try:
                    sample_rows = fetch_tsv(session, sample_url, timeout)
                    stats["sample_api_calls"] += 1
                except requests.RequestException as exc:
                    print(f"  Warning: failed sample '{sample_id}' in '{study_name}': {exc}")
                    continue

                mag_ids = set()
                for sample_row in sample_rows:
                    raw_mag = (sample_row.get(SAMPLE_TSV_MAG_COL) or "").strip()
                    if raw_mag:
                        mag_ids.update(split_possible_ids(raw_mag))

                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

            if not mag_ids:
                continue

            metalog = metalog_by_sample.get(sample_id, {})
            metalog_lat = metalog.get("latitude", "")
            metalog_lon = metalog.get("longitude", "")
            has_metalog_coords = bool(metalog_lat and metalog_lon)

            canonical_lat = metalog_lat if has_metalog_coords else spire_lat
            canonical_lon = metalog_lon if has_metalog_coords else spire_lon
            coordinate_source = (
                metalog.get("coordinate_source", "metalog") if has_metalog_coords else "spire"
            )
            coordinate_granularity = metalog.get("coordinate_granularity", "")
            is_corrected = metalog.get("is_corrected", "")

            for mag_id in sorted(mag_ids):
                rows.append(
                    CoordinateRecord(
                        mag_id=mag_id,
                        sample_id=sample_id,
                        study_name=study_name,
                        latitude=canonical_lat,
                        longitude=canonical_lon,
                        latitude_spire=spire_lat,
                        longitude_spire=spire_lon,
                        latitude_metalog=metalog_lat,
                        longitude_metalog=metalog_lon,
                        coordinate_source=coordinate_source,
                        coordinate_granularity=coordinate_granularity,
                        metalog_is_corrected=is_corrected,
                        source_version=SOURCE_VERSION,
                        fetched_at_unix=fetched_at,
                    )
                )
                if has_metalog_coords:
                    stats["metalog_enriched_rows"] += 1

            stats["mag_sample_rows"] = len(rows)

        # When using local metadata, sleep between study calls (not between sample calls).
        if using_local_metadata and sleep_seconds > 0:
            time.sleep(sleep_seconds)

        stats["studies_processed"] += 1
        completed_studies.add(study_name)
        if checkpoint_path:
            save_checkpoint(checkpoint_path, completed_studies)

    rows.sort(key=lambda r: (r.mag_id, r.sample_id, r.study_name))
    return rows, stats


def write_tsv(path: Path, rows: list[CoordinateRecord]) -> None:
    fieldnames = [
        "mag_id",
        "sample_id",
        "study_name",
        "latitude",
        "longitude",
        "latitude_spire",
        "longitude_spire",
        "latitude_metalog",
        "longitude_metalog",
        "coordinate_source",
        "coordinate_granularity",
        "metalog_is_corrected",
        "source_version",
        "fetched_at_unix",
    ]
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def write_summary(path: Path, stats: dict[str, int], rows: list[CoordinateRecord]) -> None:
    unique_mags = {row.mag_id for row in rows}
    unique_samples = {row.sample_id for row in rows}
    rows_with_missing_coords = sum(1 for row in rows if not row.latitude or not row.longitude)

    summary = {
        **stats,
        "unique_mag_ids": len(unique_mags),
        "unique_sample_ids": len(unique_samples),
        "rows_with_missing_coordinates": rows_with_missing_coords,
    }

    with open(path, "w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
        handle.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build SPIRE MAG coordinate TSV from SPIRE API and optional Metalog TSV."
    )
    default_data_dir = Path(__file__).parent / "data"

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=default_data_dir,
        help=f"Directory for output files (default: {default_data_dir})",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=None,
        help=(
            "Directory for caching the enumerated study list and downloaded Metalog files. "
            "Defaults to --data-dir/.cache/."
        ),
    )
    parser.add_argument(
        "--genome-metadata",
        type=Path,
        default=None,
        help=(
            f"Path to {GENOME_METADATA_FILENAME}. "
            "When provided (or auto-detected under --data-dir), the genome_id / "
            "derived_from_sample mapping is read locally, restricting output to downloaded "
            "MAGs and eliminating all per-sample SPIRE API calls. "
            f"Auto-detected if --data-dir/{GENOME_METADATA_FILENAME} exists."
        ),
    )
    parser.add_argument(
        "--study-name",
        action="append",
        default=[],
        help=(
            "Process only this study (repeatable). "
            "When omitted all SPIRE studies are fetched from the SPIRE downloads page."
        ),
    )
    parser.add_argument(
        "--refetch-studies",
        action="store_true",
        help="Re-fetch the study list from the SPIRE downloads page even if a cache exists.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit processing to the first N studies (useful for testing).",
    )
    parser.add_argument(
        "--metalog-tsv",
        type=Path,
        default=None,
        help="Optional local Metalog TSV keyed by sample ID for coordinate enrichment.",
    )
    parser.add_argument(
        "--metalog-url",
        action="append",
        default=[],
        help=(
            "Optional URL for downloading a Metalog TSV/TSV.GZ export. "
            "Repeatable. Downloaded files are merged for enrichment."
        ),
    )
    parser.add_argument(
        "--metalog-public-preset",
        default=None,
        choices=sorted(METALOG_PUBLIC_URLS.keys()),
        help=(
            "Use predefined public Metalog metadata URLs from metalog.embl.de/downloads. "
            "Example: wide-extended-all."
        ),
    )
    parser.add_argument(
        "--metalog-token-env",
        default="METALOG_API_TOKEN",
        help=(
            "Environment variable name containing Metalog API token for "
            "Authorization: Bearer <token> (default: METALOG_API_TOKEN)."
        ),
    )
    parser.add_argument(
        "--metalog-no-auth",
        action="store_true",
        help="Do not send Authorization header for --metalog-url downloads.",
    )
    parser.add_argument(
        "--metalog-download-mapping",
        action="store_true",
        help=(
            "Download the Metalog sequencing_db_mapping file and use it to expand "
            "sample joins between SPIRE sample IDs and Metalog sample IDs."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help=f"Output TSV path (default: --data-dir/{DEFAULT_OUTPUT_NAME}).",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=None,
        help=f"Summary JSON path (default: --data-dir/{DEFAULT_SUMMARY_NAME}).",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=None,
        help=(
            "Checkpoint JSON path for completed studies "
            f"(default: --data-dir/{DEFAULT_CHECKPOINT_NAME})."
        ),
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint if present.",
    )
    parser.add_argument(
        "--base-url",
        default=SPIRE_BASE_URL,
        help=f"SPIRE API base URL (default: {SPIRE_BASE_URL}).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Per-request timeout in seconds (default: 60).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.1,
        help="Delay between sample API calls in seconds (default: 0.1).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max HTTP retries per request (default: 2).",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=0.5,
        help="Retry backoff factor in seconds (default: 0.5).",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.data_dir.mkdir(parents=True, exist_ok=True)

    cache_dir = args.cache_dir or (args.data_dir / ".cache")
    output_path = args.output or (args.data_dir / DEFAULT_OUTPUT_NAME)
    summary_output_path = args.summary_output or (args.data_dir / DEFAULT_SUMMARY_NAME)
    checkpoint_path = args.checkpoint or (args.data_dir / DEFAULT_CHECKPOINT_NAME)

    # --- Study list ---
    studies: list[str] = list(args.study_name)
    if not studies:
        study_cache = cache_dir / DEFAULT_STUDY_CACHE_NAME
        studies = enumerate_all_studies(
            cache_path=study_cache,
            timeout=args.timeout,
            max_retries=args.max_retries,
            retry_backoff=args.retry_backoff,
            refetch=args.refetch_studies,
        )

    if args.limit is not None:
        studies = studies[: args.limit]
        print(f"--limit {args.limit}: processing {len(studies)} studies.")

    # --- Genome metadata (local MAG -> sample mapping) ---
    genome_metadata_map: dict[str, list[str]] | None = None
    genome_metadata_path: Path | None = args.genome_metadata
    if genome_metadata_path is None:
        candidate = args.data_dir / GENOME_METADATA_FILENAME
        if candidate.exists():
            genome_metadata_path = candidate

    if genome_metadata_path is not None:
        print(f"Loading genome metadata from {genome_metadata_path}...")
        genome_metadata_map = load_genome_metadata_map(genome_metadata_path)
        total_mags = sum(len(v) for v in genome_metadata_map.values())
        print(
            f"Loaded {total_mags:,} MAGs across {len(genome_metadata_map):,} samples "
            "from local metadata. Sample API calls will be skipped."
        )
    else:
        print(
            f"No genome metadata file found ({GENOME_METADATA_FILENAME} not in --data-dir "
            "and --genome-metadata not set). Will call the SPIRE sample API for each sample "
            "(slower). Download the file to --data-dir to avoid this."
        )

    # --- Metalog enrichment ---
    metalog_by_sample: dict[str, dict[str, str]] = {}
    metalog_input_paths: list[Path] = []

    if args.metalog_tsv:
        metalog_input_paths.append(args.metalog_tsv)

    metalog_urls = resolve_metalog_urls(args.metalog_public_preset, args.metalog_url)
    if metalog_urls:
        download_dir = cache_dir / "metalog_downloads"
        for url in metalog_urls:
            filename = Path(url).name or "metalog_download.tsv"
            out_path = download_dir / filename
            if not out_path.exists():
                print(f"Downloading Metalog file from {url}...")
                download_metalog_tsv(
                    url=url,
                    output_path=out_path,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    retry_backoff=args.retry_backoff,
                    token_env=args.metalog_token_env,
                    no_auth=args.metalog_no_auth,
                )
                print(f"Downloaded Metalog file to {out_path}")
            else:
                print(f"Using cached Metalog file: {out_path}")
            metalog_input_paths.append(out_path)

    if metalog_input_paths:
        merged: dict[str, dict[str, str]] = {}
        for path in metalog_input_paths:
            current = load_metalog_map(path)
            for sample_id, values in current.items():
                if sample_id not in merged:
                    merged[sample_id] = values
                    continue
                if not merged[sample_id].get("latitude") and values.get("latitude"):
                    merged[sample_id] = values
        metalog_by_sample = merged
        print(
            f"Loaded Metalog mappings for {len(metalog_by_sample)} samples "
            f"from {len(metalog_input_paths)} file(s)."
        )

    mapping_path: Path | None = None
    if args.metalog_download_mapping:
        download_dir = cache_dir / "metalog_downloads"
        mapping_path = download_dir / Path(METALOG_MAPPING_URL).name
        if not mapping_path.exists():
            print(f"Downloading Metalog mapping file...")
            download_metalog_tsv(
                url=METALOG_MAPPING_URL,
                output_path=mapping_path,
                timeout=args.timeout,
                max_retries=args.max_retries,
                retry_backoff=args.retry_backoff,
                token_env=args.metalog_token_env,
                no_auth=True,
            )
            print(f"Downloaded Metalog mapping file to {mapping_path}")
        else:
            print(f"Using cached Metalog mapping file: {mapping_path}")

    if metalog_by_sample and mapping_path:
        external_to_metalog = build_external_to_metalog_index(mapping_path)
        metalog_by_sample = expand_metalog_map_with_external_ids(
            metalog_by_sample, external_to_metalog
        )
        print(
            "Expanded Metalog sample mappings with external accessions: "
            f"{len(external_to_metalog)} mapping entries"
        )

    print(f"Processing {len(studies)} studies...")
    rows, stats = build_coordinate_rows(
        studies=studies,
        base_url=args.base_url,
        timeout=args.timeout,
        sleep_seconds=args.sleep_seconds,
        checkpoint_path=checkpoint_path,
        resume=args.resume,
        metalog_by_sample=metalog_by_sample,
        max_retries=args.max_retries,
        retry_backoff=args.retry_backoff,
        genome_metadata_map=genome_metadata_map,
    )

    if genome_metadata_map is not None:
        covered_samples = {row.sample_id for row in rows}
        not_covered = len(genome_metadata_map) - len(covered_samples)
        stats["samples_in_metadata_not_covered"] = not_covered
        if not_covered > 0:
            print(
                f"Warning: {not_covered} sample(s) in the genome metadata were not seen in any "
                "study TSV fetched from SPIRE. These may belong to studies absent from the SPIRE "
                "downloads page. Re-run with --refetch-studies to rule out a stale cache."
            )

    write_tsv(output_path, rows)
    write_summary(summary_output_path, stats, rows)

    print(f"Wrote coordinates TSV: {output_path} ({len(rows)} rows)")
    print(f"Wrote summary JSON: {summary_output_path}")
    print(
        "Coverage: "
        f"{stats['studies_processed']}/{stats['studies_total']} studies, "
        f"{stats['samples_with_coordinates']}/{stats['samples_seen']} samples with lat/lon"
    )


if __name__ == "__main__":
    main()
