"""
Integration tests for the SPIRE coordinate builder.

These tests hit the live SPIRE API.  Run with:

    pytest -m integration

Requirements:
  - Internet access to spire.embl.de
"""

import csv
import importlib.util
import re
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

_MOD_PATH = Path(__file__).parent.parent / "scripts" / "spire" / "build_mag_coordinates.py"

# Use the smallest known studies so the test completes quickly.
# Coelho_2018_dog: 129 samples, all with coordinates.
TEST_STUDIES = ["Coelho_2018_dog"]


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_spire_study_api_returns_expected_columns():
    """The live SPIRE study API must return the confirmed column set."""
    import requests

    r = requests.get(
        "https://spire.embl.de/spire/api/study/Coelho_2018_dog?format=tsv",
        timeout=30,
    )
    assert r.status_code == 200, f"Unexpected status: {r.status_code}"

    lines = r.text.splitlines()
    assert lines, "Empty response from study API"

    columns = lines[0].split("\t")
    assert "sample_id" in columns, f"sample_id missing; got: {columns}"
    assert "lat" in columns, f"lat missing; got: {columns}"
    assert "lon" in columns, f"lon missing; got: {columns}"
    assert len(lines) > 1, "Study returned no data rows"


@pytest.mark.integration
def test_spire_sample_api_returns_spire_id_column():
    """The live SPIRE sample API must return 'spire_id' as the MAG ID column."""
    import requests

    # SAMN15803490 is the sample cited in Sebastian's guidance email.
    r = requests.get(
        "https://spire.embl.de/spire/api/sample/SAMN15803490?format=tsv",
        timeout=30,
    )
    assert r.status_code == 200
    lines = r.text.splitlines()
    assert lines
    columns = lines[0].split("\t")
    assert "spire_id" in columns, f"spire_id missing; got: {columns}"
    assert len(lines) > 1, "Sample returned no MAG rows"

    # All spire_id values should match the known format
    for line in lines[1:]:
        values = line.split("\t")
        spire_id = values[0]
        assert re.match(r"^spire_mag_\d+$", spire_id), f"Unexpected spire_id format: {spire_id!r}"


@pytest.mark.integration
def test_downloads_page_returns_known_studies():
    """The SPIRE page scraper must return known study names from both /downloads and /studies."""
    module = _load_module(_MOD_PATH, "bmc_integration_downloads")
    import requests

    session = requests.Session()
    studies = module._scrape_study_names_from_downloads_page(session, timeout=30)
    assert "Coelho_2018_dog" in studies, f"Expected Coelho_2018_dog in studies"
    assert "MetaHIT" in studies, f"Expected MetaHIT (from /studies page) in studies"
    assert len(studies) > 100, f"Expected many studies, got {len(studies)}"


@pytest.mark.integration
def test_full_coordinate_extraction(tmp_path):
    """End-to-end: extract coordinates for small studies, upload to containerised MinIO."""
    module = _load_module(_MOD_PATH, "bmc_integration_e2e")

    output_tsv = tmp_path / "spire_v01_mag_coordinates.tsv"
    summary_json = tmp_path / "spire_v01_mag_coordinates_summary.json"
    checkpoint = tmp_path / "checkpoint.json"

    rows, stats = module.build_coordinate_rows(
        studies=TEST_STUDIES,
        base_url="https://spire.embl.de/spire/api",
        timeout=60.0,
        sleep_seconds=0.1,
        checkpoint_path=checkpoint,
        resume=False,
        metalog_by_sample={},
        max_retries=2,
        retry_backoff=0.5,
    )

    assert len(rows) > 0, "Expected at least one MAG-sample row"
    assert stats["studies_processed"] == len(TEST_STUDIES)
    assert stats["samples_seen"] > 0
    assert stats["mag_sample_rows"] == len(rows)

    # --- Output schema ---
    module.write_tsv(output_tsv, rows)
    module.write_summary(summary_json, stats, rows)

    assert output_tsv.exists()
    assert summary_json.exists()

    with open(output_tsv, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        fieldnames = reader.fieldnames or []
        required = {
            "mag_id", "sample_id", "study_name",
            "latitude", "longitude",
            "latitude_spire", "longitude_spire",
            "coordinate_source", "source_version", "fetched_at_unix",
        }
        missing = required - set(fieldnames)
        assert not missing, f"Output TSV missing columns: {missing}"

        tsv_rows = list(reader)

    assert len(tsv_rows) == len(rows)

    # All mag_id values should follow the spire_mag_XXXXXXXX format
    for row in tsv_rows:
        assert re.match(r"^spire_mag_\d+$", row["mag_id"]), (
            f"Unexpected mag_id format: {row['mag_id']!r}"
        )

    # All rows from Coelho_2018_dog should have coordinates
    coords_present = sum(1 for r in tsv_rows if r["latitude"] and r["longitude"])
    assert coords_present == len(tsv_rows), (
        f"Expected all rows to have coordinates for {TEST_STUDIES}; "
        f"{len(tsv_rows) - coords_present} rows missing"
    )
