"""Unit tests for SPIRE coordinate builder helpers."""

from pathlib import Path
import importlib.util
import sys
import gzip
from unittest.mock import MagicMock, patch


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_MOD_PATH = Path(__file__).parent.parent / "scripts" / "spire" / "build_mag_coordinates.py"


# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------

def test_spire_study_columns_resolve_correctly():
    """Real SPIRE study API columns (lat, lon, sample_id) must be found first-try."""
    module = _load_module(_MOD_PATH, "bmc_cols")
    # Real study TSV columns: sample_id, mags, lat, lon, microntology
    fieldnames = ["sample_id", "mags", "lat", "lon", "microntology"]
    assert module.STUDY_TSV_SAMPLE_COL in fieldnames
    assert module.STUDY_TSV_LAT_COL in fieldnames
    assert module.STUDY_TSV_LON_COL in fieldnames


def test_spire_sample_mag_column_resolves_correctly():
    """Real SPIRE sample API MAG column (spire_id) must be the primary constant."""
    module = _load_module(_MOD_PATH, "bmc_sample_cols")
    # Real sample TSV columns: spire_id, sample_id, genome_size, ...
    fieldnames = ["spire_id", "sample_id", "genome_size", "num_contigs", "n50",
                  "completeness", "contamination", "gunc_css", "gunc_rrs",
                  "gene_count", "spire_cluster"]
    assert module.SAMPLE_TSV_MAG_COL in fieldnames
    assert module.SAMPLE_TSV_MAG_COL == "spire_id"


def test_find_column_name_handles_normalized_names():
    module = _load_module(_MOD_PATH, "bmc_find_col")
    # Verify case-insensitive normalised matching still works for Metalog fields
    fieldnames = ["Sample ID", "LATITUDE", "LONGITUDE"]
    assert module.find_column_name(fieldnames, ["sample_id"]) == "Sample ID"
    assert module.find_column_name(fieldnames, module.METALOG_LAT_COLUMN_CANDIDATES) == "LATITUDE"
    assert module.find_column_name(fieldnames, module.METALOG_LON_COLUMN_CANDIDATES) == "LONGITUDE"


def test_parse_tsv_rows_uses_real_study_columns():
    """parse_tsv_rows should correctly parse a realistic SPIRE study TSV."""
    module = _load_module(_MOD_PATH, "bmc_parse")
    content = (
        "sample_id\tmags\tlat\tlon\tmicrontology\n"
        "SAMEA103957793\t12\t37.38\t-92.87\t['host-associated']\n"
        "SAMEA103957794\t5\t\t\t[]\n"
    )
    rows = module.parse_tsv_rows(content)
    assert len(rows) == 2
    assert rows[0]["sample_id"] == "SAMEA103957793"
    assert rows[0]["lat"] == "37.38"
    assert rows[0]["lon"] == "-92.87"
    assert rows[1]["lat"] == ""  # missing coords preserved as empty string


def test_parse_tsv_rows_uses_real_sample_columns():
    """parse_tsv_rows should correctly parse a realistic SPIRE sample TSV."""
    module = _load_module(_MOD_PATH, "bmc_parse_sample")
    content = (
        "spire_id\tsample_id\tgenome_size\tnum_contigs\tn50\t"
        "completeness\tcontamination\tgunc_css\tgunc_rrs\tgene_count\tspire_cluster\n"
        "spire_mag_02601022\tSAMN15803490\t7196451\t1723\t4058\t"
        "64.14\t13.22\t0.37\t0.31\t8649\t\n"
    )
    rows = module.parse_tsv_rows(content)
    assert len(rows) == 1
    assert rows[0]["spire_id"] == "spire_mag_02601022"
    assert rows[0]["sample_id"] == "SAMN15803490"


# ---------------------------------------------------------------------------
# Study enumeration
# ---------------------------------------------------------------------------

def test_enumerate_all_studies_uses_cache(tmp_path):
    """enumerate_all_studies should return cached results without hitting the network."""
    module = _load_module(_MOD_PATH, "bmc_enum_cache")
    cache_file = tmp_path / "spire_study_names.json"
    import json
    cache_file.write_text(json.dumps({"studies": ["StudyA", "StudyB"]}), encoding="utf-8")

    studies = module.enumerate_all_studies(
        base_url="https://example.com",
        cache_path=cache_file,
        refetch=False,
    )
    assert studies == ["StudyA", "StudyB"]


def test_scrape_study_names_unions_both_pages():
    """_scrape_study_names_from_downloads_page unions /downloads and /studies results."""
    module = _load_module(_MOD_PATH, "bmc_scrape_union")

    # Downloads page HTML: has StudyA and StudyB as JS object keys
    downloads_html = '"StudyA": { mags_url: "..." }, "StudyB": { mags_url: "..." }'
    # Studies page HTML: has StudyB and MetaHIT in table format
    studies_html = '{ study: "StudyB", samples: 500 }, { study: "MetaHIT", samples: 816 }'

    mock_resp_downloads = MagicMock()
    mock_resp_downloads.raise_for_status = MagicMock()
    mock_resp_downloads.text = downloads_html

    mock_resp_studies = MagicMock()
    mock_resp_studies.raise_for_status = MagicMock()
    mock_resp_studies.text = studies_html

    mock_session = MagicMock()
    mock_session.get.side_effect = [mock_resp_downloads, mock_resp_studies]

    result = module._scrape_study_names_from_downloads_page(mock_session, timeout=10.0)
    assert set(result) == {"StudyA", "StudyB", "MetaHIT"}
    assert result == sorted(result)  # must be sorted


def test_enumerate_all_studies_scrapes_both_pages(tmp_path):
    """enumerate_all_studies unions study names from /downloads and /studies pages."""
    module = _load_module(_MOD_PATH, "bmc_enum_scrape")

    # Simulate: downloads page has A+B, studies page adds C (e.g. MetaHIT-like)
    fake_union = ["Coelho_2018_dog", "HMP", "MetaHIT", "TARA_oceans"]

    with patch.object(module, "_scrape_study_names_from_downloads_page",
                      return_value=sorted(fake_union)):
        studies = module.enumerate_all_studies(
            base_url="https://example.com",
            cache_path=None,
        )

    assert studies == sorted(fake_union)


def test_enumerate_all_studies_writes_cache(tmp_path):
    module = _load_module(_MOD_PATH, "bmc_enum_write")

    fake_studies = sorted(["HMP", "HMP2-IBD", "Coelho_2018_dog"])

    cache_file = tmp_path / "studies.json"
    with patch.object(module, "_scrape_study_names_from_downloads_page",
                      return_value=fake_studies):
        studies = module.enumerate_all_studies(
            base_url="https://example.com",
            cache_path=cache_file,
        )

    assert cache_file.exists()
    import json
    cached = json.loads(cache_file.read_text())
    assert set(cached["studies"]) == set(studies)


# ---------------------------------------------------------------------------
# Metalog loading
# ---------------------------------------------------------------------------

def test_load_metalog_map_real_columns(tmp_path):
    """load_metalog_map uses the Metalog candidate column lists (not SPIRE column names)."""
    module = _load_module(_MOD_PATH, "bmc_metalog_real")

    metalog_file = tmp_path / "metalog.tsv"
    metalog_file.write_text(
        "sample_id\tlatitude\tlongitude\tcoordinate_granularity\tcoordinates_corrected\n"
        "SAMN1\t1.0\t2.0\texact\ttrue\n",
        encoding="utf-8",
    )

    mapping = module.load_metalog_map(metalog_file)
    assert mapping["SAMN1"]["latitude"] == "1.0"
    assert mapping["SAMN1"]["longitude"] == "2.0"
    assert mapping["SAMN1"]["coordinate_granularity"] == "exact"
    assert mapping["SAMN1"]["is_corrected"] == "true"


def test_split_possible_ids():
    module = _load_module(_MOD_PATH, "bmc_split")
    assert module.split_possible_ids("spire_mag_00000001,spire_mag_00000002") == [
        "spire_mag_00000001", "spire_mag_00000002"
    ]
    assert module.split_possible_ids("spire_mag_00000001|spire_mag_00000002") == [
        "spire_mag_00000001", "spire_mag_00000002"
    ]
    assert module.split_possible_ids("spire_mag_00000001") == ["spire_mag_00000001"]
    assert module.split_possible_ids("") == []


def test_load_metalog_map_supports_gz(tmp_path):
    module = _load_module(_MOD_PATH, "bmc_metalog_gz")

    metalog_gz = tmp_path / "metalog.tsv.gz"
    content = (
        "sample_id\tlatitude\tlongitude\tcoordinate_source\n"
        "SAMN2\t10.1\t-20.2\tmetalog\n"
    )
    with gzip.open(metalog_gz, "wt", encoding="utf-8") as handle:
        handle.write(content)

    mapping = module.load_metalog_map(metalog_gz)
    assert mapping["SAMN2"]["latitude"] == "10.1"
    assert mapping["SAMN2"]["longitude"] == "-20.2"


# ---------------------------------------------------------------------------
# Metalog URL resolution
# ---------------------------------------------------------------------------

def test_resolve_metalog_urls_with_preset_and_explicit_url():
    module = _load_module(_MOD_PATH, "bmc_urls")

    urls = module.resolve_metalog_urls(
        "wide-extended-all",
        [
            "https://example.org/custom.tsv.gz",
            "https://metalog.embl.de/static/download/metadata/human_extended_wide_latest.tsv.gz",
        ],
    )

    assert "https://example.org/custom.tsv.gz" in urls
    assert (
        "https://metalog.embl.de/static/download/metadata/human_extended_wide_latest.tsv.gz"
        in urls
    )
    # No duplicates
    assert len(urls) == len(set(urls))


# ---------------------------------------------------------------------------
# External ID expansion
# ---------------------------------------------------------------------------

def test_expand_metalog_map_with_external_ids():
    module = _load_module(_MOD_PATH, "bmc_expand")

    base_map = {
        "MLG_001": {
            "latitude": "1.23",
            "longitude": "4.56",
            "coordinate_granularity": "exact",
            "coordinate_source": "metalog",
            "is_corrected": "",
        }
    }
    external_to_metalog = {"SAMEA123": "MLG_001"}

    expanded = module.expand_metalog_map_with_external_ids(base_map, external_to_metalog)
    assert "SAMEA123" in expanded
    assert expanded["SAMEA123"]["latitude"] == "1.23"


def test_build_external_to_metalog_index_with_kind_filter(tmp_path):
    module = _load_module(_MOD_PATH, "bmc_mapping")

    mapping_file = tmp_path / "mapping.tsv"
    mapping_file.write_text(
        "sample_id\tsample_alias\tkind\texternal_id\n"
        "MLG_1\tALIAS1\tsample\tSAMEA111\n"
        "MLG_2\tALIAS2\trun\tSRR222\n",
        encoding="utf-8",
    )

    index = module.build_external_to_metalog_index(mapping_file)
    # The index maps external_id -> sample_alias (the key used in metalog_map, not the
    # numeric sample_id which exists only as an internal Metalog row identifier).
    assert index["SAMEA111"] == "ALIAS1"
    assert "SRR222" not in index


# ---------------------------------------------------------------------------
# Genome metadata map
# ---------------------------------------------------------------------------

def test_load_genome_metadata_map_basic(tmp_path):
    """load_genome_metadata_map returns sample_id -> [genome_id] from a plain TSV."""
    module = _load_module(_MOD_PATH, "bmc_genome_meta")

    meta_file = tmp_path / "genome_metadata.tsv"
    meta_file.write_text(
        "genome_id\tspire_cluster\tderived_from_sample\n"
        "spire_mag_00000001\tcluster_A\tSAMN001\n"
        "spire_mag_00000002\tcluster_A\tSAMN001\n"
        "spire_mag_00000003\tcluster_B\tSAMN002\n",
        encoding="utf-8",
    )

    mapping = module.load_genome_metadata_map(meta_file)
    assert set(mapping["SAMN001"]) == {"spire_mag_00000001", "spire_mag_00000002"}
    assert mapping["SAMN002"] == ["spire_mag_00000003"]


def test_load_genome_metadata_map_gz(tmp_path):
    module = _load_module(_MOD_PATH, "bmc_genome_meta_gz")

    meta_gz = tmp_path / "genome_metadata.tsv.gz"
    content = (
        "genome_id\tderived_from_sample\n"
        "spire_mag_00000010\tSAMEA111\n"
    )
    with gzip.open(meta_gz, "wt", encoding="utf-8") as fh:
        fh.write(content)

    mapping = module.load_genome_metadata_map(meta_gz)
    assert mapping["SAMEA111"] == ["spire_mag_00000010"]


def test_build_coordinate_rows_uses_genome_metadata_map(tmp_path):
    """When genome_metadata_map is provided, no sample API calls should be made."""
    module = _load_module(_MOD_PATH, "bmc_e2e_local")

    study_tsv = (
        "sample_id\tmags\tlat\tlon\tmicrontology\n"
        "SAMN15803490\t2\t40.0\t-70.0\t[]\n"
        "SAMN99999999\t0\t50.0\t10.0\t[]\n"  # not in local metadata — should be skipped
    )

    genome_metadata_map = {
        "SAMN15803490": ["spire_mag_00000001", "spire_mag_00000002"],
    }

    fetch_call_urls: list[str] = []

    def fake_fetch_tsv(session, url, timeout):
        fetch_call_urls.append(url)
        if "/study/" in url:
            return module.parse_tsv_rows(study_tsv)
        raise AssertionError(f"Unexpected fetch_tsv call to sample endpoint: {url}")

    checkpoint = tmp_path / "checkpoint.json"
    with patch.object(module, "fetch_tsv", side_effect=fake_fetch_tsv):
        rows, stats = module.build_coordinate_rows(
            studies=["TestStudy"],
            base_url="https://example.com",
            timeout=10,
            sleep_seconds=0,
            checkpoint_path=checkpoint,
            resume=False,
            metalog_by_sample={},
            max_retries=0,
            retry_backoff=0,
            genome_metadata_map=genome_metadata_map,
        )

    # Only study API was called, never sample API
    assert all("/study/" in url for url in fetch_call_urls), f"Unexpected URLs: {fetch_call_urls}"
    assert stats["sample_api_calls"] == 0
    assert stats["samples_skipped_not_in_metadata"] == 1  # SAMN99999999
    assert len(rows) == 2
    assert {r.mag_id for r in rows} == {"spire_mag_00000001", "spire_mag_00000002"}
    for row in rows:
        assert row.latitude == "40.0"
        assert row.longitude == "-70.0"
        assert row.coordinate_source == "spire"


# ---------------------------------------------------------------------------
# build_coordinate_rows end-to-end (mocked HTTP)
# ---------------------------------------------------------------------------

def test_build_coordinate_rows_uses_spire_id_column(tmp_path):
    """build_coordinate_rows must extract MAG IDs from the 'spire_id' column."""
    module = _load_module(_MOD_PATH, "bmc_e2e")

    study_tsv = (
        "sample_id\tmags\tlat\tlon\tmicrontology\n"
        "SAMN15803490\t2\t40.0\t-70.0\t[]\n"
    )
    sample_tsv = (
        "spire_id\tsample_id\tgenome_size\tnum_contigs\tn50\t"
        "completeness\tcontamination\tgunc_css\tgunc_rrs\tgene_count\tspire_cluster\n"
        "spire_mag_00000001\tSAMN15803490\t1000000\t100\t5000\t"
        "90.0\t2.0\t0.1\t0.1\t900\t\n"
        "spire_mag_00000002\tSAMN15803490\t2000000\t200\t4000\t"
        "85.0\t3.0\t0.2\t0.2\t1800\t\n"
    )

    def fake_fetch_tsv(session, url, timeout):
        if "/study/" in url:
            return module.parse_tsv_rows(study_tsv)
        if "/sample/" in url:
            return module.parse_tsv_rows(sample_tsv)
        return []

    checkpoint = tmp_path / "checkpoint.json"
    with patch.object(module, "fetch_tsv", side_effect=fake_fetch_tsv):
        rows, stats = module.build_coordinate_rows(
            studies=["TestStudy"],
            base_url="https://example.com",
            timeout=10,
            sleep_seconds=0,
            checkpoint_path=checkpoint,
            resume=False,
            metalog_by_sample={},
            max_retries=0,
            retry_backoff=0,
        )

    assert stats["mag_sample_rows"] == 2
    mag_ids = {r.mag_id for r in rows}
    assert mag_ids == {"spire_mag_00000001", "spire_mag_00000002"}
    for row in rows:
        assert row.latitude == "40.0"
        assert row.longitude == "-70.0"
        assert row.coordinate_source == "spire"

