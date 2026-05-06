"""Unit tests for SPIRE descriptor helper behavior."""

from pathlib import Path
import importlib.util
import sys


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class _ValidResult:
    is_valid = True

    @staticmethod
    def summary() -> str:
        return "ok"


def test_create_descriptor_adds_optional_coordinate_resources(tmp_path, monkeypatch):
    module = _load_module(
        Path(__file__).parent.parent / "scripts" / "spire" / "create_descriptor.py",
        "spire_create_descriptor",
    )

    monkeypatch.setattr(module, "validate_descriptor", lambda descriptor: _ValidResult())

    genome_dir = tmp_path / "spire_representative_genomes"
    genome_dir.mkdir()
    (genome_dir / "specI_v4_00000.fa.gz").write_bytes(b"test")

    coordinates_tsv = tmp_path / "spire_v01_mag_coordinates.tsv"
    coordinates_tsv.write_text("mag_id\tsample_id\nA\tS1\n", encoding="utf-8")

    summary_json = tmp_path / "spire_v01_mag_coordinates_summary.json"
    summary_json.write_text("{}\n", encoding="utf-8")

    descriptor = module.create_descriptor(genome_dir, coordinates_tsv, summary_json)
    paths = {resource["path"] for resource in descriptor["resources"]}

    assert "specI_v4_00000.fa.gz" in paths
    assert "spire_v01_mag_coordinates.tsv" in paths
    assert "spire_v01_mag_coordinates_summary.json" in paths


def test_create_descriptor_without_optional_files(tmp_path, monkeypatch):
    module = _load_module(
        Path(__file__).parent.parent / "scripts" / "spire" / "create_descriptor.py",
        "spire_create_descriptor_no_optional",
    )

    monkeypatch.setattr(module, "validate_descriptor", lambda descriptor: _ValidResult())

    genome_dir = tmp_path / "spire_representative_genomes"
    genome_dir.mkdir()
    (genome_dir / "specI_v4_00001.fa.gz").write_bytes(b"test")

    descriptor = module.create_descriptor(genome_dir)
    assert len(descriptor["resources"]) == 1
    assert descriptor["resources"][0]["path"] == "specI_v4_00001.fa.gz"
