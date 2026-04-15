"""
Unit tests for the assembly summary parsing and manifest generation logic.

Tests parse_assembly_summary, get_latest_assembly_paths, compute_diff,
filter_by_prefix_range, and CRC64/NVME checksum computation without
requiring network access.
"""

import base64
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

from generate_transfer_manifest import (
    parse_assembly_summary,
    get_latest_assembly_paths,
    compute_diff,
    filter_by_prefix_range,
    write_transfer_manifest,
    write_removed_manifest,
    write_diff_summary,
    _accession_prefix,
)
from download_genomes import compute_md5, compute_crc64nvme, build_accession_path


# Minimal assembly_summary_refseq.txt content (tab-separated, 20+ columns)
# Columns: 0=accession, 1=bioproject, 2=biosample, 3=wgs_master, 4=refseq_category,
# 5=taxid, 6=species_taxid, 7=organism_name, 8=infraspecific_name, 9=isolate,
# 10=version_status, 11=assembly_level, 12=release_type, 13=genome_rep,
# 14=seq_rel_date, 15=asm_name, 16-18=filler, 19=ftp_path
SAMPLE_SUMMARY = """# assembly_accession\tbioproject\tbiosample\twgs_master\trefseq_category\ttaxid\tspecies_taxid\torganism_name\tinfraspecific_name\tisolate\tversion_status\tassembly_level\trelease_type\tgenome_rep\tseq_rel_date\tasm_name\t16\t17\t18\tftp_path
GCF_000001215.4\tPRJNA13812\tSAMN02803731\t\treference genome\t7227\t7227\tDrosophila melanogaster\t\t\tlatest\tChromosome\tMajor\tFull\t2014/10/21\tRelease_6_plus_ISO1_MT\t\t\t\thttps://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT
GCF_000001405.40\tPRJNA168\tna\t\treference genome\t9606\t9606\tHomo sapiens\t\t\tlatest\tChromosome\tPatch\tFull\t2022/02/03\tGRCh38.p14\t\t\t\thttps://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14
GCF_000005845.2\tPRJNA57779\tSAMN02604091\t\trepresentative genome\t511145\t562\tEscherichia coli\t\t\treplaced\tComplete Genome\tMajor\tFull\t2013/09/26\tASM584v2\t\t\t\thttps://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/005/845/GCF_000005845.2_ASM584v2
GCF_000009999.1\tPRJNA999\tSAMN999\t\tna\t0\t0\tTest organism\t\t\tsuppressed\tScaffold\tMajor\tFull\t2010/01/01\tASM999v1\t\t\t\thttps://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/009/999/GCF_000009999.1_ASM999v1
GCF_000099999.1\tPRJNA888\tSAMN888\t\tna\t0\t0\tTest organism 2\t\t\tlatest\tContig\tMajor\tFull\t2023/06/15\tASM9999v1\t\t\t\tna"""


class TestParseAssemblySummary(unittest.TestCase):
    """Test assembly summary parsing."""

    def test_parse_basic(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        # "na" ftp_path should be excluded, so 4 entries
        self.assertEqual(len(assemblies), 4)
        self.assertIn("GCF_000001215.4", assemblies)
        self.assertIn("GCF_000005845.2", assemblies)
        self.assertNotIn("GCF_000099999.1", assemblies)

    def test_parse_status(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        self.assertEqual(assemblies["GCF_000001215.4"]["status"], "latest")
        self.assertEqual(assemblies["GCF_000005845.2"]["status"], "replaced")
        self.assertEqual(assemblies["GCF_000009999.1"]["status"], "suppressed")

    def test_parse_seq_rel_date(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        self.assertEqual(assemblies["GCF_000001215.4"]["seq_rel_date"], "2014/10/21")

    def test_parse_assembly_dir(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        self.assertEqual(
            assemblies["GCF_000001215.4"]["assembly_dir"],
            "GCF_000001215.4_Release_6_plus_ISO1_MT",
        )

    def test_parse_ftp_path(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        self.assertEqual(
            assemblies["GCF_000001215.4"]["ftp_path"],
            "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT",
        )

    def test_parse_empty(self):
        assemblies = parse_assembly_summary("# comment only\n")
        self.assertEqual(len(assemblies), 0)

    def test_parse_skips_comments(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        for acc in assemblies:
            self.assertTrue(acc.startswith("GCF_"))

    def test_parse_from_file(self):
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".tsv", delete=False
        ) as f:
            f.write(SAMPLE_SUMMARY)
            f.flush()
            tmp_path = f.name
        try:
            assemblies = parse_assembly_summary(tmp_path)
            self.assertEqual(len(assemblies), 4)
        finally:
            os.remove(tmp_path)


class TestGetLatestAssemblyPaths(unittest.TestCase):
    """Test extraction of FTP paths for latest assemblies."""

    def setUp(self):
        self.ncbi = parse_assembly_summary(SAMPLE_SUMMARY)

    def test_only_latest(self):
        paths = get_latest_assembly_paths(self.ncbi)
        accessions = [acc for acc, _ in paths]
        self.assertIn("GCF_000001215.4", accessions)
        self.assertIn("GCF_000001405.40", accessions)
        self.assertNotIn("GCF_000005845.2", accessions)
        self.assertNotIn("GCF_000009999.1", accessions)

    def test_path_conversion(self):
        paths = get_latest_assembly_paths(self.ncbi)
        path_dict = {acc: p for acc, p in paths}
        self.assertEqual(
            path_dict["GCF_000001215.4"],
            "/genomes/all/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/",
        )

    def test_paths_end_with_slash(self):
        paths = get_latest_assembly_paths(self.ncbi)
        for _, path in paths:
            self.assertTrue(path.endswith("/"))

    def test_empty_summary(self):
        assemblies = parse_assembly_summary("# empty\n")
        paths = get_latest_assembly_paths(assemblies)
        self.assertEqual(len(paths), 0)


class TestComputeDiff(unittest.TestCase):
    """Test diff computation between current and previous assembly state."""

    def test_all_new_no_previous(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions=set())
        # Only 'latest' status assemblies count as new
        self.assertIn("GCF_000001215.4", diff["new"])
        self.assertIn("GCF_000001405.40", diff["new"])
        # replaced/suppressed should not be in "new"
        self.assertNotIn("GCF_000005845.2", diff["new"])

    def test_nothing_new_when_all_known(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        known = {"GCF_000001215.4", "GCF_000001405.40"}
        diff = compute_diff(current, previous_accessions=known)
        self.assertEqual(len(diff["new"]), 0)

    def test_detects_updated_seq_rel_date(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        # Create "previous" with same accessions but different dates
        previous = parse_assembly_summary(SAMPLE_SUMMARY)
        previous["GCF_000001215.4"]["seq_rel_date"] = "2010/01/01"
        diff = compute_diff(current, previous_assemblies=previous)
        self.assertIn("GCF_000001215.4", diff["updated"])

    def test_detects_replaced(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        known = {"GCF_000005845.2"}
        diff = compute_diff(current, previous_accessions=known)
        self.assertIn("GCF_000005845.2", diff["replaced"])

    def test_detects_suppressed(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        known = {"GCF_000009999.1"}
        diff = compute_diff(current, previous_accessions=known)
        self.assertIn("GCF_000009999.1", diff["suppressed"])

    def test_detects_withdrawn(self):
        """Accessions in previous but completely absent from current."""
        current = parse_assembly_summary("# empty\n")
        known = {"GCF_000001215.4"}
        diff = compute_diff(current, previous_accessions=known)
        self.assertIn("GCF_000001215.4", diff["suppressed"])

    def test_scan_store_fallback(self):
        """Using a set of accessions (store scan mode) instead of full summary."""
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        store = {"GCF_000001215.4"}
        diff = compute_diff(current, previous_accessions=store)
        # 000001215 is known -> not new
        self.assertNotIn("GCF_000001215.4", diff["new"])
        # 000001405 is not known -> new
        self.assertIn("GCF_000001405.40", diff["new"])


class TestPrefixFiltering(unittest.TestCase):
    """Test prefix range filtering."""

    def test_accession_prefix(self):
        self.assertEqual(_accession_prefix("GCF_000001215.4"), "000")
        self.assertEqual(_accession_prefix("GCF_123456789.1"), "123")
        self.assertIsNone(_accession_prefix("invalid"))

    def test_filter_range(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        # All test accessions start with 000
        filtered = filter_by_prefix_range(assemblies, "000", "000")
        self.assertEqual(len(filtered), len(assemblies))

    def test_filter_excludes_out_of_range(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        filtered = filter_by_prefix_range(assemblies, "001", "999")
        self.assertEqual(len(filtered), 0)

    def test_no_filter_returns_all(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        filtered = filter_by_prefix_range(assemblies)
        self.assertEqual(len(filtered), len(assemblies))


class TestManifestWriting(unittest.TestCase):
    """Test manifest file writing."""

    def test_write_transfer_manifest(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        diff = compute_diff(current, previous_accessions=set())
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as f:
            tmp_path = f.name
        try:
            paths = write_transfer_manifest(diff, current, tmp_path)
            self.assertTrue(len(paths) > 0)
            with open(tmp_path) as f:
                lines = [l.strip() for l in f if l.strip()]
            self.assertEqual(len(lines), len(paths))
            for line in lines:
                self.assertTrue(line.startswith("/genomes/"))
                self.assertTrue(line.endswith("/"))
        finally:
            os.remove(tmp_path)

    def test_write_removed_manifest(self):
        current = parse_assembly_summary(SAMPLE_SUMMARY)
        known = {"GCF_000005845.2", "GCF_000009999.1"}
        diff = compute_diff(current, previous_accessions=known)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False
        ) as f:
            tmp_path = f.name
        try:
            removed = write_removed_manifest(diff, tmp_path)
            self.assertEqual(len(removed), 2)
        finally:
            os.remove(tmp_path)

    def test_write_diff_summary(self):
        diff = {"new": ["a"], "updated": ["b"], "replaced": ["c"], "suppressed": []}
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            tmp_path = f.name
        try:
            summary = write_diff_summary(diff, tmp_path, "refseq", "000", "003")
            self.assertEqual(summary["counts"]["new"], 1)
            self.assertEqual(summary["counts"]["total_to_transfer"], 2)
            self.assertEqual(summary["prefix_range"]["from"], "000")

            with open(tmp_path) as f:
                loaded = json.load(f)
            self.assertEqual(loaded["database"], "refseq")
        finally:
            os.remove(tmp_path)


class TestChecksums(unittest.TestCase):
    """Test checksum computation utilities."""

    def test_compute_md5(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"Hello, World!")
            f.flush()
            tmp_path = f.name
        try:
            md5 = compute_md5(tmp_path)
            self.assertEqual(md5, hashlib.md5(b"Hello, World!").hexdigest())
        finally:
            os.remove(tmp_path)

    def test_compute_crc64nvme(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"Hello, World!")
            f.flush()
            tmp_path = f.name
        try:
            crc = compute_crc64nvme(tmp_path)
            decoded = base64.b64decode(crc)
            self.assertEqual(len(decoded), 8)
        finally:
            os.remove(tmp_path)

    def test_crc64nvme_deterministic(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data for checksum")
            f.flush()
            tmp_path = f.name
        try:
            crc1 = compute_crc64nvme(tmp_path)
            crc2 = compute_crc64nvme(tmp_path)
            self.assertEqual(crc1, crc2)
        finally:
            os.remove(tmp_path)


class TestBuildAccessionPath(unittest.TestCase):
    """Test accession path building."""

    def test_gcf_path(self):
        path = build_accession_path("GCF_000001215.4_Release_6_plus_ISO1_MT")
        self.assertEqual(
            path, "raw_data/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/"
        )

    def test_gca_path(self):
        path = build_accession_path("GCA_000195005.1_ASM19500v1")
        self.assertEqual(
            path, "raw_data/GCA/000/195/005/GCA_000195005.1_ASM19500v1/"
        )


if __name__ == "__main__":
    unittest.main()
