"""
Unit tests for the NCBI sync_genomes module.

Tests assembly summary parsing, path extraction, and CRC64/NVME
checksum computation without requiring network access.
"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts" / "ncbi"))
from sync_genomes import parse_assembly_summary, get_latest_assembly_paths
from download_genomes import compute_md5, compute_crc64nvme, build_accession_path

import base64
import hashlib
import tempfile


# Minimal assembly_summary_refseq.txt content (tab-separated, 20+ columns)
# Columns: 0=accession, 1=bioproject, 2=biosample, 3=wgs_master, 4=refseq_category,
#           5=taxid, 6=species_taxid, 7=organism_name, 8=infraspecific_name, 9=isolate,
#           10=version_status, 11=assembly_level, 12=release_type, 13=genome_rep,
#           14=seq_rel_date, 15=asm_name, 16-18=filler, 19=ftp_path
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
        # "na" ftp_path should be excluded, so 4 entries minus 1
        self.assertEqual(len(assemblies), 4)
        self.assertIn("GCF_000001215.4", assemblies)
        self.assertIn("GCF_000005845.2", assemblies)
        self.assertNotIn("GCF_000099999.1", assemblies)  # ftp_path == "na"

    def test_parse_status(self):
        assemblies = parse_assembly_summary(SAMPLE_SUMMARY)
        self.assertEqual(assemblies["GCF_000001215.4"]["status"], "latest")
        self.assertEqual(assemblies["GCF_000005845.2"]["status"], "replaced")
        self.assertEqual(assemblies["GCF_000009999.1"]["status"], "suppressed")

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
        # Should not include header line as an assembly
        for acc in assemblies:
            self.assertTrue(acc.startswith("GCF_"))


class TestGetLatestAssemblyPaths(unittest.TestCase):
    """Test extraction of FTP paths for latest assemblies."""

    def setUp(self):
        self.ncbi = parse_assembly_summary(SAMPLE_SUMMARY)

    def test_only_latest(self):
        paths = get_latest_assembly_paths(self.ncbi)
        accessions = [acc for acc, _ in paths]
        # Only "latest" assemblies should appear
        self.assertIn("GCF_000001215.4", accessions)
        self.assertIn("GCF_000001405.40", accessions)
        # replaced/suppressed should not
        self.assertNotIn("GCF_000005845.2", accessions)
        self.assertNotIn("GCF_000009999.1", accessions)

    def test_path_conversion(self):
        paths = get_latest_assembly_paths(self.ncbi)
        path_dict = {acc: p for acc, p in paths}
        # https:// URL should be converted to FTP path
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


class TestChecksums(unittest.TestCase):
    """Test checksum computation utilities."""

    def test_compute_md5(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"Hello, World!")
            f.flush()
            md5 = compute_md5(f.name)
        self.assertEqual(md5, hashlib.md5(b"Hello, World!").hexdigest())

    def test_compute_crc64nvme(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"Hello, World!")
            f.flush()
            crc = compute_crc64nvme(f.name)
        # Verify it's a base64-encoded 8-byte value
        decoded = base64.b64decode(crc)
        self.assertEqual(len(decoded), 8)

    def test_crc64nvme_deterministic(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test data for checksum")
            f.flush()
            crc1 = compute_crc64nvme(f.name)
            crc2 = compute_crc64nvme(f.name)
        self.assertEqual(crc1, crc2)


class TestBuildAccessionPath(unittest.TestCase):
    """Test accession path building."""

    def test_gcf_path(self):
        path = build_accession_path("GCF_000001215.4_Release_6_plus_ISO1_MT")
        self.assertEqual(path, "raw_data/GCF/000/001/215/GCF_000001215.4_Release_6_plus_ISO1_MT/")

    def test_gca_path(self):
        path = build_accession_path("GCA_000195005.1_ASM19500v1")
        self.assertEqual(path, "raw_data/GCA/000/195/005/GCA_000195005.1_ASM19500v1/")


if __name__ == "__main__":
    unittest.main()
