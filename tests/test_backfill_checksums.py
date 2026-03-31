"""
Integration tests for backfill_checksums.py.

Requires a running MinIO instance (provided by CI or local docker).
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts" / "ncbi"))

from kbase_transfers import MinioClient
from backfill_checksums import backfill


class TestBackfillChecksums(unittest.TestCase):
    """Test CRC64/NVME backfill on objects in MinIO."""

    TEST_BUCKET = "test-backfill"
    TEST_PREFIX = "backfill-test/"

    @classmethod
    def setUpClass(cls):
        cls.client = MinioClient()
        if not cls.client.bucket_exists(cls.TEST_BUCKET):
            cls.client.s3.create_bucket(Bucket=cls.TEST_BUCKET)

    def _upload_without_checksum(self, key, content=b"test data", md5=None):
        """Upload an object WITHOUT CRC64/NVME, optionally with MD5 metadata."""
        kwargs = {
            "Bucket": self.TEST_BUCKET,
            "Key": key,
            "Body": content,
        }
        if md5:
            kwargs["Metadata"] = {"md5": md5}
        self.client.s3.put_object(**kwargs)

    def _upload_with_checksum(self, key, content=b"test data", md5=None):
        """Upload an object WITH CRC64/NVME."""
        kwargs = {
            "Bucket": self.TEST_BUCKET,
            "Key": key,
            "Body": content,
            "ChecksumAlgorithm": "CRC64NVME",
        }
        if md5:
            kwargs["Metadata"] = {"md5": md5}
        self.client.s3.put_object(**kwargs)

    def _clean_prefix(self, prefix):
        """Remove all objects under a prefix."""
        paginator = self.client.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.TEST_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                self.client.s3.delete_object(
                    Bucket=self.TEST_BUCKET, Key=obj["Key"]
                )

    def test_backfill_adds_crc64nvme(self):
        """Objects without CRC64/NVME get checksums added."""
        prefix = self.TEST_PREFIX + "add/"
        self._clean_prefix(prefix)

        self._upload_without_checksum(prefix + "file1.txt")
        self._upload_without_checksum(prefix + "file2.txt")

        result = backfill(
            prefix, bucket=self.TEST_BUCKET, client=self.client
        )

        self.assertEqual(result["backfilled"], 2)
        self.assertEqual(result["skipped"], 0)

        for name in ["file1.txt", "file2.txt"]:
            info = self.client.stat_object(self.TEST_BUCKET, prefix + name)
            self.assertIsNotNone(info["crc64nvme"], f"{name} should have CRC64/NVME")

    def test_backfill_preserves_md5_metadata(self):
        """Existing MD5 user metadata must survive backfill."""
        prefix = self.TEST_PREFIX + "md5/"
        self._clean_prefix(prefix)

        md5_value = "d41d8cd98f00b204e9800998ecf8427e"
        self._upload_without_checksum(
            prefix + "with_md5.txt", md5=md5_value
        )

        result = backfill(
            prefix, bucket=self.TEST_BUCKET, client=self.client
        )
        self.assertEqual(result["backfilled"], 1)

        info = self.client.stat_object(self.TEST_BUCKET, prefix + "with_md5.txt")
        self.assertIsNotNone(info["crc64nvme"], "CRC64/NVME should be added")
        self.assertEqual(info["md5"], md5_value, "MD5 metadata must be preserved")

    def test_backfill_skips_objects_with_crc64nvme(self):
        """Objects that already have CRC64/NVME are skipped."""
        prefix = self.TEST_PREFIX + "skip/"
        self._clean_prefix(prefix)

        self._upload_with_checksum(prefix + "already.txt")

        result = backfill(
            prefix, bucket=self.TEST_BUCKET, client=self.client
        )

        self.assertEqual(result["backfilled"], 0)
        self.assertEqual(result["skipped"], 1)

    def test_backfill_mixed_objects(self):
        """Mix of objects with and without CRC64/NVME."""
        prefix = self.TEST_PREFIX + "mixed/"
        self._clean_prefix(prefix)

        md5_a = "aaaa1111bbbb2222cccc3333dddd4444"
        md5_b = "eeee5555ffff6666777788889999aaaa"
        self._upload_without_checksum(prefix + "needs_it.txt", md5=md5_a)
        self._upload_with_checksum(prefix + "has_it.txt", md5=md5_b)

        result = backfill(
            prefix, bucket=self.TEST_BUCKET, client=self.client
        )

        self.assertEqual(result["backfilled"], 1)
        self.assertEqual(result["skipped"], 1)

        needs = self.client.stat_object(self.TEST_BUCKET, prefix + "needs_it.txt")
        self.assertIsNotNone(needs["crc64nvme"])
        self.assertEqual(needs["md5"], md5_a, "MD5 must be preserved on backfilled object")

        has = self.client.stat_object(self.TEST_BUCKET, prefix + "has_it.txt")
        self.assertIsNotNone(has["crc64nvme"])
        self.assertEqual(has["md5"], md5_b, "MD5 must be preserved on skipped object")

    def test_dry_run_does_not_modify(self):
        """Dry-run reports what would be done but changes nothing."""
        prefix = self.TEST_PREFIX + "dryrun/"
        self._clean_prefix(prefix)

        self._upload_without_checksum(prefix + "untouched.txt", md5="abcd1234")

        result = backfill(
            prefix, dry_run=True, bucket=self.TEST_BUCKET, client=self.client
        )

        self.assertEqual(result["backfilled"], 1)  # counted but not applied

        info = self.client.stat_object(self.TEST_BUCKET, prefix + "untouched.txt")
        self.assertIsNone(info["crc64nvme"], "Dry-run should not add checksum")
        self.assertEqual(info["md5"], "abcd1234", "Dry-run should not alter metadata")

    def test_limit_stops_early(self):
        """Limit parameter stops processing after N objects."""
        prefix = self.TEST_PREFIX + "limit/"
        self._clean_prefix(prefix)

        for i in range(5):
            self._upload_without_checksum(prefix + f"file{i}.txt")

        result = backfill(
            prefix, limit=3, bucket=self.TEST_BUCKET, client=self.client
        )

        total = result["backfilled"] + result["skipped"] + result["failed"]
        self.assertEqual(total, 3, "Should stop after processing 3 objects")

    @classmethod
    def tearDownClass(cls):
        paginator = cls.client.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=cls.TEST_BUCKET):
            for obj in page.get("Contents", []):
                cls.client.s3.delete_object(
                    Bucket=cls.TEST_BUCKET, Key=obj["Key"]
                )
        cls.client.s3.delete_bucket(Bucket=cls.TEST_BUCKET)


if __name__ == "__main__":
    unittest.main()
