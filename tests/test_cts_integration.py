"""
Integration tests for CRC64/NVME checksum support.

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
from download_genomes import compute_crc64nvme, compute_md5


class TestCRC64NVMEIntegration(unittest.TestCase):
    """Test that CRC64/NVME checksums are stored and retrievable in MinIO."""

    @classmethod
    def setUpClass(cls):
        cls.client = MinioClient()
        cls.test_bucket = "test-cts-checksums"

        if not cls.client.bucket_exists(cls.test_bucket):
            cls.client.s3.create_bucket(Bucket=cls.test_bucket)

    def test_upload_with_crc64nvme(self):
        """Upload a file with CRC64/NVME and verify the checksum is stored."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"test content for checksum verification")
            f.flush()
            tmp_path = f.name

        object_name = "test_crc64nvme.txt"
        expected_crc = compute_crc64nvme(tmp_path)

        self.client.upload_file(
            self.test_bucket,
            object_name,
            tmp_path,
            metadata={"md5": compute_md5(tmp_path)},
            checksum_algorithm="CRC64NVME",
        )

        info = self.client.stat_object(self.test_bucket, object_name)
        self.assertIsNotNone(info)
        self.assertIsNotNone(info.get("crc64nvme"), "CRC64NVME checksum should be stored")
        self.assertEqual(info["crc64nvme"], expected_crc)
        self.assertIsNotNone(info.get("md5"), "MD5 metadata should be stored")

        os.remove(tmp_path)

    def test_put_json_with_crc64nvme(self):
        """Upload JSON with CRC64/NVME and verify."""
        data = {"test": "value", "number": 42}
        object_name = "test_json_crc.json"

        self.client.put_json_object(
            self.test_bucket,
            object_name,
            data,
            checksum_algorithm="CRC64NVME",
        )

        info = self.client.stat_object(self.test_bucket, object_name)
        self.assertIsNotNone(info)
        self.assertIsNotNone(info.get("crc64nvme"))

    def test_stat_without_crc64nvme(self):
        """Objects uploaded without CRC64NVME should have crc64nvme=None."""
        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"no checksum here")
            f.flush()
            tmp_path = f.name

        object_name = "test_no_crc.txt"
        self.client.upload_file(self.test_bucket, object_name, tmp_path)

        info = self.client.stat_object(self.test_bucket, object_name)
        self.assertIsNotNone(info)
        self.assertIsNone(info.get("crc64nvme"))

        os.remove(tmp_path)

    @classmethod
    def tearDownClass(cls):
        # Clean up test bucket
        paginator = cls.client.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=cls.test_bucket):
            for obj in page.get("Contents", []):
                cls.client.s3.delete_object(Bucket=cls.test_bucket, Key=obj["Key"])
        cls.client.s3.delete_bucket(Bucket=cls.test_bucket)


if __name__ == "__main__":
    unittest.main()
