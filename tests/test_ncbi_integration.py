"""
Integration test for the ncbi download_genomes script.

This test uses a small list of test accessions and verifies that the script
can download genome files to MinIO and create the proper directory structure.
"""

import unittest
import sys
import logging
from pathlib import Path
import tempfile

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kbase_transfers import MinioClient

# Import functions from the download script
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts" / "ncbi"))
from download_genomes import (
    download_genome_files,
    get_minio_client,
    minio_bucket,
    minio_path_prefix,
    parse_accession,
    build_accession_path
)


class TestNcbiIntegration(unittest.TestCase):
    """Integration test for ncbi download_genomes script."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.client = MinioClient()
        
        # Path to test accessions file
        cls.test_file = Path(__file__).parent / "assets" / "ncbi_test_accessions.txt"
        
        # Read test accessions
        with open(cls.test_file, 'r') as f:
            cls.test_accessions = [line.strip() for line in f if line.strip()]
        
        # Limit to first 2 accessions for faster testing
        cls.test_limit = 2
        cls.test_accessions = cls.test_accessions[:cls.test_limit]
        
        print(f"\nUsing test accessions from: {cls.test_file}")
        print(f"Testing with {len(cls.test_accessions)} accessions: {cls.test_accessions}")
        
        # Set up MinIO bucket and folder structure
        cls._setup_minio()
        
        # Set up logging to suppress debug output during tests
        logging.getLogger('download_genomes').setLevel(logging.WARNING)
    
    @classmethod
    def _setup_minio(cls):
        """Create MinIO bucket and folder structure if needed."""
        # Create bucket if it doesn't exist
        if not cls.client.bucket_exists(minio_bucket):
            cls.client.s3.create_bucket(Bucket=minio_bucket)
            print(f"Created bucket: {minio_bucket}")
        
        # Create base path if it doesn't exist
        if not cls.client.prefix_exists(minio_bucket, minio_path_prefix):
            cls.client.s3.put_object(
                Bucket=minio_bucket,
                Key=f"{minio_path_prefix}.placeholder",
                Body=b''
            )
            print(f"Created base path: {minio_path_prefix}")
    
    def test_parse_accession(self):
        """Test accession parsing."""
        # Test full format
        prefix, db, accession = parse_accession("GB_GCA_000195005.1")
        self.assertEqual(prefix, "GB")
        self.assertEqual(db, "GCA")
        self.assertEqual(accession, "GCA_000195005.1")
        
        # Test short format
        prefix, db, accession = parse_accession("GCA_000195005.1")
        self.assertIsNone(prefix)
        self.assertEqual(db, "GCA")
        self.assertEqual(accession, "GCA_000195005.1")
        
        # Test RefSeq format
        prefix, db, accession = parse_accession("RS_GCF_000006825.1")
        self.assertEqual(prefix, "RS")
        self.assertEqual(db, "GCF")
        self.assertEqual(accession, "GCF_000006825.1")
        
        print("\n✓ Verified accession parsing")
    
    def test_minio_client_initialization(self):
        """Test that MinIO client can be initialized and bucket/path exist."""
        try:
            client = get_minio_client()
            self.assertIsNotNone(client)
            print("\n✓ MinIO client initialized successfully")
        except Exception as e:
            self.fail(f"Failed to initialize MinIO client: {e}")
    
    def test_download_single_genome(self):
        """Test downloading a single genome to MinIO."""
        # Use the first test accession
        test_accession = self.test_accessions[0]
        
        print(f"\n✓ Testing download of: {test_accession}")
        
        # Track failures
        failed_transfers = []
        no_checksum_files = []
        
        # Create temporary directory for downloads
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                download_genome_files(
                    test_accession,
                    self.client,
                    temp_dir,
                    failed_transfers,
                    no_checksum_files
                )
                
                # Verify files were uploaded to MinIO
                _, database, accession_full = parse_accession(test_accession)
                
                # The actual assembly directory name will be found by the function
                # Path structure is: prefix + raw_data/GCA/000/195/005/GCA_000195005.1_ASM.../
                # We'll search using a broader prefix
                search_prefix = f"{minio_path_prefix}raw_data/{database}/"
                objects = self.client.list_objects(minio_bucket, prefix=search_prefix)
                
                # Filter to objects that contain our accession
                matching_objects = [obj for obj in objects if accession_full in obj]
                
                self.assertGreater(len(matching_objects), 0, 
                                  f"No objects found for accession {accession_full}")
                
                # Check for expected file types
                has_fna = any('.fna.gz' in obj or '_genomic.fna.gz' in obj for obj in matching_objects)
                has_gff = any('.gff.gz' in obj for obj in matching_objects)
                has_md5 = any('md5checksums.txt' in obj for obj in matching_objects)
                
                self.assertTrue(has_fna or has_gff, 
                              "Should have at least .fna.gz or .gff.gz file")
                
                print(f"✓ Downloaded {len(matching_objects)} files to MinIO")
                print(f"  Files include: FNA={has_fna}, GFF={has_gff}, MD5={has_md5}")
                
                # Store for cleanup
                self._test_objects = matching_objects
                
            except Exception as e:
                self.fail(f"Failed to download genome: {e}")
    
    def test_verify_uploaded_structure(self):
        """Test that uploaded files have correct structure and metadata."""
        # Use first test accession
        test_accession = self.test_accessions[0]
        _, database, accession_full = parse_accession(test_accession)
        
        # Find objects for this accession
        search_prefix = f"{minio_path_prefix}raw_data/{database}/"
        all_objects = self.client.list_objects(minio_bucket, prefix=search_prefix)
        objects = [obj for obj in all_objects if accession_full in obj]
        
        if not objects:
            self.skipTest("No objects found from previous test")
        
        # Verify each object can be retrieved and has metadata
        for obj_key in objects[:3]:  # Check first 3 objects
            response = self.client.s3.head_object(Bucket=minio_bucket, Key=obj_key)
            self.assertIn('ContentLength', response)
            self.assertGreater(response['ContentLength'], 0)
        
        print(f"\n✓ Verified structure and metadata for uploaded files")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test data from MinIO."""
        print("\nCleaning up test data...")
        
        total_deleted = 0
        for accession in cls.test_accessions:
            try:
                _, database, accession_full = parse_accession(accession)
                search_prefix = f"{minio_path_prefix}raw_data/{database}/"
                
                all_objects = cls.client.list_objects(minio_bucket, prefix=search_prefix)
                objects = [obj for obj in all_objects if accession_full in obj]
                
                for obj in objects:
                    cls.client.s3.delete_object(Bucket=minio_bucket, Key=obj)
                    total_deleted += 1
            except Exception as e:
                print(f"Warning: Failed to clean up {accession}: {e}")
        
        print(f"✓ Cleaned up {total_deleted} objects")


if __name__ == '__main__':
    unittest.main()
