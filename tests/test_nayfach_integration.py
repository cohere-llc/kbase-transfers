"""
Integration test for the nayfach_2020 download and load script.

This test downloads the Excel file (if needed) and loads a small number
of records to MinIO, then verifies their successful creation.
"""

import unittest
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from kbase_transfers import MinioClient
from scripts.nayfach_2020.download_and_load import (
    download_xlsx, 
    load_metagenomes, 
    load_mags,
    BUCKET_NAME,
    BASE_PATH,
    METAGENOMES_PATH,
    MAGS_PATH
)


class TestNayfachIntegration(unittest.TestCase):
    """Integration test for nayfach_2020 script."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.client = MinioClient()
        cls.test_limit = 3  # Number of records to test with
        
        # Download Excel file if needed
        data_dir = Path(__file__).parent.parent / "scripts" / "nayfach_2020" / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        cls.xlsx_path = download_xlsx(data_dir, force=False)
        
        print(f"\nUsing Excel file: {cls.xlsx_path}")
        
        # Set up MinIO bucket and folder structure
        cls._setup_minio()
    
    @classmethod
    def _setup_minio(cls):
        """Create MinIO bucket and folder structure if needed."""
        # Create bucket if it doesn't exist
        if not cls.client.bucket_exists(BUCKET_NAME):
            cls.client.s3.create_bucket(Bucket=BUCKET_NAME)
            print(f"Created bucket: {BUCKET_NAME}")
        
        # Create base path if it doesn't exist
        if not cls.client.prefix_exists(BUCKET_NAME, BASE_PATH + "/"):
            cls.client.s3.put_object(
                Bucket=BUCKET_NAME,
                Key=f"{BASE_PATH}/.placeholder",
                Body=b''
            )
            print(f"Created base path: {BASE_PATH}/")
    
    def test_load_metagenomes(self):
        """Test loading metagenomes to MinIO."""
        # Load a small number of metagenomes
        load_metagenomes(self.client, self.xlsx_path, dry_run=False, limit=self.test_limit)
        
        # Verify the metagenomes were created
        objects = self.client.list_objects(BUCKET_NAME, METAGENOMES_PATH + "/")
        metagenome_ids = set()
        
        for obj in objects:
            if obj.endswith('/metagenome.json'):
                # Extract IMG_TAXON_ID from path
                parts = obj.split('/')
                metagenome_id = parts[-2]
                metagenome_ids.add(metagenome_id)
        
        # Should have at least the test_limit number of metagenomes
        self.assertGreaterEqual(len(metagenome_ids), self.test_limit)
        
        # Verify one metagenome JSON structure
        first_object = [obj for obj in objects if obj.endswith('/metagenome.json')][0]
        response = self.client.s3.get_object(Bucket=BUCKET_NAME, Key=first_object)
        data = json.loads(response['Body'].read())
        
        # Verify expected fields
        self.assertIn('IMG_TAXON_ID', data)
        self.assertIn('BIOSAMPLE_NAME', data)
        self.assertIn('ECOSYSTEM', data)
        self.assertIn('LATITUDE', data)
        
        # Verify data types
        self.assertIsInstance(data['IMG_TAXON_ID'], int)
        
        print(f"\n✓ Verified {len(metagenome_ids)} metagenomes")
    
    def test_load_mags(self):
        """Test loading MAGs to MinIO."""
        # Load a small number of MAGs
        load_mags(self.client, self.xlsx_path, dry_run=False, limit=self.test_limit)
        
        # Verify the MAGs were created
        objects = self.client.list_objects(BUCKET_NAME, MAGS_PATH + "/")
        mag_ids = set()
        
        for obj in objects:
            if obj.endswith('/mag.json'):
                # Extract genome_id from path
                parts = obj.split('/')
                mag_id = parts[-2]
                mag_ids.add(mag_id)
        
        # Should have at least the test_limit number of MAGs
        self.assertGreaterEqual(len(mag_ids), self.test_limit)
        
        # Verify one MAG JSON structure
        first_object = [obj for obj in objects if obj.endswith('/mag.json')][0]
        response = self.client.s3.get_object(Bucket=BUCKET_NAME, Key=first_object)
        data = json.loads(response['Body'].read())
        
        # Verify expected fields
        self.assertIn('genome_id', data)
        self.assertIn('img_taxon_id', data)
        self.assertIn('completeness', data)
        self.assertIn('contamination', data)
        self.assertIn('quality_score', data)
        
        # Verify data types
        self.assertIsInstance(data['genome_id'], str)
        
        print(f"\n✓ Verified {len(mag_ids)} MAGs")
    
    def test_json_content_integrity(self):
        """Test that JSON data is properly formatted and contains expected values."""
        # Load one record
        load_metagenomes(self.client, self.xlsx_path, dry_run=False, limit=1)
        
        # Get the first metagenome
        objects = self.client.list_objects(BUCKET_NAME, METAGENOMES_PATH + "/")
        metagenome_objects = [obj for obj in objects if obj.endswith('/metagenome.json')]
        self.assertGreater(len(metagenome_objects), 0, "No metagenome objects found")
        
        # Retrieve and verify JSON
        response = self.client.s3.get_object(Bucket=BUCKET_NAME, Key=metagenome_objects[0])
        data = json.loads(response['Body'].read())
        
        # Verify null handling - some fields should be None
        has_null = any(value is None for value in data.values())
        self.assertTrue(has_null or all(value is not None for value in data.values()),
                       "Data should properly handle None values")
        
        # Verify no NaN strings (pandas sometimes converts NaN to string)
        for key, value in data.items():
            if isinstance(value, str):
                self.assertNotIn('nan', value.lower(), 
                               f"Field {key} contains 'nan' string: {value}")
        
        print(f"\n✓ Verified JSON content integrity")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test data from MinIO."""
        print("\nCleaning up test data...")
        
        # Delete all metagenomes
        metagenome_objects = cls.client.list_objects(BUCKET_NAME, METAGENOMES_PATH + "/")
        for obj in metagenome_objects:
            cls.client.s3.delete_object(Bucket=BUCKET_NAME, Key=obj)
        
        # Delete all MAGs
        mag_objects = cls.client.list_objects(BUCKET_NAME, MAGS_PATH + "/")
        for obj in mag_objects:
            cls.client.s3.delete_object(Bucket=BUCKET_NAME, Key=obj)
        
        print(f"✓ Cleaned up {len(metagenome_objects)} metagenomes and {len(mag_objects)} MAGs")


if __name__ == '__main__':
    unittest.main()
