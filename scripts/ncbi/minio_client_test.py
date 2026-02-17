# Test for the MinIO client using a local MinIO server
import unittest
from minio_client import MinioClient
import os
import tempfile

class TestMinioClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize MinIO client
        cls.client = MinioClient()
        cls.test_bucket = "test-bucket"
        
        # Create test bucket if it doesn't exist
        existing_buckets = cls.client.list_buckets()
        if cls.test_bucket not in existing_buckets:
            cls.client.s3.create_bucket(Bucket=cls.test_bucket)
    
    def test_upload_and_download_file(self):
        # Create a temporary file to upload
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"Hello, MinIO!")
            tmp_file_path = tmp_file.name
        
        object_name = "test_upload.txt"
        
        # Upload the file
        self.client.upload_file(self.test_bucket, object_name, tmp_file_path)
        
        # Download the file to a new temporary location
        download_path = tmp_file_path + "_downloaded"
        self.client.download_file(self.test_bucket, object_name, download_path)
        
        # Verify the contents
        with open(download_path, 'rb') as f:
            content = f.read()
            self.assertEqual(content, b"Hello, MinIO!")
        
        # Clean up temporary files
        os.remove(tmp_file_path)
        os.remove(download_path)
    
    def test_list_objects(self):
        # Ensure the test object exists
        object_name = "test_list.txt"
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(b"List Test")
            tmp_file_path = tmp_file.name
        
        self.client.upload_file(self.test_bucket, object_name, tmp_file_path)
        
        # List objects in the bucket
        objects = self.client.list_objects(self.test_bucket)
        self.assertIn(object_name, objects)
        
        # Clean up
        os.remove(tmp_file_path)
    
    @classmethod
    def tearDownClass(cls):
        # Optionally delete the test bucket and its contents
        objects = cls.client.list_objects(cls.test_bucket)
        for obj in objects:
            cls.client.s3.delete_object(Bucket=cls.test_bucket, Key=obj)
        cls.client.s3.delete_bucket(Bucket=cls.test_bucket)


if __name__ == '__main__':
    unittest.main()
