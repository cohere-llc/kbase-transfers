# MinIO client for loading files into the KBase Lakehouse Object Store
import boto3
import os
import json
from botocore.exceptions import ClientError

endpoint_url = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "minioadmin"

# Load from environment variables if set
if "MINIO_ACCESS_KEY" in os.environ:
    access_key = os.environ["MINIO_ACCESS_KEY"]
if "MINIO_SECRET_KEY" in os.environ:
    secret_key = os.environ["MINIO_SECRET_KEY"]
if "MINIO_ENDPOINT_URL" in os.environ:
    endpoint_url = os.environ["MINIO_ENDPOINT_URL"]

class MinioClient:
    def __init__(self, endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def upload_file(self, bucket_name, object_name, file_path, metadata=None):
        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata
        self.s3.upload_file(file_path, bucket_name, object_name,
                            ExtraArgs=extra_args if extra_args else None)

    def download_file(self, bucket_name, object_name, file_path):
        self.s3.download_file(bucket_name, object_name, file_path)

    def stat_object(self, bucket_name, object_name):
        """Return {'size': int, 'md5': str|None} via a single HEAD request.

        'md5' comes from the user-metadata key 'md5', stored when files are
        uploaded with upload_file(metadata={'md5': <hex>}).
        Returns None if the object does not exist or the request fails.
        """
        try:
            response = self.s3.head_object(Bucket=bucket_name, Key=object_name)
            return {
                'size': response.get('ContentLength'),
                'md5': response.get('Metadata', {}).get('md5'),
            }
        except Exception:
            return None

    def list_objects(self, bucket_name, prefix=''):
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    
    def list_buckets(self):
        response = self.s3.list_buckets()
        return [bucket['Name'] for bucket in response.get('Buckets', [])]
    
    def put_json_object(self, bucket_name, object_name, data):
        """Upload a JSON object to MinIO."""
        json_bytes = json.dumps(data, indent=2).encode('utf-8')
        self.s3.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=json_bytes,
            ContentType='application/json'
        )
    
    def prefix_exists(self, bucket_name, prefix):
        """Check if a prefix (folder path) exists in the bucket."""
        try:
            response = self.s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=1
            )
            return 'Contents' in response
        except ClientError:
            return False
    
    def bucket_exists(self, bucket_name):
        """Check if a bucket exists."""
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False
    
