# MinIO client for loading files into the KBase Lakehouse Object Store
import boto3
import logging
import os
import json
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

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

    def upload_file(self, bucket_name, object_name, file_path, metadata=None, checksum_algorithm=None):
        extra_args = {}
        if metadata:
            extra_args['Metadata'] = metadata
        if checksum_algorithm:
            extra_args['ChecksumAlgorithm'] = checksum_algorithm
        self.s3.upload_file(file_path, bucket_name, object_name,
                            ExtraArgs=extra_args if extra_args else None)

    def download_file(self, bucket_name, object_name, file_path):
        self.s3.download_file(bucket_name, object_name, file_path)

    def update_metadata(self, bucket_name, object_name, metadata):
        """Update user metadata on an existing object via a server-side copy.

        Copies the object to itself with MetadataDirective='REPLACE' so only
        the metadata changes — no bytes are transferred over the network.
        Returns True on success, False if the operation fails.
        """
        try:
            self.s3.copy_object(
                Bucket=bucket_name,
                Key=object_name,
                CopySource={'Bucket': bucket_name, 'Key': object_name},
                Metadata=metadata,
                MetadataDirective='REPLACE',
            )
            return True
        except Exception:
            return False

    def stat_object(self, bucket_name, object_name):
        """Return object info via a single HEAD request.

        Returns {'size': int, 'md5': str|None, 'crc64nvme': str|None}.
        'md5' comes from user-metadata. 'crc64nvme' comes from the S3-native
        checksum (base64-encoded), retrieved via ChecksumMode='ENABLED'.
        Returns None if the object does not exist.
        """
        try:
            response = self.s3.head_object(
                Bucket=bucket_name, Key=object_name, ChecksumMode='ENABLED'
            )
            return {
                'size': response.get('ContentLength'),
                'md5': response.get('Metadata', {}).get('md5'),
                'crc64nvme': response.get('ChecksumCRC64NVME'),
            }
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ('404', 'NoSuchKey'):
                return None
            # ChecksumMode may not be supported; retry without it
            logger.warning(
                "head_object with ChecksumMode failed for %s/%s (%s), retrying without",
                bucket_name, object_name, error_code,
            )
            try:
                response = self.s3.head_object(
                    Bucket=bucket_name, Key=object_name
                )
                return {
                    'size': response.get('ContentLength'),
                    'md5': response.get('Metadata', {}).get('md5'),
                    'crc64nvme': None,
                }
            except ClientError as e2:
                error_code2 = e2.response.get('Error', {}).get('Code', '')
                if error_code2 in ('404', 'NoSuchKey'):
                    return None
                logger.error(
                    "head_object failed for %s/%s: %s", bucket_name, object_name, e2
                )
                raise

    def list_objects(self, bucket_name, prefix=''):
        response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    
    def list_buckets(self):
        response = self.s3.list_buckets()
        return [bucket['Name'] for bucket in response.get('Buckets', [])]
    
    def put_json_object(self, bucket_name, object_name, data, checksum_algorithm=None):
        """Upload a JSON object to MinIO."""
        json_bytes = json.dumps(data, indent=2).encode('utf-8')
        kwargs = {
            'Bucket': bucket_name,
            'Key': object_name,
            'Body': json_bytes,
            'ContentType': 'application/json',
        }
        if checksum_algorithm:
            kwargs['ChecksumAlgorithm'] = checksum_algorithm
        self.s3.put_object(**kwargs)
    
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
    
