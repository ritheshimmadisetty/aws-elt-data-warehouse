import boto3
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# List all buckets
response = s3.list_buckets()
print("Your S3 buckets:")
for bucket in response['Buckets']:
    print(f"  - {bucket['Name']}")

# Test uploading a small file
test_content = "Hello from Rithesh's data pipeline!"
s3.put_object(
    Bucket=os.getenv('S3_BUCKET_NAME'),
    Key='test/connection_test.txt',
    Body=test_content
)
print(f"\n✅ Successfully uploaded test file to S3!")