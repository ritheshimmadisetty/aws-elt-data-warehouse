import boto3
import os
from dotenv import load_dotenv

load_dotenv()

glue = boto3.client(
    'glue',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

# List tables in our catalog
response = glue.get_tables(DatabaseName='stocks_catalog')
tables   = response['TableList']

print(f"✅ Glue Catalog — stocks_catalog database")
print(f"   Tables found: {len(tables)}\n")

for table in tables:
    print(f"Table: {table['Name']}")
    print(f"  Location: {table['StorageDescriptor']['Location']}")
    print(f"  Columns:")
    for col in table['StorageDescriptor']['Columns']:
        print(f"    - {col['Name']} ({col['Type']})")
    print()