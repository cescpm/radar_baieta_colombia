import boto3
from botocore import UNSIGNED
from botocore.config import Config

bucket_name = "s3-radaresideam"

s3 = boto3.client(
    "s3",
    config=Config(signature_version=UNSIGNED)
)

years = s3.list_objects_v2(
    Bucket=bucket_name,
    Prefix='l2_data/',
    Delimiter="/"
)
print(years)
for y in years.get("CommonPrefixes", []):
    months = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=y['Prefix'],
        Delimiter="/"
    )
    print(months)
