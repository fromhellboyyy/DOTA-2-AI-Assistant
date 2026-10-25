import os

import boto3
from botocore.exceptions import ClientError

ENDPOINT = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
KEY = os.getenv("AWS_ACCESS_KEY_ID", "minio")
SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("MLFLOW_S3_BUCKET", "mlflow")


def main() -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=KEY,
        aws_secret_access_key=SECRET,
        region_name=REGION,
    )
    existing = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    print("buckets:", existing)
    if BUCKET in existing:
        print(f"bucket {BUCKET} already exists")
        return
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(
                Bucket=BUCKET,
                CreateBucketConfiguration={"LocationConstraint": REGION},
            )
        print(f"created bucket {BUCKET}")
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists", "409"}:
            print(f"bucket {BUCKET} already exists")
        else:
            raise


if __name__ == "__main__":
    main()