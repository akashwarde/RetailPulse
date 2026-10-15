import os
import time

from minio import Minio

client = Minio(
    "minio:9000",
    access_key=os.environ["MINIO_ROOT_USER"],
    secret_key=os.environ["MINIO_ROOT_PASSWORD"],
    secure=False,
)

bucket = os.getenv("MINIO_BUCKET", "retailpulse")

for attempt in range(10):
    try:
        if client.bucket_exists(bucket):
            print(f"Bucket '{bucket}' already exists.", flush=True)
        else:
            client.make_bucket(bucket)
            print(f"Bucket '{bucket}' created.", flush=True)
        break
    except Exception as exc:
        print(
            f"MinIO initialization attempt {attempt + 1}/10 failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        if attempt == 9:
            raise
        time.sleep(2)
