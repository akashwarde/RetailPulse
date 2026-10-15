
#!/bin/sh
set -eu

BUCKET_NAME="${MINIO_BUCKET:-retailpulse}"

echo "Waiting for MinIO..."
until mc alias set local \
  http://minio:9000 \
  "$MINIO_ROOT_USER" \
  "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  sleep 2
done

if mc ls "local/$BUCKET_NAME" >/dev/null 2>&1; then
  echo "Bucket '$BUCKET_NAME' already exists."
else
  mc mb "local/$BUCKET_NAME"
  echo "Bucket '$BUCKET_NAME' created."
fi
