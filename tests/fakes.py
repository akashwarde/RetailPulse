"""Small in-memory MinIO substitute that stores real Parquet bytes."""
import io
from minio.error import S3Error


class Lake:
    def __init__(self):
        self.objects = {}
        self.writes = 0

    def put_object(self, bucket, name, data, length, **kwargs):
        self.objects[bucket, name] = data.read(length)
        self.writes += 1

    def stat_object(self, bucket, name):
        if (bucket, name) not in self.objects:
            raise S3Error("NoSuchKey", "missing", name, "test", "test", None)

    def get_object(self, bucket, name):
        self.stat_object(bucket, name)
        response = io.BytesIO(self.objects[bucket, name])
        response.release_conn = lambda: None
        return response
