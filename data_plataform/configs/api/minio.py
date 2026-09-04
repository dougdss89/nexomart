import io

import urllib3
from minio import Minio
from minio.error import S3Error

from .settings import settings

# No http_client meant the SDK's default urllib3.PoolManager, which has no read/connect
# timeout — a stalled TCP connection during put_object() (seen live 2026-09-04, ingest_sales
# hung 44+ min with Postgres-side fetches still cycling normally) blocks forever with no
# exception ever raised, so ingestion_common.py's retry-with-backoff around write_on_minio()
# never even gets a chance to run. Explicit timeouts turn that hang into a real exception.
_minio_http_client = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=10, read=60),
    retries=urllib3.Retry(total=0),  # our own caller retries with backoff; don't double up
)

minio_client = Minio(
    endpoint=settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_SECURE,
    http_client=_minio_http_client,
)


def get_minio_client() -> Minio:
    return minio_client


def write_on_minio(bucket_name: str, object_name: str, data: bytes) -> bool:
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created")

        data_stream = io.BytesIO(data)
        minio_client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_stream,
            length=len(data),
        )
        print(f"Object '{object_name}' uploaded to '{bucket_name}'")
        return True

    except (S3Error, urllib3.exceptions.HTTPError, OSError) as e:
        # S3Error: a real error response from MinIO. HTTPError/OSError: the connection-level
        # failures the timeout above now actually raises instead of hanging indefinitely.
        print(f"MinIO write error: {e}")
        return False
