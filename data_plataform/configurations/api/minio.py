import io

from minio import Minio
from minio.error import S3Error

from .settings import settings

minio_client = Minio(
    endpoint=settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY,
    secure=settings.MINIO_SECURE,
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

    except S3Error as e:
        print(f"MinIO write error: {e}")
        return False
