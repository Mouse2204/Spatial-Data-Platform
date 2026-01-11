from minio import Minio
from minio.error import S3Error

def main():
    client = Minio(
        "localhost:9000",
        access_key="becadata_admin",
        secret_key="becadata_storage_secret",
        secure=False
    )

    buckets = ["becadata-geo", "becadata-standard"]

    for bucket_name in buckets:
        try:
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
                print(f"Created: {bucket_name}")
            else:
                print(f"Exists: {bucket_name}")
        except S3Error as exc:
            print(f"Error: {exc}")

if __name__ == "__main__":
    main()