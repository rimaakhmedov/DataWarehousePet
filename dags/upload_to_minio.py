import os
import boto3
from botocore.exceptions import ClientError


def upload_files_to_minio(
    minio_endpoint: str,
    access_key: str,
    secret_key: str,
    bucket_name: str,
    source_folder: str = 'data'
):
    s3 = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f'Bucket "{bucket_name}" уже существует.')
    except ClientError:
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f'Создан bucket: {bucket_name}')
        except ClientError as e:
            print(f'Ошибка при создании bucket: {e}')
            return

    for root, _, files in os.walk(source_folder):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, source_folder)

            try:
                s3.upload_file(file_path, bucket_name, object_name)
                print(f'Загружен файл: {file_path} → {bucket_name}/{object_name}')
            except ClientError as e:
                print(f'Ошибка при загрузке {file}: {e}')
