import os
from dotenv import load_dotenv, find_dotenv
from pyspark.sql import SparkSession
import boto3

load_dotenv(find_dotenv())

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
BUCKET_NAME = os.environ.get("MINIO_BUCKET_NAME")
POSTGRES_URL = os.environ.get("POSTGRES_URL")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")

spark = SparkSession.builder \
    .appName("CSV to Postgres from MinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

response = s3.list_objects_v2(Bucket=BUCKET_NAME)
csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

for csv_file in csv_files:
    print(f"Обработка файла: {csv_file}")

    s3a_path = f"s3a://{BUCKET_NAME}/{csv_file}"

    table_name = os.path.splitext(os.path.basename(csv_file))[0]

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(s3a_path)

    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print(f"Таблица '{table_name}' создана в PostgreSQL.")

spark.stop()
