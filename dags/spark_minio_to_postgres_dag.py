import os
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv, find_dotenv
from upload_to_minio import upload_files_to_minio
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


load_dotenv(find_dotenv())


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="spark_etl_minio_to_postgres",
    default_args=default_args,
    description="Запуск Spark job через docker exec в spark-master",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["spark", "etl"],
) as dag:

    upload_files_to_minio = PythonOperator(
        task_id="upload_files_to_minio",
        python_callable=upload_files_to_minio,
        op_kwargs={
            'minio_endpoint': os.environ.get("MINIO_ENDPOINT"),
            'access_key': os.environ.get("MINIO_ACCESS_KEY"),
            'secret_key': os.environ.get("MINIO_SECRET_KEY"),
            'bucket_name': os.environ.get("MINIO_BUCKET_NAME"),
            'source_folder': '/opt/airflow/dags/data'
        },

    )

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            "docker exec datawarehousepet-spark-master-1 "
            "spark-submit --jars /opt/spark/jars/postgresql.jar "
            "/opt/airflow/spark_jobs/minio_to_pg_job.py"
        ),
    )

    upload_files_to_minio >> run_spark_etl
