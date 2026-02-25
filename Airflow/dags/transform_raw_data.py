from datetime import datetime

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
    dag_id="transform_raw_data",
    description="DAG for transforming raw data",
    start_date=datetime(2026, 1, 1),
    catchup=False
)

def transform_raw_data():
    prepare_initial_data = SparkSubmitOperator(
        task_id="prepare_initial_data",
        application="/opt/airflow/files/spark/transformation_jobs/prepare_initial_data.py",
        conn_id="SPARK_CONNECTION",
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/raw_data/all_regions_youtube_trending_data.csv",
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_data",
        ],
    )

    (
        prepare_initial_data
    )

transform_raw_data()
