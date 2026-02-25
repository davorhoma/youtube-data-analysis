from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="load_transform_analytics",
    description="Load CSV to HDFS, transform with Spark, then run analytics and save to MongoDB",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
)
def load_transform_analytics():

    @task
    def load_to_hdfs():
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        local_source = "files/data/all_regions_youtube_trending_data.csv"
        hdfs_destination = "/raw_data/all_regions_youtube_trending_data.csv"
        hdfs_hook.load_file(
            source=local_source, destination=hdfs_destination, overwrite=True
        )

    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    MONGO_URI = Variable.get("MONGO_URI")

    prepare_initial_data = SparkSubmitOperator(
        task_id="prepare_initial_data",
        application="/opt/airflow/files/spark/transformation_jobs/prepare_initial_data.py",
        conn_id="SPARK_CONNECTION",
        application_args=[
            f"{HDFS_DEFAULT_FS}/raw_data/all_regions_youtube_trending_data.csv",
            f"{HDFS_DEFAULT_FS}/transformed_data",
        ],
    )

    top_categories = SparkSubmitOperator(
        task_id="top_categories",
        application="/opt/airflow/files/spark/analytical_jobs/top_categories.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf={
            "spark.jars": (
                "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
                "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
                "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
                "/opt/spark/jars/bson-4.8.2.jar"
            ),
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "top_categories",
        ],
    )

    load = load_to_hdfs()
    load >> prepare_initial_data >> top_categories


load_transform_analytics()
