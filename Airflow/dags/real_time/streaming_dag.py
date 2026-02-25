from datetime import datetime
from airflow.sdk import dag, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
    dag_id="youtube_streaming_start",
    description="DAG for starting YouTube Trending Spark Structured Streaming job",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    max_active_tasks=1,
)
def start_streaming():
    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    MONGO_URI = Variable.get("MONGO_URI")
    KAFKA_BOOTSTRAP_SERVERS = Variable.get("KAFKA_BOOTSTRAP_SERVERS")

    start_stream = SparkSubmitOperator(
        task_id="start_youtube_streaming",
        application="/opt/airflow/files/spark/realtime/main.py",
        conn_id="SPARK_CONNECTION",
        conf={
            "spark.jars": (
                "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
                "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
                "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
                "/opt/spark/jars/bson-4.8.2.jar,"
                "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar,"
            ),
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
        },
        verbose=True,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            KAFKA_BOOTSTRAP_SERVERS,
            MONGO_URI,
            "youtube_trending",
        ],
    )

    start_stream


start_streaming()
