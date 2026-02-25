from datetime import datetime

from airflow.sdk import dag, Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk.bases.hook import BaseHook


@dag(
    dag_id="analytical_queries",
    description="DAG for analytical queries",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=2,
)
def analyse():
    HDFS_DEFAULT_FS = Variable.get("HDFS_DEFAULT_FS")
    MONGO_URI = Variable.get("MONGO_URI")

    prepare_initial_data = SparkSubmitOperator(
        task_id="prepare_initial_data",
        application="/opt/airflow/files/spark/transformation_jobs/prepare_initial_data.py",
        conn_id="SPARK_CONNECTION",
        application_args=[
            "{{ var.value.HDFS_DEFAULT_FS }}/raw_data/all_regions_youtube_trending_data.csv",
            "{{ var.value.HDFS_DEFAULT_FS }}/transformed_data",
        ],
    )

    spark_jars = {
        "spark.jars": (
            "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
            "/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
            "/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
            "/opt/spark/jars/bson-4.8.2.jar"
        ),
        "spark.executor.extraClassPath": "/opt/spark/jars/*",
        "spark.driver.extraClassPath": "/opt/spark/jars/*",
    }

    top_categories = SparkSubmitOperator(
        task_id="top_categories",
        application="/opt/airflow/files/spark/analytical_jobs/top_categories.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "top_categories",
        ],
    )

    top_engagement = SparkSubmitOperator(
        task_id="top_engagement",
        application="/opt/airflow/files/spark/analytical_jobs/top_engagement.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "top_engagement",
        ],
    )

    popularity_over_time = SparkSubmitOperator(
        task_id="popularity_over_time",
        application="/opt/airflow/files/spark/analytical_jobs/1_popularity_over_time.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "popularity_over_time",
        ],
    )

    view_count_change_per_day = SparkSubmitOperator(
        task_id="view_count_change_per_day",
        application="/opt/airflow/files/spark/analytical_jobs/4_view_count_change_per_day.py",
        conn_id="SPARK_CONNECTION",
        conf=spark_jars,
        verbose=True,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "view_count_change_per_day",
        ],
    )

    fastest_growth_videos = SparkSubmitOperator(
        task_id="fastest_growth_videos",
        application="/opt/airflow/files/spark/analytical_jobs/5_fastest_growth_videos.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "fastest_growth_videos",
        ],
    )

    engagement_by_region = SparkSubmitOperator(
        task_id="engagement_by_region",
        application="/opt/airflow/files/spark/analytical_jobs/6_engagement_by_region.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "engagement_by_region",
        ],
    )

    top_tags = SparkSubmitOperator(
        task_id="top_tags",
        application="/opt/airflow/files/spark/analytical_jobs/7_top_tags.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "top_tags",
        ],
    )

    top_tag_combinations = SparkSubmitOperator(
        task_id="top_tag_combinations",
        application="/opt/airflow/files/spark/analytical_jobs/8_top_tag_combinations.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "top_tag_combinations",
            "US",
        ],
    )

    avg_comments_per_category = SparkSubmitOperator(
        task_id="avg_comments_per_category",
        application="/opt/airflow/files/spark/analytical_jobs/9_avg_comments_per_category.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "avg_comments_per_category",
        ],
    )

    trending_by_day_of_week = SparkSubmitOperator(
        task_id="trending_by_day_of_week",
        application="/opt/airflow/files/spark/analytical_jobs/10_trending_by_day_of_week.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "trending_by_day_of_week",
        ],
    )

    duration_vs_popularity = SparkSubmitOperator(
        task_id="duration_vs_popularity",
        application="/opt/airflow/files/spark/analytical_jobs/10_duration_vs_popularity.py",
        conn_id="SPARK_CONNECTION",
        verbose=True,
        conf=spark_jars,
        application_args=[
            f"{HDFS_DEFAULT_FS}/transformed_data",
            MONGO_URI,
            "youtube",
            "duration_vs_popularity",
        ],
    )

    (
        prepare_initial_data
        >> [
            top_categories,
            top_engagement,
            popularity_over_time,
            view_count_change_per_day,
            fastest_growth_videos,
            engagement_by_region,
            top_tags,
            top_tag_combinations,
            avg_comments_per_category,
            trending_by_day_of_week,
            duration_vs_popularity,
        ]
    )


analyse()
