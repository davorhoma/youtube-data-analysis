from datetime import datetime

# from airflow.sdk import dag, task
from airflow.decorators import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook

@dag(
    dag_id="load_raw_data",
    description="DAG for loading raw data to HDFS",
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    catchup=False,
)

def load_raw_data():

    @task
    def load_data():
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        local_source = "files/data/all_regions_youtube_trending_data.csv"
        hdfs_destination = "/raw_data/all_regions_youtube_trending_data.csv"
        hdfs_hook.load_file(
            source=local_source, destination=hdfs_destination, overwrite=True
        )

    @task
    def check_hdfs_file(file_path):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        
        if hdfs_hook.check_for_path(hdfs_path=file_path):
            print(f"File {file_path} found in HDFS.")
        else:
            print(f"File {file_path} not found in HDFS.")

    file_path = "/raw_data/all_regions_youtube_trending_data.csv"

    load = load_data()
    check = check_hdfs_file(file_path)

    load >> check

load_raw_data()