from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("duration_vs_popularity").getOrCreate()

    df = spark.read.parquet(input_file_path)

    df_with_length = df.withColumn(
        "duration_length",
        when(col("duration_seconds") < 240, "short (< 4 min)")
        .when((col("duration_seconds") >= 240) & (col("duration_seconds") <= 1200), "medium (4-20 min)")
        .otherwise("long (> 20 min)")
    )

    avg_views_per_duration = (
        df_with_length.groupBy("duration_length")
        .agg(F.avg("view_count").alias("average_views"))
        .orderBy("duration_length")
    )

    avg_views_per_duration.show(truncate=False)

    (
        avg_views_per_duration.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()