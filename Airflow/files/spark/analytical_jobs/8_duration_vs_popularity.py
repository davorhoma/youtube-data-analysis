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

    # Binning: kratki (<4 min), srednji (4-20 min), dugi (>20 min)
    df_binned = df.withColumn(
        "duration_bin",
        when(col("duration_seconds") < 240, "kratki")
        .when((col("duration_seconds") >= 240) & (col("duration_seconds") <= 1200), "srednji")
        .otherwise("dugi")
    )

    avg_views_per_duration = (
        df_binned.groupBy("duration_bin")
        .agg(F.avg("view_count").alias("avg_views"))
        .orderBy("duration_bin")
    )

    avg_views_per_duration.show(truncate=False)

    # Save results to MongoDB
    (
        avg_views_per_duration.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()