from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, count
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("regional_engagement").getOrCreate()

    df = spark.read.parquet(input_file_path)

    avg_engagement_per_channel = (
        df.groupBy("region_code")
        .agg(F.avg("engagement").alias("avg_engagement"))
        .orderBy(F.desc("avg_engagement"))
        .limit(20)
    )

    avg_engagement_per_channel.show(truncate=False)

    (
        avg_engagement_per_channel.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
