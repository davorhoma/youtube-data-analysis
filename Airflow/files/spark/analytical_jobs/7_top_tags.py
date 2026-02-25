from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, explode, split
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]

    spark = SparkSession.builder.appName("top_tags").getOrCreate()

    df = spark.read.parquet(input_file_path)

    tags_count = (
        df
        .withColumn("tag", explode(split(col("tags"), r"\|")))
        .groupBy("tag")
        .agg(F.count("*").alias("number_of_videos"))
        .orderBy(F.desc("number_of_videos"))
        .limit(20)
    )

    tags_count.show(truncate=False)

    (
        tags_count.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()