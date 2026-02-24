from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, explode, split
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]
    top_percent = float(sys.argv[5])  # npr. 0.1 za top 10%

    spark = SparkSession.builder.appName("top_tags").getOrCreate()

    # Učitavanje podataka
    df = spark.read.parquet(input_file_path)

    # Određivanje praga za top X% po view_count
    view_threshold = df.approxQuantile("view_count", [1 - top_percent], 0.01)[0]

    top_videos = df.filter(col("view_count") >= view_threshold)

    # Parsiranje tagova i brojanje
    tags_count = (
        top_videos
        .withColumn("tag", explode(split(col("tags"), r"\|")))
        .groupBy("tag")
        .agg(F.count("*").alias("count"))
        .orderBy(F.desc("count"))
    )

    tags_count.show(truncate=False)

    # Slanje rezultata u MongoDB
    (
        tags_count.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()