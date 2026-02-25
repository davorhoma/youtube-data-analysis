from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    avg,
    split,
    explode,
    array_sort,
    expr,
)
import sys

if __name__ == "__main__":
    input_file_path = sys.argv[1]
    mongo_uri = sys.argv[2]
    mongo_db = sys.argv[3]
    mongo_collection = sys.argv[4]
    region = (
        sys.argv[5]
        if len(sys.argv) > 5
        and sys.argv[5]
        in ["MX", "RU", "FR", "GB", "DE", "KR", "JP", "IN", "CA", "BR", "US"]
        else "US"
    )

    spark = SparkSession.builder.appName(
        "tag_combinations_trending_duration"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(input_file_path)

    df = df.filter(col("region_code") == "US")

    trending_days = df.groupBy("video_id").agg(
        countDistinct("trending_date").alias("trending_days")
    )

    videos = df.select("video_id", "tags").dropDuplicates(["video_id"])

    videos = videos.join(trending_days, on="video_id", how="inner")

    videos = videos.withColumn("tag_array", split(col("tags"), "\\|"))

    videos = videos.withColumn("tag_array", array_sort(col("tag_array")))
    videos = videos.withColumn(
        "tag_pairs",
        expr("""
            transform(
                sequence(0, size(tag_array) - 2),
                i -> transform(
                    slice(tag_array, i + 2, size(tag_array)),
                    x -> concat(tag_array[i], '|', x)
                )
            )
        """),
    )

    exploded = videos.withColumn("tag_pair", explode(expr("flatten(tag_pairs)")))

    result = (
        exploded.groupBy("tag_pair")
        .agg(
            avg("trending_days").alias("average_trending_days"),
            count("*").alias("video_count"),
        )
        .filter(col("video_count") >= 20)  # filtriranje retkih kombinacija
        .orderBy(col("average_trending_days").desc())
        .limit(20)
    )

    result.show(truncate=False)

    (
        result.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", mongo_collection)
        .save()
    )

    spark.stop()
