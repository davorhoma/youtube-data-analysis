from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from sinks import mongo_sink
from config import MONGO_DB, CHECKPOINT_BASE, MONGO_URI


def duration_distribution(parsed: DataFrame) -> DataFrame:
    dd_p = (
        parsed.groupBy(F.window("event_time", "30 seconds"))
        .agg(
            F.sum(F.when(F.col("duration_sec") < 240, 1).otherwise(0)).alias(
                "short_videos_count (< 4 min)"
            ),
            F.sum(
                F.when(
                    (F.col("duration_sec") >= 240) & (F.col("duration_sec") < 1200), 1
                ).otherwise(0)
            ).alias("medium_videos_count (4-20 min)"),
            F.sum(F.when(F.col("duration_sec") >= 1200, 1).otherwise(0)).alias(
                "long_videos_count (>= 20 min)"
            ),
        )
        .withColumn("window_start", F.col("window.start").cast("string"))
        .withColumn("window_end", F.col("window.end").cast("string"))
        .drop("window")
    )
    return mongo_sink(
        dd_p, "duration_distribution", "p6_duration_dist", output_mode="update"
    )


def channel_regional_reach(parsed: DataFrame) -> DataFrame:
    crr_agg = (
        parsed.groupBy(
            F.window("event_time", "30 seconds"),
            "channel_id",
            "channel_title",
        )
        .agg(
            F.approx_count_distinct("region_code").alias("regional_reach"),
        )
        .withColumn("window_start", F.col("window.start").cast("string"))
        .withColumn("window_end", F.col("window.end").cast("string"))
        .drop("window", "channel_id")
    )

    def write_crr_batch(batch_df: DataFrame, _epoch_id: int):
        if batch_df.count() == 0:
            return

        try:
            existing = (
                batch_df.sparkSession.read.format("mongodb")
                .option("connection.uri", MONGO_URI)
                .option("database", MONGO_DB)
                .option("collection", "channel_regional_reach")
                .load()
                .select("channel_title", "regional_reach")
            )
            combined = (
                batch_df.select("channel_title", "regional_reach")
                .union(existing)
                .groupBy("channel_title")
                .agg(F.max("regional_reach").alias("regional_reach"))
            )
        except Exception:
            combined = batch_df.select("channel_title", "regional_reach")

        rank_window = Window.orderBy(F.desc("regional_reach"))
        result = (
            combined.withColumn("reach_rank", F.rank().over(rank_window))
            .filter(F.col("reach_rank") <= 15)
            .select("channel_title", "regional_reach")
        )

        (
            result.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "channel_regional_reach")
            .option("upsertDocument", "true")
            .option("idFieldList", "channel_title")
            .save()
        )

    return (
        crr_agg.writeStream.foreachBatch(write_crr_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/p8_channel_reach")
        .trigger(processingTime="10 seconds")
        .start()
    )


def trending_freshness(parsed: DataFrame) -> DataFrame:
    tf_agg = (
        parsed.groupBy(
            F.window("event_time", "30 seconds"),
            "video_id",
        )
        .agg(
            F.max("view_count").alias("view_count"),
        )
        .withColumn("window_start", F.col("window.start").cast("string"))
        .drop("window")
    )

    def write_tf_batch(batch_df: DataFrame, epoch_id: int):
        if batch_df.count() == 0:
            return

        try:
            seen_df = (
                batch_df.sparkSession.read.format("mongodb")
                .option("connection.uri", MONGO_URI)
                .option("database", MONGO_DB)
                .option("collection", "seen_videos")
                .load()
                .select("video_id")
                .distinct()
            )
            has_seen = seen_df.count() > 0
        except Exception:
            has_seen = False

        if has_seen:
            fresh_df = batch_df.join(seen_df, on="video_id", how="left_anti")
        else:
            fresh_df = batch_df

        window_info = batch_df.select("window_start").distinct()

        summary = fresh_df.agg(
            F.countDistinct("video_id").alias("fresh_count")
        ).crossJoin(window_info)

        (
            summary.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "trending_freshness")
            .save()
        )

        new_seen = batch_df.select("video_id").distinct()
        (
            new_seen.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "seen_videos")
            .save()
        )

    return (
        tf_agg.writeStream.foreachBatch(write_tf_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/p11_freshness")
        .trigger(processingTime="15 seconds")
        .start()
    )


def historical_overlap(parsed: DataFrame, hist_channel_ids: DataFrame) -> DataFrame:
    ho_agg = (
        parsed.groupBy(F.window("event_time", "30 seconds"), "channel_id")
        .agg(F.max("view_count").alias("view_count"))
        .withColumn("window_start", F.col("window.start").cast("string"))
        .drop("window")
    )

    def write_batch(batch_df: DataFrame, epoch_id: int):
        if batch_df.count() == 0:
            return

        total = batch_df.select("channel_id").distinct().count()
        already_seen = (
            batch_df.join(F.broadcast(hist_channel_ids), on="channel_id", how="inner")
            .select("channel_id")
            .distinct()
            .count()
        )

        window_start = batch_df.select("window_start").first()[0]
        new_channels = total - already_seen
        overlap_pct = round(already_seen / total * 100, 2) if total > 0 else 0.0

        summary = batch_df.limit(1).select(
            F.lit(epoch_id).alias("batch_id"),
            F.lit(window_start).alias("window_start"),
            F.lit(total).alias("total_channels"),
            F.lit(already_seen).alias("hist_overlap"),
            F.lit(new_channels).alias("new_channels"),
            F.lit(overlap_pct).alias("overlap_pct"),
        )

        (
            summary.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "historical_overlap")
            .save()
        )

    return (
        ho_agg.writeStream.foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/p_hist_overlap")
        .trigger(processingTime="30 seconds")
        .start()
    )


def current_vs_historical_views(
    parsed: DataFrame, batch_stats_raw: DataFrame
) -> DataFrame:
    hist_row = batch_stats_raw.agg(
        F.max("view_count").alias("hist_max_views"),
        F.avg("view_count").alias("hist_avg_views"),
    ).first()
    hist_max = float(hist_row["hist_max_views"])
    hist_avg = round(float(hist_row["hist_avg_views"]), 2)

    p_agg = (
        parsed.groupBy(F.window("event_time", "30 seconds"))
        .agg(
            F.max("view_count").alias("current_max_views"),
            F.avg("view_count").alias("current_avg_views"),
        )
        .withColumn("window_start", F.col("window.start").cast("string"))
        .drop("window")
    )

    def write_batch(batch_df: DataFrame, epoch_id: int):
        if batch_df.count() == 0:
            return

        row = batch_df.first()

        summary = batch_df.limit(1).select(
            F.lit(row["window_start"]).alias("window_start"),
            F.lit(row["current_max_views"]).alias("current_max_views"),
            F.lit(round(row["current_avg_views"], 2)).alias("current_avg_views"),
            F.lit(hist_max).alias("hist_max_views"),
            F.lit(hist_avg).alias("hist_avg_views"),
        )

        (
            summary.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", "current_vs_historical_views")
            .save()
        )

    return (
        p_agg.writeStream.foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/p13_current_vs_hist_views")
        .trigger(processingTime="30 seconds")
        .start()
    )
