from pyspark.sql import DataFrame
from config import MONGO_URI, MONGO_DB, CHECKPOINT_BASE


def mongo_sink(
    df: DataFrame,
    collection: str,
    checkpoint_suffix: str,
    output_mode: str = "append",
):
    def write_batch(batch_df: DataFrame, _epoch_id: int):
        if batch_df.count() == 0:
            return
        (
            batch_df.write.format("mongodb")
            .mode("append")
            .option("connection.uri", MONGO_URI)
            .option("database", MONGO_DB)
            .option("collection", collection)
            .save()
        )

    print(
        f"🚀 Pokrećem streaming query za kolekciju '{collection}' (checkpoint: '{checkpoint_suffix}')"
    )

    return (
        df.writeStream.foreachBatch(write_batch)
        .outputMode(output_mode)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_suffix}")
        .trigger(processingTime="10 seconds")
        .start()
    )
