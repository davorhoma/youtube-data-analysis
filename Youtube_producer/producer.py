import os

import pandas as pd
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

TOPIC_NAME = os.getenv("TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}]")

CSV_FILE = "youtube_realtime_data.csv"
df = pd.read_csv(CSV_FILE)

BATCH_SIZE = int(os.getenv("BATCH_SIZE"))  # 50 video zapisa * 11 regiona
INTERVAL_SECONDS = int(os.getenv("INTERVAL_SECONDS"))  # koliko često šaljemo batch

num_rows = len(df)
i = 0

while True:
    batch_df = df.iloc[i:i+BATCH_SIZE].copy()

    if batch_df.empty:
        break

    now_iso = datetime.now(timezone.utc).isoformat()
    batch_df['collected_at'] = now_iso

    for _, row in batch_df.iterrows():
        message_value = row.to_json()
        producer.produce(
            TOPIC_NAME,
            key=row['video_id'],
            value=message_value,
            callback=delivery_report
        )

    producer.flush()
    print(f"Sent batch of {len(batch_df)} records at {now_iso}")

    i += BATCH_SIZE

    time.sleep(INTERVAL_SECONDS)