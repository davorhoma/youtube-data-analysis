import sys

BATCH_STATS_PATH = sys.argv[1] if len(sys.argv) > 1 else "hdfs://namenode:9000"
KAFKA_BOOTSTRAP_SERVERS = sys.argv[2] if len(sys.argv) > 2 else "broker1:9092"
MONGO_URI = sys.argv[3] if len(sys.argv) > 3 else "mongodb://admin:admin@mongodb:27017"
MONGO_DB = sys.argv[4] if len(sys.argv) > 4 else "youtube_trending"
TOPIC_NAME = "youtube_stream_data"
CHECKPOINT_BASE = f"{BATCH_STATS_PATH}/spark_checkpoints"