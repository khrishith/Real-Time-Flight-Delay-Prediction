"""Shared constants: feature schema, HDFS paths, Kafka topic."""

HDFS = "hdfs://hdfs-namenode:9000"
CLEANED_PATH = f"{HDFS}/data/flights/cleaned"
MODEL_PATH = f"{HDFS}/models/gbt_pipeline"
STREAM_OUT_PATH = f"{HDFS}/output/streaming_predictions"
CHECKPOINT_PATH = f"{HDFS}/checkpoints/streaming"

KAFKA_TOPIC = "flight-events"
DELAY_THRESHOLD = 15.0  # label = 1 when ARR_DELAY >= 15 minutes

CATEGORICAL = ["OP_UNIQUE_CARRIER", "ORIGIN", "DEST"]
NUMERIC = ["DAY_OF_WEEK", "CRS_DEP_TIME", "DEP_DELAY", "CRS_ELAPSED_TIME", "DISTANCE", "MONTH"]
FEATURES = CATEGORICAL + NUMERIC
