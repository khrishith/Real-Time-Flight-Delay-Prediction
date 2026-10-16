"""Spark Structured Streaming: read flight events from Kafka, score with the trained GBT, write Parquet to HDFS."""
import argparse
import sys

sys.path.insert(0, "/opt/spark-apps/src")
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from common import CHECKPOINT_PATH, KAFKA_TOPIC, MODEL_PATH, STREAM_OUT_PATH

SCHEMA = StructType(
    [StructField(c, StringType()) for c in ("OP_UNIQUE_CARRIER", "ORIGIN", "DEST")]
    + [StructField(c, DoubleType()) for c in
       ("DAY_OF_WEEK", "CRS_DEP_TIME", "DEP_DELAY", "CRS_ELAPSED_TIME", "DISTANCE", "MONTH", "ARR_DELAY")]
)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="kafka:9092")
    p.add_argument("--topic", default=KAFKA_TOPIC)
    p.add_argument("--model-path", default=MODEL_PATH)
    p.add_argument("--output", default=STREAM_OUT_PATH)
    p.add_argument("--checkpoint", default=CHECKPOINT_PATH)
    p.add_argument("--run-seconds", type=int, default=120, help="stop after this many seconds")
    p.add_argument("--available-now", action="store_true", help="process all queued events and exit")
    args = p.parse_args()

    spark = SparkSession.builder.appName("StreamingInference").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    model = PipelineModel.load(args.model_path)

    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic)
              .option("startingOffsets", "earliest")
              .load()
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"),
                      F.col("timestamp").alias("kafka_timestamp"))
              .select("e.*", "kafka_timestamp"))

    scored = (model.transform(events)
              .select("OP_UNIQUE_CARRIER", "ORIGIN", "DEST", "DAY_OF_WEEK", "CRS_DEP_TIME",
                      "DEP_DELAY", "CRS_ELAPSED_TIME", "DISTANCE", "MONTH", "ARR_DELAY",
                      F.col("prediction"),
                      vector_to_array("probability")[1].alias("delay_probability"),
                      "kafka_timestamp"))

    writer = (scored.writeStream.format("parquet")
             .option("path", args.output)
             .option("checkpointLocation", args.checkpoint)
             .outputMode("append"))
    if args.available_now:
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    else:
        query = writer.trigger(processingTime="5 seconds").start()
        query.awaitTermination(args.run_seconds)
        query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
