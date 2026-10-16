"""Read the BTS January 2024 CSV, clean it, and write Parquet to HDFS."""
import argparse
import sys

sys.path.insert(0, "/opt/spark-apps/src")
from pyspark.sql import SparkSession, functions as F

from common import CLEANED_PATH, DELAY_THRESHOLD, FEATURES

COLUMN_MAP = {
    "Reporting_Airline": "OP_UNIQUE_CARRIER", "Origin": "ORIGIN", "Dest": "DEST",
    "DayOfWeek": "DAY_OF_WEEK", "CRSDepTime": "CRS_DEP_TIME", "DepDelay": "DEP_DELAY",
    "CRSElapsedTime": "CRS_ELAPSED_TIME", "Distance": "DISTANCE", "Month": "MONTH",
    "ArrDelay": "ARR_DELAY", "Cancelled": "CANCELLED", "Diverted": "DIVERTED",
    "OP_CARRIER": "OP_UNIQUE_CARRIER",
}
NUMERIC = ["DAY_OF_WEEK", "CRS_DEP_TIME", "DEP_DELAY", "CRS_ELAPSED_TIME", "DISTANCE", "MONTH", "ARR_DELAY"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="BTS CSV path visible to Spark")
    p.add_argument("--output", default=CLEANED_PATH)
    args = p.parse_args()

    spark = SparkSession.builder.appName("IngestBTS").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.option("header", "true").csv(args.input)
    for old, new in COLUMN_MAP.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    raw = df.count()
    for flag in ("CANCELLED", "DIVERTED"):
        if flag in df.columns:
            df = df.filter(F.col(flag).cast("double") == 0)
    for c in NUMERIC:
        df = df.withColumn(c, F.col(c).cast("double"))
    # Drop cancelled/diverted flights (no arrival delay) and rows missing any feature
    df = df.filter(F.col("ARR_DELAY").isNotNull()).dropna(subset=FEATURES)
    df = df.withColumn("label", (F.col("ARR_DELAY") >= DELAY_THRESHOLD).cast("int"))
    df = df.select(*FEATURES, "ARR_DELAY", "label")

    if not df.take(1):
        raise ValueError("No valid flights remain after cleaning")
    df.write.mode("overwrite").parquet(args.output)
    clean = df.count()
    delayed = df.filter("label = 1").count()
    print(f"Raw records: {raw:,} | Cleaned: {clean:,} | Delayed: {delayed:,} ({100*delayed/clean:.1f}%)")
    print(f"Written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
