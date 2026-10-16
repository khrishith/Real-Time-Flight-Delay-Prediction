#!/usr/bin/env bash
# BTS Data -> HDFS -> Spark -> GBT Model -> Kafka -> Spark Streaming -> HDFS
# Usage: bash scripts/run_pipeline.sh data/On_Time_Reporting_Carrier_On_Time_Performance_2024_1.csv
set -euo pipefail

cd "$(dirname "$0")/.."
CSV="${1:?Usage: $0 <path to January 2024 BTS CSV inside ./data>}"
CSV_NAME="$(basename "$CSV")"
[[ -f "$CSV" ]] || { echo "CSV not found: $CSV" >&2; exit 1; }
PYTHON="${PYTHON:-.venv/bin/python}"
KAFKA_PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
SUBMIT="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy --conf spark.cores.max=4 --conf spark.sql.shuffle.partitions=8 --executor-memory 1g --driver-memory 2g"

echo "[1/5] Starting HDFS, Spark and Kafka"
docker compose build spark-master
docker compose up -d --wait --wait-timeout 300 hdfs-namenode hdfs-datanode spark-master spark-worker-1 spark-worker-2 kafka
docker compose run --rm --no-deps kafka-init
docker exec hdfs-namenode hdfs dfsadmin -safemode wait
bash scripts/setup_hdfs.sh

echo "[2/5] Upload raw CSV to HDFS, then clean with Spark"
docker exec hdfs-namenode hdfs dfs -put -f "/data-import/$CSV_NAME" "/data/flights/raw/$CSV_NAME"
$SUBMIT /opt/spark-apps/src/ingestion/ingest_bts_to_hdfs.py --input "hdfs://hdfs-namenode:9000/data/flights/raw/$CSV_NAME"

echo "[3/5] Train GBT model with Spark MLlib -> HDFS"
$SUBMIT /opt/spark-apps/src/training/train_model.py

echo "[4/5] Publish 1,000 flight events to Kafka"
"$PYTHON" src/streaming/kafka_producer.py --input "$CSV" --num-events 1000

echo "[5/5] Score queued events using Spark Structured Streaming"
$SUBMIT --packages "$KAFKA_PKG" /opt/spark-apps/src/streaming/streaming_consumer.py --available-now

echo "Streaming predictions in HDFS:"
docker exec hdfs-namenode hdfs dfs -ls /output/streaming_predictions
$SUBMIT /opt/spark-apps/src/verify_pipeline.py
echo "Start the dashboard with: bash scripts/run_dashboard.sh"
