#!/usr/bin/env bash
# Create the HDFS directories used by the pipeline.
set -euo pipefail
for d in /data/flights/raw /models /output/streaming_predictions /checkpoints/streaming /tmp; do
  docker exec hdfs-namenode hdfs dfs -mkdir -p "$d"
done
docker exec hdfs-namenode hdfs dfs -chmod -R 777 /data/flights /models /output/streaming_predictions /checkpoints/streaming /tmp
docker exec hdfs-namenode hdfs dfs -ls /
