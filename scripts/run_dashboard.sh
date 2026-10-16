#!/usr/bin/env bash
# Serve the trained Spark GBT model at http://localhost:5050.
# Add --detach to keep the dashboard running in the container.
set -euo pipefail
if [[ "${1:-}" == "--detach" ]]; then
  exec docker exec -d spark-master sh -c 'exec /opt/spark/bin/spark-submit --master "local[2]" --driver-memory 1g --conf spark.ui.port=4041 /opt/spark-apps/src/dashboard/app.py > /tmp/flight-dashboard.log 2>&1'
fi
exec docker exec spark-master /opt/spark/bin/spark-submit --master 'local[2]' \
  --driver-memory 1g --conf spark.ui.port=4041 /opt/spark-apps/src/dashboard/app.py
