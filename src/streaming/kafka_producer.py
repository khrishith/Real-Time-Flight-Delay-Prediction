"""Publish sampled flight records from the BTS CSV to Kafka to simulate live events."""
import argparse
import csv
import json
import random
import time
from pathlib import Path

from kafka import KafkaProducer

TOPIC = "flight-events"
COLUMN_MAP = {
    "Reporting_Airline": "OP_UNIQUE_CARRIER", "Origin": "ORIGIN", "Dest": "DEST",
    "DayOfWeek": "DAY_OF_WEEK", "CRSDepTime": "CRS_DEP_TIME", "DepDelay": "DEP_DELAY",
    "CRSElapsedTime": "CRS_ELAPSED_TIME", "Distance": "DISTANCE", "Month": "MONTH",
    "ArrDelay": "ARR_DELAY",
}
NUMERIC = ["DAY_OF_WEEK", "CRS_DEP_TIME", "DEP_DELAY", "CRS_ELAPSED_TIME", "DISTANCE", "MONTH", "ARR_DELAY"]


def load_valid_rows(path):
    rows = []
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            rec = {new: r.get(new, r.get(old, "")) for old, new in COLUMN_MAP.items()}
            rec["OP_UNIQUE_CARRIER"] = rec["OP_UNIQUE_CARRIER"] or r.get("OP_CARRIER", "")
            if any(r.get(c, r.get(c.title(), "0")) in ("1", "1.0", "1.00") for c in ("CANCELLED", "DIVERTED")):
                continue
            if any(rec[c] == "" for c in NUMERIC) or not all(rec[c] for c in ("OP_UNIQUE_CARRIER", "ORIGIN", "DEST")):
                continue  # cancelled / diverted / incomplete
            for c in NUMERIC:
                rec[c] = float(rec[c])
            rows.append(rec)
    return rows


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="BTS CSV file")
    p.add_argument("--bootstrap", default="localhost:9093")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--num-events", type=int, default=1000)
    p.add_argument("--rate", type=float, default=100, help="events per second")
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    rows = load_valid_rows(Path(args.input))
    if len(rows) < args.num_events:
        raise ValueError(f"Need {args.num_events} valid records; found {len(rows)}")
    sample = random.Random(args.seed).sample(rows, min(args.num_events, len(rows)))
    producer = KafkaProducer(bootstrap_servers=args.bootstrap,
                             value_serializer=lambda v: json.dumps(v).encode())
    deliveries = []
    for i, rec in enumerate(sample, 1):
        deliveries.append(producer.send(args.topic, rec))
        time.sleep(1.0 / args.rate)
    producer.flush()
    for delivery in deliveries:
        delivery.get(timeout=30)
    producer.close()
    print(f"Published {len(sample)} events to topic '{args.topic}'")


if __name__ == "__main__":
    main()
