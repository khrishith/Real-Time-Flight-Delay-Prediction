"""Verify persisted data, model metrics and streaming predictions in HDFS."""
import json
from pyspark.sql import SparkSession, functions as F
from common import CLEANED_PATH, MODEL_PATH, STREAM_OUT_PATH

spark = SparkSession.builder.appName('VerifyFlightPipeline').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
clean = spark.read.parquet(CLEANED_PATH)
pred = spark.read.parquet(STREAM_OUT_PATH)
count = pred.count()
invalid = pred.filter(
    F.col('prediction').isNull() | ~F.col('prediction').isin(0.0, 1.0)
    | F.col('delay_probability').isNull() | F.isnan('delay_probability')
    | ~F.col('delay_probability').between(0.0, 1.0)
).count()
assert count >= 1000, f'Expected at least 1000 predictions; got {count}'
assert invalid == 0, f'Found {invalid} invalid predictions'
metrics = json.loads(spark.read.text(MODEL_PATH + '_metrics').first().value)
print(json.dumps({'cleaned_rows': clean.count(), 'streaming_predictions': count,
                  'invalid_predictions': invalid, 'metrics': metrics}, indent=2))
spark.stop()
