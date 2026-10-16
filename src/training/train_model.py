"""Train a Gradient Boosted Trees delay classifier with Spark MLlib and save it to HDFS."""
import argparse
import json
import sys

sys.path.insert(0, "/opt/spark-apps/src")
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession, functions as F

from common import CATEGORICAL, CLEANED_PATH, MODEL_PATH, NUMERIC


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=CLEANED_PATH)
    p.add_argument("--model-path", default=MODEL_PATH)
    p.add_argument("--max-iter", type=int, default=30)
    args = p.parse_args()

    spark = SparkSession.builder.appName("TrainGBT").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.input).select(*CATEGORICAL, *NUMERIC, "label")
    train, test = df.randomSplit([0.8, 0.2], seed=42)

    # Class weighting: delayed flights are the minority class
    delayed = train.filter("label = 1").count()
    total = train.count()
    if delayed == 0 or delayed == total:
        raise ValueError("Training requires both delayed and on-time flights")
    max_bins = max(32, *(train.select(c).distinct().count() + 1 for c in CATEGORICAL))
    w_pos = (total - delayed) / delayed
    train = train.withColumn("weight", F.when(F.col("label") == 1, w_pos).otherwise(1.0))
    print(f"Train rows: {total:,} | delayed weight: {w_pos:.2f}")

    # maxBins must cover the number of distinct airports (~350) for categorical splits
    stages = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]
    stages.append(VectorAssembler(inputCols=[f"{c}_idx" for c in CATEGORICAL] + NUMERIC,
                                  outputCol="features", handleInvalid="keep"))
    stages.append(GBTClassifier(labelCol="label", featuresCol="features", weightCol="weight",
                                maxIter=args.max_iter, maxDepth=5, maxBins=max_bins, seed=42))
    model = Pipeline(stages=stages).fit(train)

    pred = model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC").evaluate(pred)
    ev = MulticlassClassificationEvaluator(labelCol="label")
    metrics = {"auc": auc, "train_rows": total, "test_rows": test.count()}
    for m in ["accuracy", "f1", "weightedPrecision", "weightedRecall"]:
        metrics[m] = ev.setMetricName(m).evaluate(pred)
        print(f"{m}: {metrics[m]:.4f}")
    print(f"AUC: {auc:.4f}")

    model.write().overwrite().save(args.model_path)
    spark.createDataFrame([(json.dumps(metrics),)], ["value"]).coalesce(1).write.mode("overwrite").text(args.model_path + "_metrics")
    print(f"Model saved to {args.model_path}")
    spark.stop()


if __name__ == "__main__":
    main()
