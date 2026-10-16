"""Web dashboard + prediction API. Loads the trained Spark GBT pipeline and scores a single flight."""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, "/opt/spark-apps/src")
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

from common import MODEL_PATH

STRING_FIELDS = ["OP_UNIQUE_CARRIER", "ORIGIN", "DEST"]
NUM_FIELDS = ["DAY_OF_WEEK", "CRS_DEP_TIME", "DEP_DELAY", "CRS_ELAPSED_TIME", "DISTANCE", "MONTH"]
HERE = Path(__file__).parent

spark = SparkSession.builder.master("local[2]").appName("Dashboard").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
model = PipelineModel.load(MODEL_PATH)


def predict(payload):
    row = {f: str(payload[f]).strip().upper() for f in STRING_FIELDS}
    row.update({f: float(payload[f]) for f in NUM_FIELDS})
    out = model.transform(spark.createDataFrame([row])).collect()[0]
    p = out["probability"]
    return {"prediction": int(out["prediction"]),
            "label": "Delayed (15+ min)" if out["prediction"] == 1 else "On time",
            "probability_on_time": float(p[0]), "probability_delayed": float(p[1])}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/meta.json":
            return self._send(200, (HERE / "meta.json").read_bytes())
        self._send(200, (HERE / "index.html").read_bytes(), "text/html; charset=utf-8")

    def do_POST(self):
        if self.path != "/predict":
            return self._send(404, b"{}")
        try:
            data = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self._send(200, json.dumps(predict(data)).encode())
        except Exception as exc:
            self._send(400, json.dumps({"error": str(exc)}).encode())


if __name__ == "__main__":
    print("Dashboard on http://localhost:5000")
    ThreadingHTTPServer(("0.0.0.0", 5000), Handler).serve_forever()
