<div align="center">

# Real-Time Flight Delay Prediction

**Predicting whether a flight will arrive 15+ minutes late, using only HDFS, Spark and Kafka**

BTS flight data → HDFS → Spark MLlib (GBT) → Kafka → Spark Structured Streaming → HDFS

<br/>

<img src="https://img.shields.io/badge/HDFS-Hadoop%203.2.1-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=white" />
<img src="https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" />
<img src="https://img.shields.io/badge/Apache%20Kafka-3.7.0-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" />
<img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
<img src="https://img.shields.io/badge/Python-3-3776AB?style=for-the-badge&logo=python&logoColor=white" />

<br/><br/>

[About](#about) &nbsp;|&nbsp;
[How it works](#how-it-works) &nbsp;|&nbsp;
[Results](#results) &nbsp;|&nbsp;
[Reproduce the work](#reproduce-the-work-step-by-step) &nbsp;|&nbsp;
[Dashboard](#step-9--use-the-web-dashboard) &nbsp;|&nbsp;
[Troubleshooting](#troubleshooting) &nbsp;|&nbsp;
[Project structure](#project-structure)

</div>

<br/>

---

## About

This project predicts whether a US domestic flight will arrive **15 minutes or more late**. It is a Big Data pipeline that uses exactly three tools:

- **HDFS** stores the data, the trained model and the predictions.
- **Apache Spark** (MLlib and Structured Streaming) cleans the data, trains the model and scores live events.
- **Apache Kafka** carries the flight events.

The data is the January 2024 Airline On-Time Performance file from the US Bureau of Transportation Statistics (BTS): 547,271 raw records, 525,370 after cleaning. One Gradient Boosted Trees (GBT) model is trained on it. Then 1,000 simulated flight events are streamed through Kafka and scored in real time. A small web dashboard lets anyone enter a flight and see the prediction.

The project was adapted from the public reference project [aneessaheba/realtime-flight-delay-predictor-bigdata](https://github.com/aneessaheba/realtime-flight-delay-predictor-bigdata) and simplified to the three tools above.

<br/>

## How it works

```
                 BATCH LAYER
  BTS CSV ──▶ HDFS ──▶ Spark (clean, features, MLlib) ──▶ GBT model (saved to HDFS)
                                                               │
                 STREAMING LAYER                               ▼
  1,000 flight events ──▶ Kafka ──▶ Spark Structured Streaming ──▶ predictions (HDFS)
                                                               │
                                                               ▼
                                              Web dashboard (/predict API)
```

**Label.** A flight is *delayed* (1) if its arrival delay is 15 minutes or more, otherwise *on time* (0). About 24.1% of flights are delayed.

**Features (9).**

| Feature | Type |
|:--|:--|
| Airline, Origin, Destination | Categorical |
| Day of week, Month | Numeric |
| Scheduled departure time, Departure delay | Numeric |
| Scheduled elapsed time, Distance | Numeric |

**Model.** One Spark MLlib pipeline: `StringIndexer` → `VectorAssembler` → `GBTClassifier` (30 iterations, depth 5). Delayed flights are weighted about 3.15 times more during training, so the model does not simply predict "on time" for everything. The full pipeline is saved to HDFS and reused by the streaming job and the dashboard.

**Cluster (Docker).** One HDFS namenode, one HDFS datanode, one Spark master, two Spark workers and one Kafka broker.

<br/>

## Results

| Item | Value |
|:--|--:|
| Raw records | 547,271 |
| Cleaned records | 525,370 |
| Delayed flights | 126,410 (24.1%) |
| Training / test rows | 420,308 / 105,062 |

| Model metric (test split) | Value |
|:--|--:|
| Accuracy | 0.8951 |
| F1 score (weighted) | 0.8959 |
| Precision (weighted) | 0.8970 |
| Recall (weighted) | 0.8951 |
| ROC AUC | 0.9312 |
| Baseline: always predict "on time" | about 0.759 |

| Streaming check | Result |
|:--|--:|
| Events published to Kafka | 1,000 |
| Predictions stored in HDFS | 1,000 |
| Invalid predictions | 0 |
| Correct predictions | 899 of 1,000 (89.9%) |

Departure delay is by far the most important feature (about 74%), followed by origin airport (about 12%) and destination airport (about 9%). Your own numbers can differ slightly from run to run because the train/test split is random.

<br/>

---

## Reproduce the work (step by step)

Follow the steps in order. Each step says what it does and what you should see.

### Step 1 – Check the requirements

| You need | Details |
|:--|:--|
| Operating system | macOS, Windows (with WSL2) or Linux |
| Docker Desktop | Installed and **running**, with **at least 8 GB memory** (see Step 2) |
| Python | Python 3.8 or newer, only for the Kafka producer on your own machine |
| Git | To download the project |
| Disk space | About 6 GB free for Docker images, plus about 1 GB for the dataset |
| Free ports | 9000, 9870, 9092, 9093, 8080, 7077, 5050 |
| Internet | Needed for the first run to download Docker images and Spark's Kafka connector |

Check that everything is installed:

```bash
docker --version
docker compose version
python3 --version
git --version
```

Each command should print a version number.

### Step 2 – Start Docker Desktop and give it enough memory

1. Install Docker Desktop from <https://www.docker.com/products/docker-desktop> if you do not have it, then open it.
2. Wait until Docker Desktop says it is running.
3. Open **Settings → Resources** and set **Memory to at least 8 GB** (10 GB is more comfortable). Click **Apply & restart**.
4. Check that Docker works:

```bash
docker run --rm hello-world
```

You should see "Hello from Docker!".

> On Apple Silicon Macs the HDFS images run through emulation (`platform: linux/amd64` in `docker-compose.yml`). This is normal, but the first start is slower.

### Step 3 – Download the project

```bash
git clone https://github.com/khrishith/Real-Time-Flight-Delay-Prediction.git
cd Real-Time-Flight-Delay-Prediction
```

### Step 4 – Download the dataset

1. Open <https://www.transtats.bts.gov> and go to **Airline On-Time Performance Data**.
2. Choose **Reporting Carrier On-Time Performance (1987–present)**.
3. Select **Year 2024** and **Period January**.
4. Tick the option for the **prezipped file** and click **Download**.
5. Unzip the downloaded file.
6. Create a folder called `data` inside the project and move the CSV into it:

```bash
mkdir -p data
mv ~/Downloads/On_Time_Reporting_Carrier_On_Time_Performance_*_2024_1.csv data/
ls -lh data/
```

You should see a file like `On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv` of about 235 MB.

Check the number of rows (the file has 547,271 records plus one header line):

```bash
wc -l data/On_Time_Reporting_Carrier_On_Time_Performance_*_2024_1.csv
```

You should see about `547272`.

### Step 5 – Create the Python environment

Only the Kafka producer runs on your own machine, and it needs one package (`kafka-python`).

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

On Windows (WSL2) and Linux the commands are the same. Check the install:

```bash
.venv/bin/python -c "import kafka; print('kafka-python OK')"
```

### Step 6 – Run the whole pipeline (one command)

```bash
bash scripts/run_pipeline.sh "data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"
```

Keep the quotes, because the file name contains brackets. The first run builds the Spark image and downloads images, so it takes several minutes. The script runs these stages:

| Stage | What happens | What you should see |
|:--|:--|:--|
| 1 | Builds and starts HDFS, Spark (master + 2 workers) and Kafka; creates the Kafka topic `flight-events` and the HDFS folders | Containers become healthy |
| 2 | Uploads the CSV to HDFS and cleans it with Spark | Raw records `547,271`, cleaned records `525,370` |
| 3 | Trains the GBT model with Spark MLlib and saves it to HDFS | Accuracy about `0.895`, AUC about `0.931` |
| 4 | Publishes 1,000 sampled flight events to Kafka | "Published 1,000 events" |
| 5 | Spark Structured Streaming scores the events and writes predictions to HDFS | Parquet files listed in `/output/streaming_predictions` |
| End | `verify_pipeline.py` checks the results | `streaming_predictions: 1000` and `invalid_predictions: 0` |

If the last check shows 1,000 predictions and 0 invalid, the pipeline worked.

### Step 7 – (Optional) Run each stage by hand

Use this if you want to see or repeat one stage on its own. Run these from the project folder.

**7a. Start the cluster**

```bash
docker compose build spark-master
docker compose up -d --wait --wait-timeout 300 hdfs-namenode hdfs-datanode spark-master spark-worker-1 spark-worker-2 kafka
docker compose run --rm --no-deps kafka-init
docker exec hdfs-namenode hdfs dfsadmin -safemode wait
bash scripts/setup_hdfs.sh
```

**7b. Set two helper variables**

```bash
CSV="data/On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2024_1.csv"
CSV_NAME="$(basename "$CSV")"
SUBMIT="docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy --conf spark.cores.max=4 --conf spark.sql.shuffle.partitions=8 --executor-memory 1g --driver-memory 2g"
```

**7c. Upload the CSV to HDFS and clean it**

```bash
docker exec hdfs-namenode hdfs dfs -put -f "/data-import/$CSV_NAME" "/data/flights/raw/$CSV_NAME"
$SUBMIT /opt/spark-apps/src/ingestion/ingest_bts_to_hdfs.py --input "hdfs://hdfs-namenode:9000/data/flights/raw/$CSV_NAME"
```

**7d. Train the model**

```bash
$SUBMIT /opt/spark-apps/src/training/train_model.py
```

**7e. Publish 1,000 events to Kafka**

```bash
.venv/bin/python src/streaming/kafka_producer.py --input "$CSV" --num-events 1000
```

**7f. Score the events with Spark Structured Streaming**

```bash
$SUBMIT --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /opt/spark-apps/src/streaming/streaming_consumer.py --available-now
```

**7g. Verify the results**

```bash
docker exec hdfs-namenode hdfs dfs -ls /output/streaming_predictions
$SUBMIT /opt/spark-apps/src/verify_pipeline.py
```

### Step 8 – Look inside the cluster

| What | Where |
|:--|:--|
| HDFS web page | <http://localhost:9870> → Utilities → Browse the file system |
| Spark master page | <http://localhost:8080> (you should see two workers) |
| Cleaned data in HDFS | `docker exec hdfs-namenode hdfs dfs -ls /data/flights/cleaned` |
| Trained model in HDFS | `docker exec hdfs-namenode hdfs dfs -ls /models` |
| Streaming predictions | `docker exec hdfs-namenode hdfs dfs -ls /output/streaming_predictions` |
| Kafka topic | `docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic flight-events` |

### Step 9 – Use the web dashboard

Start the dashboard (Spark loads the trained model, so wait about a minute):

```bash
bash scripts/run_dashboard.sh --detach
```

Open **<http://localhost:5050>**, then:

1. Pick an airline.
2. Pick the origin and destination airports (type a city or a code).
3. Choose the month, day and scheduled departure time.
4. Slide the departure delay if the plane left late.
5. Press **Predict delay**.

The page shows whether the flight is likely on time or delayed, with both probabilities. Distance and flight time are filled in from real flights on that route.

You can also call the API directly:

```bash
curl -X POST localhost:5050/predict -d '{"OP_UNIQUE_CARRIER":"UA","ORIGIN":"ORD","DEST":"DEN","DAY_OF_WEEK":5,"CRS_DEP_TIME":1730,"DEP_DELAY":75,"CRS_ELAPSED_TIME":170,"DISTANCE":888,"MONTH":1}'
```

A flight that leaves 75 minutes late should come back as delayed with a high probability. A flight with `"DEP_DELAY":0` on a normal route usually comes back as on time.

### Step 10 – (Optional) Rebuild the dashboard's airline and airport lists

`src/dashboard/meta.json` is generated from the CSV. You only need this if you change the data:

```bash
python3 scripts/build_dashboard_meta.py "$CSV" src/dashboard/meta.json
```

### Step 11 – Stop everything

```bash
docker compose down        # stop the containers, keep the HDFS data
docker compose down -v     # stop and also delete the HDFS data
```

To run the whole thing again from a clean start, use `docker compose down -v` and then repeat Step 6. Running the pipeline again is safe; it overwrites the cleaned data and the model.

<br/>

## Troubleshooting

| Problem | What to do |
|:--|:--|
| Containers stop or Spark jobs are killed | Give Docker Desktop more memory (8 to 10 GB) and run again |
| `docker: command not found` | Install Docker Desktop and open it, then try again |
| A port is already in use | Stop the program using it. The dashboard uses 5050 because macOS already uses 5000 |
| First run is very slow | The Docker images and Spark's Kafka connector are being downloaded. Wait for it to finish |
| `CSV not found` | Check the file is in `data/` and that you kept the quotes around the file name |
| Producer cannot connect to Kafka | Make sure the `kafka` container is healthy: `docker ps` |
| Dashboard page does not load | The model is still loading. Wait a minute and refresh. Logs: `docker exec spark-master cat /tmp/flight-dashboard.log` |
| HDFS says it is in safe mode | Run `docker exec hdfs-namenode hdfs dfsadmin -safemode wait` and try again |

<br/>

## Project structure

```
Real-Time-Flight-Delay-Prediction/
├── docker-compose.yml          HDFS, Spark (master + 2 workers) and Kafka
├── Dockerfile.spark            Spark image with the extra Python packages
├── requirements.txt            Python package for the Kafka producer
├── scripts/
│   ├── run_pipeline.sh         runs the whole pipeline
│   ├── run_dashboard.sh        starts the web dashboard
│   ├── setup_hdfs.sh           creates the HDFS folders
│   └── build_dashboard_meta.py builds the airline and airport lists
└── src/
    ├── common.py               shared paths, features and topic name
    ├── ingestion/              cleans the CSV and writes Parquet to HDFS
    ├── training/               trains and evaluates the GBT model
    ├── streaming/              Kafka producer and Spark Structured Streaming job
    ├── dashboard/              web page and /predict API
    └── verify_pipeline.py      checks the results in HDFS
```

<br/>


## Things to know

- **Departure delay is an input.** It is the strongest signal, so the model tells you whether a flight will still arrive late once you know how it departed. It cannot warn you long before takeoff.
- **One month of data.** January is a winter month, so results may not match other times of the year.
- The data files and the trained model are not in the repo. They are created when you run the pipeline.

<br/>

---

## Built By

### N Vinuthna – 2023BCS0150
### K Hrishith – 2023BCS0168
### J Deepak – 2023BCS0038

---

<div align="center">

Built with HDFS (Hadoop 3.2.1), Apache Spark 3.5.0, Apache Kafka 3.7.0, Docker and Python 3

</div>
