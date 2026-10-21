# ClimaFlu-GNN — Climate-aware influenza forecasting on a Hadoop + Spark pipeline

**23AID302 Big Data Analytics — Group 04**, Amrita Vishwa Vidyapeetham
Madhu Hasini (CB.AI.U4AID24060) · Saanvika (CB.AI.U4AID24047) · Dharshini (CB.AI.U4AID24013)

Forecasting weekly influenza-like-illness (ILI) for 50 US states from 26 years of NOAA GHCN-Daily climate records and CDC
ILINet surveillance, processed end to end on a Dockerised Hadoop 3.2.1 cluster with Spark 3.5.1 (DataFrame API only), and
modelled with a climate-similarity graph neural network (PyTorch Geometric) against Spark MLlib models.

## 1. File structure

```
climate-adaptive-flu-forecasting/
│
├── README.md                    this guide: structure, workflow, file-by-file, results
├── run_pipeline.ps1             ★ ONE entry point: 12 stages  (.\run_pipeline.ps1 <stage>)
├── docker-compose.yml           cluster: NameNode, 3 DataNodes, YARN, history servers, Spark, PyTorch, dashboard
├── hadoop.env                   Hadoop / YARN settings shared by the Hadoop containers
├── docker/                      pytorch.Dockerfile, dashboard.Dockerfile, hadoop-conf/
│
├── bigdata_pipeline/            ── BIG DATA LAYER (Hadoop + Spark, runs on the cluster)
│   ├── hadoop_mapreduce/          2 MapReduce jobs in Java on YARN + run_mapreduce.sh
│   ├── spark_etl/                 raw → clean → weekly state features → climate graph (7 Scala jobs)
│   ├── spark_mllib/               Spark ML: GBT baselines + Linear Regression / Random Forest / GBT comparison
│   ├── spark_streaming/           Structured Streaming on a live climate feed + the feed script
│   ├── spark_tuning/              Spark vs pandas benchmark workload + partitioning/caching experiment
│   ├── spark_checks/              smoke test, data-quality audit, MapReduce = Spark check, publish to HDFS, UI demo
│   └── dashboard/app.py           Streamlit dashboard (reads result files only)
│
├── src/                         ── ML LAYER (Python, runs in the pytorch container)
│   ├── gnn/                       GNN + baselines: features, training, evaluation (pre-registered protocol)
│   └── reporting/                 performance analysis, evidence documents, dashboard map data
│
├── scripts/                     long-running helpers called by run_pipeline.ps1 (benchmark, tuning, screenshots)
├── docs/                        protocol, results, evidence matrices, figures/, screenshots/, PROJECT_GUIDE.md
├── results/                     every measured result (JSON, predictions); also copied to HDFS /flu_project/results
└── logs/                        one log per run
```

Not in the repository (too large; they live in HDFS): `datasets/` (raw downloads), `exports/` (Parquet copied out of
HDFS for the GNN), `results/spark-events/` (Spark event logs).

## 2. How to run

Requirements: Windows with Docker Desktop (WSL2, 8 GB memory for the VM), PowerShell, ≥ 15 GB free disk.
Run everything from the project folder:

```powershell
.\run_pipeline.ps1                 # prints the list of stages
.\run_pipeline.ps1 up              # start the cluster
.\run_pipeline.ps1 etl             # run one stage
.\run_pipeline.ps1 all             # run every stage in order
.\run_pipeline.ps1 job Preprocess  # run one Spark job by name
```

| # | Stage | What happens | Main output |
|---|---|---|---|
| 1 | `up` | start the containers, wait until HDFS leaves safe mode | cluster running |
| 2 | `ingest` | copy 26 yearly GHCN CSVs + stations file + 4 CDC FluView CSVs into HDFS | HDFS `/flu_project/raw` (34.6 GB) |
| 3 | `etl` | Spark: parse stations, clean 968.9 M climate records, clean FluView, aggregate to state-weeks, join with ILI, build the KNN climate graph | HDFS `/flu_project/processed` |
| 4 | `mapreduce` | Hadoop MapReduce on YARN: element × year totals (combiner) and state × year join (distributed cache) | `results/mapreduce_jobs.json` |
| 5 | `gnn` | features, Python + Spark GBT baselines, 120 GNN training runs, statistical tests | `docs/RIGOUR_RESULTS.md` |
| 6 | `v3` | lag-aware features for 1–4 week horizons in Spark, GBT + GNN, tests | `docs/V3_RESULTS.md` |
| 7 | `mllib` | Spark MLlib Linear Regression, Random Forest, GBT tuned on 2022, compared with the GNN | `docs/MLLIB_RESULTS.md` |
| 8 | `streaming` | 60 daily climate files dripped into HDFS while a Structured Streaming query processes them | `results/streaming_summary.json` |
| 9 | `performance` | Spark vs pandas at 1–26 years, partitions × caching grid | `docs/BENCHMARK_RESULTS.md`, `docs/TUNING_RESULTS.md` |
| 10 | `checks` | smoke test, veracity audit, MapReduce = Spark DataFrame SHA-256 check | `results/cross_engine_consistency.json` |
| 11 | `report` | cluster health snapshot, dashboard data, evidence documents, HDFS screenshot | `docs/COURSE_COMPLIANCE_MATRIX.md` |
| 12 | `publish` | copy every result file into HDFS with a SHA-256 manifest | HDFS `/flu_project/results` |

**Web UIs** (while the cluster runs): HDFS http://localhost:9870 · YARN http://localhost:8088 · YARN job history
http://localhost:8188 · Spark History Server (every finished Spark job, its stages and SQL plans) http://localhost:18080 ·
live Spark UI http://localhost:4040 (only while a job runs) · dashboard http://localhost:8501

## 3. Workflow (execution flow)

```
datasets/ (GHCN 34.6 GB, CDC FluView)
   │  ingest: hdfs dfs -put
   ▼
HDFS /flu_project/raw ─────────────► hadoop_mapreduce (YARN) ──► element×year, state×year totals ─┐
   │                                                                                               │ same SHA-256
   ▼  spark_etl (DataFrame API)                                                                    │
StationsPreprocess ─► WeatherPreprocess (303.6 M clean rows) ─► WeatherAggregate (state-weeks)     │
Preprocess (FluView) ──────────────────────────────────────────► FinalJoin ─► GraphConstruction    │
   │                                                              (modeling_table)  (KNN edges)    │
   ├─► spark_checks/DataFrameConsistency ◄─────────────────────────────────────────────────────────┘
   ├─► spark_mllib (GBT, Linear Regression, Random Forest) ─────────┐
   ├─► exports/ ─► src/gnn (PyTorch Geometric GNN, 10 seeds) ───────┼─► evaluation: Diebold-Mariano, Holm, bootstrap
   ├─► spark_streaming (live feed) ─► streaming results             │
   └─► spark_tuning (benchmark, tuning) ─► performance results      ▼
                                                     results/ ─► src/reporting ─► docs/ + dashboard ─► HDFS (publish)
```

## 4. File-by-file

**Entry point and cluster**

| File | Role |
|---|---|
| `run_pipeline.ps1` | the one command: runs any stage or a single Spark job, parses MapReduce counters, writes the cluster health snapshot |
| `docker-compose.yml` | all containers and their mounts; HDFS data lives in Docker volumes |
| `hadoop.env` | Hadoop/YARN settings (container memory, NodeManager resources, history server); HDFS replication 2 is set in `docker-compose.yml` |
| `docker/pytorch.Dockerfile`, `docker/dashboard.Dockerfile` | Python images for the GNN and the dashboard |

**`bigdata_pipeline/hadoop_mapreduce/`** (Java, Hadoop MapReduce on YARN)

| File | Role |
|---|---|
| `GhcnRecord.java` | parses one GHCN line and applies the filter (US station, TMAX/TMIN/PRCP, no quality flag) |
| `CountSumWritable.java` | custom Hadoop `Writable` carrying (count, sum) |
| `ElementYearAggregate.java` | MR1: totals per element × year, with a combiner |
| `StateYearCoverage.java` | MR2: map-side join with the stations file (distributed cache), totals per state × year × element |
| `run_mapreduce.sh` | compiles the jar inside the NameNode and runs both jobs on YARN |

**`bigdata_pipeline/spark_etl/`** (Scala, Spark DataFrame API)

| File | Role |
|---|---|
| `StationsPreprocess.scala` | fixed-width `ghcnd-stations.txt` → US stations with state, lat, lon |
| `WeatherPreprocess.scala` | 26 yearly CSVs → quality-filtered TMAX/TMIN/PRCP joined to states (303.6 M rows) |
| `Preprocess.scala` | CDC ILINet and NREVSS CSVs → cleaned tables (title line removed, types fixed, missing values handled) |
| `WeatherAggregate.scala` | daily climate → MMWR state-week features |
| `FinalJoin.scala` | state-week climate ⋈ ILI → `modeling_table` (New York + New York City merged into one state) |
| `GraphConstruction.scala` | weekly climate-similarity KNN graph between states |
| `LagAwareFeaturesV3.scala` | lagged climate + 1–4 week horizon targets (v3) |

**`bigdata_pipeline/spark_mllib/`**

| File | Role |
|---|---|
| `BaselineGBTv2.scala` | Spark GBT baselines for next-week ILI (rigour pass) |
| `BaselineGBTv3.scala` | Spark GBT baselines for horizons 1–4 (v3) |
| `MLlibComparison.scala` | Linear Regression, Random Forest, GBT with parameter grids and a 2022 validation year |

**`bigdata_pipeline/spark_streaming/`**

| File | Role |
|---|---|
| `PrepareStreamingV2.scala` | splits 2025 climate data into 60 daily files (the feed source) |
| `StreamingV2.scala` | Structured Streaming query: QC filter, station join, state × day aggregates, latency log |
| `drip_feed.sh` | moves one daily file every 3 s into the watched HDFS folder |

**`bigdata_pipeline/spark_tuning/`**

| File | Role |
|---|---|
| `BenchmarkWorkload.scala` | the Spark side of the Spark vs pandas benchmark |
| `benchmark_pandas.py` | the identical workload in pandas (same result checksum) |
| `TuningExperiment.scala` | partition count × persistence × AQE experiment |

**`bigdata_pipeline/spark_checks/`**

| File | Role |
|---|---|
| `ClusterSmokeTest.scala` | reads every processed HDFS table and records row counts |
| `VeracityAudit.scala` | re-measures data-quality facts (missing values, duplicate state-weeks, nulls) and the raw format inventory |
| `DataFrameConsistency.scala` | proves MapReduce and Spark DataFrame give identical results (SHA-256) |
| `PublishResultsToHdfs.scala` | copies `results/` into HDFS with a SHA-256 manifest |
| `SparkUiDemo.scala` | keeps a real aggregation running so the live Spark UI (4040) can be shown |

**`bigdata_pipeline/dashboard/app.py`** — Streamlit dashboard: 4 V's, cluster, batch, streaming, forecasts, graph.

**`src/gnn/`** (Python, PyTorch Geometric; protocol in `docs/RESEARCH_PROTOCOL.md`)

| File | Role |
|---|---|
| `common.py` | shared code: feature table, exact calendar lags, graphs, GNN model, metrics |
| `prepare_v2.py` | builds the canonical feature table and checks the graph against Spark's |
| `baselines_v2.py` | persistence and other Python baselines |
| `train_v2.py`, `run_all.py` | trains one model × seed; runs all models × 10 seeds in parallel |
| `evaluate_v2.py` | tests on 2023–2025 with Diebold-Mariano, Holm correction, bootstrap |
| `train_v3.py`, `run_all_v3.py`, `evaluate_v3.py` | the same for horizons 1–4 (v3) |
| `evaluate_mllib.py` | compares the MLlib models with the GNN |

**`src/reporting/`**

| File | Role |
|---|---|
| `analyze_performance.py` | benchmark, tuning and streaming analysis → summaries, Markdown and figures |
| `build_reports.py` | Spark event-log summary, MapReduce/Spark evidence, course compliance matrix |
| `dashboard_data.py` | state centroids and test-week graphs for the dashboard maps |

**`scripts/`**

| File | Role |
|---|---|
| `run_benchmark.ps1` | resumable benchmark runner: fresh container per run, equal memory caps |
| `run_tuning.ps1` | resumable tuning grid with a disk watchdog |
| `capture_screenshots.ps1` | screenshots of HDFS, YARN, Spark History and dashboard pages |

## 5. Main results (all numbers generated from `results/`)

| Area | Result | Evidence |
|---|---|---|
| Course requirements | 19 / 19 verified | [docs/COURSE_COMPLIANCE_MATRIX.md](docs/COURSE_COMPLIANCE_MATRIX.md) |
| Consistency | Hadoop MapReduce = Spark DataFrame = pandas (identical SHA-256), for both the aggregate and the station join | [docs/HADOOP_SPARK_API_EVIDENCE.md](docs/HADOOP_SPARK_API_EVIDENCE.md) |
| Scalability | 26 years (34.6 GB), equal 4 GB memory cap: Spark 90.1 s vs pandas (chunked) 1,619 s — 18.0× | [docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) |
| Tuning | Spark defaults with AQE (285 s) beat every hand-set partition count; caching slows 3 queries under a 3 GB heap | [docs/TUNING_RESULTS.md](docs/TUNING_RESULTS.md) |
| Streaming | 5.97 M rows; 38.1 k rows/s vs 15.3 k rows/s arrival; median latency 4.7 s; exactly-once checks pass | `results/streaming_summary.json` |
| Forecasting (next week) | GNN not significantly better than Spark GBT; climate graph does not beat a no-graph model | [docs/RIGOUR_RESULTS.md](docs/RIGOUR_RESULTS.md) |
| Forecasting (1–4 weeks) | GNN significantly beats the fixed-setting Spark GBT at a 3-week horizon (Holm-adjusted p = 0.017); climate benefit grows with horizon but is not significant after correction | [docs/V3_RESULTS.md](docs/V3_RESULTS.md) |
| Spark MLlib comparison | tuned Random Forest / GBT with climate match the GNN (no significant difference at any horizon) and clearly beat Linear Regression; climate features improve Random Forest at every horizon | [docs/MLLIB_RESULTS.md](docs/MLLIB_RESULTS.md) |

All claims were tested under a protocol frozen before the runs ([docs/RESEARCH_PROTOCOL.md](docs/RESEARCH_PROTOCOL.md)),
including the claims the evidence does not support ([docs/RESEARCH_EVIDENCE_MATRIX.md](docs/RESEARCH_EVIDENCE_MATRIX.md)).

**Base paper:** Y. Zhao *et al.*, "A New Two-Way Ensemble Prediction for Infectious Diseases," *IEEE Trans. Computational
Social Systems*, vol. 13, no. 5, pp. 6359–6370, Oct. 2026, doi:10.1109/TCSS.2025.3640950 — gap analysis in
[docs/BASE_PAPER.md](docs/BASE_PAPER.md).

Environment notes and the issue log are in [docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md).
