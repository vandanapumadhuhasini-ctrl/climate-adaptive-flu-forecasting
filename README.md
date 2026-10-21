# ClimaFlu-GNN — Climate-aware influenza forecasting on a Hadoop + Spark pipeline

**23AID302 Big Data Analytics — Group 04**, Amrita Vishwa Vidyapeetham
Madhu Hasini (CB.AI.U4AID24060) · Saanvika (CB.AI.U4AID24047) · Dharshini (CB.AI.U4AID24013)

Forecasting weekly influenza-like-illness (ILI) for 50 US states from 26 years of NOAA GHCN-Daily climate records and CDC
ILINet surveillance, processed end to end on a Dockerised Hadoop 3.2.1 cluster with Spark 3.5.1, and modelled with a
climate-similarity graph neural network (PyTorch Geometric) against Spark MLlib baselines.

## Architecture

```
NOAA GHCN-Daily (34.6 GB, 968.9 M records) + CDC ILINet/NREVSS + station metadata
        │  hdfs dfs -put
        ▼
HDFS (NameNode + 3 DataNodes) ──► Hadoop MapReduce on YARN (Java: combiner + Writable, map-side join)
        │
        ▼
Spark (Scala), DataFrame API only ──► cleaning (303.6 M rows) ──► MMWR state-week features
        │                                            ──► weekly climate-similarity graph (KNN, K = 8)
        ├──► Spark Structured Streaming (live climate feed)          ──► Spark MLlib GBT baselines
        ▼
PyTorch Geometric GNN + ablations (10 seeds, pre-registered protocol) ──► results published back to HDFS
        ▼
Streamlit dashboard (reads result files only)
```

## Main results (all numbers generated from `results/`; see the linked documents)

| Area | Result | Evidence |
|---|---|---|
| Course requirements | 19 / 19 verified | [docs/COURSE_COMPLIANCE_MATRIX.md](docs/COURSE_COMPLIANCE_MATRIX.md) |
| Consistency | Hadoop MapReduce = Spark DataFrame = pandas (identical SHA-256), for both the aggregate and the station join | [docs/HADOOP_SPARK_API_EVIDENCE.md](docs/HADOOP_SPARK_API_EVIDENCE.md) |
| Scalability | 26 years (34.6 GB), equal 4 GB memory cap: Spark 90.1 s vs pandas (chunked) 1,619 s — 18.0× | [docs/BENCHMARK_RESULTS.md](docs/BENCHMARK_RESULTS.md) |
| Tuning | Spark defaults with AQE (285 s) beat every hand-set partition count; caching slows 3 queries under a 3 GB heap | [docs/TUNING_RESULTS.md](docs/TUNING_RESULTS.md) |
| Streaming | 5.97 M rows; 38.1 k rows/s vs 15.3 k rows/s arrival; median latency 4.7 s; exactly-once checks pass | `results/streaming_summary.json` |
| Forecasting (next week) | GNN not significantly better than Spark GBT; climate graph does not beat a no-graph model | [docs/RIGOUR_RESULTS.md](docs/RIGOUR_RESULTS.md) |
| Forecasting (1–4 weeks) | GNN significantly beats the fixed-setting Spark GBT at a 3-week horizon (Holm-adjusted p = 0.017); climate benefit grows with horizon but is not significant after correction | [docs/V3_RESULTS.md](docs/V3_RESULTS.md) |
| Spark MLlib comparison | Linear Regression, Random Forest and GBT tuned on 2022, horizons 1–4: tuned Random Forest / GBT with climate match the GNN (no significant difference at any horizon) and clearly beat Linear Regression; climate features improve Random Forest at every horizon | [docs/MLLIB_RESULTS.md](docs/MLLIB_RESULTS.md) |

All claims were tested under a protocol frozen before the runs ([docs/RESEARCH_PROTOCOL.md](docs/RESEARCH_PROTOCOL.md)),
including the claims that the evidence does not support ([docs/RESEARCH_EVIDENCE_MATRIX.md](docs/RESEARCH_EVIDENCE_MATRIX.md)).

**Base paper:** Y. Zhao *et al.*, "A New Two-Way Ensemble Prediction for Infectious Diseases," *IEEE Trans. Computational
Social Systems*, vol. 13, no. 5, pp. 6359–6370, Oct. 2026, doi:10.1109/TCSS.2025.3640950 — gap analysis in
[docs/BASE_PAPER.md](docs/BASE_PAPER.md).

## Repository layout

| Path | Contents |
|---|---|
| `docker-compose.yml`, `hadoop.env`, `docker/` | cluster (HDFS, YARN, Spark, PyTorch, dashboard containers) |
| `scala/` | Spark jobs (ETL, audits, MLlib, streaming, tuning, feature engineering, publishing to HDFS) |
| `mapreduce/` | Java MapReduce jobs and their build/run script |
| `python/` | PyTorch Geometric models, pandas benchmark, analysis and document generators |
| `dashboard/` | Streamlit dashboard |
| `benchmarks/`, `streaming/`, `scripts/` | benchmark/tuning runners, streaming feed, verification and screenshot scripts |
| `results/` | every measured result (JSON, predictions, logs); also published to HDFS `/flu_project/results/` |
| `docs/` | protocols, generated results, matrices, figures, screenshots |

Raw data (`datasets/`) and HDFS exports are not in the repository; they live in HDFS.

## Running it

See [docs/RESUME.md](docs/RESUME.md) (start the cluster, verify, regenerate the evidence) and `PROJECT_GUIDE.md` (full
run commands, environment notes and issue log). Requirements: Windows + Docker Desktop (WSL2), ≥ 15 GB free disk.
