# PROJECT_GUIDE.md — ClimaFlu-GNN (23AID302 Big Data Analytics, Group 04)

Handoff from the earlier planning chat (2026-10-10), merged with the rules and working methods of the sister project
`C:\Project\real-time-misinformation-detection` (same professor, same course). Read it top to bottom before touching anything.
Anything not marked VERIFIED was not re-checked.

---

## 0. Ground rules (from the user — obey these)

- Team **Group 04**: Madhu Hasini (CB.AI.U4AID24060, the person you talk to), Saanvika (CB.AI.U4AID24047), Dharshini (CB.AI.U4AID24013).
- The professor **strictly requires Hadoop and Docker**. Never replace them with something easier.
- Work is organised in **Parts**. Finish a Part, report, and **wait for the user to say "continue"**. Strictly sequential; one heavy job at a time.
- Goal: publishable IEEE work. Primary target IEEE JBHI; fallbacks BigData 2027, ICHI 2027.
- Professor wants a clear research gap, problem statement, objectives, and a **2026 IEEE journal base paper** — FOUND 2026-10-11: Zhao et al., "A New Two-Way Ensemble Prediction for Infectious Diseases", IEEE TCSS, Oct 2026, DOI 10.1109/TCSS.2025.3640950 (see `docs/BASE_PAPER.md`, with gap + novelty plan).
- Windows + Docker Desktop (WSL2), **PowerShell** in VS Code. Give PowerShell commands (section 4).
- **Honesty:** report only measured numbers, taken from result files/logs. A "Written to..." println is not evidence; check HDFS.
- Never delete legacy artefacts unless verified unused; ask before deleting anything.
- **Authorship: only the Group 04 students.** Never add any AI assistant or tool as author, co-author or contributor — no
  `Co-Authored-By` or "Generated with" lines in commits/PRs, no AI bylines in the paper, slides, proposal or README.
  Keep AI-tool configuration files out of anything submitted or uploaded (e.g. via `.gitignore`).
- Do not run this project's heavy jobs alongside another project's GPU/RAM-heavy work (laptop: ~16 GB RAM, RTX 4060).

---

## 1. Professor's mandatory rubric (same as the misinformation project)

Every component must map to one of these rows. Final deliverable is a **course compliance matrix** (`docs/COURSE_COMPLIANCE_MATRIX.md`)
with columns Requirement | Implementation | Actual Execution | Evidence | Measured Result | Status, built from result files, not typed by hand.
A row is VERIFIED only when its evidence file exists.

| Requirement | ClimaFlu status (2026-10-10) |
|---|---|
| Volume ≥ 25 GB in HDFS | **DONE**: 34.59 GB raw, atomic snapshot `results/verification.json` + `hdfs_overview_final.png` |
| Velocity (streaming) | **DONE 2026-10-11 (streaming v2)**: live GHCN feed, 60 daily files / 5,972,783 rows, 38.1k rows/s vs 15.3k arrival, latency median 4.7 s, exactly-once checks pass; `results/streaming_summary.json` |
| Variety ≥ 2 data types | **DONE**: 4 raw format families + 13 Parquet tables, `results/variety_inventory.json` |
| Veracity | **DONE**: `results/veracity_audit.json` (re-measured: WEIGHTED_ILI 99.85% missing, NY+NYC 827 raw / 782 merged weeks, 0 dup keys, 0 implausible values) |
| Hadoop HDFS | Done (3 DataNodes, HEALTHY, 34.59 GB raw). Screenshots + results/cluster_verification.json DONE |
| Hadoop MapReduce on YARN | **DONE 2026-10-10**: 2 Java jobs (combiner + custom Writable; map-side join via distributed cache), 268 maps each, `results/mapreduce_jobs.json` |
| Spark RDD | **DONE**: textFile→map→filter→flatMap→reduceByKey, lineage in `results/spark_api_evidence.txt` |
| Spark DataFrame | Done (all jobs, benchmark workload). Typed + DF plans captured |
| Dataset API (typed) | **DONE**: case classes, `joinWith` broadcast, typed `Aggregator`; plan shows DeserializeToObject/MapElements/ObjectHashAggregate |
| Cross-engine consistency | **DONE**: MR1 = RDD = DataFrame = pandas (SHA `a5c81104…`), MR2 = typed Dataset (SHA `eb02de06…`), `results/cross_engine_consistency.json`, `docs/HADOOP_SPARK_API_EVIDENCE.md` |
| Scala AND Python | Done (Scala Spark jobs; Python PyG + pandas) |
| Spark ML | Done (MLlib GBT). Could add a 2nd MLlib model (RF / LR) |
| Performance tuning | **DONE 2026-10-11**: partitions {8,24,96,384} × {none, MEMORY_AND_DISK} + Spark defaults, 3 repeats, 25 runs; `docs/TUNING_RESULTS.md`, `results/tuning_summary.json` |
| Visualization dashboard | **DONE 2026-10-11**: Streamlit `dashboard/app.py`, container `flu-dashboard`, http://localhost:8501 (localhost only), reads results/ + docs/ read-only; 8 screenshots `docs/screenshots/dashboard_*.png` |
| YARN (8088) + HDFS (9870) UI screenshots | DONE (docs/screenshots/, 2026-10-10). YARN fixed; MapReduce pi SUCCEEDED |

Methods carried over from the misinformation project (they worked):
- Every stage writes a JSON to `results/` and a log to `logs/`; docs and matrices are generated from those files.
- Freeze the experimental protocol (splits, metrics, seeds, decision rules) in `docs/RESEARCH_PROTOCOL.md` **before** the rigour runs.
- A `docs/RESUME.md` checklist so a reboot or new session can pick up exactly where work stopped.
- Check free RAM/disk before each heavy stage; defer rather than OOM. Keep ≥15 GB free disk.
- Atomic final verification script (HDFS du + fsck + UI screenshot in one run).

---

## 2. What the project is

**ClimaFlu-GNN** forecasts next-week influenza-like-illness (ILI) rate for each US state with a climate-adaptive dynamic GNN on a Hadoop + Spark pipeline.

Data (~31 GB raw): **NOAA GHCN-Daily** yearly CSVs 2000–2025 + fixed-width stations file (303,631,696 cleaned rows); **CDC ILINet** state-level weekly ILI.

Pipeline: raw → HDFS → Spark/Scala cleaning, state-week aggregation, join with ILINet → modeling table → weekly KNN climate-similarity graph →
Parquet export → PyTorch Geometric model → compared with Spark MLlib GBT and persistence. Streaming ingestion and Spark-vs-pandas benchmark are separate Parts.

Model: graph rebuilt **each week from climate similarity only** (K=8). Two branches → fusion MLP: MLP over ILI lags; 2-layer GCN over climate features with similarity edge weights.
Temporal split: **train ≤ 2022, test ≥ 2023**.

---

## 3. Docker topology (`docker-compose.yml`, network `flu_network`)

**Container names (changed 2026-10-10):** the Hadoop containers are prefixed `flu-` because the misinformation project
(`C:\Project\real-time-misinformation-detection\hadoop-docker`) owns the plain names `namenode`, `resourcemanager`, `nodemanager`,
`historyserver`. **Never touch that project's containers.** The two clusters share host ports, so do not run both at once.
Inside `flu_network` the service names/hostnames are unchanged (`hdfs://namenode:9000` still works).

| Container | Image | Host ports | Notes |
|---|---|---|---|
| flu-namenode | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 | 9870, 9000 | repl 2; mounts `./datasets/weather_raw:/mnt/weather_raw:ro`, `./exports:/export`; also the MapReduce job client |
| flu-datanode1/2/3 | bde2020/hadoop-datanode | 9864/9865/9866 | named volumes `flu-hadoop-project_datanode{1,2,3}_data` (~23 GB each) |
| flu-resourcemanager | bde2020/hadoop-resourcemanager | 8088 | YARN works (1 node, 4096 MB, 4 vcores) |
| flu-nodemanager | bde2020/hadoop-nodemanager | — | registered, RUNNING |
| flu-historyserver | bde2020/hadoop-historyserver | **8188** | YARN Timeline / Application History server (not 19888 — that is the MR JobHistory server, not in this image) |
| spark | apache/spark:3.5.1 | **4040** | `tail -f /dev/null`; mounts `./hadoop-conf`, `./scala`, `./datasets`, `./results:/opt/results` |
| pytorch | climaflu-pytorch:1.0 (built from `docker/pytorch/Dockerfile`) | — | `mem_limit: 4g`; mounts `./exports:/data:ro`, `./python:/workspace`, `./models/climaflu_gnn:/output`, `./datasets/weather_raw:/rawdata:ro`, `./results:/results` |

- `hadoop.env` holds all YARN/MapReduce settings (env_file for namenode, resourcemanager, nodemanager, historyserver).
  Encoding: `_`→`.`, `__`→`_`, `___`→`-`. **Never use a literal `-` in a variable name** — the old `YARN_CONF_yarn_nodemanager_aux-services`
  made the entrypoint skip every YARN setting (NodeManager dialled 0.0.0.0:8031, YARN had 0 nodes).
- Spark jobs run `--master local[*]` in the spark container (not on YARN); Hadoop MapReduce runs on YARN from `flu-namenode`.
- Spark UI 4040 exists only while a job runs (`scala/SparkUiDemo.scala` keeps one open 120 s).
- `hadoop-conf/` holds only `.gitkeep`, hence `fs.defaultFS` is passed explicitly everywhere.
- pytorch packages are baked into the image (torch 2.4.1+cpu, PyG 2.6.1, pandas 2.2.3, pyarrow, scipy, scikit-learn, matplotlib).
- Docker WSL disk is `E:\dockerData\DockerDesktopWSL\disk\docker_data.vhdx` (169.5 GB on 2026-10-11, ~121 GB real data).
  **It is NOT sparse and never shrinks**: anything written inside containers grows it permanently. On 2026-10-10 Spark
  shuffle files written inside the spark container grew it until E: hit 0 bytes and WSL crashed (HDFS survived, fsck HEALTHY).
  Rules: big Spark shuffles use `spark.local.dir=/opt/spark-local` (= `.\.spark-local` on C:, a bind mount, so deleted
  files free real space); check free space on C: and E: before heavy jobs; long jobs run under a disk watchdog
  (see `benchmarks/run_tuning.ps1`). Reclaiming the dead space needs Docker stopped + admin `diskpart compact vdisk`.
- **Docker VM has 8 GB RAM** shared with the Hadoop daemons (~2.8 GB): Spark `--driver-memory` must be **3g**, not 4g
  (4g was killed by the VM-wide OOM killer). Check `dmesg` (privileged alpine) to tell container-cap kills
  (`CONSTRAINT_MEMCG`) from VM-wide kills (`CONSTRAINT_NONE`).
- After restarts the NameNode is in safe mode for a while: wait (`hdfs dfsadmin -safemode get`), don't retry blindly.
- Spark on YARN would need `core-site.xml`/`yarn-site.xml` in `hadoop-conf` and Spark's Hadoop client is 3.3.x vs cluster 3.2.1 — test before promising.
  MapReduce/Hadoop Streaming jobs can instead be submitted from inside the namenode/resourcemanager containers.

---

## 4. File layout (`C:\Project\flu-hadoop-project`)

```
docker-compose.yml
datasets/weather_raw/   GHCN CSVs + stations (31 GB, git-ignore)
exports/                Parquet from HDFS for pytorch: graph_edges_knn/ modeling_table/
hadoop-conf/            only .gitkeep
scala/                  Spark jobs (spark container)
python/                 PyTorch + pandas scripts (pytorch container)
python_legacy/          old PySpark scripts (spark_hdfs_test, spark_inspect, spark_preprocess, verify)
models/climaflu_gnn/  models/baseline_spark_mllib/
scripts/                01-04 old, 05-09 empty placeholders, drip.sh broken (CRLF)
mapreduce/ streaming/ benchmarks/ paper/   EMPTY
```

Scala: `Preprocess` (ILINet), `WeatherPreprocess`, `WeatherAggregate`, `FinalJoin`, `GraphConstruction`, `BaselineModel`, `PrepareStreamingDemo`,
`StreamingIngest`, `BenchmarkSpark`; diagnostics `CheckIliNulls`, `CheckYearRange`, `CheckDuplicates`, `VerifyNyMerge`.
Python: `prepare_data.py`, `train_model.py`, `benchmark_pandas.py` (naive), `benchmark_pandas_optimized.py`.
HDFS `/flu_project/`: `raw`, `processed`, `streaming_source`, `streaming_incoming`, `streaming_checkpoint`, `streaming_output`.

Cleanup done 2026-10-11 (user: "keep the important ones"): only the five empty scripts 05-09 were deleted; old scripts
01-04, `drip.sh` and `verify_snippet.scala` moved to `scripts/legacy/` (with a README); README rewritten; `.gitignore` added
(datasets, exports, Spark event logs, IDE folders). The nested models folder from the handoff no longer existed.

---

## 5. How to run things

**Scala jobs** — every `.scala` file must start with `:paste` on line 1 (otherwise the REPL orphans statements silently).
Always run through the runner (logs to `logs/`, Spark event log to `results/spark-events/`):
```powershell
.\scripts\run_scala.ps1 Foo            # runs scala\Foo.scala
```
- **Do not** use `Get-Content -Raw ... | docker exec -i spark ...`: PowerShell prepends a BOM, `:paste` fails (`illegal character '﻿'`)
  and the REPL silently runs line by line. The runner uses `cmd /c "... < file"` instead.
- End scripts with `System.exit(0)`. Pass `-DriverMemory 3g` while the Hadoop cluster runs (the runner default 4g can be OOM-killed); `persist(DISK_ONLY)` for big frames.
- Typed Datasets: Spark 3.5 cannot encode case classes defined in the same `:paste` block (NPE in `OuterScopes`).
  Put them in `scala/lib/*.scala` and preload: `.\scripts\run_scala.ps1 RddDatasetAudit -DriverMemory 3g -Preload lib/ClimaFluTypes.scala`.
- Logs written by PowerShell 5 `Tee-Object`/`Out-File` default to UTF-16; `run_scala.ps1` now writes plain bytes via cmd.
- Write each stage's evidence as JSON to `/opt/results/<stage>.json` (= `results\` on the host). Confirm HDFS outputs with `hdfs dfs -ls` + row count.

**Cluster checks / evidence:**
```powershell
docker compose up -d
.\scripts\verify_cluster.ps1             # -> results\cluster_verification.json
.\scripts\capture_ui_screenshots.ps1     # -> docs\screenshots\*.png (headless Edge)
```

**HDFS from host:**
```powershell
docker exec -it flu-namenode hdfs dfs -ls /flu_project/processed
docker exec -it flu-namenode hdfs dfsadmin -safemode get
docker exec -it flu-namenode hdfs fsck /flu_project -files -blocks | Select-String "Under replicated|Corrupt|HEALTHY"
```

**MapReduce on YARN** (from the namenode container, which has the client config; `./mapreduce` is mounted at `/opt/mapreduce`):
```powershell
docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh           # build jar + MR1 + MR2 (~25 min), prints SHA-256
.\scripts\summarize_mapreduce.ps1                                        # -> results\mapreduce_jobs.json
docker exec flu-namenode hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.1.jar pi 4 1000
```
**Step 3 evidence regeneration:**
```powershell
.\scripts\run_scala.ps1 RddDatasetAudit -DriverMemory 3g -Preload lib/ClimaFluTypes.scala   # -> results\cross_engine_consistency.json
docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/build_api_evidence.py
.\benchmarks\run_tuning.ps1                                              # resumable, disk watchdog; ~8 h for the full grid
docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/analyze_tuning.py
```
- Bulk data in: bind mount + `hdfs dfs -put` from inside namenode (`/mnt/weather_raw`). Never `docker cp` bulk data.
- Export: `hdfs dfs -get <src> /export/<name>` — delete the destination first or it nests.

**PowerShell traps:** no `tail`/`grep` (use `Select-Object -Last N`, `Select-String`); single-quote anything with `$`;
CRLF files break Linux shell scripts. In Git Bash, set `MSYS_NO_PATHCONV=1` for `docker exec` with `/paths`.

**Python:** `docker exec -it pytorch python prepare_data.py` / `train_model.py`.

---

## 6. Pipeline outputs and results (VERIFIED in chat logs; not yet saved as result files)

| Dataset | Rows |
|---|---|
| weather_cleaned | 303,631,696 |
| us_stations | 78,567 |
| weather_state_week | 68,833 |
| modeling_table | 40,408 (39,673 after null drop) |
| graph edges (KNN, K=8) | 323,264 |
| PyG snapshots | 635 train / 156 test |

**Part 04 v2 rigour pass (FINAL, 2026-10-11)** — protocol frozen first in `docs/RESEARCH_PROTOCOL.md`; results generated in
`docs/RIGOUR_RESULTS.md`, `results/rigour/summary.json`, figures `docs/figures/rigour_test_rmse.png`, `rigour_forecast.png`.
Exact calendar lags (v1 row-shift crossed 12 week gaps; 208 rows changed), train-only standardisation, graphs rebuilt in
Python from train-only statistics (Python builder reproduces the Spark graph 317,384/317,384 edges), epoch chosen on 2022,
refit on ≤2022, test 2023–2025 (7,691 state-weeks, 156 weeks), 10 seeds, DM tests + block bootstrap, all models on identical rows.

| Model (test 2023–25) | RMSE mean ± sd (10 seeds) |
|---|---|
| MLP-noGraph | 0.9181 ± 0.0110 |
| GNN-K4 | 0.9195 ± 0.0158 |
| **GNN-K8 (ClimaFlu-GNN)** | **0.9236 ± 0.0159** (ensemble 0.9169) |
| GNN-geo / GNN-static | 0.9237 / 0.9256 |
| MLP-lagsOnly | 0.9309 |
| GNN-K16 | 0.9383 |
| Spark GBT-lags / GBT-all (deterministic) | 0.9401 / 0.9422 |
| GNN-random | 0.9511 |
| Ridge / Persistence | 0.9919 / 1.0377 |

**Pre-registered claims: C1 (beats GBT) NO — DM p = 0.058, 8/10 seeds; C2 (graph helps) NO — no-graph MLP is as good;
C3 (dynamic graph helps) NO.** GNN-K8 is significantly better than Ridge, Persistence and GNN-K16 only. Climate adds
little over ILI lags. The v1 numbers (GNN 0.8979 selected on test, GBT 0.9538 on row-shift lags) are superseded — never cite them.
Runs reproduce exactly only for a fixed PyTorch thread count (3); 4 vs 3 threads changed RMSE by 0.00016.

**v3 novelty experiment (2026-10-11, amendment A1 + A1.1, `docs/V3_RESULTS.md`)** — lag-aware climate + horizons 1–4 weeks.
Features built in Spark on HDFS (`scala/LagAwareFeaturesV3.scala` → `/flu_project/processed/v3_features/h1..h4`), identical
to the v2 Python table (39,365 rows, max |diff| 0). Lag scan (≤ 2021) chose **lead 0 for every feature and horizon**.
160 neural runs + Spark GBT per horizon (`scala/BaselineGBTv3.scala`). Holm-adjusted claims: **N1 NO** (models identical),
**N2 NO** (climate gain grows with horizon: h=3 1.251 vs 1.293, 10/10 seeds, raw p 0.028, Holm 0.080), **N3 NO**,
**N4 YES at h = 3** (GNN 1.251 vs GBT 1.289, Holm p 0.017, 9/10 seeds).

**Everything is in HDFS:** `scala/PublishResultsToHdfs.scala` copies all of `results/` (predictions, metrics, logs, Spark event
logs; 840 files, 1.45 GB) to `/flu_project/results/` with `results/hdfs_publish_manifest.json` (SHA-256). Re-run it after any
new result (it skips `.inprogress` event logs).

**Part 06 benchmark (FINAL, 2026-10-10, 71 runs)** — protocol `docs/BENCHMARK_PROTOCOL.md`, table `docs/BENCHMARK_RESULTS.md`,
data `results/benchmark/runs.jsonl` + `results/benchmark_summary.json`, figures `docs/figures/bench_runtime.png`, `bench_memory.png`.
Same workload (filter + group by element×year), equal 4 GB cap in fresh containers, identical SHA-256 result for all 7 sizes.
- 26 years / 34.57 GB: Spark-HDFS 24 cores **90.1 s** (383.6 MB/s, peak 2.50 GB) vs pandas-chunked **1619 s** → **17.96×**.
- Spark-HDFS 4 cores 225.9 s (7.17×); Spark on the same local files 126.8 s (HDFS path is faster than the Windows bind mount).
- Equal parallelism: Spark 1 core vs pandas-chunked (1 thread): 1.45× (1 yr) → 1.85× (5 yr).
- In-memory pandas (naive / optimized / pyarrow) **OOM-killed at 1 year under 4 GB** (kernel: container cap, 4.27–4.28 GB).
  Experiment B (6 GB cap): the kills were **VM-wide** (`CONSTRAINT_NONE`, ~5.0–5.1 GB), i.e. the 8 GB Docker VM ran out
  before the 6 GB cap — reported as such (`docs/BENCHMARK_PROTOCOL.md`, post-run correction; `results/benchmark/kernel_oom_log.txt`).

**Step 3 results (2026-10-10/11):**
- MR1 707 s / MR2 760 s on YARN (268 rack-local maps each; 968,905,773 input records = KEPT + dropped, exact accounting);
  303,631,696 kept rows = `weather_cleaned` count; all 303,631,696 join to a state (78,567 stations per mapper).
- Spark RDD 59.5 s, typed Dataset 212.1 s; SHAs match MapReduce exactly (see section 1).
- Tuning (no-persist, AQE off): 8→24→96→384 partitions = 510→363→346→326 s; **Spark defaults with AQE 285 s = fastest**.
  MEMORY_AND_DISK is 2.1–2.7× slower for 3 queries (cache build 423–728 s); per-query saving at P=384 is 24.3 s →
  break-even ≈ 18 queries; at P=8 the cache makes queries slower; P=24 + cache failed (`SparkOutOfMemoryError`, 3 GB heap).
  Shuffle went through the `.spark-local` bind mount, so absolute times include host I/O (equal for all configs).
- Old numbers (Spark 10.9→87.8 s, pandas 59.7 s) used a count-only workload, uncapped Spark and float32 pandas: superseded.
- Re-run: `.\benchmarks\run_benchmark.ps1` (resumable), then the `analyze_benchmark.py` command in its docstring.

Streaming v1 (Part 05, ILINet, 5 tiny files, no metrics) is superseded by **streaming v2** (2026-10-11): raw GHCN 2025
1 Jan–1 Mar split into 60 daily files (`scala/PrepareStreamingV2.scala` → HDFS `/flu_project/streaming_v2/source`), dripped
by atomic HDFS renames (`benchmarks/drip_streaming_v2.ps1` → `streaming/drip_feed.sh` in flu-namenode), processed by
`scala/StreamingV2.scala` (5 s trigger, QC filter + broadcast station join → state×day×element aggregates appended to HDFS,
progress listener + per-batch file log), analysed by `python/analyze_streaming.py` → `results/streaming_summary.json`,
`docs/figures/streaming_latency.png`. 59 batches, 2,122,649 US observations → 9,060 aggregates; all checks pass.
Re-run: rerun PrepareStreamingV2 (refills source), clear `streaming_v2/{incoming,output,checkpoint}`, start StreamingV2, then drip.

**Dashboard:** `docker compose up -d dashboard` → http://localhost:8501; pages via `?page=overview|cluster|batch|streaming|forecast|graph`
(`&view=benchmark|tuning` on batch, `&shot=1` collapses the sidebar). Screenshots: `.\scripts\capture_dashboard.ps1` (drives headless
Edge over the DevTools protocol — `msedge --screenshot` only captures Streamlit's loading skeleton). Map data from
`python/rigour/dashboard_data.py` → `results/dashboard/`.

Data decisions: MMWR epi-weeks; GHCN yearly CSVs have an undocumented header row, drop QC-flagged values, values in tenths;
ILINet "X" = missing, `WEIGHTED_ILI` 99.85% null → target `UNWEIGHTED_ILI`; NY + NYC merged by summing `ILITOTAL`/`TOTAL_PATIENTS` (782 dup state-weeks);
graph from climate only (z-score, Gaussian kernel σ≈1.86, no label leakage); edges `state_j → state_i`, `edge_attr` = similarity.
Model config: hidden 16, Adam 1e-3, wd 1e-5, clip 1.0, batch 8, 100 epochs, input NaN check, standardisation fitted on train only.

---

## 7. Part status

| Part | Content | Status |
|---|---|---|
| 01 | Cluster + raw data into HDFS | Done (fsck HEALTHY) |
| 02 | Spark preprocessing, aggregation, join | Done |
| 03 | Graph construction + Spark baseline | Done |
| 04 | PyTorch GNN | **Rigour pass done 2026-10-11** — claims C1–C3 not supported (see section 6) |
| 05 | Streaming ingestion | **v2 done 2026-10-11** (live GHCN feed with measured throughput/latency) |
| 06 | Scalability benchmark Spark vs pandas | **Done 2026-10-10** (fair protocol, 71 runs) |
| 07 | Manuscript, PPT, updated proposal | Not started |

Step 1 of section 10 (cluster verification) — **DONE 2026-10-10**: evidence in `results/cluster_verification.json`,
`results/cluster_smoke_test.json` (all table counts re-measured from HDFS), `logs/step1_yarn_mapreduce_pi.log`,
15 screenshots in `docs/screenshots/`.

## 8. Issues log (fixed)

1 `docker cp` 31 GB failed → bind mount + put. 2 PowerShell `$y` expansion → single quotes. 3 2000–2008 at repl 1 → `setrep -w 2`.
4 REPL line-by-line → `:paste`. 5 NPE at 1 GB → 4g + DISK_ONLY. 6 WSL vhdx crash (C: full) → Docker disk to E:. 7 safe mode → wait.
8 Preprocess wrong columns → backticks + rerun. 9 WEIGHTED_ILI null → UNWEIGHTED_ILI. 10 NY/NYC dups → merge. 11 GNN NaN → drop nulls, fix edge direction, NaN check, clipping.
12 streaming perms/CRLF/fake success → chmod, PowerShell loop, redo. 13 compose indentation → rewrite (now valid). 14 shutdown mid-Part 06 → no loss.
15 nested `-get` export → delete dest, re-export. 16 pandas header `DATA_VALUE` → `skiprows=1`.
17 (2026-10-10) flu Hadoop containers had been removed and their names taken by the misinformation project → renamed ours to `flu-*`; HDFS volumes intact.
18 pytorch packages lost → `docker/pytorch/Dockerfile`. 19 PowerShell pipe BOM broke `:paste` → `scripts/run_scala.ps1`.
20 YARN 0 active nodes (hyphen in env var name) → `hadoop.env` with `___` encoding; pi job SUCCEEDED.
21 historyserver mapped on 19888 but listens on 8188 → port fixed.
22 typed Dataset NPE in `OuterScopes` (case classes in the same `:paste`) → `scala/lib/ClimaFluTypes.scala` preloaded with `-i`.
23 Spark driver 4g killed by the VM-wide OOM killer → 3g while the cluster runs.
24 Part 06 Experiment B "6 GB" kills were VM-wide, not the container cap → kernel-log audit added, wording corrected.
25 (2026-10-10) tuning v1 kept 12 shuffles per config inside the container → Docker VHDX filled E:, WSL crashed →
   shuffle released between iterations, `spark.local.dir` on a bind mount, two-drive watchdog; v1 archived.
26 Stopping a `docker exec` from the host (TaskStop / Ctrl-C) does NOT stop the processes inside the container: list them via
   `/proc/*/cmdline` (the python image has no ps/pkill) and `kill` them, then clear partial outputs.
27 Folders created by the root `pytorch` container are 755 → the spark user (uid 185) cannot write; `chmod -R a+rwX` them.
28 Spark `partitionBy(col)` drops that column from the written files → partition on a copy (`day`) when files are reused.
29 PowerShell 5 mangles `"` inside native arguments (inline `bash -c` scripts) → put the script in a `.sh` file, feed via `cmd /c "... < file"`.
30 A bind mount nested inside a read-only bind mount cannot be created → dashboard code at `/app/code`, siblings for results/docs.

## 9. Known weaknesses (reviewer will find these)

1–3. ~~Test-set checkpoint, single run, no ablations~~ RESOLVED 2026-10-11 (Part 04 v2). **New central weakness: the GNN's
   advantage is not significant and the graph does not help** — the paper framing must change (see section 10, Part 07).
4. ~~Benchmark fairness~~ RESOLVED 2026-10-10 (equal caps, I/O split, core sweep, checksum gate — docs/BENCHMARK_PROTOCOL.md).
5. ~~No MapReduce/YARN job~~ RESOLVED. 6. No dashboard (rubric). 7. ~~No RDD / typed Dataset evidence, no tuning experiment~~ RESOLVED 2026-10-11.
8. ~~Base paper~~ FOUND (docs/BASE_PAPER.md; full texts paywalled — read them via college IEEE access). 9. Stale README, scripts, proposal (mentions WEIGHTED_ILI, humidity, "Group 12").
10. ~~GNN gain over GBT is modest~~ → measured properly: not significant (0.924 vs 0.940, DM p = 0.058).

## 10. Remaining work, in order (stop after each Part)

1. ~~Verification + screenshots + 4040~~ DONE 2026-10-10.
2. ~~Finish Part 06~~ DONE 2026-10-10.
3. ~~Rubric gaps: MapReduce, RDD + typed Dataset, tuning~~ DONE 2026-10-11.
4. ~~Rigour pass on Part 04~~ DONE 2026-10-11. Re-run: `docker exec -w /workspace/rigour pytorch python prepare_v2.py`,
   `baselines_v2.py`, `.\scripts\run_scala.ps1 BaselineGBTv2 -DriverMemory 3g`, `run_all.py --workers 6` (~15 min), then
   `docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/rigour/evaluate_v2.py`.
5. ~~Dashboard + streaming v2 metrics~~ DONE 2026-10-11.
6. ~~Evidence + compliance matrix~~ DONE 2026-10-11: **21/21 VERIFIED** (`docs/COURSE_COMPLIANCE_MATRIX.md`, `docs/RESEARCH_EVIDENCE_MATRIX.md`); resume/re-verify steps in `docs/RESUME.md`.
7. Repo cleanup (with permission), README rewrite, `.gitignore` for datasets/exports/models.
7b. ~~Base paper + novelty experiment (v3)~~ DONE 2026-10-11 (`docs/BASE_PAPER.md` sections 1–6, `docs/V3_RESULTS.md`).
8. Part 07: IEEE manuscript, PPT, updated proposal (gap / problem statement / objectives), base paper.
