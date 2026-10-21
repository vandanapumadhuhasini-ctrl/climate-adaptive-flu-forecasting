# Part 06 benchmark protocol: Spark vs pandas on GHCN-Daily

Written before the full run (2026-10-10) so that the comparison rules cannot be tuned after the results are seen.
Results are in `docs/BENCHMARK_RESULTS.md`; the raw per-run records are in `results/benchmark/runs.jsonl`.

## Question

How do the distributed engine (Spark on HDFS) and the single-node tool (pandas) scale on the project's raw climate data
when both get **the same workload, the same memory budget and the same input files**? Where does each stop working,
and how much of Spark's advantage comes from parallelism and how much from I/O?

## Workload (identical in both engines)

1. Read N yearly GHCN-Daily CSV files, years 2000 … 2000+N−1 (1.23–1.46 GB each, 26 files = 34.59 GB).
2. Keep rows with a US station id (`US…`), element ∈ {TMAX, TMIN, PRCP}, and an empty quality flag.
3. Group by (element, year) and compute `count(*)` and `sum(value)` (value in tenths, summed as 64-bit integers).
4. Sort the result rows `element|year|count|sum` and take their SHA-256.

Implementations: `bigdata_pipeline/spark_tuning/BenchmarkWorkload.scala` (Spark DataFrame API) and `bigdata_pipeline/spark_tuning/benchmark_pandas.py` (pandas).
**Correctness gate:** for every N, every engine that finishes must produce the same SHA-256. If it doesn't, that run is invalid.

## Configurations

| Label | Engine | Input | Parallelism | Purpose |
|---|---|---|---|---|
| spark-hdfs-local[*] | Spark 3.5.1 | HDFS (3 DataNodes, repl 2) | 24 cores | the project's deployment |
| spark-localfs-local[*] | Spark 3.5.1 | the same local files pandas reads (`/rawdata` bind mount) | 24 cores | separates I/O from compute |
| spark-hdfs-local[4] | Spark 3.5.1 | HDFS | 4 cores (`--cpus 4`) | a modest laptop |
| spark-hdfs-local[1] | Spark 3.5.1 | HDFS | 1 core (`--cpus 1`) | same parallelism as single-threaded pandas |
| pandas-naive | pandas 2.2.3 | local files | 1 thread | typical first attempt: all columns as `str`, concat, then filter |
| pandas-optimized | pandas 2.2.3 | local files | 1 thread | `usecols`, category/int32 dtypes, filter each file before concat |
| pandas-pyarrow | pandas 2.2.3 + pyarrow 17 | local files | multi-threaded parser | strongest in-memory pandas |
| pandas-chunked | pandas 2.2.3 | local files | 1 thread | streaming `read_csv(chunksize=2M)`, partial aggregates; bounded memory |

## Fairness controls

* **Fresh container per run** (`docker run --rm`), so no JVM, Python or allocator state carries over between runs.
* **Equal memory cap, swap disabled** (`--memory = --memory-swap`):
  * **Experiment A (main): 4 GB for every engine.** Spark gets `--driver-memory 3g` inside the 4 GB container.
  * **Experiment B: 6 GB, in-memory pandas only**, to find where those variants fail when given more room
    (the original Part 06 "run until failure" item). The Docker VM has 8 GB in total, so 6 GB is the largest safe cap.
* **Same memory metric for both engines:** peak resident set size of the worker process (`VmHWM` from `/proc/self/status`)
  plus the container's cgroup `memory.peak` (which also counts page cache).
* **Timing:** in-process wall-clock time from the first read to the collected result (`elapsed_s`), which excludes
  JVM / interpreter start-up. Start-up-inclusive wall time is recorded too (`wall_s_incl_startup`) and reported.
* **Repeats:** 3 for every Spark local[*] point and for every point up to 5 years; 1 for larger points of slower
  configurations (time budget). The median is reported with min–max. Run 1 is not discarded; caches are warm after it
  (the HDFS blocks sit in the Docker VM page cache and the bind-mounted files in the Windows file cache), so the
  comparison is warm-cache for both engines.
* **Failure rule:** if a configuration is OOM-killed (exit 137) at size N, larger sizes are not attempted and the
  failure is recorded. A failure is a result, not a missing value.
* **Untuned Spark defaults** (e.g. `spark.sql.shuffle.partitions = 200`). Tuning is a separate experiment.
* Runs are strictly sequential; nothing else heavy runs on the machine during the benchmark.

## Known asymmetries (stated, not hidden)

* Spark local[*] uses 24 cores; most pandas variants are single-threaded by design. That is the point of the comparison,
  so local[1] and local[4] are included to show how much of the gap comes from parallelism alone.
* Spark-HDFS reads through the DataNodes over the Docker network, while pandas reads a Windows bind mount.
  `spark-localfs-local[*]` reads exactly the pandas input path, so the I/O path can be compared directly.
* Spark is a single-JVM `local[*]` deployment on HDFS, not a multi-node YARN cluster. Single-node CPU limits still apply.

## Earlier numbers this protocol replaces

The earlier Part 06 numbers (Spark 1→26 years: 10.9 → 87.8 s; optimized pandas 1 year 59.7 s) used a count-only
workload, an uncapped Spark container and `float32` values in pandas, so their memory use and results cannot be compared.
They are superseded by this protocol and are kept only as history.

## Post-run correction (2026-10-10, after the full run) — Experiment B memory limit

The kernel log (`results/benchmark/kernel_oom_log.txt`, captured with `dmesg`) records *why* each run was killed, and
`src/reporting/analyze_performance.py` now matches every `oom_killed` run to its kernel record:

* **Experiment A (4 GB): every kill is `CONSTRAINT_MEMCG`**, i.e. the container's own 4 GB cap (RSS 4.27–4.28 GB at kill).
  Experiment A is valid as designed.
* **Experiment B (6 GB): both kills are `CONSTRAINT_NONE`**, i.e. the whole 8 GB Docker VM ran out of memory
  (RSS 5.04 / 5.10 GB at kill) **before the 6 GB container cap was reached**. The Hadoop cluster (NameNode, 3 DataNodes,
  YARN, timeline server: ~2.8 GB) shares that VM. Experiment B therefore measured "in-memory pandas on a machine with
  ~5 GB actually free", not "6 GB". The one B pass of pandas-optimized at 5.68 GB (smoke pass) happened when more VM memory was free.

The headline Experiment A conclusions are unaffected. Experiment B is reported with this caveat; re-running it with the
Hadoop cluster stopped would give the true 6 GB behaviour (left undone because the benchmark is not decision-relevant there:
in-memory pandas already needs > 4× the input size in RAM per file).
