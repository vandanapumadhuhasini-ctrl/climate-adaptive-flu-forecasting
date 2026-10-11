# Resume / re-verify checklist (ClimaFlu-GNN)

Use after a reboot or in a new session. Read `PROJECT_GUIDE.md` first (rules, pitfalls, issue log).

## 1. Before starting anything
- Free space: **C: ≥ 15 GB, E: ≥ 15 GB** (the Docker VHDX on E: never shrinks; Spark shuffles go to `.spark-local` on C:).
- Do not run the misinformation project's cluster at the same time (same host ports).

## 2. Start and check the cluster
```powershell
docker compose up -d
docker exec flu-namenode hdfs dfsadmin -safemode get      # wait for OFF
.\scripts\verify_cluster.ps1                               # HDFS / YARN / UIs -> results\cluster_verification.json
```

## 3. Final evidence (regenerates the matrices; ~5 min)
```powershell
.\scripts\verify_project.ps1                               # atomic snapshot -> results\verification.json
docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/spark_event_summary.py
docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/build_compliance_matrix.py
```
Outputs: `docs/COURSE_COMPLIANCE_MATRIX.md` (21/21 VERIFIED on 2026-10-11), `docs/RESEARCH_EVIDENCE_MATRIX.md`.

## 4. Demo for the professor
- HDFS http://localhost:9870 · YARN http://localhost:8088 · YARN timeline http://localhost:8188 (MR1/MR2 SUCCEEDED)
- Dashboard http://localhost:8501 (pages: overview, cluster, batch, streaming, forecast, graph)
- Live Spark UI: `.\scripts\run_scala.ps1 SparkUiDemo -DriverMemory 3g`, then http://localhost:4040 for 120 s

## 5. Where each stage's evidence comes from (re-run only if needed)
| Stage | Command | Output |
|---|---|---|
| MapReduce on YARN | `docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh` then `.\scripts\summarize_mapreduce.ps1` | results/mapreduce_jobs.json |
| RDD + typed Dataset | `.\scripts\run_scala.ps1 RddDatasetAudit -DriverMemory 3g -Preload lib/ClimaFluTypes.scala` | results/cross_engine_consistency.json |
| Veracity / Variety / DF plan | `.\scripts\run_scala.ps1 VeracityAudit -DriverMemory 3g` | results/veracity_audit.json, variety_inventory.json |
| Benchmark | `.\benchmarks\run_benchmark.ps1` + analyze_benchmark.py | results/benchmark_summary.json |
| Tuning (~8 h) | `.\benchmarks\run_tuning.ps1` + analyze_tuning.py | results/tuning_summary.json |
| Streaming | see PROJECT_GUIDE.md "streaming v2" | results/streaming_summary.json |
| GNN rigour pass | prepare_v2 → baselines_v2 → BaselineGBTv2 → run_all → evaluate_v2 | results/rigour/summary.json |
| Dashboard screenshots | `.\scripts\capture_dashboard.ps1` | docs/screenshots/dashboard_*.png |

## 6. Added after 2026-10-11 (v3, HDFS publishing, audit)
| Stage | Command | Output |
|---|---|---|
| v3 features on HDFS (lag scan, horizons 1–4) | `.\scripts\run_scala.ps1 LagAwareFeaturesV3 -DriverMemory 3g` | HDFS `processed/v3_features/h1..h4`, results/v3/lag_scan.json |
| v3 models | `docker exec -w /workspace/rigour pytorch python run_all_v3.py --workers 6` + `.\scripts\run_scala.ps1 BaselineGBTv3 -DriverMemory 3g` | results/v3/runs.jsonl, predictions |
| v3 evaluation | `docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/rigour/evaluate_v3.py` | docs/V3_RESULTS.md, results/v3/summary.json |
| Publish everything to HDFS | `.\scripts\run_scala.ps1 PublishResultsToHdfs -DriverMemory 3g` | HDFS `/flu_project/results/`, results/hdfs_publish_manifest.json |
| Audit pass 1 (Python, local files) | `docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/audit_project.py` | results/audit/pass1.json |
| Audit pass 2 (Spark, HDFS copies) | `.\scripts\run_scala.ps1 AuditPass2 -DriverMemory 3g` | results/audit/pass2.json |

Order after any new result: evaluate → `verify_project.ps1` → `spark_event_summary.py` → `build_compliance_matrix.py` →
PublishResultsToHdfs → both audit passes → commit and push.
