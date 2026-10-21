"""Independent audit, pass 1. Recomputes every headline number from the raw result files with code written separately from
the analysis scripts (own RMSE, Diebold-Mariano, Holm, medians, SHA-256), and checks the numbers stated in the docs.
Output: results/audit/pass1.json + a PASS/FAIL table on stdout.
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/audit_project.py
"""
import glob
import hashlib
import json
import math
import os
import re
import statistics

import numpy as np
import pandas as pd
from scipy import stats

P = "/proj"
checks = []


def check(area, name, claimed, measured, ok):
    checks.append({"area": area, "check": name, "claimed": claimed, "measured": measured, "pass": bool(ok)})


def jl(path):
    return [json.loads(l) for l in open(f"{P}/{path}", encoding="utf-8-sig") if l.strip()]


def js(path):
    return json.load(open(f"{P}/{path}", encoding="utf-8-sig"))


def near(a, b, tol):
    return a is not None and b is not None and abs(a - b) <= tol


def sha_lines(lines):
    return hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest()


# ---------------------------------------------------------------- volume
cv, sm = js("results/cluster_verification.json"), js("results/cluster_smoke_test.json")
check("Volume", "raw data in HDFS >= 25 GB", ">= 25 GB (README: 34.6 GB)", cv["hdfs"]["raw_logical_GB"], cv["hdfs"]["raw_logical_GB"] >= 25)
check("Volume", "cleaned weather rows", 303631696, sm["tables"]["weather_cleaned"]["rows"], sm["tables"]["weather_cleaned"]["rows"] == 303631696)

# ---------------------------------------------------------------- MapReduce: counters straight from the job log
log = open(f"{P}/logs/mapreduce_mr1_mr2.log", encoding="utf-8-sig", errors="ignore").read()
mr1_part = log.split("== MR2 start")[0]
cnt = {k: int(re.search(rf"^\s*{k}=(\d+)", mr1_part, re.M).group(1)) for k in
       ("KEPT", "NOT_US", "OTHER_ELEMENT", "QC_FLAGGED", "HEADER", "Map input records")}
mal = re.search(r"^\s*MALFORMED=(\d+)", mr1_part, re.M)
total = sum(cnt[k] for k in ("KEPT", "NOT_US", "OTHER_ELEMENT", "QC_FLAGGED", "HEADER")) + (int(mal.group(1)) if mal else 0)
check("MapReduce", "record accounting (log counters)", cnt["Map input records"], total, total == cnt["Map input records"])
check("MapReduce", "raw records (README 968.9 M)", 968905773, cnt["Map input records"], cnt["Map input records"] == 968905773)
check("MapReduce", "kept rows = weather_cleaned rows", 303631696, cnt["KEPT"], cnt["KEPT"] == 303631696)
cc = js("results/cross_engine_consistency.json")
mr1_sha = sha_lines([l.rstrip("\n") for l in open(f"{P}/mapreduce/output/mr1_element_year.txt") if l.strip()])
mr2_sha = sha_lines([l.rstrip("\n") for l in open(f"{P}/mapreduce/output/mr2_state_year_element.txt") if l.strip()])
check("Consistency", "MR1 output SHA (recomputed from MR text output)", cc["element_year"]["sha256"]["spark_dataframe"][:12], mr1_sha[:12],
      mr1_sha == cc["element_year"]["sha256"]["spark_dataframe"])
check("Consistency", "MR2 output SHA = Spark DataFrame join SHA", cc["state_year_element"]["sha256"]["spark_dataframe"][:12], mr2_sha[:12],
      mr2_sha == cc["state_year_element"]["sha256"]["spark_dataframe"])
check("Consistency", "DataFrame rewrite reproduces existing HDFS tables", True, cc["dataframe_rewrite_equivalence"]["all_identical"],
      cc["dataframe_rewrite_equivalence"]["all_identical"] is True)
API_RE = re.compile(r"\.rdd\b|sparkContext\.textFile|\bsc\.textFile|read\.textFile|reduceByKey|parallelize|joinWith|groupByKey|" r"\bDataset\[|\.as\[|Aggregator\[|mapPartitions|toDebugString")
api_hits = [f"{p}:{i}" for p in glob.glob(f"{P}/scala/**/*.scala", recursive=True) + glob.glob(f"{P}/python/**/*.py", recursive=True)
            + glob.glob(f"{P}/dashboard/*.py") for i, line in enumerate(open(p, encoding="utf-8", errors="ignore"), 1)
            if API_RE.search(line) and not line.lstrip().startswith(("//", "#")) and "API_RE" not in line]
check("Spark API", "RDD / typed-Dataset API calls in scala/, python/, dashboard/ (DataFrame only)", 0, len(api_hits), not api_hits)

# ---------------------------------------------------------------- benchmark (raw per-run records)
runs = jl("results/benchmark/runs.jsonl")
def med(label, y):
    v = [r["elapsed_s"] for r in runs if r["label"] == label and r["years"] == y and r["status"] == "ok" and r["experiment"] == "A"]
    return statistics.median(v) if v else None
s26, p26 = med("spark-hdfs-local[*]", 26), med("pandas-chunked", 26)
check("Benchmark", "Spark 26 years median (README 90.1 s)", 90.1, round(s26, 3), near(s26, 90.1, 0.05))
check("Benchmark", "pandas-chunked 26 years (README 1,619 s)", 1619, round(p26, 1), near(p26, 1619, 0.5))
check("Benchmark", "speed-up (README 18.0x)", 18.0, round(p26 / s26, 2), near(p26 / s26, 18.0, 0.05))
sha_by_y = {}
for r in runs:
    if r["status"] == "ok":
        sha_by_y.setdefault(r["years"], set()).add(r["result_sha256"])
check("Benchmark", "identical result SHA across engines, every size", "1 SHA per size", {y: len(s) for y, s in sorted(sha_by_y.items())},
      all(len(s) == 1 for s in sha_by_y.values()))
check("Benchmark", "26-year benchmark SHA = MapReduce MR1 SHA", mr1_sha[:12], list(sha_by_y[26])[0][:12], list(sha_by_y[26])[0] == mr1_sha)
oom = [r for r in runs if r["experiment"] == "A" and r["label"] in ("pandas-naive", "pandas-optimized", "pandas-pyarrow") and r["years"] == 1]
check("Benchmark", "in-memory pandas OOM at 1 year under 4 GB", "all oom_killed", [r["status"] for r in oom], oom and all(r["status"] == "oom_killed" for r in oom))

# ---------------------------------------------------------------- streaming (raw logs)
drip, bat = jl("results/streaming_v2/drip.jsonl"), jl("results/streaming_v2/batches.jsonl")
end = {}
for b in bat:
    for f in b["files"]:
        end.setdefault(f, []).append(b["end_ms"])
lat = sorted((end[d["file"]][0] - d["arrival_ms"]) / 1000 for d in drip)
rows_in = sum(b["rows_in"] for b in bat)
busy = sum(b["end_ms"] - b["start_ms"] for b in bat) / 1000
span = (drip[-1]["arrival_ms"] - drip[0]["arrival_ms"]) / 1000
check("Streaming", "median latency (README 4.7 s)", 4.7, round(statistics.median(lat), 2), near(statistics.median(lat), 4.7, 0.05))
check("Streaming", "throughput (README 38.1k rows/s)", 38.1, round(rows_in / busy / 1000, 2), near(rows_in / busy / 1000, 38.1, 0.05))
check("Streaming", "arrival rate (README 15.3k rows/s)", 15.3, round(rows_in / span / 1000, 2), near(rows_in / span / 1000, 15.3, 0.05))
check("Streaming", "each file exactly once, rows = source", "60 files once, 5,972,783 rows", (len(end), max(len(v) for v in end.values()), rows_in),
      len(end) == 60 and all(len(v) == 1 for v in end.values()) and rows_in == 5972783)

# ---------------------------------------------------------------- tuning
tr = jl("results/tuning/runs.jsonl")
def tmed(label):
    v = [r["total_s"] for r in tr if r["label"] == label and r.get("status", "ok") == "ok"]
    return statistics.median(v) if v else None
check("Tuning", "Spark defaults AQE (README 285 s)", 285.2, tmed("spark-defaults-aqe-p200-none"), near(tmed("spark-defaults-aqe-p200-none"), 285.2, 0.05))
grid = {l: tmed(l) for l in {r["label"] for r in tr} if not l.startswith("spark-defaults")}
best = min((v, l) for l, v in grid.items() if v is not None)
check("Tuning", "AQE beats every grid configuration", f"< best grid", f"best grid {best[1]} {best[0]}", tmed("spark-defaults-aqe-p200-none") < best[0])
check("Tuning", "p24 + caching failed (OOM)", "failed", [r.get("status") for r in tr if r["label"] == "p24-memory_and_disk"],
      any(r.get("status") == "failed" for r in tr if r["label"] == "p24-memory_and_disk"))

# ---------------------------------------------------------------- forecasting: own RMSE / DM / Holm
def rmse(p, y):
    return float(np.sqrt(np.mean((np.asarray(p) - np.asarray(y)) ** 2)))

def load_pred(path):
    df = pd.concat(pd.read_parquet(f) for f in sorted(glob.glob(path)))
    df[["year", "week"]] = df[["year", "week"]].astype(int)
    return df.sort_values(["state", "year", "week"]).reset_index(drop=True)

def dm_test(df, pa, pb, lags=4):
    d = pd.Series((pa - df.target.values) ** 2 - (pb - df.target.values) ** 2).groupby([df.year.values, df.week.values]).mean().values
    T, m = len(d), d.mean()
    acov = lambda k: float(np.sum((d[k:] - m) * (d[:T - k] - m)) / T)
    v = acov(0) + 2 * sum((1 - k / (lags + 1)) * acov(k) for k in range(1, lags + 1))
    stat = m / math.sqrt(v / T) * math.sqrt((T - 1) / T)
    return 2 * stats.t.sf(abs(stat), T - 1)

def holm(ps):
    idx = sorted(range(len(ps)), key=lambda i: ps[i])
    out, run = [0] * len(ps), 0
    for r, i in enumerate(idx):
        run = max(run, min(1, (len(ps) - r) * ps[i])); out[i] = run
    return out

base = load_pred(f"{P}/results/rigour/predictions/GNN-K8_s0.parquet")
gnn = [load_pred(f) for f in sorted(glob.glob(f"{P}/results/rigour/predictions/GNN-K8_s*.parquet"))]
gbt = load_pred(f"{P}/results/rigour/predictions_spark/GBT-lags/*.parquet")
nog = [load_pred(f) for f in sorted(glob.glob(f"{P}/results/rigour/predictions/MLP-noGraph_s*.parquet"))]
assert all(g[["state", "year", "week"]].equals(base[["state", "year", "week"]]) for g in gnn + [gbt] + nog)
r_g = [rmse(g.pred, g.target) for g in gnn]
check("Forecast v2", "GNN-K8 RMSE mean ± sd (0.9236 ± 0.0159)", "0.9236 ± 0.0159", f"{np.mean(r_g):.4f} ± {np.std(r_g, ddof=1):.4f}",
      near(np.mean(r_g), 0.9236, 5e-5) and near(np.std(r_g, ddof=1), 0.0159, 5e-5))
check("Forecast v2", "Spark GBT-lags RMSE (0.9401)", 0.9401, round(rmse(gbt.pred, gbt.target), 4), near(rmse(gbt.pred, gbt.target), 0.9401, 5e-5))
check("Forecast v2", "MLP-noGraph RMSE (0.9181)", 0.9181, round(np.mean([rmse(g.pred, g.target) for g in nog]), 4),
      near(np.mean([rmse(g.pred, g.target) for g in nog]), 0.9181, 5e-5))
ens = np.mean([g.pred.values for g in gnn], axis=0)
p_v2 = dm_test(base, ens, gbt.pred.values)
check("Forecast v2", "DM p GNN vs GBT (0.0582, not significant)", 0.0582, round(p_v2, 4), near(p_v2, 0.0582, 5e-4) and p_v2 > 0.05)

v3 = {}
for h in (1, 2, 3, 4):
    gs = [load_pred(f) for f in sorted(glob.glob(f"{P}/results/v3/predictions/GNN-lagK8_h{h}_s*.parquet"))]
    gk = [load_pred(f) for f in sorted(glob.glob(f"{P}/results/v3/predictions/GNN-K8_h{h}_s*.parquet"))]
    gb = load_pred(f"{P}/results/v3/predictions_spark/GBT-lags_h{h}/*.parquet")
    assert all(g[["state", "year", "week"]].equals(gb[["state", "year", "week"]]) for g in gs)
    e = np.mean([g.pred.values for g in gs], axis=0)
    v3[h] = {"rm": [rmse(g.pred, g.target) for g in gs], "gbt": rmse(gb.pred, gb.target), "p": dm_test(gb, e, gb.pred.values),
             "identical": all(np.allclose(a.pred, b.pred) for a, b in zip(gs, gk)), "n_seeds": len(gs)}
adj = holm([v3[h]["p"] for h in (1, 2, 3, 4)])
h3 = v3[3]
wins3 = sum(r < h3["gbt"] for r in h3["rm"])
check("Forecast v3", "h=3 GNN-lagK8 mean RMSE (1.2507)", 1.2507, round(np.mean(h3["rm"]), 4), near(np.mean(h3["rm"]), 1.2507, 5e-5))
check("Forecast v3", "h=3 Spark GBT-lags RMSE (1.2891)", 1.2891, round(h3["gbt"], 4), near(h3["gbt"], 1.2891, 5e-5))
check("Forecast v3", "h=3 Holm-adjusted DM p (0.0169 < 0.05)", 0.0169, round(adj[2], 4), near(adj[2], 0.0169, 5e-4) and adj[2] < 0.05)
check("Forecast v3", "h=3 seeds beating GBT (9/10, rule needs >= 8)", 9, wins3, wins3 == 9)
check("Forecast v3", "h=4 fails seed rule (7/10)", 7, sum(r < v3[4]["gbt"] for r in v3[4]["rm"]), sum(r < v3[4]["gbt"] for r in v3[4]["rm"]) == 7)
check("Forecast v3", "GNN-lagK8 identical to GNN-K8 (lead 0)", True, all(v3[h]["identical"] for h in v3), all(v3[h]["identical"] for h in v3))
check("Forecast v3", "10 seeds per horizon", 10, [v3[h]["n_seeds"] for h in v3], all(v3[h]["n_seeds"] == 10 for h in v3))
lag = js("results/v3/lag_scan.json")
check("Forecast v3", "lag scan chose lead 0 everywhere", "all 0", lag["chosen_leads"], all(v == 0 for h in lag["chosen_leads"].values() for v in h.values()))
check("Forecast v3", "Spark h=1 features identical to v2", True, lag["v2_consistency_h1"]["identical"], lag["v2_consistency_h1"]["identical"])

# ---------------------------------------------------------------- MLlib comparison (A2): own RMSE from the raw predictions
if os.path.exists(f"{P}/results/mllib/summary.json"):
    sel = [json.loads(l) for l in open(f"{P}/results/mllib/selection.jsonl") if l.strip()]
    check("MLlib", "24 configurations (3 models x 2 feature sets x 4 horizons)", 24, len(sel), len(sel) == 24)
    mism = 0
    for s in sel:
        d = load_pred(f"{P}/results/mllib/predictions/{s['model']}_{s['features']}_h{s['horizon']}/*.parquet")
        mism += not near(rmse(d.pred, d.target), s["test_rmse"], 1e-9)
    check("MLlib", "recomputed test RMSE = logged RMSE (all 24)", 0, mism, mism == 0)
    rf1 = load_pred(f"{P}/results/mllib/predictions/RandomForest_all_h1/*.parquet")
    check("MLlib", "tuned RF + climate, h=1 (0.9240)", 0.9240, round(rmse(rf1.pred, rf1.target), 4), near(rmse(rf1.pred, rf1.target), 0.9240, 5e-5))
    best_by_h = {}
    for h in (1, 2, 3, 4):
        cand = {f"{s['model']}-{s['features']}": s["test_rmse"] for s in sel if s["horizon"] == h}
        best_by_h[h] = min(cand, key=cand.get)
    check("MLlib", "best MLlib model per horizon", "RF-all, GBT-all, RF-all, RF-all", best_by_h,
          best_by_h == {1: "RandomForest-all", 2: "GBT-all", 3: "RandomForest-all", 4: "RandomForest-all"})
    ml = js("results/mllib/summary.json")
    check("MLlib", "claim M1 (GNN beats best tuned MLlib) not supported", False, ml["claim_M1"]["holds"], ml["claim_M1"]["holds"] is False)
    lin_worst = all(min(s["test_rmse"] for s in sel if s["horizon"] == h and s["model"] == "LinearRegression") >
                    max(s["test_rmse"] for s in sel if s["horizon"] == h and s["model"] != "LinearRegression") for h in (1, 2, 3, 4))
    check("MLlib", "Linear Regression worst at every horizon", True, lin_worst, lin_worst)
    rf_clim = all(next(s["test_rmse"] for s in sel if s["horizon"] == h and s["model"] == "RandomForest" and s["features"] == "all") <
                  next(s["test_rmse"] for s in sel if s["horizon"] == h and s["model"] == "RandomForest" and s["features"] == "lags") for h in (1, 2, 3, 4))
    check("MLlib", "climate improves tuned RF at every horizon", True, rf_clim, rf_clim)

# ---------------------------------------------------------------- veracity
va = js("results/veracity_audit.json")
check("Veracity", "WEIGHTED_ILI missing (99.85 %)", 0.9985, round(va["ilinet_raw"]["weighted_ili_missing_fraction"], 4),
      near(va["ilinet_raw"]["weighted_ili_missing_fraction"], 0.9985, 5e-5))
check("Veracity", "NY+NYC weeks raw / merged (827 / 782)", "827/782",
      f"{va['ilinet_raw']['new_york_plus_nyc_duplicate_state_weeks_raw']}/{va['ilinet_raw']['new_york_plus_nyc_duplicate_state_weeks_after_finaljoin_filter']}",
      va["ilinet_raw"]["new_york_plus_nyc_duplicate_state_weeks_raw"] == 827 and va["ilinet_raw"]["new_york_plus_nyc_duplicate_state_weeks_after_finaljoin_filter"] == 782)

# ---------------------------------------------------------------- numbers written in README
matrix = open(f"{P}/docs/COURSE_COMPLIANCE_MATRIX.md", encoding="utf-8").read()
n_rows = sum(1 for l in matrix.splitlines() if l.startswith("| ") and l.rstrip().endswith("VERIFIED** |"))
readme = open(f"{P}/README.md", encoding="utf-8").read()
for s in ("34.6 GB", "968.9 M", "303.6 M", "90.1 s", "1,619 s", "18.0×", "285 s", "5.97 M", "38.1 k", "15.3 k", "4.7 s", "p = 0.017", f"{n_rows} / {n_rows}"):
    check("Docs", f"README states '{s}'", s, s in readme, s in readme)
n_ver = matrix.count("**VERIFIED**")
check("Docs", "compliance matrix rows VERIFIED (all rows)", n_rows, n_ver, n_ver == n_rows and n_rows >= 19 and "NOT VERIFIED" not in matrix)

os.makedirs(f"{P}/results/audit", exist_ok=True)
with open(f"{P}/results/audit/pass1.json", "w") as f:
    json.dump({"checks": checks, "passed": sum(c["pass"] for c in checks), "total": len(checks)}, f, indent=2, default=str)
for c in checks:
    print(f"{'PASS' if c['pass'] else 'FAIL'} | {c['area']:<12} | {c['check']:<52} | claimed {str(c['claimed'])[:40]:<40} | measured {str(c['measured'])[:60]}")
print(f"\nPASS {sum(c['pass'] for c in checks)} / {len(checks)}")
