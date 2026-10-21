"""Rigour pass step 1: canonical feature table with exact calendar lags + check that the Python graph builder
reproduces the Spark KNN graph (GraphConstruction.scala) when given the same all-years statistics; then the
deterministic Python baselines (docs/RESEARCH_PROTOCOL.md section 5):
  Persistence: y_hat = ILI_lag1        Ridge: alpha = 1.0 on all features, standardised on YEAR <= 2022
Outputs: results/rigour/feature_table.parquet, results/rigour/prepare_summary.json,
         results/rigour/predictions/{Persistence,Ridge}.parquet (same columns as the neural predictions)
Run: docker exec -w /workspace/gnn pytorch python prepare_v2.py
"""
import glob
import json
import os

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from common import CLIMATE, DATA, FEATURES, OUT, build_feature_table, knn_edges, load_modeling_table, rmse

os.makedirs(OUT, exist_ok=True)
raw = load_modeling_table()
feat, counts = build_feature_table(raw)
feat.drop(columns=["week_start"]).assign(week_start=feat["week_start"].astype(str)).to_parquet(f"{OUT}/feature_table.parquet", index=False)

# v1 lag rule (previous row) vs exact calendar rule: how many rows differ?
d = raw.dropna(subset=CLIMATE + ["UNWEIGHTED_ILI"]).sort_values(["state_abbrev", "YEAR", "WEEK"]).copy()
g = d.groupby("state_abbrev")["UNWEIGHTED_ILI"]
d["v1_lag1"], d["v1_target"] = g.shift(1), g.shift(-1)
m = d.merge(feat[["state_abbrev", "YEAR", "WEEK", "ILI_lag1", "target"]], on=["state_abbrev", "YEAR", "WEEK"], how="left")
v1_rows = m.dropna(subset=["v1_lag1", "v1_target"])
changed = int(((v1_rows["v1_lag1"] != v1_rows["ILI_lag1"]) | (v1_rows["v1_target"] != v1_rows["target"])).sum())

# Graph builder check: Spark used the full modeling table (nulls ignored by avg/stddev), all years, K = 8.
spark_edges = pd.concat([pd.read_parquet(f) for f in glob.glob(f"{DATA}/graph_edges_knn/*.parquet")])
spark_edges = spark_edges.dropna(subset=["similarity"])
full = raw.dropna(subset=CLIMATE)
zstats = {c: (float(raw[c].mean()), float(raw[c].std())) for c in CLIMATE}
dists = []
for _, w in full.groupby(["YEAR", "WEEK"]):
    z = np.column_stack([(w[c].values - zstats[c][0]) / zstats[c][1] for c in CLIMATE])
    dd = np.sqrt(((z[:, None, :] - z[None, :, :]) ** 2).sum(-1))
    dists.append(dd[~np.eye(len(z), dtype=bool)])
sigma = float(np.concatenate(dists).mean())
py_edges = []
for (y, w), wd in full.groupby(["YEAR", "WEEK"]):
    e = knn_edges(wd, zstats, sigma, 8); e["YEAR"], e["WEEK"] = y, w
    py_edges.append(e)
py_edges = pd.concat(py_edges)
key = ["state_i", "state_j", "YEAR", "WEEK"]
both = spark_edges.merge(py_edges, on=key, suffixes=("_spark", "_py"))
agree = len(both) / len(spark_edges)
max_sim_diff = float((both["similarity_spark"] - both["similarity_py"]).abs().max())

summary = {
    **counts,
    "states": int(feat["state_abbrev"].nunique()),
    "years": [int(feat["YEAR"].min()), int(feat["YEAR"].max())],
    "split_rows": {"select_train_le2021": int((feat.YEAR <= 2021).sum()), "select_val_2022": int((feat.YEAR == 2022).sum()),
                   "final_train_le2022": int((feat.YEAR <= 2022).sum()), "test_ge2023": int((feat.YEAR >= 2023).sum())},
    "test_weeks": int(feat[feat.YEAR >= 2023][["YEAR", "WEEK"]].drop_duplicates().shape[0]),
    "v1_rows_whose_lag1_or_target_changes_under_exact_calendar_rule": changed,
    "graph_builder_check": {"spark_edges": len(spark_edges), "python_edges": len(py_edges), "matched_edges": len(both),
                            "agreement": round(agree, 5), "sigma_python": round(sigma, 4),
                            "max_abs_similarity_diff_on_matched": max_sim_diff, "passes_99pct_rule": agree >= 0.99},
}
with open(f"{OUT}/prepare_summary.json", "w") as f:
    json.dump(summary, f, indent=2)
print(json.dumps(summary, indent=2))

# ---------------------------------------------------------------- deterministic Python baselines (read back from the saved table)
feat = pd.read_parquet(f"{OUT}/feature_table.parquet")
train, test = feat[feat.YEAR <= 2022], feat[feat.YEAR >= 2023]
os.makedirs(f"{OUT}/predictions", exist_ok=True)
base = test[["state_abbrev", "YEAR", "WEEK", "target"]].rename(columns={"state_abbrev": "state", "YEAR": "year", "WEEK": "week"})
base.assign(pred=test["ILI_lag1"].values).to_parquet(f"{OUT}/predictions/Persistence.parquet", index=False)
ridge = make_pipeline(StandardScaler(), Ridge(alpha=1.0)).fit(train[FEATURES], train["target"])
base.assign(pred=ridge.predict(test[FEATURES])).to_parquet(f"{OUT}/predictions/Ridge.parquet", index=False)
print("Persistence RMSE", round(rmse(test["ILI_lag1"], test["target"]), 4),
      "| Ridge RMSE", round(rmse(ridge.predict(test[FEATURES]), test["target"]), 4))
