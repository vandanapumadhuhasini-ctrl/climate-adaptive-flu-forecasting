"""Data for the dashboard's map views (results files only):
  results/dashboard/state_centroids.csv   state, lat, lon (mean of its GHCN stations), n_stations
  results/dashboard/graph_test_K8.parquet weekly climate KNN graph (K = 8) for every test week, built from training-period
                                          (YEAR <= 2022) statistics exactly as the final GNN-K8 models saw it
Run: docker exec -w /workspace/reporting pytorch python dashboard_data.py
"""
import glob
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "gnn"))   # shared GNN code
from common import DATA, climate_stats, knn_edges

OUT = "/results/dashboard"
os.makedirs(OUT, exist_ok=True)
st = pd.concat([pd.read_parquet(f) for f in glob.glob(f"{DATA}/us_stations/*.parquet")])
st.groupby("state").agg(lat=("lat", "mean"), lon=("lon", "mean"), n_stations=("stationId", "count")).reset_index() \
  .to_csv(f"{OUT}/state_centroids.csv", index=False)

feat = pd.read_parquet("/results/rigour/feature_table.parquet")
zstats, sigma = climate_stats(feat[feat.YEAR <= 2022])
parts = []
for (y, w), wd in feat[feat.YEAR >= 2023].groupby(["YEAR", "WEEK"]):
    e = knn_edges(wd, zstats, sigma, 8)
    e["year"], e["week"] = int(y), int(w)
    parts.append(e)
g = pd.concat(parts)
g.to_parquet(f"{OUT}/graph_test_K8.parquet", index=False)
print(f"centroids: {st['state'].nunique()} states | test-week edges: {len(g)} over {g[['year', 'week']].drop_duplicates().shape[0]} weeks | sigma {sigma:.4f}")
