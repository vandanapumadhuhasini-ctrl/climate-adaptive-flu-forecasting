import glob
import json
import pandas as pd
import numpy as np
import torch
from torch_geometric.data import Data

modeling_files = glob.glob("/data/modeling_table/*.parquet")
edge_files = glob.glob("/data/graph_edges_knn/*.parquet")

df = pd.concat([pd.read_parquet(f) for f in modeling_files], ignore_index=True)
df = df[["state_abbrev", "YEAR", "WEEK", "UNWEIGHTED_ILI", "TMAX", "TMIN", "PRCP", "DIURNAL_RANGE"]]

before_na = len(df)
df = df.dropna(subset=["TMAX", "TMIN", "PRCP", "DIURNAL_RANGE", "UNWEIGHTED_ILI"]).reset_index(drop=True)
print(f"Rows after dropping null climate/ILI values: {len(df)} (dropped {before_na - len(df)})")

edges = pd.concat([pd.read_parquet(f) for f in edge_files], ignore_index=True)
print(f"modeling_table rows: {len(df)} | graph_edges_knn rows: {len(edges)}")

df = df.sort_values(["state_abbrev", "YEAR", "WEEK"]).reset_index(drop=True)
g = df.groupby("state_abbrev")["UNWEIGHTED_ILI"]
for lag in range(1, 5):
    df[f"ILI_lag{lag}"] = g.shift(lag)
df["ILI_trend"] = df["ILI_lag1"] - df["ILI_lag2"]
df["target_next_week_ili"] = g.shift(-1)

before = len(df)
df = df.dropna(subset=["ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "target_next_week_ili"]).reset_index(drop=True)
print(f"Rows after lag/lead + dropna: {len(df)} (dropped {before - len(df)})")

train_df = df[df["YEAR"] <= 2022].copy()
test_df  = df[df["YEAR"] >= 2023].copy()
print(f"Train rows: {len(train_df)} | Test rows: {len(test_df)}")

climate_cols = ["TMAX", "TMIN", "PRCP", "DIURNAL_RANGE"]
stats = {c: {"mean": float(train_df[c].mean()), "std": float(train_df[c].std())} for c in climate_cols}
ili_mean, ili_std = float(train_df["UNWEIGHTED_ILI"].mean()), float(train_df["UNWEIGHTED_ILI"].std())
trend_mean, trend_std = float(train_df["ILI_trend"].mean()), float(train_df["ILI_trend"].std())
stats["ILI"] = {"mean": ili_mean, "std": ili_std}
stats["ILI_trend"] = {"mean": trend_mean, "std": trend_std}

def standardize(frame):
    out = frame.copy()
    for c in climate_cols:
        out[c + "_z"] = (out[c] - stats[c]["mean"]) / stats[c]["std"]
    for lag in range(1, 5):
        out[f"ILI_lag{lag}_z"] = (out[f"ILI_lag{lag}"] - ili_mean) / ili_std
    out["ILI_trend_z"] = (out["ILI_trend"] - trend_mean) / trend_std
    out["target_z"] = (out["target_next_week_ili"] - ili_mean) / ili_std
    return out

train_df = standardize(train_df)
test_df  = standardize(test_df)

feature_cols = ["TMAX_z", "TMIN_z", "PRCP_z", "DIURNAL_RANGE_z",
                 "ILI_lag1_z", "ILI_lag2_z", "ILI_lag3_z", "ILI_lag4_z", "ILI_trend_z"]

def build_snapshots(frame, edges_df):
    snapshots = []
    meta = []
    for (year, week), week_df in frame.groupby(["YEAR", "WEEK"]):
        states = sorted(week_df["state_abbrev"].unique().tolist())
        state_to_idx = {s: i for i, s in enumerate(states)}

        week_df = week_df.set_index("state_abbrev").loc[states]
        x = torch.tensor(week_df[feature_cols].values, dtype=torch.float)
        y = torch.tensor(week_df["target_z"].values, dtype=torch.float)
        y_raw = torch.tensor(week_df["target_next_week_ili"].values, dtype=torch.float)

        wk_edges = edges_df[
            (edges_df["YEAR"] == year) & (edges_df["WEEK"] == week) &
            (edges_df["state_i"].isin(state_to_idx)) & (edges_df["state_j"].isin(state_to_idx))
        ]
        if len(wk_edges) == 0 or len(states) < 2:
            continue

        # neighbor (state_j) is the source, queried state (state_i) is the target
        src = wk_edges["state_j"].map(state_to_idx).values
        dst = wk_edges["state_i"].map(state_to_idx).values
        edge_index = torch.tensor(np.vstack([src, dst]), dtype=torch.long)
        edge_attr = torch.tensor(wk_edges["similarity"].values, dtype=torch.float).unsqueeze(1)

        data = Data(x=x, edge_index=edge_index, edge_attr=edge_attr, y=y)
        snapshots.append(data)
        meta.append({"year": int(year), "week": int(week), "states": states, "y_raw": y_raw.tolist()})
    return snapshots, meta

train_snapshots, train_meta = build_snapshots(train_df, edges)
test_snapshots, test_meta = build_snapshots(test_df, edges)

print(f"Train snapshots: {len(train_snapshots)} | Test snapshots: {len(test_snapshots)}")
avg_nodes = np.mean([d.num_nodes for d in train_snapshots])
avg_edges = np.mean([d.num_edges for d in train_snapshots])
print(f"Avg nodes/snapshot: {avg_nodes:.1f} | Avg edges/snapshot: {avg_edges:.1f}")
print(f"Example snapshot — x: {train_snapshots[0].x.shape} | edge_index: {train_snapshots[0].edge_index.shape} | y: {train_snapshots[0].y.shape}")

torch.save(train_snapshots, "/output/train_snapshots.pt")
torch.save(test_snapshots, "/output/test_snapshots.pt")
with open("/output/train_meta.json", "w") as f:
    json.dump(train_meta, f)
with open("/output/test_meta.json", "w") as f:
    json.dump(test_meta, f)
with open("/output/feature_stats.json", "w") as f:
    json.dump(stats, f, indent=2)

print("Saved: train_snapshots.pt, test_snapshots.pt, train_meta.json, test_meta.json, feature_stats.json")