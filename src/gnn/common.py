"""Shared code for the Part 04 rigour pass (docs/RESEARCH_PROTOCOL.md): features with exact calendar lags,
graph builders (weekly climate KNN, static, geographic, random), PyG snapshots and the model variants."""
import glob
import math
from datetime import date, timedelta

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv

DATA = "/data"
OUT = "/results/rigour"
CLIMATE = ["TMAX", "TMIN", "PRCP", "DIURNAL_RANGE"]
LAGS = ["ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4"]
FEATURES = CLIMATE + LAGS + ["ILI_trend"]          # x column order: 4 climate, then 5 time-series features


def mmwr_week_start(year: int, week: int) -> date:
    """Sunday that starts MMWR week `week` of MMWR year `year` (week 1 contains 4 January)."""
    jan4 = date(year, 1, 4)
    week1_start = jan4 - timedelta(days=(jan4.weekday() + 1) % 7)   # Python: Monday=0 ... Sunday=6
    return week1_start + timedelta(weeks=week - 1)


def load_modeling_table() -> pd.DataFrame:
    df = pd.concat([pd.read_parquet(f) for f in glob.glob(f"{DATA}/modeling_table/*.parquet")], ignore_index=True)
    return df[["state_abbrev", "YEAR", "WEEK", "UNWEIGHTED_ILI"] + CLIMATE]


def build_feature_table(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Exact calendar lags: lag k = same state exactly 7k days earlier; target = exactly 7 days later."""
    d = df.dropna(subset=CLIMATE + ["UNWEIGHTED_ILI"]).copy()
    d["week_start"] = [mmwr_week_start(int(y), int(w)) for y, w in zip(d["YEAR"], d["WEEK"])]
    dup = int(d.duplicated(["state_abbrev", "week_start"]).sum())
    if dup:
        raise RuntimeError(f"{dup} duplicate (state, week_start) rows - MMWR week mapping is inconsistent")
    ili = {(s, ws): v for s, ws, v in zip(d["state_abbrev"], d["week_start"], d["UNWEIGHTED_ILI"])}
    get = lambda s, ws, k: ili.get((s, ws + timedelta(weeks=k)), np.nan)   # noqa: E731
    for k in range(1, 5):
        d[f"ILI_lag{k}"] = [get(s, ws, -k) for s, ws in zip(d["state_abbrev"], d["week_start"])]
    d["target"] = [get(s, ws, 1) for s, ws in zip(d["state_abbrev"], d["week_start"])]
    d["ILI_trend"] = d["ILI_lag1"] - d["ILI_lag2"]
    n0 = len(d)
    d = d.dropna(subset=LAGS + ["target"]).sort_values(["YEAR", "WEEK", "state_abbrev"]).reset_index(drop=True)
    return d, {"rows_with_climate_and_ili": n0, "rows_with_exact_lags_and_target": len(d)}


# ----------------------------------------------------------------------------------------------- graphs
def knn_edges(week_df: pd.DataFrame, zstats: dict, sigma: float, k: int) -> pd.DataFrame:
    """Top-k most climate-similar other states for every state in one week (same rule as GraphConstruction.scala)."""
    z = np.column_stack([(week_df[c].values - zstats[c][0]) / zstats[c][1] for c in CLIMATE])
    dist = np.sqrt(((z[:, None, :] - z[None, :, :]) ** 2).sum(-1))
    sim = np.exp(-dist ** 2 / (2 * sigma ** 2))
    np.fill_diagonal(sim, -np.inf)
    states = week_df["state_abbrev"].values
    rows = []
    for i in range(len(states)):
        for j in np.argsort(-sim[i], kind="stable")[:k]:
            rows.append((states[i], states[j], sim[i, j]))
    return pd.DataFrame(rows, columns=["state_i", "state_j", "similarity"])


def climate_stats(train_rows: pd.DataFrame) -> tuple[dict, float]:
    """z-score statistics (sample std, as Spark's stddev) and kernel bandwidth sigma = mean pairwise distance,
    both from training rows only."""
    zstats = {c: (float(train_rows[c].mean()), float(train_rows[c].std())) for c in CLIMATE}
    dists = []
    for _, w in train_rows.groupby(["YEAR", "WEEK"]):
        z = np.column_stack([(w[c].values - zstats[c][0]) / zstats[c][1] for c in CLIMATE])
        dd = np.sqrt(((z[:, None, :] - z[None, :, :]) ** 2).sum(-1))
        dists.append(dd[~np.eye(len(z), dtype=bool)])
    return zstats, float(np.concatenate(dists).mean())


def static_graph(train_rows: pd.DataFrame, zstats: dict, k: int = 8) -> pd.DataFrame:
    """One graph for all weeks: KNN over each state's mean training-period climate."""
    means = train_rows.groupby("state_abbrev")[CLIMATE].mean().reset_index()
    z = np.column_stack([(means[c].values - zstats[c][0]) / zstats[c][1] for c in CLIMATE])
    dd = np.sqrt(((z[:, None, :] - z[None, :, :]) ** 2).sum(-1))
    sigma = float(dd[~np.eye(len(z), dtype=bool)].mean())
    return knn_edges(means, zstats, sigma, k)


def geo_graph(k: int = 8) -> pd.DataFrame:
    """K nearest states by centroid (mean station lat/lon) great-circle distance, Gaussian-kernel weights."""
    st = pd.concat([pd.read_parquet(f) for f in glob.glob(f"{DATA}/us_stations/*.parquet")])
    c = st.groupby("state")[["lat", "lon"]].mean()
    lat, lon = np.radians(c["lat"].values), np.radians(c["lon"].values)
    dl = lat[:, None] - lat[None, :]; dn = lon[:, None] - lon[None, :]
    a = np.sin(dl / 2) ** 2 + np.cos(lat[:, None]) * np.cos(lat[None, :]) * np.sin(dn / 2) ** 2
    dist = 2 * 6371.0 * np.arcsin(np.sqrt(a))
    sigma = float(dist[~np.eye(len(c), dtype=bool)].mean())
    sim = np.exp(-dist ** 2 / (2 * sigma ** 2)); np.fill_diagonal(sim, -np.inf)
    states = c.index.values
    rows = [(states[i], states[j], sim[i, j]) for i in range(len(states)) for j in np.argsort(-sim[i], kind="stable")[:k]]
    return pd.DataFrame(rows, columns=["state_i", "state_j", "similarity"])


# ----------------------------------------------------------------------------------------------- snapshots
def standardiser(train_rows: pd.DataFrame) -> dict:
    s = {c: (float(train_rows[c].mean()), float(train_rows[c].std())) for c in CLIMATE}
    ili = (float(train_rows["target"].mean()), float(train_rows["target"].std()))
    for c in LAGS:
        s[c] = ili                                   # lags share the ILI scale with the target
    s["ILI_trend"] = (float(train_rows["ILI_trend"].mean()), float(train_rows["ILI_trend"].std()))
    s["target"] = ili
    return s


def snapshots(rows: pd.DataFrame, std: dict, graph_for_week) -> tuple[list, list]:
    """One PyG graph per (YEAR, WEEK). graph_for_week(year, week, week_df) -> edges DataFrame or None (no graph)."""
    out, meta = [], []
    for (y, w), wd in rows.groupby(["YEAR", "WEEK"], sort=True):
        wd = wd.sort_values("state_abbrev")
        states = wd["state_abbrev"].tolist()
        idx = {s: i for i, s in enumerate(states)}
        x = torch.tensor(np.column_stack([(wd[c].values - std[c][0]) / std[c][1] for c in FEATURES]), dtype=torch.float)
        yz = torch.tensor((wd["target"].values - std["target"][0]) / std["target"][1], dtype=torch.float)
        e = graph_for_week(int(y), int(w), wd)
        if e is not None:
            e = e[e["state_i"].isin(idx) & e["state_j"].isin(idx)]
            ei = torch.tensor(np.vstack([e["state_j"].map(idx).values, e["state_i"].map(idx).values]), dtype=torch.long)
            ea = torch.tensor(e["similarity"].values, dtype=torch.float)
        else:
            ei, ea = torch.empty((2, 0), dtype=torch.long), torch.empty(0)
        out.append(Data(x=x, y=yz, edge_index=ei, edge_attr=ea))
        meta.append({"year": int(y), "week": int(w), "states": states, "target": wd["target"].tolist()})
    return out, meta


# ----------------------------------------------------------------------------------------------- models
class ClimaFluGNN(nn.Module):
    """v1 architecture: MLP over the 5 ILI features || 2-layer GCN over the 4 climate features -> fusion MLP."""

    def __init__(self, climate_branch: str = "gcn", hidden: int = 16):
        super().__init__()
        self.climate_branch = climate_branch            # "gcn" | "mlp" | "none"
        self.ts = nn.Sequential(nn.Linear(5, hidden), nn.ReLU(), nn.Linear(hidden, hidden), nn.ReLU())
        if climate_branch == "gcn":
            self.g1, self.g2 = GCNConv(4, hidden), GCNConv(hidden, hidden)
        elif climate_branch == "mlp":
            self.cm = nn.Sequential(nn.Linear(4, hidden), nn.ReLU(), nn.Linear(hidden, hidden), nn.ReLU())
        width = hidden * (1 if climate_branch == "none" else 2)
        self.fusion = nn.Sequential(nn.Linear(width, hidden), nn.ReLU(), nn.Linear(hidden, 1))

    def forward(self, x, edge_index, edge_weight):
        h = self.ts(x[:, 4:9])
        if self.climate_branch == "gcn":
            g = F.relu(self.g1(x[:, 0:4], edge_index, edge_weight))
            g = F.relu(self.g2(g, edge_index, edge_weight))
            h = torch.cat([h, g], dim=1)
        elif self.climate_branch == "mlp":
            h = torch.cat([h, self.cm(x[:, 0:4])], dim=1)
        return self.fusion(h).squeeze(-1)


def rmse(a, b) -> float:
    return float(math.sqrt(np.mean((np.asarray(a) - np.asarray(b)) ** 2)))
