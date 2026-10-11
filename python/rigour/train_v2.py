"""Rigour pass step 2: train one model variant with one seed under docs/RESEARCH_PROTOCOL.md.
  phase "select": train YEAR <= 2021, validate on 2022 every epoch (max 200, patience 30) -> best epoch
  phase "final" : refit from scratch on YEAR <= 2022 for exactly best-epoch epochs -> predict YEAR >= 2023 once
Writes results/rigour/runs/<model>_s<seed>.json and writes results/rigour/predictions/<model>_s<seed>.parquet.
Run: docker exec -w /workspace/rigour pytorch python train_v2.py --model GNN-K8 --seed 0
"""
import argparse
import json
import os
import random
import time

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
from torch_geometric.loader import DataLoader

from common import OUT, ClimaFluGNN, climate_stats, geo_graph, knn_edges, rmse, snapshots, standardiser, static_graph

MODELS = {   # id -> (climate branch, graph kind, K)
    "GNN-K8": ("gcn", "knn", 8), "GNN-K4": ("gcn", "knn", 4), "GNN-K16": ("gcn", "knn", 16),
    "GNN-static": ("gcn", "static", 8), "GNN-geo": ("gcn", "geo", 8), "GNN-random": ("gcn", "random", 8),
    "MLP-noGraph": ("mlp", None, 0), "MLP-lagsOnly": ("none", None, 0),
}
MAX_EPOCHS, PATIENCE, BATCH, LR, WD, CLIP = 200, 30, 8, 1e-3, 1e-5, 1.0


def graph_fn(kind, k, train_rows, seed):
    """Return graph_for_week(year, week, week_df) built ONLY from training-phase statistics."""
    if kind is None:
        return lambda y, w, wd: None
    zstats, sigma = climate_stats(train_rows)
    if kind == "knn":
        return lambda y, w, wd: knn_edges(wd, zstats, sigma, k)
    if kind == "static":
        g = static_graph(train_rows, zstats, k)
        return lambda y, w, wd: g
    if kind == "geo":
        g = geo_graph(k)
        return lambda y, w, wd: g
    if kind == "random":   # same degree as K8; neighbours drawn at random, the week's K8 similarities permuted
        def rnd(y, w, wd):
            rng = np.random.default_rng([seed, y, w])
            e = knn_edges(wd, zstats, sigma, k)
            states = wd["state_abbrev"].tolist()
            e["state_j"] = [rng.choice([s for s in states if s != si], 1)[0] for si in e["state_i"]]
            e = e.drop_duplicates(["state_i", "state_j"])
            e["similarity"] = rng.permutation(e["similarity"].values)
            return e
        return rnd
    raise ValueError(kind)


def make_snaps(feat, train_mask, eval_mask, kind, k, seed):
    train_rows = feat[train_mask]
    std = standardiser(train_rows)
    gf = graph_fn(kind, k, train_rows, seed)
    tr, _ = snapshots(train_rows, std, gf)
    ev, meta = snapshots(feat[eval_mask], std, gf)
    return tr, ev, meta, std


def predict(model, snaps, std):
    model.eval()
    out = []
    with torch.no_grad():
        for d in snaps:
            out.append(model(d.x, d.edge_index, d.edge_attr).numpy() * std["target"][1] + std["target"][0])
    return np.concatenate(out)


def train(model, snaps, epochs, seed, val=None):
    """Train for `epochs` epochs (or with early stopping on `val` = (snaps, meta, std))."""
    opt = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=WD)
    loader = DataLoader(snaps, batch_size=BATCH, shuffle=True, generator=torch.Generator().manual_seed(seed))
    curve, best, best_ep, since = [], float("inf"), 0, 0
    for ep in range(1, epochs + 1):
        model.train()
        for b in loader:
            opt.zero_grad()
            loss = F.mse_loss(model(b.x, b.edge_index, b.edge_attr), b.y)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), CLIP)
            opt.step()
        if val is not None:
            vs, vm, vstd = val
            r = rmse(predict(model, vs, vstd), np.concatenate([m["target"] for m in vm]))
            curve.append(round(r, 5))
            if r < best - 1e-6:
                best, best_ep, since = r, ep, 0
            else:
                since += 1
                if since >= PATIENCE:
                    break
    return best_ep, best, curve


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, choices=MODELS)
    ap.add_argument("--seed", type=int, required=True)
    a = ap.parse_args()
    torch.set_num_threads(int(os.environ.get("TORCH_THREADS", "2")))
    random.seed(a.seed); np.random.seed(a.seed); torch.manual_seed(a.seed)
    branch, kind, k = MODELS[a.model]
    feat = pd.read_parquet(f"{OUT}/feature_table.parquet")
    t0 = time.time()

    # ---- selection phase: train <= 2021, validate on 2022
    tr, va, vmeta, vstd = make_snaps(feat, feat.YEAR <= 2021, feat.YEAR == 2022, kind, k, a.seed)
    torch.manual_seed(a.seed)
    best_ep, best_val, curve = train(ClimaFluGNN(branch), tr, MAX_EPOCHS, a.seed, val=(va, vmeta, vstd))

    # ---- final phase: refit on <= 2022 for best_ep epochs, predict the test set once
    tr2, te, tmeta, std2 = make_snaps(feat, feat.YEAR <= 2022, feat.YEAR >= 2023, kind, k, a.seed)
    torch.manual_seed(a.seed)
    model = ClimaFluGNN(branch)
    train(model, tr2, best_ep, a.seed)
    pred = predict(model, te, std2)
    target = np.concatenate([m["target"] for m in tmeta])
    rows = [(s, m["year"], m["week"]) for m in tmeta for s in m["states"]]
    os.makedirs(f"{OUT}/predictions", exist_ok=True)
    pd.DataFrame(rows, columns=["state", "year", "week"]).assign(pred=pred, target=target).to_parquet(
        f"{OUT}/predictions/{a.model}_s{a.seed}.parquet", index=False)

    ss_res, ss_tot = np.sum((target - pred) ** 2), np.sum((target - target.mean()) ** 2)
    rec = {"model": a.model, "seed": a.seed, "best_epoch": best_ep, "val_rmse_best": round(best_val, 5),
           "epochs_run_select": len(curve), "test_rmse": round(rmse(pred, target), 5),
           "test_mae": round(float(np.mean(np.abs(pred - target))), 5), "test_r2": round(float(1 - ss_res / ss_tot), 5),
           "n_test_rows": int(len(target)), "n_params": sum(p.numel() for p in model.parameters()),
           "seconds": round(time.time() - t0, 1), "val_curve": curve}
    os.makedirs(f"{OUT}/runs", exist_ok=True)          # one file per run: safe with parallel workers
    with open(f"{OUT}/runs/{a.model}_s{a.seed}.json", "w") as f:
        json.dump(rec, f)
    print(json.dumps({k_: v for k_, v in rec.items() if k_ != "val_curve"}))


if __name__ == "__main__":
    main()
