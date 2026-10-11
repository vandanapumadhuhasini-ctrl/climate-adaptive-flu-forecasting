"""v3 (docs/RESEARCH_PROTOCOL.md amendment A1): train one model, one horizon, one seed on the Spark-built feature tables
(results/v3/features/h<h>/, copied from HDFS processed/v3_features/h<h>). Same training rules as train_v2.py:
epochs chosen on 2022 (train <= 2021), refit on <= 2022, predict 2023-2025 once.
Writes results/v3/runs/<model>_h<h>_s<seed>.json and results/v3/predictions/<model>_h<h>_s<seed>.parquet.
Run: docker exec -w /workspace/rigour pytorch python train_v3.py --model GNN-lagK8 --h 2 --seed 0
"""
import argparse
import glob
import json
import os
import random
import time

import numpy as np
import pandas as pd
import torch

from common import CLIMATE, ClimaFluGNN, rmse
from train_v2 import make_snaps, predict, train, MAX_EPOCHS

OUT = "/results/v3"
MODELS = {   # id -> (climate branch, graph kind, K, climate source)
    "GNN-lagK8": ("gcn", "knn", 8, "aligned"),
    "GNN-K8": ("gcn", "knn", 8, "same_week"),
    "MLP-lagNoGraph": ("mlp", None, 0, "aligned"),
    "MLP-lagsOnly": ("none", None, 0, "same_week"),     # climate columns present but unused by the model
}


def load(h, source):
    df = pd.concat(pd.read_parquet(f) for f in glob.glob(f"{OUT}/features/h{h}/*.parquet"))
    if source == "aligned":                       # the model and the graph see the lead-aligned climate
        for f in CLIMATE:
            df[f] = df[f"{f}_al"]
    return df.sort_values(["YEAR", "WEEK", "state_abbrev"]).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True, choices=MODELS)
    ap.add_argument("--h", type=int, required=True, choices=[1, 2, 3, 4])
    ap.add_argument("--seed", type=int, required=True)
    a = ap.parse_args()
    torch.set_num_threads(int(os.environ.get("TORCH_THREADS", "3")))
    random.seed(a.seed); np.random.seed(a.seed); torch.manual_seed(a.seed)
    branch, kind, k, source = MODELS[a.model]
    feat = load(a.h, source)
    t0 = time.time()

    tr, va, vmeta, vstd = make_snaps(feat, feat.YEAR <= 2021, feat.YEAR == 2022, kind, k, a.seed)
    torch.manual_seed(a.seed)
    best_ep, best_val, curve = train(ClimaFluGNN(branch), tr, MAX_EPOCHS, a.seed, val=(va, vmeta, vstd))

    tr2, te, tmeta, std2 = make_snaps(feat, feat.YEAR <= 2022, feat.YEAR >= 2023, kind, k, a.seed)
    torch.manual_seed(a.seed)
    model = ClimaFluGNN(branch)
    train(model, tr2, best_ep, a.seed)
    pred = predict(model, te, std2)
    target = np.concatenate([m["target"] for m in tmeta])
    rows = [(s, m["year"], m["week"]) for m in tmeta for s in m["states"]]
    os.makedirs(f"{OUT}/predictions", exist_ok=True)
    pd.DataFrame(rows, columns=["state", "year", "week"]).assign(pred=pred, target=target).to_parquet(
        f"{OUT}/predictions/{a.model}_h{a.h}_s{a.seed}.parquet", index=False)

    ss_res, ss_tot = np.sum((target - pred) ** 2), np.sum((target - target.mean()) ** 2)
    rec = {"model": a.model, "horizon": a.h, "seed": a.seed, "best_epoch": best_ep, "val_rmse_best": round(best_val, 5),
           "test_rmse": round(rmse(pred, target), 5), "test_mae": round(float(np.mean(np.abs(pred - target))), 5),
           "test_r2": round(float(1 - ss_res / ss_tot), 5), "n_test_rows": int(len(target)),
           "seconds": round(time.time() - t0, 1), "val_curve": curve}
    os.makedirs(f"{OUT}/runs", exist_ok=True)
    with open(f"{OUT}/runs/{a.model}_h{a.h}_s{a.seed}.json", "w") as f:
        json.dump(rec, f)
    print(json.dumps({k_: v for k_, v in rec.items() if k_ != "val_curve"}))


if __name__ == "__main__":
    main()
