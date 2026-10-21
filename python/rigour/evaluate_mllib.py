"""A2 evaluation: Spark MLlib comparison (LinearRegression, RandomForest, GBT x lags/all x horizons 1-4) and claim M1
(ClimaFlu-GNN vs the best MLlib model per horizon, Holm across horizons). Inputs: results/mllib/selection.jsonl,
results/mllib/predictions/, results/v3/predictions/ (GNN-lagK8). Outputs: results/mllib/summary.json,
docs/MLLIB_RESULTS.md, docs/figures/mllib_comparison.png.
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/rigour/evaluate_mllib.py
"""
import glob
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from evaluate_v2 import block_bootstrap, diebold_mariano, metrics  # noqa: E402
from evaluate_v3 import holm  # noqa: E402

PROJ = os.environ.get("PROJ", "/proj")
M = f"{PROJ}/results/mllib"
KEY = ["state", "year", "week"]
H = [1, 2, 3, 4]
MODELS = ["LinearRegression", "RandomForest", "GBT"]
FS = ["lags", "all"]
COL = {"LinearRegression": "#8c8b85", "RandomForest": "#1baf7a", "GBT": "#eb6834", "GNN": "#2a78d6"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def norm(df):
    df = df[KEY + ["pred", "target"]].copy()
    df[["year", "week"]] = df[["year", "week"]].astype(int)
    return df.sort_values(KEY).reset_index(drop=True)


def main():
    sel = {(r["model"], r["features"], r["horizon"]): r for r in
           (json.loads(l) for l in open(f"{M}/selection.jsonl", encoding="utf-8") if l.strip())}
    table, m1 = {}, []
    for h in H:
        preds = {(m, f): norm(pd.concat(pd.read_parquet(p) for p in glob.glob(f"{M}/predictions/{m}_{f}_h{h}/*.parquet")))
                 for m in MODELS for f in FS}
        gnn = [norm(pd.read_parquet(p)) for p in sorted(glob.glob(f"{PROJ}/results/v3/predictions/GNN-lagK8_h{h}_s*.parquet"))]
        ref = gnn[0]
        for k, df in preds.items():
            if not (df[KEY].equals(ref[KEY]) and np.allclose(df["target"], ref["target"])):
                raise RuntimeError(f"h={h} {k}: rows differ from the GNN test rows")
        y = ref["target"].values
        rows = {}
        for (m, f), df in preds.items():
            x = metrics(df["pred"].values, y)
            s = sel[(m, f, h)]
            rows[f"{m}-{f}"] = {**x, "val_rmse": s["val_rmse"], "chosen": s["chosen"], "fit_seconds": s["fit_seconds"],
                                "selection_test_rmse": s["test_rmse"]}
        g_r = [metrics(g["pred"].values, y)["rmse"] for g in gnn]
        rows["ClimaFlu-GNN (10 seeds)"] = {"rmse": float(np.mean(g_r)), "rmse_sd": float(np.std(g_r, ddof=1)),
                                           "mae": float(np.mean([metrics(g["pred"].values, y)["mae"] for g in gnn])),
                                           "r2": float(np.mean([metrics(g["pred"].values, y)["r2"] for g in gnn]))}
        table[h] = rows
        best = min(((m, f) for m in MODELS for f in FS), key=lambda k: rows[f"{k[0]}-{k[1]}"]["rmse"])
        pb = preds[best]["pred"].values
        ens = np.mean([g["pred"].values for g in gnn], axis=0)
        dm, p = diebold_mariano(ref, ens, pb)
        ci = block_bootstrap(ref, ens, pb)
        b_rmse = rows[f"{best[0]}-{best[1]}"]["rmse"]
        m1.append({"horizon": h, "best_mllib": f"{best[0]}-{best[1]}", "best_mllib_rmse": b_rmse, "gnn_mean_rmse": float(np.mean(g_r)),
                   "dm_p": p, "boot_ci": ci, "wins": int(sum(r < b_rmse for r in g_r))})
    for r, a in zip(m1, holm([r["dm_p"] for r in m1])):
        r["dm_p_holm"] = a
        better = r["gnn_mean_rmse"] < r["best_mllib_rmse"] and a < 0.05 and r["boot_ci"][1] < 0 and r["wins"] >= 8
        worse = r["gnn_mean_rmse"] > r["best_mllib_rmse"] and a < 0.05 and r["boot_ci"][0] > 0 and r["wins"] <= 2
        r["verdict"] = "better" if better else "worse" if worse else "no significant difference"
    summary = {"per_horizon": table, "claim_M1": {"holds": any(r["verdict"] == "better" for r in m1), "by_horizon": m1},
               "importances_h1": {f"{s['model']}-{s['features']}": s.get("feature_importances") or s.get("coefficients")
                                  for s in sel.values() if s["horizon"] == 1}}
    with open(f"{M}/summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=float)
    figure(table)
    write_md(summary, sel)
    print(json.dumps({"M1": [(r["horizon"], r["best_mllib"], round(r["best_mllib_rmse"], 4), round(r["gnn_mean_rmse"], 4),
                              r["verdict"], round(r["dm_p_holm"], 4)) for r in m1]}, indent=1))


def figure(table):
    fig, ax = plt.subplots(figsize=(8.5, 4.6), dpi=200)
    width = 0.13
    keys = [f"{m}-{f}" for m in MODELS for f in FS] + ["ClimaFlu-GNN (10 seeds)"]
    for i, k in enumerate(keys):
        m = k.split("-")[0] if not k.startswith("ClimaFlu") else "GNN"
        vals = [table[h][k]["rmse"] for h in H]
        ax.bar([h + (i - 3) * width for h in H], vals, width=width * 0.92, color=COL[m],
               alpha=0.55 if k.endswith("-lags") else 1.0, label=k)
    ax.set_xticks(H); ax.set_xticklabels([f"{h} wk" for h in H])
    lo = min(table[h][k]["rmse"] for h in H for k in keys)
    ax.set_ylim(lo * 0.9, None)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9); ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
    ax.set_xlabel("Forecast horizon", color=INK2, fontsize=10); ax.set_ylabel("Test RMSE, ILI % (2023-2025)", color=INK2, fontsize=10)
    ax.set_title("Spark MLlib models vs ClimaFlu-GNN (light bars: ILI lags only; dark: lags + climate)", color=INK, fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=7, labelcolor=INK2, ncol=4, loc="upper left")
    fig.savefig(f"{PROJ}/docs/figures/mllib_comparison.png", facecolor="white", bbox_inches="tight"); plt.close(fig)


def write_md(s, sel):
    out = ["# Spark MLlib model comparison (amendment A2)", "",
           "Generated by `python/rigour/evaluate_mllib.py` from `results/mllib/` (Spark job `scala/MLlibComparison.scala`, data from HDFS "
           "`processed/v3_features`). Protocol frozen beforehand as amendment A2 in `docs/RESEARCH_PROTOCOL.md`. Hyper-parameters chosen on "
           "the 2022 validation year (temporal hold-out), refit on 2010-2022, tested once on 2023-2025.", "",
           "## Test RMSE (MAE) by horizon", "", "| Model | " + " | ".join(f"h={h}" for h in H) + " |", "|---|" + "---|" * len(H)]
    for k in [f"{m}-{f}" for m in MODELS for f in FS] + ["ClimaFlu-GNN (10 seeds)"]:
        out.append(f"| {k} | " + " | ".join(f"{s['per_horizon'][h][k]['rmse']:.4f} ({s['per_horizon'][h][k]['mae']:.4f})" for h in H) + " |")
    out += ["", "## Chosen hyper-parameters (validation RMSE on 2022) and refit time", "",
            "| Model | h | chosen | val RMSE | fit s |", "|---|---|---|---|---|"]
    for (m, f, h), r in sorted(sel.items(), key=lambda kv: (kv[0][2], kv[0][0], kv[0][1])):
        out.append(f"| {m}-{f} | {h} | {', '.join(f'{k}={v}' for k, v in r['chosen'].items())} | {r['val_rmse']:.4f} | {r['fit_seconds']:.1f} |")
    out += ["", "## Claim M1: ClimaFlu-GNN vs the best MLlib model (Holm across horizons)", "",
            f"**Holds: {'YES' if s['claim_M1']['holds'] else 'NO'}**", "",
            "| h | best MLlib model | RMSE best MLlib / GNN | DM p (Holm) | bootstrap CI | seeds better | verdict |", "|---|---|---|---|---|---|---|"]
    for r in s["claim_M1"]["by_horizon"]:
        out.append(f"| {r['horizon']} | {r['best_mllib']} | {r['best_mllib_rmse']:.4f} / {r['gnn_mean_rmse']:.4f} | {r['dm_p_holm']:.4f} | "
                   f"[{r['boot_ci'][0]:.4f}, {r['boot_ci'][1]:.4f}] | {r['wins']}/10 | {r['verdict']} |")
    out += ["", "## Feature importances / coefficients (h = 1)", ""]
    for k, v in sorted(s["importances_h1"].items()):
        out.append(f"* **{k}:** " + ", ".join(f"{c} {x:.3f}" for c, x in v.items()))
    out += ["", "Notes: (1) each MLlib model is a single deterministic run (seed 42); the ranking among MLlib models is descriptive. "
            "The best MLlib model per horizon is selected after seeing its test RMSE, which makes M1 conservative for the GNN. "
            "(2) ILI_trend = ILI_lag1 − ILI_lag2 exactly, so the linear features are collinear: MLlib's Cholesky solver reports a "
            "singular matrix and falls back to L-BFGS, and individual linear coefficients (e.g. ±108 for the lag-only model) are not "
            "identifiable — predictions are unaffected, but the coefficients must not be interpreted. Tree importances are unaffected. "
            "(3) Earlier results compared the GNN with a fixed-setting GBT (50 trees, depth 5, lags only); against these tuned MLlib "
            "models the GNN is not significantly better at any horizon.", "",
            "Figure: `docs/figures/mllib_comparison.png`", ""]
    with open(f"{PROJ}/docs/MLLIB_RESULTS.md", "w", encoding="utf-8") as f:
        f.write("\n".join(out))


if __name__ == "__main__":
    main()
