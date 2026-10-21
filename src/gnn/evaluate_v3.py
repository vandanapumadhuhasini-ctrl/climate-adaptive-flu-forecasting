"""v3 evaluation (docs/RESEARCH_PROTOCOL.md amendment A1): metrics per horizon, Diebold-Mariano + block bootstrap,
Holm adjustment across horizons inside each claim, verdicts for N1-N4.
Inputs: results/v3/predictions/*.parquet, results/v3/predictions_spark/<GBT>_h<h>/, results/v3/features/h<h>/ (Persistence).
Outputs: results/v3/summary.json, docs/V3_RESULTS.md, docs/figures/v3_rmse_by_horizon.png, docs/figures/v3_climate_benefit.png
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/src/gnn/evaluate_v3.py
"""
import glob
import json
import os
import re
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from evaluate_v2 import block_bootstrap, diebold_mariano, metrics  # noqa: E402

PROJ = os.environ.get("PROJ", "/proj")
V = f"{PROJ}/results/v3"
KEY = ["state", "year", "week"]
H = [1, 2, 3, 4]
NEURAL = ["GNN-lagK8", "GNN-K8", "MLP-lagNoGraph", "MLP-lagsOnly"]
DET = ["GBT-lags", "GBT-lagclimate", "Persistence"]
MAIN = "GNN-lagK8"
CLAIMS = {"N1 lag-aware beats same-week": ("GNN-K8", H),
          "N2 climate helps at longer horizons": ("MLP-lagsOnly", [2, 3, 4]),
          "N3 graph helps given lag-aware climate": ("MLP-lagNoGraph", H),
          "N4 beats Spark GBT": ("GBT-lags", H)}
COL = {"GNN-lagK8": "#2a78d6", "GNN-K8": "#1baf7a", "MLP-lagNoGraph": "#4a3aa7", "MLP-lagsOnly": "#e87ba4",
       "GBT-lags": "#eb6834", "GBT-lagclimate": "#eda100", "Persistence": "#8c8b85"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def norm(df):
    df = df[KEY + ["pred", "target"]].copy()
    df[["year", "week"]] = df[["year", "week"]].astype(int)
    return df.sort_values(KEY).reset_index(drop=True)


def load(h):
    p = {}
    for f in glob.glob(f"{V}/predictions/*_h{h}_s*.parquet"):
        m = re.match(rf"(.+)_h{h}_s(\d+)\.parquet$", os.path.basename(f))
        p[(m.group(1), int(m.group(2)))] = norm(pd.read_parquet(f))
    for g in ("GBT-lags", "GBT-lagclimate"):
        p[(g, None)] = norm(pd.concat(pd.read_parquet(x) for x in glob.glob(f"{V}/predictions_spark/{g}_h{h}/*.parquet")))
    ft = pd.concat(pd.read_parquet(x) for x in glob.glob(f"{V}/features/h{h}/*.parquet"))
    ft = ft[ft.YEAR >= 2023].rename(columns={"state_abbrev": "state", "YEAR": "year", "WEEK": "week"})
    p[("Persistence", None)] = norm(ft.assign(pred=ft["ILI_lag1"]))          # most recent observed ILI (A1.1)
    ref = next(iter(p.values()))
    for k, df in p.items():
        if not (df[KEY].equals(ref[KEY]) and np.allclose(df["target"], ref["target"])):
            raise RuntimeError(f"h={h} {k}: rows or targets differ")
    return p, ref


def holm(ps):
    order = np.argsort(ps)
    adj, running = [0.0] * len(ps), 0.0
    for rank, i in enumerate(order):
        running = max(running, min(1.0, (len(ps) - rank) * ps[i]))
        adj[i] = running
    return adj


def main():
    per_h, comps = {}, {c: [] for c in CLAIMS}
    for h in H:
        p, ref = load(h)
        y = ref["target"].values
        seeds = {m: {s: metrics(p[(m, s)]["pred"].values, y)["rmse"] for (mm, s) in p if mm == m and s is not None} for m in NEURAL}
        ens = {m: np.mean([p[(m, s)]["pred"].values for s in seeds[m]], axis=0) for m in NEURAL}
        table = {m: {"seeds": len(seeds[m]), "rmse_mean": float(np.mean(list(seeds[m].values()))),
                     "rmse_sd": float(np.std(list(seeds[m].values()), ddof=1)), "ensemble_rmse": metrics(ens[m], y)["rmse"],
                     "mae_mean": float(np.mean([metrics(p[(m, s)]["pred"].values, y)["mae"] for s in seeds[m]]))} for m in NEURAL}
        for d in DET:
            x = metrics(p[(d, None)]["pred"].values, y)
            table[d] = {"seeds": 1, "rmse_mean": x["rmse"], "rmse_sd": 0.0, "mae_mean": x["mae"]}
        identical = bool(np.allclose(ens["GNN-lagK8"], ens["GNN-K8"]))
        per_h[h] = {"test_rows": len(ref), "models": table, "GNN_lagK8_equals_GNN_K8": identical}
        for c, (b, hs) in CLAIMS.items():
            if h not in hs:
                continue
            pa = ens[MAIN]
            pb = ens[b] if b in ens else p[(b, None)]["pred"].values
            dm, pv = diebold_mariano(ref, pa, pb) if not np.allclose(pa, pb) else (0.0, 1.0)
            ci = block_bootstrap(ref, pa, pb)
            ra, rb = seeds[MAIN], (seeds[b] if b in seeds else {s: metrics(pb, y)["rmse"] for s in seeds[MAIN]})
            comps[c].append({"horizon": h, "vs": b, "mean_rmse_main": table[MAIN]["rmse_mean"], "mean_rmse_other": table[b]["rmse_mean"],
                             "dm_stat": dm, "dm_p": pv, "boot_ci": ci, "wins": sum(ra[s] < rb[s] for s in ra)})
    claims = {}
    for c, rows in comps.items():
        adj = holm([r["dm_p"] for r in rows])
        for r, a in zip(rows, adj):
            r["dm_p_holm"] = a
            better = r["mean_rmse_main"] < r["mean_rmse_other"] and a < 0.05 and r["boot_ci"][1] < 0 and r["wins"] >= 8
            worse = r["mean_rmse_main"] > r["mean_rmse_other"] and a < 0.05 and r["boot_ci"][0] > 0 and r["wins"] <= 2
            r["verdict"] = "better" if better else "worse" if worse else "no significant difference"
        claims[c] = {"holds": any(r["verdict"] == "better" for r in rows), "by_horizon": rows}
    summary = {"lag_scan": json.load(open(f"{V}/lag_scan.json"))["chosen_leads"], "per_horizon": per_h, "claims": claims}
    with open(f"{V}/summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=float)
    figures(per_h, claims)
    write_md(summary)
    print(json.dumps({c: {"holds": v["holds"], "verdicts": [(r["horizon"], r["verdict"], round(r["dm_p_holm"], 4)) for r in v["by_horizon"]]}
                      for c, v in claims.items()}, indent=1))


def axes_style(ax, xl, yl):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9); ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
    ax.set_xlabel(xl, color=INK2, fontsize=10); ax.set_ylabel(yl, color=INK2, fontsize=10)


def figures(per_h, claims):
    fig, ax = plt.subplots(figsize=(8, 4.6), dpi=200)
    for m in ["GNN-lagK8", "MLP-lagNoGraph", "MLP-lagsOnly", "GBT-lags", "GBT-lagclimate", "Persistence"]:
        y = [per_h[h]["models"][m]["rmse_mean"] for h in H]
        e = [per_h[h]["models"][m]["rmse_sd"] for h in H]
        ax.errorbar(H, y, yerr=e, color=COL[m], marker="o", markersize=5, linewidth=2, capsize=3, label=m,
                    linestyle=(0, (4, 2)) if m == "Persistence" else "-")
    ax.set_xticks(H); ax.set_xticklabels([f"{h} wk" for h in H])
    axes_style(ax, "Forecast horizon h in weeks (last observed ILI is one week older: 1-week reporting delay)", "Test RMSE, ILI % (2023-2025)")
    ax.set_title("Forecast error grows with horizon; mean ± sd over 10 seeds", color=INK, fontsize=10.5, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2, ncol=2)
    fig.savefig(f"{PROJ}/docs/figures/v3_rmse_by_horizon.png", facecolor="white", bbox_inches="tight"); plt.close(fig)

    rows = {r["horizon"]: r for r in claims["N2 climate helps at longer horizons"]["by_horizon"]}
    n1 = {r["horizon"]: r for r in claims["N3 graph helps given lag-aware climate"]["by_horizon"]}
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=200)
    for off, (lab, src, colr) in enumerate((("GNN-lagK8 vs no climate (N2)", rows, "#2a78d6"),
                                            ("GNN-lagK8 vs no graph (N3)", n1, "#4a3aa7"))):
        hs = sorted(src)
        d = [src[h]["mean_rmse_main"] - src[h]["mean_rmse_other"] for h in hs]
        lo = [d[i] - src[h]["boot_ci"][0] for i, h in enumerate(hs)]
        hi = [src[h]["boot_ci"][1] - d[i] for i, h in enumerate(hs)]
        ax.errorbar([h + (off - 0.5) * 0.15 for h in hs], d, yerr=[lo, hi], fmt="o", color=colr, capsize=4, markersize=6, label=lab)
    ax.axhline(0, color=INK2, linewidth=1)
    ax.set_xticks(H); ax.set_xticklabels([f"{h} wk" for h in H])
    axes_style(ax, "Forecast horizon", "RMSE difference (negative = GNN-lagK8 better)")
    ax.set_title("Climate and graph benefit by horizon: point = mean-seed difference, bar = bootstrap 95% CI", color=INK, fontsize=10, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2)
    fig.savefig(f"{PROJ}/docs/figures/v3_climate_benefit.png", facecolor="white", bbox_inches="tight"); plt.close(fig)


def write_md(s):
    out = ["# v3 — lag-aware climate and multi-horizon forecasting: results", "",
           "Generated by `src/gnn/evaluate_v3.py`; protocol frozen beforehand as amendment A1 (+ clarification A1.1) in "
           "`docs/RESEARCH_PROTOCOL.md`. Features built in Spark on HDFS (`bigdata_pipeline/spark_etl/LagAwareFeaturesV3.scala`).", "",
           f"Lag scan (training years <= 2021): chosen climate leads per horizon = `{s['lag_scan']}` — every feature's best lead "
           "is the same week, so GNN-lagK8 and GNN-K8 receive identical inputs.", "",
           "## Claims (Holm-adjusted across horizons)", "", "| Claim | Holds | Per horizon: verdict (Holm-adjusted DM p) |", "|---|---|---|"]
    for c, v in s["claims"].items():
        out.append(f"| {c} | **{'YES' if v['holds'] else 'NO'}** | " +
                   "; ".join(f"h={r['horizon']}: {r['verdict']} (p={r['dm_p_holm']:.4f})" for r in v["by_horizon"]) + " |")
    out += ["", "## Test RMSE by horizon (mean ± sd over 10 seeds; deterministic models: single run)", ""]
    models = list(s["per_horizon"][1]["models"])
    out += ["| Model | " + " | ".join(f"h={h}" for h in H) + " |", "|---|" + "---|" * len(H)]
    for m in models:
        cells = []
        for h in H:
            r = s["per_horizon"][h]["models"][m]
            cells.append(f"{r['rmse_mean']:.4f}" + (f" ± {r['rmse_sd']:.4f}" if r["seeds"] > 1 else ""))
        out.append(f"| {m} | " + " | ".join(cells) + " |")
    out += ["", "## Comparisons", "", "| Claim | h | vs | mean RMSE main / other | DM p (raw) | DM p (Holm) | bootstrap CI | seeds better | verdict |",
            "|---|---|---|---|---|---|---|---|---|"]
    for c, v in s["claims"].items():
        for r in v["by_horizon"]:
            out.append(f"| {c.split()[0]} | {r['horizon']} | {r['vs']} | {r['mean_rmse_main']:.4f} / {r['mean_rmse_other']:.4f} | "
                       f"{r['dm_p']:.4f} | {r['dm_p_holm']:.4f} | [{r['boot_ci'][0]:.4f}, {r['boot_ci'][1]:.4f}] | {r['wins']}/10 | {r['verdict']} |")
    out += ["", "Test rows per horizon: " + ", ".join(f"h={h}: {s['per_horizon'][h]['test_rows']:,}" for h in H) +
            ". Limitation (stated in A1): the 2023-2025 test set was also used in v2.", "",
            "Notes: (1) a comparison is 'better' only if all three pre-registered rules hold with the Holm-adjusted p; results that "
            "pass the bootstrap and seed rules but not the adjusted p (e.g. N2 at h = 3) are reported as not significant. "
            "(2) GNN-K8 at h = 1 reproduces the v2 result exactly (Spark-built and Python-built feature tables are identical). "
            "(3) Spark GBT-lags at h = 1 gives 0.9377 here vs 0.9401 in v2 on identical data: MLlib bins features from a sample whose "
            "order depends on the input partitioning, so the HDFS layout of the input changes the fitted trees slightly.", "",
            "Figures: `docs/figures/v3_rmse_by_horizon.png`, `docs/figures/v3_climate_benefit.png`", ""]
    with open(f"{PROJ}/docs/V3_RESULTS.md", "w", encoding="utf-8") as f:
        f.write("\n".join(out))


if __name__ == "__main__":
    main()
