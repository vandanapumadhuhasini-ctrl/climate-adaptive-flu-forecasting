"""Rigour pass step 3: evaluate every model on the identical test rows and apply the pre-registered decision rules
(docs/RESEARCH_PROTOCOL.md sections 6-7). Inputs: results/rigour/predictions/*.parquet (neural + Python baselines),
results/rigour/predictions_spark/<GBT>/ (Spark MLlib), results/rigour/runs.jsonl.
Outputs: results/rigour/summary.json, docs/RIGOUR_RESULTS.md, docs/figures/rigour_test_rmse.png, docs/figures/rigour_forecast.png
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/rigour/evaluate_v2.py
"""
import glob
import json
import math
import os
import re

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from scipy import stats  # noqa: E402

PROJ = os.environ.get("PROJ", "/proj")
R = f"{PROJ}/results/rigour"
KEY = ["state", "year", "week"]
NEURAL = ["GNN-K8", "GNN-K4", "GNN-K16", "GNN-static", "GNN-geo", "GNN-random", "MLP-noGraph", "MLP-lagsOnly"]
DETERMINISTIC = ["GBT-lags", "GBT-all", "Ridge", "Persistence"]
MAIN = "GNN-K8"
CLAIMS = {"C1 accuracy: beats Spark GBT": ["GBT-lags", "GBT-all"],
          "C2 graph: beats no-graph and random graph": ["MLP-noGraph", "GNN-random"],
          "C3 dynamic: beats static and geographic graphs": ["GNN-static", "GNN-geo"]}
NW_LAGS, BLOCK, N_BOOT = 4, 4, 2000
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"


def load():
    preds = {}
    for f in glob.glob(f"{R}/predictions/*.parquet"):
        name = os.path.basename(f)[:-8]
        m = re.match(r"(.+)_s(\d+)$", name)
        preds[(m.group(1), int(m.group(2))) if m else (name, None)] = pd.read_parquet(f)
    for d in glob.glob(f"{R}/predictions_spark/*"):
        preds[(os.path.basename(d), None)] = pd.concat(pd.read_parquet(p) for p in glob.glob(f"{d}/*.parquet"))
    ref = None
    for k, df in preds.items():   # every model must be scored on exactly the same rows with the same targets
        df = df[KEY + ["pred", "target"]].sort_values(KEY).reset_index(drop=True)
        df[["year", "week"]] = df[["year", "week"]].astype(int)
        preds[k] = df
        if ref is None:
            ref = df
        elif not (df[KEY].equals(ref[KEY]) and np.allclose(df["target"], ref["target"])):
            raise RuntimeError(f"{k}: test rows or targets differ from the reference")
    return preds, ref


def metrics(pred, target):
    e = pred - target
    return {"rmse": float(np.sqrt(np.mean(e ** 2))), "mae": float(np.mean(np.abs(e))),
            "r2": float(1 - np.sum(e ** 2) / np.sum((target - target.mean()) ** 2))}


def weekly_sq_err(df, pred):
    return pd.Series((pred - df["target"].values) ** 2).groupby([df["year"].values, df["week"].values]).mean().values


def diebold_mariano(ref, pa, pb):
    """DM test on weekly mean squared-error differentials, Newey-West variance, Harvey-Leybourne-Newbold correction."""
    d = weekly_sq_err(ref, pa) - weekly_sq_err(ref, pb)
    T, dbar = len(d), d.mean()
    gam = [np.mean((d[l:] - dbar) * (d[:T - l] - dbar)) for l in range(NW_LAGS + 1)]
    var = gam[0] + 2 * sum((1 - l / (NW_LAGS + 1)) * gam[l] for l in range(1, NW_LAGS + 1))
    dm = dbar / math.sqrt(var / T) * math.sqrt((T - 1) / T)            # HLN factor for h = 1
    return float(dm), float(2 * stats.t.sf(abs(dm), df=T - 1))


def block_bootstrap(ref, pa, pb):
    weeks = ref[["year", "week"]].drop_duplicates().apply(tuple, axis=1).tolist()
    wid = ref[["year", "week"]].apply(tuple, axis=1).map({w: i for i, w in enumerate(weeks)}).values
    ea, eb = (pa - ref["target"].values) ** 2, (pb - ref["target"].values) ** 2
    sa, sb, n = np.bincount(wid, ea), np.bincount(wid, eb), np.bincount(wid)
    rng, T = np.random.default_rng(0), len(weeks)
    diffs = []
    for _ in range(N_BOOT):
        starts = rng.integers(0, T - BLOCK + 1, size=math.ceil(T / BLOCK))
        idx = np.concatenate([np.arange(s, s + BLOCK) for s in starts])[:T]
        diffs.append(math.sqrt(sa[idx].sum() / n[idx].sum()) - math.sqrt(sb[idx].sum() / n[idx].sum()))
    return [float(np.percentile(diffs, 2.5)), float(np.percentile(diffs, 97.5))]


def compare(a, b, preds, ref, ens, per_seed):
    """Apply the three pre-registered rules for 'a better than b' (and the mirror for 'worse')."""
    pa = ens[a]
    pb = ens[b] if b in ens else preds[(b, None)]["pred"].values
    dm, p = diebold_mariano(ref, pa, pb)
    ci = block_bootstrap(ref, pa, pb)
    ra = per_seed[a]
    rb = per_seed[b] if b in per_seed else {s: metrics(pb, ref["target"].values)["rmse"] for s in ra}
    wins = sum(ra[s] < rb[s] for s in ra if s in rb)
    mean_a, mean_b = np.mean(list(ra.values())), np.mean(list(rb.values()))
    better = mean_a < mean_b and p < 0.05 and ci[1] < 0 and wins >= 8
    worse = mean_a > mean_b and p < 0.05 and ci[0] > 0 and wins <= 2
    return {"a": a, "b": b, "mean_rmse_a": round(mean_a, 4), "mean_rmse_b": round(mean_b, 4),
            "ensemble_rmse_a": round(metrics(pa, ref["target"].values)["rmse"], 4),
            "rmse_b_ensemble_or_single": round(metrics(pb, ref["target"].values)["rmse"], 4),
            "dm_stat": round(dm, 3), "dm_p": round(p, 4), "boot_ci_rmse_diff": [round(c, 4) for c in ci],
            "seeds_a_better": f"{wins}/{len(ra)}",
            "verdict": "better" if better else "worse" if worse else "no significant difference"}


def fig_rmse(table, per_seed, path):
    order = sorted(table, key=lambda m: table[m]["rmse_mean"])
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=200)
    for i, m in enumerate(order):
        color = "#2a78d6" if m == MAIN else ("#eb6834" if m.startswith("GBT") else "#8c8b85" if m in DETERMINISTIC else "#4a3aa7")
        if m in per_seed:
            vals = list(per_seed[m].values())
            ax.scatter(vals, [i] * len(vals), s=14, color=color, alpha=0.45, linewidths=0)
            ax.errorbar(np.mean(vals), i, xerr=np.std(vals, ddof=1), fmt="o", color=color, markersize=6, capsize=3)
            label = f"{np.mean(vals):.3f} ± {np.std(vals, ddof=1):.3f}"
        else:
            ax.scatter([table[m]["rmse_mean"]], [i], marker="D", s=40, color=color)
            label = f"{table[m]['rmse_mean']:.3f}"
        ax.annotate(label, (table[m]["rmse_mean"], i), xytext=(0, 7), textcoords="offset points", ha="center", fontsize=7, color=INK2)
    ax.set_yticks(range(len(order))); ax.set_yticklabels(order, fontsize=8, color=INK)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8); ax.grid(axis="x", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
    ax.set_xlabel("Test RMSE, ILI % (2023-2025, 7,691 state-weeks); dots = seeds, bar = mean ± sd", color=INK2, fontsize=9)
    ax.set_title("Next-week ILI forecast error on identical test rows (lower is better)", color=INK, fontsize=10.5, loc="left")
    fig.savefig(path, facecolor="white", bbox_inches="tight"); plt.close(fig)


def fig_forecast(ref, ens, preds, path):
    w = ref.assign(gnn=ens[MAIN], gbt=preds[("GBT-lags", None)]["pred"].values).groupby(["year", "week"])[["target", "gnn", "gbt"]].mean().reset_index()
    x = np.arange(len(w))
    fig, ax = plt.subplots(figsize=(9, 3.8), dpi=200)
    ax.plot(x, w["target"], color=INK, linewidth=1.6, label="observed (mean over states)")
    ax.plot(x, w["gnn"], color="#2a78d6", linewidth=1.4, label=f"{MAIN} (10-seed ensemble)")
    ax.plot(x, w["gbt"], color="#eb6834", linewidth=1.2, linestyle=(0, (4, 2)), label="Spark GBT-lags")
    ticks = [i for i, wk in enumerate(w["week"]) if wk in (1, 27)]
    ax.set_xticks(ticks); ax.set_xticklabels([f"{w['year'][i]}-W{w['week'][i]:02d}" for i in ticks], fontsize=7)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8); ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
    ax.set_ylabel("Unweighted ILI %", color=INK2, fontsize=9)
    ax.set_title("Test period: next-week forecasts vs observed ILI, averaged over 50 states", color=INK, fontsize=10.5, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2)
    fig.savefig(path, facecolor="white", bbox_inches="tight"); plt.close(fig)


def main():
    preds, ref = load()
    target = ref["target"].values
    runs = [json.loads(l) for l in open(f"{R}/runs.jsonl")]
    per_seed = {m: {s: metrics(preds[(m, s)]["pred"].values, target)["rmse"] for (mm, s) in preds if mm == m}
                for m in NEURAL if (m, 0) in preds}
    ens = {m: np.mean([preds[(m, s)]["pred"].values for s in per_seed[m]], axis=0) for m in per_seed}
    table = {}
    for m in per_seed:
        ms = [metrics(preds[(m, s)]["pred"].values, target) for s in per_seed[m]]
        table[m] = {"seeds": len(ms), **{f"{k}_mean": float(np.mean([x[k] for x in ms])) for k in ("rmse", "mae", "r2")},
                    **{f"{k}_sd": float(np.std([x[k] for x in ms], ddof=1)) for k in ("rmse", "mae", "r2")},
                    "ensemble": metrics(ens[m], target),
                    "best_epoch_median": float(np.median([r["best_epoch"] for r in runs if r["model"] == m]))}
    for m in DETERMINISTIC:
        x = metrics(preds[(m, None)]["pred"].values, target)
        table[m] = {"seeds": 1, "rmse_mean": x["rmse"], "mae_mean": x["mae"], "r2_mean": x["r2"]}

    comps = [compare(MAIN, b, preds, ref, ens, per_seed) for b in [m for m in NEURAL if m != MAIN] + DETERMINISTIC]
    by_b = {c["b"]: c for c in comps}
    claims = {c: {"holds": all(by_b[b]["verdict"] == "better" for b in bs),
                  "against": {b: by_b[b]["verdict"] for b in bs}} for c, bs in CLAIMS.items()}

    trial = [json.loads(l) for l in open(f"{R}/trial_runs.jsonl")] if os.path.exists(f"{R}/trial_runs.jsonl") else []
    det = None
    if trial:
        rerun = next(r for r in runs if r["model"] == trial[0]["model"] and r["seed"] == trial[0]["seed"])
        det = {"trial_test_rmse": trial[0]["test_rmse"], "rerun_test_rmse": rerun["test_rmse"],
               "identical": trial[0]["test_rmse"] == rerun["test_rmse"]}

    os.makedirs(f"{PROJ}/docs/figures", exist_ok=True)
    fig_rmse(table, per_seed, f"{PROJ}/docs/figures/rigour_test_rmse.png")
    fig_forecast(ref, ens, preds, f"{PROJ}/docs/figures/rigour_forecast.png")
    summary = {"test_rows": len(ref), "test_weeks": int(ref[["year", "week"]].drop_duplicates().shape[0]),
               "models": table, "comparisons_vs_main": comps, "claims": claims, "determinism_check": det}
    with open(f"{R}/summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    write_md(summary)
    print(json.dumps({"claims": claims, "determinism": det}, indent=2))


def write_md(s):
    t = s["models"]
    out = ["# Part 04 v2 — ClimaFlu-GNN rigour pass: results", "",
           "Generated by `python/rigour/evaluate_v2.py` from `results/rigour/`; protocol frozen beforehand in "
           "`docs/RESEARCH_PROTOCOL.md`. Every model is scored on the same "
           f"{s['test_rows']:,} test state-weeks ({s['test_weeks']} weeks, 2023-2025).", "",
           "## Claims (pre-registered decision rules)", "", "| Claim | Holds | Verdicts |", "|---|---|---|"]
    for c, v in s["claims"].items():
        out.append(f"| {c} | **{'YES' if v['holds'] else 'NO'}** | " + "; ".join(f"vs {b}: {r}" for b, r in v["against"].items()) + " |")
    out += ["", "## Test accuracy", "", "| Model | Seeds | RMSE mean ± sd | MAE mean ± sd | R² mean | Ensemble RMSE | Median best epoch |",
            "|---|---|---|---|---|---|---|"]
    for m in sorted(t, key=lambda m: t[m]["rmse_mean"]):
        r = t[m]
        if r["seeds"] > 1:
            out.append(f"| {m} | {r['seeds']} | {r['rmse_mean']:.4f} ± {r['rmse_sd']:.4f} | {r['mae_mean']:.4f} ± {r['mae_sd']:.4f} | "
                       f"{r['r2_mean']:.4f} | {r['ensemble']['rmse']:.4f} | {r['best_epoch_median']:.0f} |")
        else:
            out.append(f"| {m} | 1 (deterministic) | {r['rmse_mean']:.4f} | {r['mae_mean']:.4f} | {r['r2_mean']:.4f} | - | - |")
    out += ["", f"## {MAIN} against every other model", "",
            "DM = Diebold–Mariano on weekly squared-error differentials (Newey–West, 4 lags, HLN correction); "
            "CI = 4-week moving-block bootstrap 95 % CI of RMSE difference (negative = GNN better).", "",
            "| vs | mean RMSE GNN / other | ensemble RMSE GNN / other | DM stat | DM p | bootstrap CI | seeds GNN better | verdict |",
            "|---|---|---|---|---|---|---|---|"]
    for c in s["comparisons_vs_main"]:
        out.append(f"| {c['b']} | {c['mean_rmse_a']} / {c['mean_rmse_b']} | {c['ensemble_rmse_a']} / {c['rmse_b_ensemble_or_single']} | "
                   f"{c['dm_stat']} | {c['dm_p']} | [{c['boot_ci_rmse_diff'][0]}, {c['boot_ci_rmse_diff'][1]}] | {c['seeds_a_better']} | {c['verdict']} |")
    if s["determinism_check"]:
        d = s["determinism_check"]
        out += ["", f"Determinism check: GNN-K8 seed 0 trial run RMSE {d['trial_test_rmse']} vs re-run {d['rerun_test_rmse']} "
                f"-> identical: {d['identical']}."]
        if not d["identical"]:
            sd = s["models"][MAIN]["rmse_sd"]
            out += [f"The trial ran with 4 PyTorch CPU threads and the batch with 3; the thread count changes the order of "
                    f"floating-point reductions, so runs are reproducible only for a fixed thread count. The difference "
                    f"({abs(d['trial_test_rmse'] - d['rerun_test_rmse']):.5f}) is {sd / max(abs(d['trial_test_rmse'] - d['rerun_test_rmse']), 1e-12):.0f}x "
                    f"smaller than the seed-to-seed sd ({sd:.4f}); all reported runs used 3 threads."]
    out += ["", "Figures: `docs/figures/rigour_test_rmse.png`, `docs/figures/rigour_forecast.png`", ""]
    with open(f"{PROJ}/docs/RIGOUR_RESULTS.md", "w", encoding="utf-8") as f:
        f.write("\n".join(out))


if __name__ == "__main__":
    main()
