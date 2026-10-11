"""Step 3c analysis: results/tuning/runs.jsonl -> results/tuning_summary.json, docs/TUNING_RESULTS.md,
docs/figures/tuning_partitions_cache.png. Every number is computed from runs.jsonl.

Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/analyze_tuning.py
"""
import json
import os
import statistics as st
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PROJ = os.environ.get("PROJ", "/proj")
RUNS = f"{PROJ}/results/tuning/runs.jsonl"
COLORS = {"none": "#eb6834", "memory_and_disk": "#2a78d6"}      # reference palette slots 2 and 1
NAMES = {"none": "no persistence (base recomputed per query)", "memory_and_disk": "persist(MEMORY_AND_DISK)"}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"
BREAK_EVEN = {}   # partitions -> caching break-even (filled by summarise)


def summarise():
    runs = [json.loads(l) for l in open(RUNS, encoding="utf-8") if l.strip()]
    by = defaultdict(list)
    for r in runs:
        by[r["label"]].append(r)
    rows = []
    for label, all_rs in by.items():
        rs = [r for r in all_rs if r.get("status", "ok") == "ok"]
        if not rs:   # the configuration failed: report it as a result (with the error), not as missing data
            f = all_rs[0]
            rows.append({"label": label, "partitions": f["partitions"], "persist": f["persist"], "aqe": f["aqe"],
                         "repeats": 0, "status": "failed", "failed_at_repeat": f.get("repeat"),
                         "error_class": f.get("error_class"), "error": f.get("error")})
            continue
        med = lambda k: round(st.median(r[k] for r in rs), 2)  # noqa: E731
        rows.append({
            "status": "ok" if len(rs) == len(all_rs) else "partial",
            "label": label, "partitions": rs[0]["partitions"], "persist": rs[0]["persist"], "aqe": rs[0]["aqe"],
            "repeats": len(rs), "total_median_s": med("total_s"),
            "total_min_s": round(min(r["total_s"] for r in rs), 2), "total_max_s": round(max(r["total_s"] for r in rs), 2),
            "materialize_median_s": med("materialize_s"), "q1_median_s": med("q1_s"), "q2_median_s": med("q2_s"),
            "q3_median_s": med("q3_s"), "tasks_median": int(st.median(r["tasks"] for r in rs)),
            "shuffle_write_GB": round(st.median(r["shuffle_write_bytes"] for r in rs) / 1e9, 2),
            "shuffle_read_GB": round(st.median(r["shuffle_read_bytes"] for r in rs) / 1e9, 2),
            "disk_spill_GB": round(st.median(r["disk_spill_bytes"] for r in rs) / 1e9, 2),
            "base_rows": rs[0]["base_rows"], "app_ids": sorted({r["app_id"] for r in rs}),
            "max_tmp_after_cleanup_MB": round(max(r.get("tmp_bytes_after_cleanup", 0) for r in rs) / 1e6, 1),
        })
    rows.sort(key=lambda r: (r["aqe"], r["partitions"], r["persist"]))
    grid = [r for r in rows if not r["aqe"] and r.get("total_median_s") is not None]
    best = min(grid, key=lambda r: r["total_median_s"]) if grid else None
    cache_gain = {}
    for p in sorted({r["partitions"] for r in grid}):
        a = next((r for r in grid if r["partitions"] == p and r["persist"] == "none"), None)
        b = next((r for r in grid if r["partitions"] == p and r["persist"] == "memory_and_disk"), None)
        if a and b:
            cache_gain[str(p)] = round(a["total_median_s"] / b["total_median_s"], 2)
            q_none = a["q1_median_s"] + a["q2_median_s"] + a["q3_median_s"]
            q_cached = b["q1_median_s"] + b["q2_median_s"] + b["q3_median_s"]
            saving = (q_none - q_cached) / 3                      # seconds saved per reusing query
            BREAK_EVEN[str(p)] = {"query_saving_s": round(saving, 1), "materialize_s": b["materialize_median_s"],
                                  "break_even_queries": round(b["materialize_median_s"] / saving, 1) if saving > 0 else None}
    return runs, rows, best, cache_gain


def figure(rows):
    grid = [r for r in rows if not r["aqe"] and r.get("total_median_s") is not None]
    failed = [r for r in rows if r.get("status") == "failed"]
    fig, ax = plt.subplots(figsize=(8, 4.4), dpi=200)
    for persist in ("none", "memory_and_disk"):
        pts = sorted((r["partitions"], r["total_median_s"], r["total_min_s"], r["total_max_s"]) for r in grid if r["persist"] == persist)
        if not pts:
            continue
        # break the line wherever a configuration of this persistence failed, so no value is implied there
        fail_p = {r["partitions"] for r in failed if r["persist"] == persist}
        segs, cur = [], []
        for pt in pts:
            if cur and any(cur[-1][0] < fp < pt[0] for fp in fail_p):
                segs.append(cur); cur = []
            cur.append(pt)
        segs.append(cur)
        for i, seg in enumerate(segs):
            x = [p[0] for p in seg]; y = [p[1] for p in seg]
            err = [[p[1] - p[2] for p in seg], [p[3] - p[1] for p in seg]]
            ax.errorbar(x, y, yerr=err, color=COLORS[persist], linewidth=2, marker="o", markersize=5, capsize=3,
                        label=NAMES[persist] if i == 0 else None, markeredgecolor="white", markeredgewidth=1)
            for xi, yi in zip(x, y):
                ax.annotate(f"{yi:.0f} s", (xi, yi), xytext=(7, 3), textcoords="offset points", ha="left", fontsize=8, color=INK2)
    for r in failed:
        ax.scatter([r["partitions"]], [0], marker="x", s=60, color=COLORS[r["persist"]], zorder=5, clip_on=False)
        ax.annotate(f"{NAMES[r['persist']].split(' (')[0]}: failed\n({r['error_class'].split('.')[-1]})", (r["partitions"], 0),
                    xytext=(0, 10), textcoords="offset points", ha="center", fontsize=7, color=INK2)
    ref = next((r for r in rows if r["aqe"] and r.get("total_median_s") is not None), None)
    if ref:
        ax.axhline(ref["total_median_s"], color=INK2, linewidth=1, linestyle=(0, (4, 3)))
        ax.text(0.01, ref["total_median_s"], f"Spark defaults (AQE on, 200 partitions): {ref['total_median_s']:.0f} s",
                transform=ax.get_yaxis_transform(), va="bottom", fontsize=8, color=INK2)   # x in axes units, y in data
    ax.set_xscale("log", base=2)
    ax.set_xticks([8, 24, 96, 384]); ax.set_xticklabels(["8", "24\n(= cores)", "96", "384"])
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
    ax.set_xlabel("Partitions (repartition by state + spark.sql.shuffle.partitions), AQE off", color=INK2, fontsize=10)
    ax.set_ylabel("Median total time, 3 queries (s)", color=INK2, fontsize=10)
    ax.set_ylim(bottom=0)
    ax.set_title("Tuning on 303.6 M weather rows: partition count x persistence (median, min-max of 3)", color=INK, fontsize=10.5, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2, loc="upper center")
    # layout: bbox_inches="tight" at save time (annotations sit outside the axes)
    path = f"{PROJ}/docs/figures/tuning_partitions_cache.png"
    fig.savefig(path, facecolor="white", bbox_inches="tight"); plt.close(fig)
    return path


def markdown(rows, best, gain, fig, n):
    out = ["# Step 3c - Spark performance tuning: partitions x persistence", "",
           "Generated by `python/analyze_tuning.py` from `results/tuning/runs.jsonl`; nothing is typed by hand.", "",
           "Workload (`scala/TuningExperiment.scala`): `weather_cleaned` (303.6 M rows) broadcast-joined with `us_stations`, "
           "`repartition(P, state)`, optionally persisted, then three queries that reuse it (state x year x element mean; "
           "distinct stations per state x year; state x month mean TMAX). One warm-up iteration + 3 measured repeats per "
           "configuration, each configuration in a fresh JVM (`local[*]`, 24 cores, driver 3 GB). AQE is off in the grid so it "
           "cannot override P; the last row is Spark's defaults (AQE on, 200 partitions) for reference.", "",
           f"* Measured repeats: {n}",
           f"* Fastest grid configuration: **{best['label']}** ({best['total_median_s']} s median)" if best else "",
           f"* Speed-up from persistence at each partition count (none / persisted): {gain}",
           "* Caching break-even (materialisation time / average time saved per reusing query): "
           + "; ".join(f"P={p}: saves {v['query_saving_s']} s per query, costs {v['materialize_s']} s to build -> pays off after ~{v['break_even_queries']} queries"
                       if v["break_even_queries"] is not None else
                       f"P={p}: each query is {-v['query_saving_s']} s slower on the cache (too few partitions to read it in parallel) -> never pays off"
                       for p, v in BREAK_EVEN.items()), "",
           "Setup notes: shuffle and spill go to `spark.local.dir` on a Windows bind mount (`.spark-local`), because writing "
           "them inside the Docker VHDX grew the disk until WSL crashed (first attempt, archived as "
           "`results/tuning/archive_v1_runs.jsonl`). Host I/O therefore inflates absolute times for every configuration "
           "equally; comparisons between configurations remain fair. In the archived attempt p8-memory_and_disk failed with "
           "`SparkOutOfMemoryError` while it passed here: caching 303.6 M rows in a 3 GB heap is at the edge of feasibility.", "",
           "| Configuration | P | Persist | AQE | n | Total s median (min-max) | Materialise s | Q1 s | Q2 s | Q3 s | Tasks | Shuffle write GB | Spill GB |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        if r.get("status") == "failed":
            out.append(f"| {r['label']} | {r['partitions']} | {r['persist']} | {r['aqe']} | 0 | **failed** at repeat {r['failed_at_repeat']}: "
                       f"`{r['error_class']}` {r['error']} | | | | | | | |")
            continue
        out.append(f"| {r['label']} | {r['partitions']} | {r['persist']} | {r['aqe']} | {r['repeats']} | "
                   f"{r['total_median_s']} ({r['total_min_s']}-{r['total_max_s']}) | {r['materialize_median_s']} | "
                   f"{r['q1_median_s']} | {r['q2_median_s']} | {r['q3_median_s']} | {r['tasks_median']:,} | "
                   f"{r['shuffle_write_GB']} | {r['disk_spill_GB']} |")
    out += ["", f"Figure: `{fig.replace(PROJ + '/', '')}`", ""]
    return "\n".join(out)


if __name__ == "__main__":
    runs, rows, best, gain = summarise()
    fig = figure(rows)
    with open(f"{PROJ}/results/tuning_summary.json", "w", encoding="utf-8") as f:
        json.dump({"measured_repeats": len(runs), "best_grid_config": best, "persistence_speedup_by_partitions": gain,
                   "caching_break_even": BREAK_EVEN,
                   "rows": rows}, f, indent=2)
    with open(f"{PROJ}/docs/TUNING_RESULTS.md", "w", encoding="utf-8") as f:
        f.write(markdown(rows, best, gain, fig, len(runs)))
    print(f"repeats={len(runs)} configs={len(rows)} best={best['label'] if best else None} cache_gain={gain}")
