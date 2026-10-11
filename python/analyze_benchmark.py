"""Part 06 analysis: results/benchmark/runs.jsonl -> results/benchmark_summary.json, docs/BENCHMARK_RESULTS.md,
docs/figures/bench_runtime.png, docs/figures/bench_memory.png.

Run (from the project root):
  docker run --rm -v "C:/Project/flu-hadoop-project:/proj" -v "C:/Project/flu-hadoop-project/datasets/weather_raw:/rawdata:ro" \
      climaflu-pytorch:1.0 python /proj/python/analyze_benchmark.py
Every number written here is computed from runs.jsonl; nothing is typed by hand.
"""
import json
import re
import os
import statistics as st
from collections import defaultdict
from datetime import datetime, timezone

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

PROJ = os.environ.get("PROJ", "/proj")
RAW = os.environ.get("RAW_DIR", "/rawdata")
RUNS = os.environ.get("RUNS", f"{PROJ}/results/benchmark/runs.jsonl")
FIG_DIR = f"{PROJ}/docs/figures"
BASELINE = ("A", "pandas-chunked")          # best pandas variant that fits the equal 4 GB cap

# Fixed colour per entity (reference categorical palette, light mode, fixed order).
STYLE = {
    "spark-hdfs-local[*]":    ("#2a78d6", "Spark, HDFS, 24 cores"),
    "spark-localfs-local[*]": ("#1baf7a", "Spark, local files, 24 cores"),
    "spark-hdfs-local[4]":    ("#4a3aa7", "Spark, HDFS, 4 cores"),
    "spark-hdfs-local[1]":    ("#e87ba4", "Spark, HDFS, 1 core"),
    "pandas-chunked":         ("#eb6834", "pandas chunked (4 GB)"),
    "pandas-pyarrow":         ("#eda100", "pandas + pyarrow, in-memory (6 GB)"),
    "pandas-optimized":       ("#e34948", "pandas optimized, in-memory (6 GB)"),
}
INK, INK2, GRID = "#0b0b0b", "#52514e", "#e4e3df"

# filled in __main__ from results/benchmark/kernel_oom_log.txt
KILLS = []


def input_bytes(years):
    return sum(os.path.getsize(f"{RAW}/{y}.csv") for y in range(2000, 2000 + years))


def utc_epoch(iso):
    """PowerShell writes 7 fractional digits ('...42.7218085Z'); trim to microseconds before parsing."""
    m = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?", iso)
    frac = (m.group(2) or "0")[:6].ljust(6, "0")
    return datetime.strptime(f"{m.group(1)}.{frac}", "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc).timestamp()


def kernel_oom_kills():
    """Parse results/benchmark/kernel_oom_log.txt (dmesg capture) -> list of kills with wall-clock time and cause.
    CONSTRAINT_MEMCG = the container's own memory cap was hit; CONSTRAINT_NONE = the whole Docker VM ran out."""
    path = f"{PROJ}/results/benchmark/kernel_oom_log.txt"
    if not os.path.exists(path):
        return []
    text = open(path, encoding="utf-8").read()
    now, up = float(re.search(r"NOW_EPOCH (\S+)", text).group(1)), float(re.search(r"UPTIME (\S+)", text).group(1))
    kills, constraint = [], None
    for line in text.splitlines():
        m = re.match(r"\[\s*([\d.]+)\] oom-kill:constraint=(\w+)", line)
        if m:
            constraint = m.group(2)
            continue
        m = re.match(r"\[\s*([\d.]+)\] .*Killed process \d+ \((\w+)\).*anon-rss:(\d+)kB", line)
        if m:
            kills.append({"epoch": now - up + float(m.group(1)), "process": m.group(2),
                          "anon_rss_GB": round(int(m.group(3)) * 1024 / 1e9, 2),
                          "cause": {"CONSTRAINT_MEMCG": "container memory cap", "CONSTRAINT_NONE": "Docker VM out of memory"}
                          .get(constraint, constraint)})
    return kills


def attach_oom_causes(recs, kills, tolerance_s=60):
    """Match each oom_killed run to the kernel kill that happened just before the run ended."""
    for r in recs:
        if r.get("status") != "oom_killed":
            continue
        end = utc_epoch(r["finished_at"])
        cands = [k for k in kills if k["process"] == "python" and 0 <= end - k["epoch"] <= tolerance_s]
        if cands:
            k = max(cands, key=lambda k: k["epoch"])
            r["oom_cause"], r["oom_anon_rss_GB"] = k["cause"], k["anon_rss_GB"]
        else:
            r["oom_cause"] = "unmatched"


def load():
    runs = [json.loads(l) for l in open(RUNS, encoding="utf-8") if l.strip()]
    attach_oom_causes(runs, KILLS)
    groups = defaultdict(list)
    for r in runs:
        groups[(r["experiment"], r["label"], r["years"])].append(r)
    rows = []
    for (exp, label, years), rs in sorted(groups.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2])):
        ok = [r for r in rs if r["status"] == "ok"]
        row = {"experiment": exp, "label": label, "engine": rs[0]["engine"], "years": years,
               "mem_limit": rs[0]["mem_limit"], "runs": len(rs), "ok_runs": len(ok),
               "status": "ok" if ok and len(ok) == len(rs) else rs[-1]["status"],
               "input_bytes": input_bytes(years)}
        if rs[-1].get("oom_cause"):
            row["oom_cause"] = rs[-1]["oom_cause"]
            row["oom_anon_rss_GB"] = rs[-1].get("oom_anon_rss_GB")
        if ok:
            t = [r["elapsed_s"] for r in ok]
            row.update({
                "elapsed_median_s": round(st.median(t), 3), "elapsed_min_s": min(t), "elapsed_max_s": max(t),
                "wall_median_s_incl_startup": round(st.median(r["wall_s_incl_startup"] for r in ok), 2),
                "throughput_MBps": round(row["input_bytes"] / 1e6 / st.median(t), 1),
                "peak_rss_GB": round(max(r["peak_rss_bytes"] for r in ok) / 1e9, 3),
                "cgroup_peak_GB": round(max(r["cgroup_peak_bytes"] for r in ok) / 1e9, 3),
                "rows_kept": ok[0]["rows_kept"], "result_sha256": ok[0]["result_sha256"],
                "cores": ok[0].get("cores"),
            })
        rows.append(row)
    return runs, rows


def checksum_agreement(rows):
    by_years = defaultdict(set)
    for r in rows:
        if r.get("result_sha256"):
            by_years[r["years"]].add(r["result_sha256"])
    return {str(y): len(s) == 1 for y, s in sorted(by_years.items())}


def add_speedups(rows):
    base = {r["years"]: r["elapsed_median_s"] for r in rows
            if (r["experiment"], r["label"]) == BASELINE and r.get("elapsed_median_s")}
    for r in rows:
        if r.get("elapsed_median_s") and r["years"] in base:
            r["speedup_vs_pandas_chunked"] = round(base[r["years"]] / r["elapsed_median_s"], 2)


def series(rows, exp, label, key):
    pts = [(r["years"], r[key]) for r in rows if r["experiment"] == exp and r["label"] == label and r.get(key) is not None]
    return [p[0] for p in pts], [p[1] for p in pts]


def style_axes(ax, xlabel, ylabel):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel(xlabel, color=INK2, fontsize=10)
    ax.set_ylabel(ylabel, color=INK2, fontsize=10)


def fig_runtime(rows):
    fig, ax = plt.subplots(figsize=(8, 4.8), dpi=200)
    lines = [("A", "spark-hdfs-local[*]"), ("A", "spark-localfs-local[*]"), ("A", "spark-hdfs-local[4]"),
             ("A", "spark-hdfs-local[1]"), ("A", "pandas-chunked")]  # Experiment A only (equal 4 GB cap)
    for exp, label in lines:
        x, y = series(rows, exp, label, "elapsed_median_s")
        if not x:
            continue
        color, name = STYLE[label]
        ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=5, label=name,
                markeredgecolor="white", markeredgewidth=1)
        ax.annotate(f"{y[-1]:.0f} s", (x[-1], y[-1]), xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=8, color=INK2)
    style_axes(ax, "Years of GHCN-Daily data (1.2-1.5 GB raw CSV per year)", "Median run time (s), log scale")
    ax.set_yscale("log")
    ax.set_title("Same workload, same 4 GB cap: run time vs data volume (in-memory pandas: OOM at 1 year)", color=INK, fontsize=10.5, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2, loc="upper left")
    fig.tight_layout()
    path = f"{FIG_DIR}/bench_runtime.png"
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    return path


def fig_memory(rows):
    fig, ax = plt.subplots(figsize=(8, 4.4), dpi=200)
    lines = [("A", "spark-hdfs-local[*]"), ("A", "spark-hdfs-local[1]"), ("A", "pandas-chunked"),
             ("B", "pandas-pyarrow"), ("B", "pandas-optimized")]
    for exp, label in lines:
        x, y = series(rows, exp, label, "peak_rss_GB")
        if not x:
            continue
        color, name = STYLE[label]
        ax.plot(x, y, color=color, linewidth=2, marker="o", markersize=5, label=name,
                markeredgecolor="white", markeredgewidth=1)
    ax.axhline(4.0, color=INK2, linewidth=1, linestyle=(0, (4, 3)))
    ax.text(ax.get_xlim()[1], 4.0, "  4 GB cap (Experiment A)", va="bottom", ha="right", fontsize=8, color=INK2)
    style_axes(ax, "Years of GHCN-Daily data", "Peak process memory, VmHWM (GB)")
    ax.set_ylim(bottom=0)
    ax.set_title("Peak memory: in-memory pandas needs more than the 4 GB cap from the first year", color=INK, fontsize=11, loc="left")
    ax.legend(frameon=False, fontsize=8, labelcolor=INK2, loc="upper right", bbox_to_anchor=(1.0, 0.74))
    x, y = series(rows, "B", "pandas-pyarrow", "peak_rss_GB")
    if x:
        ax.annotate(f"pandas + pyarrow: {y[0]:.1f} GB for one 1.2 GB file;\nkilled at 3 years when the 8 GB Docker VM ran out (~5 GB)",
                    (x[0], y[0]), xytext=(10, -4),
                    textcoords="offset points", va="top", fontsize=8, color=INK2)
    fig.tight_layout()
    path = f"{FIG_DIR}/bench_memory.png"
    fig.savefig(path, facecolor="white")
    plt.close(fig)
    return path


def fmt(v, nd=1):
    return "-" if v is None else (f"{v:,.{nd}f}" if isinstance(v, float) else f"{v:,}")


def markdown(rows, agree, figs, n_runs):
    out = ["# Part 06 - Spark vs pandas benchmark: results",
           "",
           "Generated by `python/analyze_benchmark.py` from `results/benchmark/runs.jsonl`; nothing is typed by hand. "
           "Protocol: `docs/BENCHMARK_PROTOCOL.md`.",
           "",
           f"* Runs recorded: {n_runs}",
           f"* Result checksum identical across every engine that finished, per data size: {agree}",
           "",
           "| Exp | Configuration | Mem cap | Years | Input GB | Status | Median s (min-max) | Wall s incl. startup | MB/s | Peak RSS GB | Speed-up vs pandas-chunked |",
           "|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        rng = (f"{r['elapsed_median_s']:.1f} ({r['elapsed_min_s']:.1f}-{r['elapsed_max_s']:.1f})"
               if r.get("elapsed_median_s") else "-")
        out.append("| {experiment} | {label} | {mem_limit} | {years} | {gb:.2f} | {status}{cause} | {rng} | {wall} | {tp} | {rss} | {sp} |".format(
            gb=r["input_bytes"] / 1e9, rng=rng, cause=(f" ({r['oom_cause']}, {r.get('oom_anon_rss_GB')} GB)" if r.get("oom_cause") else ""), wall=fmt(r.get("wall_median_s_incl_startup")),
            tp=fmt(r.get("throughput_MBps")), rss=fmt(r.get("peak_rss_GB"), 2),
            sp=fmt(r.get("speedup_vs_pandas_chunked"), 2), **r))
    out += ["", "## All in-memory pandas attempts (smoke pass + full run)", "",
            "In-memory pandas sits close to the memory cap, so the same configuration can pass once and fail once. "
            "Every attempt from both run files is listed; none is dropped.", "",
            "| Run file | Exp | Configuration | Mem cap | Years | Status | Kill cause (kernel log) | RSS at kill GB | Elapsed s | Peak RSS GB |", "|---|---|---|---|---|---|---|---|---|---|"]
    for name, recs in attempts():
        attach_oom_causes(recs, KILLS)
        for r in recs:
            rss = f"{r['peak_rss_bytes'] / 1e9:.2f}" if r.get("peak_rss_bytes") else "-"
            out.append(f"| {name} | {r['experiment']} | {r['label']} | {r['mem_limit']} | {r['years']} | {r['status']} | {r.get('oom_cause', '-')} | {r.get('oom_anon_rss_GB', '-')} | "
                       f"{r.get('elapsed_s', '-')} | {rss} |")
    out += ["", "Figures:", ""] + [f"* `{p.replace(PROJ + '/', '')}`" for p in figs] + [""]
    return "\n".join(out)


def attempts():
    files = [("smoke_runs.jsonl", f"{PROJ}/results/benchmark/smoke_runs.jsonl"), ("runs.jsonl", RUNS)]
    for name, path in files:
        if os.path.exists(path):
            recs = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
            yield name, [r for r in recs if r["engine"] == "pandas" and r["label"] != "pandas-chunked"]


if __name__ == "__main__":
    os.makedirs(FIG_DIR, exist_ok=True)
    KILLS.extend(kernel_oom_kills())
    runs, rows = load()
    add_speedups(rows)
    agree = checksum_agreement(rows)
    figs = [fig_runtime(rows), fig_memory(rows)]
    summary = {"runs_recorded": len(runs), "checksum_agreement_by_years": agree,
               "all_checksums_agree": all(agree.values()), "baseline": "/".join(BASELINE), "rows": rows}
    with open(f"{PROJ}/results/benchmark_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    with open(f"{PROJ}/docs/BENCHMARK_RESULTS.md", "w", encoding="utf-8") as f:
        f.write(markdown(rows, agree, figs, len(runs)))
    print(f"runs={len(runs)} configs={len(rows)} checksums_agree={summary['all_checksums_agree']}")
    for p in figs:
        print("figure:", p)
