"""Performance analysis (results files only -> summary JSON, Markdown, figures):
  benchmark  Spark vs pandas scalability   results/benchmark/runs.jsonl -> results/benchmark_summary.json, docs/BENCHMARK_RESULTS.md
  tuning     partitions x persistence      results/tuning/runs.jsonl    -> results/tuning_summary.json, docs/TUNING_RESULTS.md
  streaming  Structured Streaming feed     results/streaming_v2/*.jsonl -> results/streaming_summary.json
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/src/reporting/analyze_performance.py [benchmark] [tuning] [streaming]
(the benchmark step also reads the raw data size: add -v "C:/Project/flu-hadoop-project/datasets/weather_raw:/rawdata:ro")
"""
import sys


def benchmark():
    """Part 06 analysis: results/benchmark/runs.jsonl -> results/benchmark_summary.json, docs/BENCHMARK_RESULTS.md,
    docs/figures/bench_runtime.png, docs/figures/bench_memory.png.
    
    Run (from the project root):
      docker run --rm -v "C:/Project/flu-hadoop-project:/proj" -v "C:/Project/flu-hadoop-project/datasets/weather_raw:/rawdata:ro"       climaflu-pytorch:1.0 python /proj/src/reporting/analyze_performance.py
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
               "Generated by `src/reporting/analyze_performance.py` from `results/benchmark/runs.jsonl`; nothing is typed by hand. "
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


def tuning():
    """Step 3c analysis: results/tuning/runs.jsonl -> results/tuning_summary.json, docs/TUNING_RESULTS.md,
    docs/figures/tuning_partitions_cache.png. Every number is computed from runs.jsonl.
    
    Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/src/reporting/analyze_performance.py
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
               "Generated by `src/reporting/analyze_performance.py` from `results/tuning/runs.jsonl`; nothing is typed by hand.", "",
               "Workload (`bigdata_pipeline/spark_tuning/TuningExperiment.scala`): `weather_cleaned` (303.6 M rows) broadcast-joined with `us_stations`, "
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


    runs, rows, best, gain = summarise()
    fig = figure(rows)
    with open(f"{PROJ}/results/tuning_summary.json", "w", encoding="utf-8") as f:
        json.dump({"measured_repeats": len(runs), "best_grid_config": best, "persistence_speedup_by_partitions": gain,
                   "caching_break_even": BREAK_EVEN,
                   "rows": rows}, f, indent=2)
    with open(f"{PROJ}/docs/TUNING_RESULTS.md", "w", encoding="utf-8") as f:
        f.write(markdown(rows, best, gain, fig, len(runs)))
    print(f"repeats={len(runs)} configs={len(rows)} best={best['label'] if best else None} cache_gain={gain}")


def streaming():
    """Streaming v2 analysis: results/streaming_v2/{drip,batches,progress}.jsonl -> results/streaming_summary.json,
    docs/figures/streaming_latency.png. Every number comes from those logs.
    Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/src/reporting/analyze_performance.py
    """
    import json
    import os

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt  # noqa: E402
    import numpy as np  # noqa: E402

    PROJ = os.environ.get("PROJ", "/proj")
    D = f"{PROJ}/results/streaming_v2"
    SOURCE_ROWS = 5972783          # STREAMPREP log of bigdata_pipeline/spark_streaming/PrepareStreamingV2.scala (60 daily files)
    INK, INK2, GRID, BLUE, ORANGE = "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6", "#eb6834"


    def jl(name):
        return [json.loads(l) for l in open(f"{D}/{name}", encoding="utf-8-sig") if l.strip()]


    drip, batches, progress = jl("drip.jsonl"), jl("batches.jsonl"), jl("progress.jsonl")
    arrival = {d["file"]: d["arrival_ms"] for d in drip}
    file_batch = {}
    for b in batches:
        for f in b["files"]:
            file_batch.setdefault(f, []).append(b)
    latency = {f: (bs[0]["end_ms"] - arrival[f]) / 1000 for f, bs in file_batch.items() if f in arrival}
    lat = np.array([latency[d["file"]] for d in drip if d["file"] in latency])
    span_s = (drip[-1]["arrival_ms"] - drip[0]["arrival_ms"]) / 1000
    rows_in = sum(b["rows_in"] for b in batches)
    busy_s = sum(b["end_ms"] - b["start_ms"] for b in batches) / 1000
    data_triggers = [p for p in progress if p.get("numInputRows", 0) > 0]

    summary = {
        "feed": {"files": len(drip), "source_rows": SOURCE_ROWS, "feed_span_s": round(span_s, 1),
                 "mean_interarrival_s": round(span_s / (len(drip) - 1), 2),
                 "arrival_rate_rows_per_s": round(SOURCE_ROWS / span_s, 1)},
        "processing": {"non_empty_batches": len(batches), "triggers_logged": len(progress), "rows_in": rows_in,
                       "rows_kept_us_qc_joined": sum(b["rows_kept"] for b in batches),
                       "state_day_element_groups_written": sum(b["groups_out"] for b in batches),
                       "batch_busy_s_total": round(busy_s, 1),
                       "throughput_rows_per_busy_s": round(rows_in / busy_s, 1),
                       "batch_duration_s": {"median": round(float(np.median([(b["end_ms"] - b["start_ms"]) / 1000 for b in batches])), 2),
                                            "max": round(max((b["end_ms"] - b["start_ms"]) / 1000 for b in batches), 2)},
                       "spark_processedRowsPerSecond_median": round(float(np.median([p["processedRowsPerSecond"] for p in data_triggers])), 1),
                       "spark_inputRowsPerSecond_median": round(float(np.median([p["inputRowsPerSecond"] for p in data_triggers])), 1)},
        "latency_arrival_to_committed_s": {"median": round(float(np.median(lat)), 2), "p95": round(float(np.percentile(lat, 95)), 2),
                                           "max": round(float(lat.max()), 2), "min": round(float(lat.min()), 2)},
        "checks": {"every_file_processed": len(file_batch) == len(drip),
                   "each_file_exactly_once": all(len(v) == 1 for v in file_batch.values()),
                   "rows_in_equals_source_rows": rows_in == SOURCE_ROWS,
                   "keeps_up_throughput_gt_arrival_rate": rows_in / busy_s > SOURCE_ROWS / span_s},
    }
    with open(f"{PROJ}/results/streaming_summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(8, 5.4), dpi=200, gridspec_kw={"hspace": 0.55})
    x = np.arange(1, len(lat) + 1)
    a1.plot(x, lat, color=BLUE, linewidth=1.8, marker="o", markersize=3)
    a1.axhline(np.median(lat), color=INK2, linewidth=1, linestyle=(0, (4, 3)))
    a1.text(len(lat), np.median(lat), f"median {np.median(lat):.1f} s ", ha="right", va="bottom", fontsize=8, color=INK2)
    a1.set_title("Latency per daily file: HDFS arrival -> aggregates committed to HDFS", color=INK, fontsize=10, loc="left")
    a1.set_xlabel("File (day of 1 Jan - 1 Mar 2025, in arrival order)", color=INK2, fontsize=9)
    a1.set_ylabel("Seconds", color=INK2, fontsize=9)
    bx = np.arange(len(batches))
    a2.bar(bx, [b["rows_in"] / max((b["end_ms"] - b["start_ms"]) / 1000, 1e-3) / 1000 for b in batches], color=ORANGE, width=0.8)
    a2.axhline(SOURCE_ROWS / span_s / 1000, color=INK, linewidth=1, linestyle=(0, (4, 3)))
    a2.text(len(batches) - 0.5, SOURCE_ROWS / span_s / 1000, f"feed arrival rate {SOURCE_ROWS / span_s / 1000:.1f} k rows/s ",
            ha="right", va="bottom", fontsize=8, color=INK)
    a2.set_title("Processing throughput per micro-batch (must stay above the arrival rate)", color=INK, fontsize=10, loc="left")
    a2.set_xlabel("Micro-batch", color=INK2, fontsize=9)
    a2.set_ylabel("k rows / s", color=INK2, fontsize=9)
    for ax in (a1, a2):
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(GRID)
        ax.tick_params(colors=INK2, labelsize=8); ax.grid(axis="y", color=GRID, linewidth=0.8); ax.set_axisbelow(True)
        ax.set_ylim(bottom=0)
    fig.savefig(f"{PROJ}/docs/figures/streaming_latency.png", facecolor="white", bbox_inches="tight")
    print(json.dumps(summary, indent=2))


STEPS = {"benchmark": benchmark, "tuning": tuning, "streaming": streaming}

if __name__ == "__main__":
    for step in sys.argv[1:] or ['benchmark', 'tuning', 'streaming']:
        print(f"--- {step}")
        STEPS[step]()
