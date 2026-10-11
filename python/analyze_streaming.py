"""Streaming v2 analysis: results/streaming_v2/{drip,batches,progress}.jsonl -> results/streaming_summary.json,
docs/figures/streaming_latency.png. Every number comes from those logs.
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/analyze_streaming.py
"""
import json
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

PROJ = os.environ.get("PROJ", "/proj")
D = f"{PROJ}/results/streaming_v2"
SOURCE_ROWS = 5972783          # STREAMPREP log of scala/PrepareStreamingV2.scala (60 daily files)
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
