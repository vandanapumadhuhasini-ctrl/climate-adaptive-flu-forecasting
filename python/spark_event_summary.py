"""Summarise every Spark event log in results/spark-events/ -> results/spark_event_log_summary.json.
Each application is named after the job that ran it, by matching the 'app id = local-...' line that spark-shell prints
into the run logs (logs/*.log, logs/tuning/*.log).
Run: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/spark_event_summary.py
"""
import glob
import json
import os
import re

PROJ = os.environ.get("PROJ", "/proj")
names = {}
for log in glob.glob(f"{PROJ}/logs/*.log") + glob.glob(f"{PROJ}/logs/tuning/*.log"):
    try:
        raw = open(log, "rb").read(400000)
    except OSError:
        continue
    # early logs were written by PowerShell 5 Tee-Object in UTF-16 LE (NUL bytes between ASCII characters)
    text = raw.decode("utf-16-le", errors="ignore") if raw[1:200:2].count(0) > 50 else raw.decode("utf-8", errors="ignore")
    name = re.sub(r"_\d{8}_\d{6}$", "", os.path.basename(log)[:-4])
    if name.endswith(".v1"):
        name = "tuning-attempt1-" + name[:-3]      # first tuning attempt (archived; it filled the Docker disk)
    for app in re.findall(r"app id = (local-\d+)", text):
        names[app] = name

apps = []
for f in sorted(glob.glob(f"{PROJ}/results/spark-events/*")):
    a = {"app_id": os.path.basename(f).split(".")[0], "jobs": 0, "stages": 0, "tasks": 0, "failed_tasks": 0,
         "shuffle_write_bytes": 0, "input_bytes": 0, "start_ms": None, "end_ms": None}
    with open(f, encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            try:
                e = json.loads(line)
            except ValueError:
                continue
            ev = e.get("Event")
            if ev == "SparkListenerApplicationStart":
                a["start_ms"] = e.get("Timestamp")
            elif ev == "SparkListenerApplicationEnd":
                a["end_ms"] = e.get("Timestamp")
            elif ev == "SparkListenerJobStart":
                a["jobs"] += 1
            elif ev == "SparkListenerStageCompleted":
                a["stages"] += 1
            elif ev == "SparkListenerTaskEnd":
                a["tasks"] += 1
                if e.get("Task End Reason", {}).get("Reason") != "Success":
                    a["failed_tasks"] += 1
                m = e.get("Task Metrics") or {}
                a["shuffle_write_bytes"] += (m.get("Shuffle Write Metrics") or {}).get("Shuffle Bytes Written", 0)
                a["input_bytes"] += (m.get("Input Metrics") or {}).get("Bytes Read", 0)
    a["duration_s"] = round((a["end_ms"] - a["start_ms"]) / 1000, 1) if a["start_ms"] and a["end_ms"] else None
    # no surviving log: the run log was overwritten when the same configuration was re-run (watchdog-aborted tuning runs)
    a["job"] = names.get(a["app_id"], "aborted run (log overwritten by its re-run)")
    apps.append(a)

summary = {"applications": len(apps), "total_tasks": sum(a["tasks"] for a in apps),
           "total_input_GB": round(sum(a["input_bytes"] for a in apps) / 1e9, 2),
           "apps": sorted(apps, key=lambda a: a["start_ms"] or 0)}
with open(f"{PROJ}/results/spark_event_log_summary.json", "w") as f:
    json.dump(summary, f, indent=2)
print(f"{len(apps)} applications, {summary['total_tasks']:,} tasks, {summary['total_input_GB']} GB read")
for a in summary["apps"]:
    print(f"  {a['app_id']:<22} {a['job']:<34} {a['duration_s']!s:>8}s tasks={a['tasks']:>6} failed={a['failed_tasks']} in={a['input_bytes'] / 1e9:.2f}GB")
