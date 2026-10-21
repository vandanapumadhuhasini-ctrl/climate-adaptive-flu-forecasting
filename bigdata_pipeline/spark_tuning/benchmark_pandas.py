"""Part 06 benchmark workload (pandas side). Identical logic to bigdata_pipeline/spark_tuning/BenchmarkWorkload.scala:
read N yearly GHCN CSVs (2000..2000+N-1) -> keep US stations, TMAX/TMIN/PRCP, no quality flag
-> aggregate by (element, year): count, sum(value) -> sha256 of the sorted result for cross-engine equality.

Variants (the "how would a competent pandas user do it" ladder):
  naive      read everything as str, concat, then filter                  (typical first attempt)
  optimized  per-file read, usecols + compact dtypes, filter, concat kept rows   (C parser, 1 thread)
  pyarrow    as optimized but engine="pyarrow" (multi-threaded CSV parser)
  chunked    streaming read_csv(chunksize), aggregate per chunk, combine  (bounded memory)

Usage: python benchmark_workload.py <variant> <years>     Prints exactly one line starting with BENCHJSON.
"""
import hashlib
import json
import os
import sys
import time

import pandas as pd

RAW = os.environ.get("RAW_DIR", "/rawdata")
COLS = ["id", "date", "element", "value", "mflag", "qflag", "sflag", "obstime"]
HEADER = {"ID": "id", "DATE": "date", "ELEMENT": "element", "DATA_VALUE": "value", "Q_FLAG": "qflag"}
# compact dtypes: date has <=366 distinct values per file, values are tenths that fit in int32 (summed as int64)
DTYPES = {"id": "category", "date": "category", "element": "category", "value": "int32", "qflag": "category"}
ELEMENTS = ["TMAX", "TMIN", "PRCP"]
CHUNK = 2_000_000


def keep(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["id"].astype(str).str.startswith("US")]
    df = df[df["element"].isin(ELEMENTS)]
    return df[df["qflag"].isna() | (df["qflag"].astype(str) == "")]


def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    g = df.assign(year=df["date"].astype(str).str[:4], element=df["element"].astype(str), value=df["value"].astype("int64"))
    return g.groupby(["element", "year"], observed=True)["value"].agg(n="size", s="sum").reset_index()


def read(path, engine="c", **kw):
    # The yearly files carry a header row (ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME); use it directly
    # (the pyarrow engine rejects names= combined with skiprows=).
    dtypes = {raw: DTYPES[name] for raw, name in HEADER.items()}
    out = pd.read_csv(path, header=0, usecols=list(HEADER), dtype=dtypes, engine=engine, **kw)
    if "chunksize" in kw:
        return (c.rename(columns=HEADER) for c in out)
    return out.rename(columns=HEADER)


def run(variant: str, paths):
    if variant == "naive":
        df = pd.concat([pd.read_csv(p, header=None, skiprows=1, names=COLS, dtype=str, low_memory=False) for p in paths],
                       ignore_index=True)
        df = keep(df)
        df["value"] = df["value"].astype("int64")
        return aggregate(df)
    if variant in ("optimized", "pyarrow"):
        engine = "pyarrow" if variant == "pyarrow" else "c"
        df = pd.concat([keep(read(p, engine=engine)) for p in paths], ignore_index=True)
        return aggregate(df)
    if variant == "chunked":
        parts = [aggregate(keep(chunk)) for p in paths for chunk in read(p, chunksize=CHUNK)]
        return pd.concat(parts).groupby(["element", "year"], as_index=False)[["n", "s"]].sum()
    raise SystemExit(f"unknown variant {variant}")


def proc_status(key):
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith(key):
                return int(line.split()[1]) * 1024
    return -1


def cgroup_peak():
    try:
        with open("/sys/fs/cgroup/memory.peak") as f:
            return int(f.read().strip())
    except OSError:
        return -1


if __name__ == "__main__":
    variant, years = sys.argv[1], int(sys.argv[2])
    paths = [f"{RAW}/{y}.csv" for y in range(2000, 2000 + years)]
    t0 = time.perf_counter()
    res = run(variant, paths)
    elapsed = time.perf_counter() - t0
    lines = sorted(f"{r.element}|{r.year}|{int(r.n)}|{int(r.s)}" for r in res.itertuples(index=False))
    print("BENCHJSON " + json.dumps({
        "engine": "pandas", "label": f"pandas-{variant}", "source": "local", "master": "single-process",
        "cores": os.cpu_count(), "years": years, "rows_kept": int(res["n"].sum()), "groups": len(lines),
        "result_sha256": hashlib.sha256("\n".join(lines).encode()).hexdigest(), "elapsed_s": round(elapsed, 3),
        "peak_rss_bytes": proc_status("VmHWM"), "cgroup_peak_bytes": cgroup_peak(), "status": "ok"}))
