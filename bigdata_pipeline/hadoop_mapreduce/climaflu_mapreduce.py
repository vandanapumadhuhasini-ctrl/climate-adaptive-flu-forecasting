#!/usr/bin/env python3
"""The two ClimaFlu MapReduce jobs in Python, run on YARN by Hadoop Streaming (see run_mapreduce.sh).
Written for Python 3.5 (the NodeManager's Debian 9 python3), so no f-strings.

  map_element_year   MR1 mapper: kept observation -> "element|year <TAB> 1 <TAB> value"
  map_state_year     MR2 mapper: map-side join with the stations file shipped by the distributed cache
                     (-files ...ghcnd-stations.txt#stations) -> "state|year|element <TAB> 1 <TAB> value"
  reduce             combiner AND reducer of both jobs: adds (count, sum) per key -> "key <TAB> count <TAB> sum"

Filter (identical to the Spark and pandas jobs): US station, element in {TMAX, TMIN, PRCP}, empty quality flag,
integer value. Record accounting goes to Hadoop counters (group ClimaFlu) through the streaming
"reporter:counter:<group>,<name>,<amount>" lines on stderr, written once per task.
"""
import io
import re
import sys

ELEMENTS = {"TMAX", "TMIN", "PRCP"}
INTEGER = re.compile(r"[+-]?[0-9]+\Z")      # what a strict long parser accepts (no spaces, no decimals)


def parse(line):
    """GHCN yearly CSV line ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME -> (outcome, record or None)."""
    f = line.split(",")
    if len(f) < 6:
        return "MALFORMED", None
    if f[0] == "ID":
        return "HEADER", None
    if not f[0].startswith("US"):
        return "NOT_US", None
    if f[2] not in ELEMENTS:
        return "OTHER_ELEMENT", None
    if f[5] != "":
        return "QC_FLAGGED", None
    if len(f[1]) < 4 or not INTEGER.match(f[3]):
        return "MALFORMED", None
    return "KEPT", (f[0], f[1][:4], f[2], int(f[3]))


def counters(values):
    for name, amount in sorted(values.items()):
        if amount:
            sys.stderr.write("reporter:counter:ClimaFlu,{},{}\n".format(name, amount))


def lines():
    for line in io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace"):
        yield line.rstrip("\r\n")


def map_element_year():
    seen = dict.fromkeys(("KEPT", "HEADER", "MALFORMED", "NOT_US", "OTHER_ELEMENT", "QC_FLAGGED"), 0)
    out = sys.stdout.write
    for line in lines():
        outcome, rec = parse(line)
        seen[outcome] += 1
        if rec:
            out("{}|{}\t1\t{}\n".format(rec[2], rec[1], rec[3]))
    counters(seen)


def map_state_year():
    state_of = {}
    with io.open("stations", encoding="utf-8", errors="replace") as f:       # distributed-cache symlink
        for l in f:
            l = l.rstrip("\r\n")
            if len(l) >= 40 and l.startswith("US") and l[38:40].strip():
                state_of[l[0:11].strip()] = l[38:40].strip()
    seen = {"JOINED": 0, "NO_STATE": 0, "STATIONS_LOADED_PER_MAPPER": len(state_of)}
    out = sys.stdout.write
    for line in lines():
        outcome, rec = parse(line)
        if not rec:
            continue
        state = state_of.get(rec[0])
        if state is None:
            seen["NO_STATE"] += 1
            continue
        seen["JOINED"] += 1
        out("{}|{}|{}\t1\t{}\n".format(state, rec[1], rec[2], rec[3]))
    counters(seen)


def reduce():
    """Input is sorted by key, so equal keys are consecutive; (count, sum) pairs add up (also valid as a combiner)."""
    out = sys.stdout.write
    key, n, s = None, 0, 0
    for line in lines():
        k, c, v = line.split("\t")
        if k != key:
            if key is not None:
                out("{}\t{}\t{}\n".format(key, n, s))
            key, n, s = k, 0, 0
        n += int(c)
        s += int(v)
    if key is not None:
        out("{}\t{}\t{}\n".format(key, n, s))


if __name__ == "__main__":
    {"map_element_year": map_element_year, "map_state_year": map_state_year, "reduce": reduce}[sys.argv[1]]()
