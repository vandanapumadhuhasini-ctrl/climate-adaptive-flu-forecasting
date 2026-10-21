"""Run every neural model x seed of docs/RESEARCH_PROTOCOL.md in parallel worker processes (resumable), then merge the
per-run JSON files into one runs.jsonl.
  --version v2   rigour pass, next week:       train_v2.py, models x 10 seeds        -> results/rigour/runs.jsonl
  --version v3   amendment A1, horizons 1-4:   train_v3.py, models x 4 h x 10 seeds  -> results/v3/runs.jsonl
Run: docker exec -w /workspace/gnn pytorch python run_all.py --version v2 [--workers 6]
"""
import argparse
import glob
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

SEEDS = range(10)


def run(job):
    """job = (script, out_dir, run name, extra CLI args); skipped when its result file already exists."""
    script, out, name, args = job
    if os.path.exists(f"{out}/runs/{name}.json"):
        return f"skip {name}"
    t0 = time.time()
    p = subprocess.run(["python", script] + args, capture_output=True, text=True, env={**os.environ, "TORCH_THREADS": "3"})
    os.makedirs(f"{out}/logs", exist_ok=True)
    with open(f"{out}/logs/{name}.log", "w") as f:
        f.write(p.stdout + p.stderr)
    status = "ok" if p.returncode == 0 else f"FAILED (exit {p.returncode})"
    return f"{time.strftime('%H:%M:%S')} {name:<22} {status} {time.time() - t0:5.0f}s {p.stdout.strip()[-120:]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", choices=["v2", "v3"], required=True)
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    if a.version == "v2":
        from common import OUT
        from train_v2 import MODELS
        # seed-major: every model gets early seeds first
        jobs = [("train_v2.py", OUT, f"{m}_s{s}", ["--model", m, "--seed", str(s)]) for s in SEEDS for m in MODELS]
        order = lambda r: (r["model"], r["seed"])
    else:
        from train_v3 import MODELS, OUT
        jobs = [("train_v3.py", OUT, f"{m}_h{h}_s{s}", ["--model", m, "--h", str(h), "--seed", str(s)])
                for s in SEEDS for h in (1, 2, 3, 4) for m in MODELS]
        order = lambda r: (r["model"], r["horizon"], r["seed"])
    with ThreadPoolExecutor(a.workers) as ex:
        for line in ex.map(run, jobs):
            print(line, flush=True)
    recs = [json.load(open(f)) for f in sorted(glob.glob(f"{OUT}/runs/*.json"))]
    with open(f"{OUT}/runs.jsonl", "w") as f:
        for r in sorted(recs, key=order):
            f.write(json.dumps(r) + "\n")
    print(f"merged {len(recs)} runs into {OUT}/runs.jsonl (expected {len(jobs)})")
