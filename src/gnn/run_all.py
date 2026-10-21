"""Run every neural model x seed of docs/RESEARCH_PROTOCOL.md in parallel worker processes (resumable), then merge
results/rigour/runs/*.json into results/rigour/runs.jsonl.
Run: docker exec -w /workspace/gnn pytorch python run_all.py [--workers 6]
"""
import argparse
import glob
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from common import OUT
from train_v2 import MODELS

SEEDS = range(10)


def run(job):
    model, seed = job
    if os.path.exists(f"{OUT}/runs/{model}_s{seed}.json"):
        return f"skip {model} s{seed}"
    t0 = time.time()
    p = subprocess.run(["python", "train_v2.py", "--model", model, "--seed", str(seed)], capture_output=True, text=True,
                       env={**os.environ, "TORCH_THREADS": "3"})
    os.makedirs(f"{OUT}/logs", exist_ok=True)
    with open(f"{OUT}/logs/{model}_s{seed}.log", "w") as f:
        f.write(p.stdout + p.stderr)
    status = "ok" if p.returncode == 0 else f"FAILED (exit {p.returncode})"
    return f"{time.strftime('%H:%M:%S')} {model:<13} s{seed} {status} {time.time() - t0:5.0f}s {p.stdout.strip()[-120:]}"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    jobs = [(m, s) for s in SEEDS for m in MODELS]          # seed-major: every model gets early seeds first
    with ThreadPoolExecutor(a.workers) as ex:
        for line in ex.map(run, jobs):
            print(line, flush=True)
    recs = [json.load(open(f)) for f in sorted(glob.glob(f"{OUT}/runs/*.json"))]
    with open(f"{OUT}/runs.jsonl", "w") as f:
        for r in sorted(recs, key=lambda r: (r["model"], r["seed"])):
            f.write(json.dumps(r) + "\n")
    print(f"merged {len(recs)} runs into {OUT}/runs.jsonl (expected {len(jobs)})")
