"""Run every v3 model x horizon x seed (A1) in parallel workers, resumable; merge into results/v3/runs.jsonl.
Run: docker exec -w /workspace/rigour pytorch python run_all_v3.py [--workers 6]
"""
import argparse
import glob
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor

from train_v3 import MODELS, OUT

JOBS = [(m, h, s) for s in range(10) for h in (1, 2, 3, 4) for m in MODELS]


def run(job):
    m, h, s = job
    if os.path.exists(f"{OUT}/runs/{m}_h{h}_s{s}.json"):
        return f"skip {m} h{h} s{s}"
    t0 = time.time()
    p = subprocess.run(["python", "train_v3.py", "--model", m, "--h", str(h), "--seed", str(s)], capture_output=True,
                       text=True, env={**os.environ, "TORCH_THREADS": "3"})
    os.makedirs(f"{OUT}/logs", exist_ok=True)
    with open(f"{OUT}/logs/{m}_h{h}_s{s}.log", "w") as f:
        f.write(p.stdout + p.stderr)
    return f"{time.strftime('%H:%M:%S')} {m:<15} h{h} s{s} {'ok' if p.returncode == 0 else 'FAILED'} {time.time() - t0:4.0f}s"


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=6)
    a = ap.parse_args()
    with ThreadPoolExecutor(a.workers) as ex:
        for line in ex.map(run, JOBS):
            print(line, flush=True)
    recs = [json.load(open(f)) for f in sorted(glob.glob(f"{OUT}/runs/*.json"))]
    with open(f"{OUT}/runs.jsonl", "w") as f:
        for r in sorted(recs, key=lambda r: (r["model"], r["horizon"], r["seed"])):
            f.write(json.dumps(r) + "\n")
    print(f"merged {len(recs)} runs (expected {len(JOBS)})")
