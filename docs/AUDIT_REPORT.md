# Project audit — 2026-10-11

Two independent passes, then a re-run of both after the fixes. This report states what was verified, what differed, and
what is **not** done. It does not claim the project is finished.

## 1. How the audit was done

| Pass | Method | Source of truth | Result |
|---|---|---|---|
| Repository | GitHub API vs local git | `vandanapumadhuhasini-ctrl/climate-adaptive-flu-forecasting` | see section 2 |
| Pass 1 | new Python code (`python/audit_project.py`): own RMSE, Diebold–Mariano, Holm, medians, SHA-256; README numbers | local raw logs, prediction files | **49 / 49 pass** (run twice, identical) |
| Pass 2 | new Scala/Spark job (`scala/AuditPass2.scala`) | the **HDFS copies** (`/flu_project/results`, `/flu_project/mapreduce`, `processed/v3_features`) | **13 / 13 pass** |
| HDFS | fresh `fsck` + `dfsadmin`; every manifest entry vs HDFS listing; SHA-256 of 18 key files read back from HDFS | HDFS | healthy; 845/845 files present with equal size (2nd run); 18/18 hashes equal |
| Docs | all generators re-run, output diffed against the commit | `results/` | one stale document found and fixed (section 3) |

Evidence: `results/audit/pass1.json`, `results/audit/pass2.json`.

## 2. Repository (GitHub)

* Remote `main` = local `main` (same commit hash); 1,039 files on both sides at audit time.
* Both commits authored by **Madhu Hasini**, linked to the `vandanapumadhuhasini-ctrl` account; contributors list: that account only.
* No co-author lines, no AI-tool names in any tracked file name, file content or commit message; no secret-like strings;
  no personal e-mail addresses or home-directory paths in tracked files.
* Shell scripts are pinned to LF line endings (`.gitattributes`) so they still run in the Linux containers after a Windows clone.
* **Visibility: PUBLIC** (it was created as Private; check this is intended).

## 3. Differences found (and what was done)

| Finding | Effect | Action |
|---|---|---|
| Diebold–Mariano p-values differ in the 4th decimal between the analysis code and the audit's own implementation (v2 0.0582 vs 0.0584; v3 Holm 0.0169 vs 0.0167) | none: same conclusions; caused by dividing autocovariances by T−k vs T | reported, not changed |
| README speed-up "18.0×" is 17.96× | rounding | none |
| Compliance matrix hard-coded "80 GNN runs" (stale after v3) and was generated before the last HDFS publish (839 vs 840 files) | wrong text in a committed document | builder now computes the counts; matrix regenerated (still 21/21) |
| Publish job copied its own Spark event log while still being written (`.inprogress`), leaving a partial copy in HDFS | one stale file in HDFS | removed; the job now skips only logs written in the last 10 minutes |
| While fixing that, two genuine event logs of crashed runs (also `.inprogress`) were briefly removed from HDFS | evidence temporarily missing | restored by the next publish; verified present |
| `models/climaflu_gnn/` holds the leaky v1 outputs without a warning | could be cited by mistake | `models/climaflu_gnn/README.md` marks them superseded |
| `docs/RESUME.md` did not cover v3, HDFS publishing or the audit | incomplete re-run instructions | section 6 added |

## 4. Not done / open items

1. **Part 07 not started:** IEEE manuscript, presentation slides, updated proposal (the old proposal still mentions
   WEIGHTED_ILI, humidity and "Group 12").
2. **Dashboard screenshots are stale:** `docs/screenshots/dashboard_*.png` were taken before the v3 section was added to the
   Forecasting page (re-run `scripts/capture_dashboard.ps1`).
3. Supporting 2026 IEEE papers (Kim & Jang; Xie et al.; Ye et al.) were read as abstracts only; only the base paper was read in full.

## 5. Limitations to state in the paper / viva (verified, not hidden)

* v2 and v3 use the same 2023–2025 test set (A1 hypotheses were fixed beforehand and Holm-corrected, but reuse can still be optimistic).
* v3: N1–N3 not supported; N2 trend (h = 3: 10/10 seeds, CI below 0) fails only the Holm correction; N4 holds at h = 3 only (h = 4 has 7/10 seeds).
* Tuning: Spark shuffle went through a Windows bind mount, so absolute times include host I/O (same for every configuration).
* Benchmark Experiment B (6 GB cap) failures were VM-wide memory exhaustion (~5 GB), not the container cap.
* Spark runs `local[*]` in one container (MapReduce runs on YARN); all MapReduce maps were rack-local (single host).
* MLlib GBT results depend slightly on input partitioning (0.9377 vs 0.9401 on identical data).
* Replication: raw data at 2, Spark-written outputs and published results at 3 (client default) — consistent within HDFS but not uniform.
* The Docker VHDX holds ~48 GB of dead space that has not been compacted.
