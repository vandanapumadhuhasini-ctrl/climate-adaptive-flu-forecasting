# Research protocol — ClimaFlu-GNN rigour pass (Part 04 v2)

**Frozen 2026-10-11, before any v2 model was trained.** Changes after this date are appended at the end under
"Amendments", with a date and reason; nothing above that line is edited after results exist.

## 1. Question and claims to test

ClimaFlu-GNN forecasts next-week unweighted ILI % per US state from the last four weeks of ILI and the current week's
climate (TMAX, TMIN, PRCP, diurnal range), passing climate information between states over a graph rebuilt every week
from climate similarity. Three claims are tested; each is accepted only by the decision rules in section 7:

* **C1 (accuracy):** ClimaFlu-GNN forecasts more accurately than the Spark MLlib GBT baseline.
* **C2 (graph):** message passing over the climate-similarity graph helps, compared with the same model without a graph
  and with a random graph of the same degree.
* **C3 (dynamic):** rebuilding the graph every week helps, compared with one static climate graph and a geographic graph.

## 2. Data and features (identical for every model)

* Source: `modeling_table` (HDFS `/flu_project/processed`, exported to `exports/modeling_table`), 50 states, MMWR weeks 2010–2025.
* Rows with a null in TMAX, TMIN, PRCP, DIURNAL_RANGE or UNWEIGHTED_ILI are dropped.
* **Exact calendar lags (fix):** each state-week gets its MMWR week start date (weeks start on Sunday; MMWR week 1 is
  the week that contains 4 January). ILI_lag*k* is the value exactly 7·*k* days earlier for the same state and the target is the value
  exactly 7 days later; a row is used only if all four lags and the target exist. (v1 used the previous *row*, which crosses
  12 gaps of up to 18 weeks in 7 states.)
* Features: ILI_lag1–4, ILI_trend = lag1 − lag2, TMAX, TMIN, PRCP, DIURNAL_RANGE. Target: next-week UNWEIGHTED_ILI.
* The canonical feature table is written once (`results/rigour/feature_table.parquet`) and every model, including the
  Spark GBT, reads exactly these rows. Test metrics are computed on the identical set of (state, year, week) rows.

## 3. Splits (temporal, no shuffling across time)

| Phase | Train | Validation | Test |
|---|---|---|---|
| Model selection (epochs) | YEAR ≤ 2021 | YEAR = 2022 | not touched |
| Final fit and evaluation | YEAR ≤ 2022 | — | YEAR ≥ 2023 |

Validation = 2022 because it is the most recent full year before the test period and had a normal flu season;
2020–2021 seasons were suppressed by COVID-19 measures and are poor for selecting a model. The test set is used **once**
per trained model, after training finishes.

## 4. Preprocessing without test information

* All standardisation statistics (feature z-scores, target z-score) are fitted on the training rows of each phase only.
* **Graphs are rebuilt in Python** with the same procedure as `scala/GraphConstruction.scala` (z-scored climate
  vector per state-week, Euclidean distance, Gaussian kernel with σ = mean pairwise distance, top-K neighbours) but with
  the z-score statistics and σ fitted on training years only. Before use, the Python builder is checked against the Spark
  edges using the Spark (all-years) statistics: the edge sets must agree on ≥ 99 % of edges.
* Edges point neighbour → node (`state_j → state_i`), weight = similarity.

## 5. Models (fixed architecture and hyper-parameters from Part 04 v1; no tuning on test)

| ID | Model | Graph | Purpose |
|---|---|---|---|
| GNN-K8 | ClimaFlu-GNN (MLP over ILI lags ‖ 2-layer GCN over climate, fusion MLP) | weekly climate KNN, K = 8 | main model |
| GNN-K4, GNN-K16 | same | weekly climate KNN, K = 4 / 16 | sensitivity to K |
| GNN-static | same | one climate KNN graph (K = 8) from each state's mean training-period climate | C3 |
| GNN-geo | same | K = 8 nearest states by centroid distance (station lat/lon), weights from a Gaussian kernel | C3 |
| GNN-random | same | each week K = 8 random neighbours per state, similarity weights permuted | C2 |
| MLP-noGraph | same branches, GCN replaced by a 2-layer MLP over the node's own climate | none | C2 |
| MLP-lagsOnly | ILI-lag branch + fusion only (no climate input) | none | value of climate |
| Ridge | linear ridge regression (α = 1.0) on all features | none | simple baseline |
| Persistence | ŷ = ILI_lag1 | none | naive baseline |
| GBT-lags | Spark MLlib GBTRegressor, 50 iterations, depth 5, features lag1–4 + trend (v1 baseline) | none | C1 |
| GBT-all | same GBT, all features (lags + trend + climate) | none | feature-parity baseline |

Neural settings: hidden 16, Adam lr 1e-3, weight decay 1e-5, gradient clip 1.0, batch 8 snapshots, MSE on z-scored target.
Selection phase: up to 200 epochs, validation RMSE every epoch, best epoch recorded, early stop after 30 epochs without
improvement. Final phase: refit from scratch on YEAR ≤ 2022 for exactly the selected number of epochs, then predict the test set.

## 6. Repetition and metrics

* **10 seeds** (0–9) for every neural model; seed controls initialisation, batch order and the random graph.
  Ridge, Persistence and GBT are deterministic (one run).
* Metrics on the raw ILI % scale: RMSE, MAE, R², reported as mean ± std over seeds, plus the metrics of the seed-ensemble
  (mean prediction over the 10 seeds).
* **Diebold–Mariano test** (squared-error loss, one-step-ahead) on the weekly series of loss differentials
  (each week = mean over states of e²_A − e²_B), Newey–West variance with 4 lags, two-sided.
* **Moving-block bootstrap** (block = 4 weeks, 2000 resamples, seed 0) 95 % CI of RMSE_A − RMSE_B over test weeks.

## 7. Decision rules (pre-registered)

Model A is declared **better** than model B only if all three hold:
1. mean test RMSE over seeds of A < that of B;
2. A's ensemble beats B (ensemble or deterministic) with Diebold–Mariano p < 0.05 and the bootstrap 95 % CI of
   RMSE_A − RMSE_B lies entirely below 0;
3. at least 8 of 10 seeds of A have lower test RMSE than B (for a neural B, seed-paired).

Otherwise the result is reported as "no significant difference" (or "worse" if the rules hold in the other direction).
* C1 holds if GNN-K8 is better than **both** GBT-lags and GBT-all.
* C2 holds if GNN-K8 is better than **both** MLP-noGraph and GNN-random.
* C3 holds if GNN-K8 is better than **both** GNN-static and GNN-geo.
Every comparison is reported, whatever its outcome. The v1 numbers (GNN RMSE 0.8979, selected on the test set) are not
used for any claim.

## 8. Outputs

`results/rigour/runs.jsonl` (one line per trained model and seed), `results/rigour/predictions/*.parquet`,
`results/rigour/summary.json`, `docs/RIGOUR_RESULTS.md`, `docs/figures/rigour_*.png` — all generated by scripts.

## Amendments

### A1 — Lag-aware climate and multi-horizon forecasting (v3). Frozen 2026-10-11, before any v3 run.

**Motivation (stated before running):** v2 found no significant benefit of same-week climate (C1–C3 not supported).
The base paper (Zhao et al., IEEE TCSS 2026, `docs/BASE_PAPER.md`) builds same-period weather into the forecast; Kim & Jang
(IEEE Access 2026) show that surveillance signals are offset in time. A1 asks **whether climate helps when it is aligned by
its lead time, and whether it helps more at longer horizons**. v2 results stay as they are; A1 adds new claims only.

**Pipeline (Spark on HDFS):** `scala/LagAwareFeaturesV3.scala` reads `modeling_table` and `weather_state_week` (climate
2000–2025, so lags reach before 2010) from HDFS, rebuilds the exact-calendar ILI lags/targets for horizons **h = 1, 2, 3, 4**
(target = ILI exactly 7·h days later), runs the lag scan and writes the feature tables to HDFS
(`/flu_project/processed/v3_features`). Check: for h = 1 the Spark rows and values must equal the v2 Python feature table.

**Lag selection (training years only, YEAR ≤ 2021 — before validation and test):** for each horizon h, regress the target on
the ILI features (lag1–4, trend) by least squares; for each climate feature f ∈ {TMAX, TMIN, PRCP, DIURNAL_RANGE} and lead
L ∈ {0, …, 8} weeks, compute the Pearson correlation between the residual and the within-state z-score of f at week t − L.
Choose L*(f, h) = argmax |corr|. One lag per feature and horizon, pooled over states (a per-state scan is reported as an
analysis only, not used by models). The chosen lags are fixed for both phases.

**Models (per horizon, same architecture and training rules as section 5; epochs chosen on 2022; test 2023–2025):**

| ID | Climate input | Graph |
|---|---|---|
| GNN-lagK8 | lead-aligned climate f(t − L*) | weekly KNN (K = 8) on lead-aligned climate |
| GNN-K8 | same-week climate (v2 model) | weekly KNN (K = 8) on same-week climate |
| MLP-lagNoGraph | lead-aligned climate | none |
| MLP-lagsOnly | none | none |
| GBT-lags (Spark MLlib) | none | none |
| GBT-lagclimate (Spark MLlib) | lead-aligned climate | none |
| Persistence | ŷ(t+h) = ILI(t) | none |

10 seeds for neural models (seeds 0–9). Same metrics, Diebold–Mariano and block bootstrap as section 6.

**Claims and decision rules:** section 7's three rules, with the Diebold–Mariano p-value **Holm-adjusted across the four
horizons** within each claim (family-wise α = 0.05).
* **N1 (lag-aware beats same-week):** GNN-lagK8 better than GNN-K8 at at least one horizon.
* **N2 (climate helps at longer horizons):** GNN-lagK8 better than MLP-lagsOnly at at least one of h = 2, 3, 4.
* **N3 (graph helps given lag-aware climate):** GNN-lagK8 better than MLP-lagNoGraph at at least one horizon.
* **N4 (beats Spark GBT):** GNN-lagK8 better than GBT-lags at at least one horizon.
Every horizon and every comparison is reported, whatever the outcome.

**Known limitation (stated in advance):** the 2023–2025 test set was already used once in v2. A1's hypotheses are new and
fixed before running, and Holm correction is applied, but repeated use of one test set can still be optimistic; the paper
will say so.

**Outputs:** `results/v3/` (runs, predictions, summary), `docs/V3_RESULTS.md`, `docs/figures/v3_*.png`; predictions and
result files are then published to HDFS `/flu_project/results/` with a SHA-256 manifest.

### A1.1 — Clarification (2026-10-11, after the lag scan, before any v3 model was trained)
* In the feature design used since v1, the row for week t contains ILI up to week t − 1 (ILI_lag1 … ILI_lag4) and climate of
  week t, reflecting the one-week reporting delay of CDC ILINet. "ILI(t)" in A1's Persistence baseline therefore means the
  most recent observed value, ILI_lag1; a horizon-h target is h + 1 weeks after the last observed ILI value.
* The lag scan selected lead L* = 0 for every feature and horizon (`results/v3/lag_scan.json`). As fixed in A1, the
  lead-aligned climate is then the same-week climate, so GNN-lagK8 and GNN-K8 receive identical inputs; both are still
  trained as specified, and N1 is evaluated as written.
