# Base paper selection and novelty plan (ClimaFlu-GNN, Group 04)

Searched 2026-10-11. Every paper below was verified through Crossref (IEEE DOI prefix 10.1109, issued 2026) and its abstract
was read through Semantic Scholar. The **base paper (section 1) was read in full** (PDF from the user, via college IEEE access);
the supporting papers (section 2) were read as abstracts only — read their full texts before citing specifics.

## 1. Recommended base paper (primary)

**Y.-J. Zhao, J.-P. Huang, R. Wang, H. Li, W. Yan, X.-B. Lian, Z.-H. Hao, "A New Two-Way Ensemble Prediction for Infectious
Diseases," *IEEE Transactions on Computational Social Systems*, Oct. 2026. DOI: [10.1109/TCSS.2025.3640950](https://doi.org/10.1109/TCSS.2025.3640950)**

**Verified from the full text** (IEEE TCSS vol. 13, no. 5, pp. 6359–6370, Oct. 2026; received 26 Apr 2025, published online
26 Dec 2025; PDF downloaded via Amrita IEEE Xplore access, kept outside the repository):
* **Data:** district-level daily COVID-19 cases for **Shanghai (5 Mar–31 May 2022)** and **Beijing (22 Apr–30 Jun 2022)**;
  average temperature, relative humidity, PM2.5, PM10 from the China Meteorological Administration; district population.
* **Step 1 — GAM with lag screening:** Poisson GAM with population offset; each meteorological variable is tested at **lags of
  1–14 days** and the lag that maximises the variance explained is kept (Beijing: 3 days temperature, 5 days RH/PM; Shanghai:
  7 days temperature, 8 days RH, 4 days PM). PM2.5/PM10 are dropped as collinear; only temperature and RH are used later.
  The screening is done on the same period that is then forecast (in-sample).
* **Step 2 — modified SEIR (S, P, E, I, Q, R, D)** per risk region; transmission rate β = β0 + β1·F(T) + β2·F(RH);
  parameters by an inverse-coefficient (least-squares) method.
* **Step 3 — spatial-weighted K-means** groups districts into risk regions (3 in Beijing, 5 in Shanghai).
* **Step 4 — TEP:** the multi-wave MDEP ensemble per risk region, then summed across regions (temporal + spatial ensemble).
* **Evaluation:** MAE, RMSE, R², cumulative-case accuracy (100 − MAPE) and empirical 95 % prediction intervals; compared only
  with **MDEP and modified SEIR**. Accuracy: TEP 84.83 % (Shanghai) / 94.27 % (Beijing) vs MDEP 67.83 % / 91.91 %.
  No machine-learning baselines and no significance tests.
* **Stated limitations:** deterministic ODEs are volatile at low incidence (stochastic SEIR suggested as future work); no
  human-mobility data; only two cities.

Why it fits: it is the closest 2026 IEEE *Transactions* paper to our question — **does weather/climate information improve
spatio-temporal infectious-disease forecasting?**

## 2. Supporting 2026 IEEE papers (cite in related work)

| Paper | Venue | Use in our paper |
|---|---|---|
| M. Kim, B. Jang, "Cell-Attentive LSTM With Lag-Aware Feature Selection for Multi-Week Epidemic Forecasting," DOI [10.1109/ACCESS.2026.3738355](https://doi.org/10.1109/ACCESS.2026.3738355) | IEEE Access, 2026 | source of the **lag-aware** idea: surveillance signals are offset in time; align each feature by its own lag (COVID-19 US/Australia, dengue Brazil) |
| S.-Y. Xie et al., "Modeling Complex Infectious Disease Dynamics by Distinguishing Susceptible Populations," DOI [10.1109/TBDATA.2026.3653627](https://doi.org/10.1109/TBDATA.2026.3653627) | IEEE Trans. Big Data, Jun 2026 | big-data-venue epidemic modelling (SLEIRD vs SIR/SEIR, 5–30-day horizons) |
| H.-B. Ye et al., "Context-Aware Time-Series Forecasting for Respiratory Infectious Diseases: Fusing Epidemiological Metrics With Textual Context," DOI [10.1109/ACCESS.2026.3733491](https://doi.org/10.1109/ACCESS.2026.3733491) | IEEE Access, 2026 | multimodal respiratory-disease forecasting (U.S. data, fine-tuned Llama3-8B) |
| J. Moon, J. Shim, E. Kim, E. Hwang, "MIFlu: Large Language Model-Based Multimodal Influenza Forecasting Scheme," *IEEE JBHI* 29(10):7790–7801 | IEEE JBHI, Oct 2025 | influenza-specific IEEE JBHI work (2025, not 2026) |

## 3. Research gap (base paper vs. this project)

| Aspect | Base paper (Zhao et al., TCSS 2026) | ClimaFlu (this project) |
|---|---|---|
| Disease / region | COVID-19, 2 cities | seasonal influenza (ILI), **50 US states**, 2010–2025 |
| Climate data scale | city-level averages | **34.6 GB / 968.9 M raw GHCN-Daily records → 303.6 M cleaned**, state-week aggregation |
| Computing | no big-data pipeline (GAM + ODE fitting on two small city datasets) | **Hadoop HDFS + MapReduce on YARN + Spark (RDD/DataFrame/Dataset) + Structured Streaming** |
| Spatial structure | ensemble over space | weekly **climate-similarity graph** + GNN, with static / geographic / random graph controls |
| Climate effect | lags screened in-sample by GAM (1–14 days), then built into SEIR | **tested**: with vs without climate, with vs without graph |
| Evaluation | vs its own variants (MDEP, modified SEIR) only; no ML baselines, no significance tests | **pre-registered protocol**, 10 seeds, Diebold–Mariano + block bootstrap, MLlib GBT/Ridge/Persistence baselines |
| Timing of climate effects | best lag chosen on the forecast period itself (in-sample) | (planned) **lag-aware** climate at 0–8 weeks lead, multi-horizon |

**Problem statement.** Climate-informed epidemic models such as the base paper assume that same-period weather improves
forecasts, but this is rarely tested at scale against strong data-driven baselines with proper statistics. Our measured
result (`docs/RIGOUR_RESULTS.md`) shows that for **next-week** state-level ILI, adding climate — directly or through a
climate-similarity graph — gives **no significant gain** over lag-only models and Spark GBT. The open question is **when**
climate helps: at which lead time and forecast horizon.

## 4. Novelty plan (what we add beyond the base paper)

1. **Lag-aware climate graph (extends Kim & Jang to climate + graphs).** For every climate feature, find its best lead time
   (0–8 weeks) by cross-correlation with ILI **on training years only**, computed for all 50 states × 4 features × 9 lags in
   **Spark** over the full 303.6 M-row climate table. Build the weekly similarity graph and node features from the
   lead-aligned climate, not same-week climate.
2. **Multi-horizon evaluation (h = 1, 2, 3, 4 weeks).** The base paper and our v1 tested one horizon. Climate may matter
   more further ahead, where the last observed ILI value is less informative.
3. **Big-data contribution.** End-to-end, verified Hadoop + Spark pipeline (21/21 course requirements, five implementations
   with identical checksums, 18× speed-up over pandas, streaming at 38k rows/s) — the base paper has no comparable
   systems evidence.
4. **Rigorous, pre-registered testing.** Every claim (including negative ones) decided by frozen rules — a methodological
   contribution that the climate-epidemic literature usually lacks.

This plan must be run as a **new, separately frozen protocol** (an amendment to `docs/RESEARCH_PROTOCOL.md` written
*before* any new run), with the same 2023–2025 test set and the same decision rules, and the result reported whatever it is.
Re-tuning the current models until they "win" would be p-hacking and is not part of the plan.

## 5. Suggested paper title (depends on the outcome)

* If lag-aware climate helps at longer horizons: *"When Does Climate Help? Lag-Aware Climate-Similarity Graphs for
  Multi-Horizon Influenza Forecasting on a Hadoop–Spark Pipeline."*
* If it does not: *"Does Climate Improve State-Level Influenza Forecasts? A Pre-Registered Big-Data Evaluation on 303 Million
  Weather Records."* — still a publishable, well-evidenced result (IEEE BigData / ICHI).

## 6. Outcome of the novelty experiment (v3, run 2026-10-11 under amendment A1)

Results: `docs/V3_RESULTS.md` (generated). Features were built in **Spark on HDFS** and are identical to the v2 Python features
(39,365 rows, max |diff| = 0); all predictions and results are published to HDFS `/flu_project/results/` (SHA-256 manifest).

* **Lag-aware climate (tests the base paper's lag idea out of sample):** the training-years scan chose **lead 0 (same week)**
  for every climate feature and horizon — the residual correlation of temperature peaks at the current week and decays
  steadily with lead (TMAX, h = 1: −0.18, −0.17, −0.13, …). The base paper's in-sample GAM lag screening (3–8 days) does not
  carry over to a weekly, out-of-sample, multi-year setting. **N1: not supported.**
* **Climate matters more at longer horizons:** the climate/graph benefit is ~0 at 1–2 weeks and grows at 3–4 weeks
  (GNN vs no-climate at h = 3: RMSE 1.251 vs 1.293, 10/10 seeds, bootstrap CI below 0, raw DM p = 0.028), but it does not
  survive the pre-registered Holm correction (p = 0.080). **N2: not supported (suggestive trend).** N3 (graph): not supported.
* **N4 supported:** at a **3-week horizon** ClimaFlu-GNN beats Spark MLlib GBT (1.251 vs 1.289, Holm-adjusted DM p = 0.017,
  CI [−0.073, −0.015], 9/10 seeds).

**Resulting contribution statement for the paper:** a verified Hadoop–Spark pipeline over 303.6 M climate records; an
out-of-sample test of climate-informed influenza forecasting showing that same-week climate is the most informative lead,
that climate's benefit grows with the horizon (a consistent trend, not significant after Holm correction), and that the climate-graph GNN significantly outperforms Spark
GBT at a 3-week horizon — with every claim decided by pre-registered rules.
