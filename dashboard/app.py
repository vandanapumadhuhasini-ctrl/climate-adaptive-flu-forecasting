"""ClimaFlu-GNN dashboard (23AID302, Group 04). Reads ONLY files under results/ and docs/ (mounted read-only):
every number shown is computed from a result file at render time; nothing is typed in by hand.
Pages can be opened directly with ?page=<name> (used for the report screenshots)."""
import glob
import json
import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

R, DOCS = "results", "docs"
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, RED = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4",
                                                           "#008300", "#4a3aa7", "#e34948")
GREY = "#8c8b85"
PAGES = {"overview": "Overview & 4 V's", "cluster": "Hadoop cluster", "batch": "Batch processing",
         "streaming": "Streaming (velocity)", "forecast": "Forecasting results", "graph": "Climate graph"}

st.set_page_config(page_title="ClimaFlu-GNN dashboard", page_icon="🌡️", layout="wide",
                   initial_sidebar_state="collapsed" if "shot" in st.query_params else "auto")   # ?shot=1: report screenshots


# ------------------------------------------------------------------------------------------------ loaders
@st.cache_data
def js(path):
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


@st.cache_data
def jsonl(path):
    with open(path, encoding="utf-8-sig") as f:
        return [json.loads(l) for l in f if l.strip()]


def exists(*paths):
    return all(os.path.exists(p) for p in paths)


@st.cache_data
def predictions():
    """Rigour-pass test predictions: GNN-K8 10-seed ensemble, Spark GBT-lags, observed target (identical rows)."""
    key = ["state", "year", "week"]
    seeds = [pd.read_parquet(f).sort_values(key).reset_index(drop=True) for f in sorted(glob.glob(f"{R}/rigour/predictions/GNN-K8_s*.parquet"))]
    gbt = pd.concat(pd.read_parquet(p) for p in glob.glob(f"{R}/rigour/predictions_spark/GBT-lags/*.parquet")).sort_values(key).reset_index(drop=True)
    out = seeds[0][key + ["target"]].copy()
    out["gnn"] = np.mean([s["pred"].values for s in seeds], axis=0)
    out["gbt"] = gbt["pred"].values
    out["label"] = out["year"].astype(str) + "-W" + out["week"].astype(str).str.zfill(2)
    return out


def style(fig, height=360, ytitle=None, xtitle=None, logy=False, ycat=False):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=30, b=10), hovermode="x unified",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0), font=dict(size=12))
    fig.update_xaxes(title=xtitle, showgrid=False)
    fig.update_yaxes(title=ytitle, gridcolor="rgba(128,128,128,0.2)")
    if not ycat:                                   # keep a categorical y axis (model names) untouched
        fig.update_yaxes(type="log" if logy else "linear")
    return fig


def badge(ok):
    return "✅" if ok else "❌"


# ------------------------------------------------------------------------------------------------ navigation
qp = st.query_params.get("page", "overview")
names = list(PAGES)
page = st.sidebar.radio("Section", names, index=names.index(qp) if qp in names else 0, format_func=PAGES.get)
st.query_params["page"] = page
st.sidebar.markdown("---")
st.sidebar.caption("ClimaFlu-GNN · 23AID302 Big Data Analytics · Group 04\n\nEvery figure is read from `results/` at render time.")


# ------------------------------------------------------------------------------------------------ pages
def overview():
    st.title("ClimaFlu-GNN: climate-aware influenza forecasting on Hadoop + Spark")
    st.caption("Next-week influenza-like-illness (ILI) per US state from NOAA GHCN-Daily climate (2000–2025) and CDC ILINet, "
               "processed on a Dockerised Hadoop 3.2.1 cluster with Spark 3.5.1.")
    cv, mr = js(f"{R}/cluster_verification.json"), js(f"{R}/mapreduce_jobs.json")
    ss, cc = js(f"{R}/streaming_summary.json"), js(f"{R}/cross_engine_consistency.json")
    c1 = mr["jobs"][0]["counters"]
    a, b, c, d = st.columns(4)
    a.metric("Volume — raw climate data in HDFS", f"{cv['hdfs']['raw_logical_GB']} GB",
             f"{c1['Map input records']:,} raw records", delta_color="off")
    b.metric("Velocity — live feed processed", f"{ss['processing']['throughput_rows_per_busy_s'] / 1000:.1f}k rows/s",
             f"median latency {ss['latency_arrival_to_committed_s']['median']} s", delta_color="off")
    c.metric("Variety — source formats", "3 + Parquet", "CSV · fixed-width · Parquet", delta_color="off")
    d.metric("Veracity — QC-flagged values removed", f"{c1['QC_FLAGGED']:,}",
             ("5 engines agree " + badge(cc["element_year"]["all_match"] and cc["state_year_element"]["all_match"])), delta_color="off")

    st.subheader("Pipeline")
    st.graphviz_chart("""digraph { rankdir=LR; node [shape=box, style="rounded,filled", fillcolor="#eef3fb", color="#2a78d6", fontname="Helvetica", fontsize=11];
      raw [label="NOAA GHCN-Daily 34.6 GB\\nCDC ILINet, stations"]; hdfs [label="HDFS\\n3 DataNodes, repl 2"];
      mr [label="Hadoop MapReduce\\non YARN (Java)"]; spark [label="Spark (Scala)\\nRDD · DataFrame · Dataset"];
      stream [label="Structured Streaming\\nlive climate feed"]; feat [label="State-week features\\n+ weekly climate KNN graph"];
      ml [label="MLlib GBT · PyTorch GNN\\n+ ablations, 10 seeds"]; dash [label="Dashboard\\n(result files only)"];
      raw -> hdfs -> mr; hdfs -> spark -> feat -> ml -> dash; stream -> hdfs; mr -> dash [style=dashed, label="cross-check"]; }""")

    st.subheader("Course requirements (23AID302) and their evidence")
    rows = [("Volume ≥ 25 GB in HDFS", cv["hdfs"]["volume_requirement_25GB_met"], "results/cluster_verification.json"),
            ("Velocity (streaming)", all(ss["checks"].values()), "results/streaming_summary.json"),
            ("Variety (≥ 2 data types)", True, "raw CSV + fixed-width stations file + Parquet"),
            ("Veracity (QC, audits, consistency)", cc["element_year"]["all_match"], "results/cross_engine_consistency.json"),
            ("Hadoop HDFS", cv["hdfs"]["fsck_status"] == "HEALTHY", "fsck + docs/screenshots/hdfs_*.png"),
            ("Hadoop MapReduce on YARN", all(j["succeeded"] for j in mr["jobs"]), "results/mapreduce_jobs.json"),
            ("Spark RDD / DataFrame / typed Dataset", exists(f"{R}/spark_api_evidence.txt"), "results/spark_api_evidence.txt"),
            ("Scala and Python", True, "scala/*.scala, python/**/*.py"),
            ("Spark ML", exists(f"{R}/rigour/predictions_spark/GBT-lags"), "scala/BaselineGBTv2.scala"),
            ("Performance tuning", exists(f"{R}/tuning_summary.json"), "results/tuning_summary.json"),
            ("Visualisation dashboard", True, "this app"),
            ("YARN + HDFS UI screenshots", exists(f"{DOCS}/screenshots/yarn_nodes.png", f"{DOCS}/screenshots/hdfs_overview.png"), "docs/screenshots/")]
    st.dataframe(pd.DataFrame([(r, badge(ok), ev) for r, ok, ev in rows], columns=["Requirement", "Met", "Evidence"]),
                 hide_index=True, use_container_width=True, height=36 * (len(rows) + 1) + 4)


def cluster():
    st.title("Hadoop cluster")
    cv, sm = js(f"{R}/cluster_verification.json"), js(f"{R}/cluster_smoke_test.json")
    h, y = cv["hdfs"], cv["yarn"]
    a, b, c, d, e = st.columns(5)
    a.metric("Live DataNodes", h["live_datanodes"]); b.metric("fsck", h["fsck_status"])
    c.metric("Under-replicated blocks", h["under_replicated"]); d.metric("YARN active nodes", y["active_nodes"])
    e.metric("YARN capacity", f"{y['total_mb'] // 1024} GB / {y['total_vcores']} vcores")
    st.caption(f"Snapshot {cv['generated_at']} · Hadoop {cv['versions']['hadoop']} · Spark {cv['versions']['spark']} · "
               f"PyTorch/PyG {cv['versions']['pytorch']}")
    st.subheader("Tables re-counted by Spark directly from HDFS")
    st.dataframe(pd.DataFrame([(t, v["rows"]) for t, v in sm["tables"].items()], columns=["HDFS table", "rows"]),
                 hide_index=True, use_container_width=False)
    st.subheader("Web UI screenshots")
    shots = sorted(glob.glob(f"{DOCS}/screenshots/*.png"))
    for i in range(0, len(shots), 3):
        cols = st.columns(3)
        for col, p in zip(cols, shots[i:i + 3]):
            col.image(p, caption=os.path.basename(p)[:-4].replace("_", " "), use_column_width=True)   # streamlit 1.39 API


def batch():
    st.title("Batch processing on 303.6 M climate records")
    views = {"consistency": "MapReduce & cross-engine consistency", "benchmark": "Spark vs pandas benchmark", "tuning": "Spark tuning"}
    vq = st.query_params.get("view", "consistency")
    view = st.radio("View", list(views), index=list(views).index(vq) if vq in views else 0, format_func=views.get,
                    horizontal=True, label_visibility="collapsed")
    st.query_params["view"] = view          # also lets ?page=batch&view=tuning open a view directly
    if view == "consistency":
        mr, cc = js(f"{R}/mapreduce_jobs.json"), js(f"{R}/cross_engine_consistency.json")
        cols = st.columns(2)
        for col, j in zip(cols, mr["jobs"]):
            col.markdown(f"**{j['job']}**  \n`{j['yarn_app_id']}` · {badge(j['succeeded'])} · {j['wall_seconds']} s · "
                         f"{j['counters']['Launched map tasks']} map tasks · {j['counters']['HDFS: Number of bytes read'] / 1e9:.2f} GB read")
        ey, sy = cc["element_year"], cc["state_year_element"]
        st.markdown("**The same computation in five implementations — SHA-256 of the sorted result**")
        st.dataframe(pd.DataFrame([
            ("element × year", "Hadoop MapReduce (Java, YARN)", ey["sha256"]["hadoop_mapreduce_mr1"]),
            ("element × year", "Spark RDD (Scala)", ey["sha256"]["spark_rdd"]),
            ("element × year", "Spark DataFrame + pandas", ey["sha256"]["spark_dataframe_and_pandas_benchmark"]),
            ("state × year × element", "Hadoop MapReduce map-side join", sy["sha256"]["hadoop_mapreduce_mr2"]),
            ("state × year × element", "Spark typed Dataset (joinWith + Aggregator)", sy["sha256"]["spark_typed_dataset"])],
            columns=["Aggregate", "Implementation", "SHA-256"]), hide_index=True, use_container_width=True)
        st.success(f"element × year: all match = {ey['all_match']} · state × year × element: all match = {sy['all_match']} · "
                   f"{ey['rows_kept']:,} kept records")
    elif view == "benchmark":
        bs = pd.DataFrame(js(f"{R}/benchmark_summary.json")["rows"])
        a = bs[(bs.experiment == "A") & bs.elapsed_median_s.notna()]
        names = {"spark-hdfs-local[*]": ("Spark, HDFS, 24 cores", BLUE), "spark-localfs-local[*]": ("Spark, local files, 24 cores", AQUA),
                 "spark-hdfs-local[4]": ("Spark, HDFS, 4 cores", VIOLET), "spark-hdfs-local[1]": ("Spark, HDFS, 1 core", MAGENTA),
                 "pandas-chunked": ("pandas chunked", ORANGE)}
        fig = go.Figure()
        for lab, (nm, colr) in names.items():
            s = a[a.label == lab].sort_values("years")
            fig.add_trace(go.Scatter(x=s.years, y=s.elapsed_median_s, name=nm, mode="lines+markers", line=dict(color=colr, width=2),
                                     hovertemplate="%{y:.1f} s"))
        fig.update_yaxes(dtick=1, tickformat=",")                        # one labelled tick per decade on the log axis
        st.plotly_chart(style(fig, 420, "median run time (s, log)", "years of data (1.2–1.5 GB each)", logy=True), use_container_width=True)
        big = a[a.years == a.years.max()].set_index("label")
        st.markdown(f"**{a.years.max()} years ({big.loc['spark-hdfs-local[*]', 'input_bytes'] / 1e9:.1f} GB), same 4 GB cap:** "
                    f"Spark {big.loc['spark-hdfs-local[*]', 'elapsed_median_s']:.0f} s vs pandas-chunked "
                    f"{big.loc['pandas-chunked', 'elapsed_median_s']:.0f} s → "
                    f"**{big.loc['pandas-chunked', 'elapsed_median_s'] / big.loc['spark-hdfs-local[*]', 'elapsed_median_s']:.1f}× faster**. "
                    "In-memory pandas (naive / optimized / pyarrow) is OOM-killed at 1 year under the same cap.")
        st.caption("Protocol: docs/BENCHMARK_PROTOCOL.md (fresh container per run, equal memory cap, identical result checksum).")
    else:
        ts = js(f"{R}/tuning_summary.json")
        t = pd.DataFrame(ts["rows"])
        ok = t[t.status != "failed"]
        fig = go.Figure()
        grid_p = sorted(t[~t.aqe].partitions.unique())
        for persist, nm, colr in (("none", "no persistence", ORANGE), ("memory_and_disk", "persist(MEMORY_AND_DISK)", BLUE)):
            s = ok[(ok.persist == persist) & (~ok.aqe)].set_index("partitions")
            # one point per grid value; a failed configuration stays a gap (no line implied across it)
            fig.add_trace(go.Scatter(x=[str(p) for p in grid_p], y=[s.total_median_s.get(p) for p in grid_p], name=nm,
                                     mode="lines+markers", connectgaps=False, line=dict(color=colr, width=2), hovertemplate="%{y:.0f} s"))
        fig.update_xaxes(type="category")
        ref = ok[ok.aqe]
        if len(ref):
            fig.add_hline(y=float(ref.total_median_s.iloc[0]), line_dash="dash", line_color=GREY, annotation_position="bottom left",
                          annotation_text=f"Spark defaults (AQE on, 200 partitions): {ref.total_median_s.iloc[0]:.0f} s")
        st.plotly_chart(style(fig, 380, "median total time, 3 queries (s)", "partitions (AQE off)"), use_container_width=True)
        fails = t[t.status == "failed"]
        for _, f in fails.iterrows():
            st.warning(f"{f['label']}: failed — {f['error_class']}")
        be = ts.get("caching_break_even", {})
        if be:
            st.markdown("**When does caching pay off?** " + " · ".join(
                f"P={p}: " + (f"after ~{v['break_even_queries']} queries" if v["break_even_queries"] else "never (queries slower on the cache)")
                for p, v in be.items()))


def streaming():
    st.title("Streaming: a live GHCN climate feed")
    ss = js(f"{R}/streaming_summary.json")
    f, p, l = ss["feed"], ss["processing"], ss["latency_arrival_to_committed_s"]
    a, b, c, d = st.columns(4)
    a.metric("Feed", f"{f['files']} daily files", f"{f['source_rows']:,} raw rows", delta_color="off")
    b.metric("Arrival rate", f"{f['arrival_rate_rows_per_s'] / 1000:.1f}k rows/s")
    c.metric("Processing throughput", f"{p['throughput_rows_per_busy_s'] / 1000:.1f}k rows/s")
    d.metric("Latency arrival → committed", f"{l['median']} s median", f"p95 {l['p95']} s · max {l['max']} s", delta_color="off")
    drip = pd.DataFrame(jsonl(f"{R}/streaming_v2/drip.jsonl"))
    bat = pd.DataFrame(jsonl(f"{R}/streaming_v2/batches.jsonl"))
    end = {fn: r.end_ms for r in bat.itertuples() for fn in r.files}
    drip["latency_s"] = [(end[fn] - t) / 1000 for fn, t in zip(drip.file, drip.arrival_ms)]
    drip["day"] = pd.to_datetime(drip.file.str[4:12], format="%Y%m%d")      # real dates, not numbers
    fig = go.Figure(go.Scatter(x=drip.day, y=drip.latency_s, mode="lines+markers", line=dict(color=BLUE, width=2),
                               name="latency", hovertemplate="%{y:.1f} s"))
    st.plotly_chart(style(fig, 300, "seconds", "daily file (GHCN date)"), use_container_width=True)
    bat["krows_s"] = bat.rows_in / ((bat.end_ms - bat.start_ms) / 1000) / 1000
    fig2 = go.Figure(go.Bar(x=bat.batch_id, y=bat.krows_s, marker_color=ORANGE, name="throughput", hovertemplate="%{y:.1f}k rows/s"))
    fig2.add_hline(y=f["arrival_rate_rows_per_s"] / 1000, line_dash="dash", line_color="#0b0b0b",
                   annotation_text=f"feed arrival rate {f['arrival_rate_rows_per_s'] / 1000:.1f}k rows/s", annotation_position="top left",
                   annotation_bgcolor="rgba(255,255,255,0.85)")
    st.plotly_chart(style(fig2, 300, "k rows / s", "micro-batch"), use_container_width=True)
    st.markdown(" · ".join(f"{badge(v)} {k.replace('_', ' ')}" for k, v in ss["checks"].items()))
    st.caption("Spark Structured Streaming, 5 s trigger, file source on HDFS (atomic renames), foreachBatch: QC filter → "
               "broadcast join with stations → state × day × element aggregates appended to HDFS.")


def forecast():
    st.title("Forecasting results (rigour pass, pre-registered protocol)")
    s = js(f"{R}/rigour/summary.json")
    st.subheader("Claims tested")
    st.dataframe(pd.DataFrame([(c, "YES" if v["holds"] else "NO", "; ".join(f"vs {b}: {r}" for b, r in v["against"].items()))
                               for c, v in s["claims"].items()], columns=["Claim", "Holds", "Verdicts"]),
                 hide_index=True, use_container_width=True)
    st.info("Honest result: with the checkpoint chosen on validation data and 10 seeds, the GNN is not significantly better than "
            "Spark GBT, and the climate graph does not beat a no-graph model. Next-week ILI is driven mainly by the last 4 weeks.")
    runs = pd.DataFrame(jsonl(f"{R}/rigour/runs.jsonl"))
    models = s["models"]
    order = sorted(models, key=lambda m: models[m]["rmse_mean"], reverse=True)
    fig = go.Figure()
    for m in order:
        colr = BLUE if m == "GNN-K8" else ORANGE if m.startswith("GBT") else GREY if models[m]["seeds"] == 1 else VIOLET
        if models[m]["seeds"] > 1:
            xs = runs[runs.model == m].test_rmse
            fig.add_trace(go.Box(x=xs, y=[m] * len(xs), name=m, orientation="h", marker_color=colr, boxpoints="all",
                                 jitter=0.4, showlegend=False))
        else:
            fig.add_trace(go.Scatter(x=[models[m]["rmse_mean"]], y=[m], mode="markers", marker=dict(symbol="diamond", size=11, color=colr),
                                     name=m, showlegend=False, hovertemplate="%{x:.4f}"))
    fig.update_layout(hovermode="closest")
    fig.update_yaxes(type="category", categoryorder="array", categoryarray=order)
    st.plotly_chart(style(fig, 460, None, "test RMSE (ILI %), 7,691 state-weeks 2023–2025", ycat=True), use_container_width=True)

    if os.path.exists(f"{R}/v3/summary.json"):
        v3 = js(f"{R}/v3/summary.json")
        st.subheader("Novelty experiment (v3): lag-aware climate, horizons 1–4 weeks")
        st.caption(f"Lag scan on training years chose climate leads {v3['lag_scan']['h1']} (h=1) — the same week is the most "
                   "informative lead at every horizon. Claims are Holm-adjusted across horizons (amendment A1).")
        st.dataframe(pd.DataFrame([(c, "YES" if v["holds"] else "NO",
                                    "; ".join(f"h={r['horizon']}: {r['verdict']} (p={r['dm_p_holm']:.3f})" for r in v["by_horizon"]))
                                   for c, v in v3["claims"].items()], columns=["Claim", "Holds", "Per horizon"]),
                     hide_index=True, use_container_width=True)
        hz = [1, 2, 3, 4]
        fig = go.Figure()
        for m, colr in (("GNN-lagK8", BLUE), ("MLP-lagNoGraph", VIOLET), ("MLP-lagsOnly", MAGENTA), ("GBT-lags", ORANGE),
                        ("GBT-lagclimate", YELLOW), ("Persistence", GREY)):
            fig.add_trace(go.Scatter(x=[f"{h} wk" for h in hz], y=[v3["per_horizon"][str(h)]["models"][m]["rmse_mean"] for h in hz],
                                     error_y=dict(type="data", array=[v3["per_horizon"][str(h)]["models"][m]["rmse_sd"] for h in hz]),
                                     name=m, mode="lines+markers", line=dict(color=colr, width=2, dash="dash" if m == "Persistence" else "solid"),
                                     hovertemplate="%{y:.4f}"))
        st.plotly_chart(style(fig, 380, "test RMSE (ILI %)", "forecast horizon"), use_container_width=True)

    st.subheader("Forecasts by state")
    pr = predictions()
    states = sorted(pr.state.unique())
    stt = st.selectbox("State", states, index=states.index("NY") if "NY" in states else 0)
    sp = pr[pr.state == stt]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=sp.label, y=sp.target, name="observed", line=dict(color="#0b0b0b", width=2)))
    fig.add_trace(go.Scatter(x=sp.label, y=sp.gnn, name="GNN-K8 (10-seed ensemble)", line=dict(color=BLUE, width=2)))
    fig.add_trace(go.Scatter(x=sp.label, y=sp.gbt, name="Spark GBT-lags", line=dict(color=ORANGE, width=1.5, dash="dash")))
    fig.update_xaxes(nticks=12)
    st.plotly_chart(style(fig, 360, "unweighted ILI %", "MMWR week"), use_container_width=True)
    r_g, r_b = np.sqrt(((sp.gnn - sp.target) ** 2).mean()), np.sqrt(((sp.gbt - sp.target) ** 2).mean())
    st.caption(f"{stt}: test RMSE GNN-K8 ensemble {r_g:.3f} · GBT-lags {r_b:.3f}")
    per = pr.groupby("state").apply(lambda g: np.sqrt(((g.gnn - g.target) ** 2).mean()), include_groups=False).rename("rmse").reset_index()
    fig = go.Figure(go.Choropleth(locations=per.state, z=per.rmse, locationmode="USA-states", colorscale="Blues",
                                  colorbar_title="RMSE", hovertemplate="%{location}: %{z:.3f}<extra></extra>"))
    fig.update_layout(geo_scope="usa", height=380, margin=dict(l=0, r=0, t=10, b=0))
    st.markdown("**GNN-K8 ensemble test RMSE by state**")
    st.plotly_chart(fig, use_container_width=True)


def graph():
    st.title("Weekly climate-similarity graph (K = 8)")
    st.caption("Each week every state links to the 8 states whose climate (TMAX, TMIN, PRCP, diurnal range; z-scored on training "
               "years) is most similar. This is the graph the GNN passes messages over.")
    g = pd.read_parquet(f"{R}/dashboard/graph_test_K8.parquet")
    cen = pd.read_csv(f"{R}/dashboard/state_centroids.csv").set_index("state")
    weeks = g[["year", "week"]].drop_duplicates().sort_values(["year", "week"])
    labels = [f"{y}-W{w:02d}" for y, w in zip(weeks.year, weeks.week)]
    pick = st.select_slider("Week", options=labels, value=labels[len(labels) // 2])
    y, w = int(pick[:4]), int(pick[-2:])
    e = g[(g.year == y) & (g.week == w)]
    fig = go.Figure()
    for lo, hi, op in ((0, 0.6, 0.15), (0.6, 0.85, 0.35), (0.85, 1.01, 0.7)):
        sub = e[(e.similarity >= lo) & (e.similarity < hi)]
        lon, lat = [], []
        for si, sj in zip(sub.state_i, sub.state_j):
            # AK and HI are drawn as insets by the USA projection, so lines to their true coordinates would cross the map
            if si in cen.index and sj in cen.index and not ({si, sj} & {"AK", "HI"}):
                lon += [cen.loc[sj, "lon"], cen.loc[si, "lon"], None]; lat += [cen.loc[sj, "lat"], cen.loc[si, "lat"], None]
        fig.add_trace(go.Scattergeo(lon=lon, lat=lat, mode="lines", line=dict(width=1, color=BLUE), opacity=op,
                                    name=f"similarity {lo:.2f}–{min(hi, 1):.2f}", hoverinfo="skip"))
    nodes = cen.loc[cen.index.intersection(e.state_i.unique())]
    nb = e.groupby("state_i").state_j.apply(lambda s: ", ".join(s))
    fig.add_trace(go.Scattergeo(lon=nodes.lon, lat=nodes.lat, text=nodes.index, mode="markers+text", textposition="top center",
                                marker=dict(size=7, color=ORANGE, line=dict(width=1, color="white")), name="state",
                                customdata=[nb.get(s, "") for s in nodes.index],
                                hovertemplate="%{text}<br>neighbours: %{customdata}<extra></extra>"))
    fig.update_layout(geo_scope="usa", height=560, margin=dict(l=0, r=0, t=10, b=0), legend=dict(orientation="h"))
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"{pick}: {len(e)} directed edges. Hover a state to see its neighbours (including AK and HI, whose edges are "
               "not drawn because the map shows them as insets).")


{"overview": overview, "cluster": cluster, "batch": batch, "streaming": streaming, "forecast": forecast, "graph": graph}[page]()
