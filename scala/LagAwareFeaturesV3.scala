:paste
// v3 (docs/RESEARCH_PROTOCOL.md, amendment A1): lag-aware climate + multi-horizon features, built in Spark on HDFS.
//   inputs : HDFS processed/modeling_table (ILI + same-week climate), processed/weather_state_week (climate 2000-2025)
//   1. exact-calendar ILI lags 1-4 and targets for h = 1..4 (MMWR week start dates; same rule as python/rigour/common.py)
//   2. lag scan on training years only (YEAR <= 2021): residual of target ~ ILI lags (MLlib OLS) vs within-state z-score of
//      each climate feature at lead L = 0..8 weeks -> L*(feature, h) = argmax |corr|; per-state scan kept as analysis
//   3. feature tables per horizon -> HDFS processed/v3_features/h<h> (+ local copy results/v3/features/h<h> for PyTorch)
//   4. check: h = 1 rows/values == v2 feature table (results/rigour/feature_table.parquet)
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import java.time.LocalDate

val base = "hdfs://namenode:9000/flu_project/processed"
val climate = Seq("TMAX", "TMIN", "PRCP", "DIURNAL_RANGE")
val lagCols = Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4", "ILI_trend")
val leads = 0 to 8
val horizons = 1 to 4
val weekStart = udf((y: Int, w: Int) => {                       // MMWR: weeks start Sunday, week 1 contains 4 January
  val jan4 = LocalDate.of(y, 1, 4)
  java.sql.Date.valueOf(jan4.minusDays(jan4.getDayOfWeek.getValue % 7).plusWeeks(w - 1))
})
def j(s: String) = "\"" + s + "\""

// ------------------------------------------------------------------ 1. ILI table with exact calendar lags / targets
val ili = spark.read.parquet(s"$base/modeling_table").na.drop((climate :+ "UNWEIGHTED_ILI"))
  .withColumn("ws", weekStart(col("YEAR"), col("WEEK"))).cache()
val dupWs = ili.groupBy("state_abbrev", "ws").count().filter(col("count") > 1).count()
require(dupWs == 0, s"$dupWs duplicate (state, week_start) rows")
val val_ = ili.select(col("state_abbrev"), col("ws"), col("UNWEIGHTED_ILI").as("v"))
def shifted(name: String, weeks: Int): DataFrame =                // value of the same state exactly `weeks` weeks later
  val_.select(col("state_abbrev"), date_sub(col("ws"), 7 * weeks).as("ws"), col("v").as(name))
var feat = ili
for (k <- 1 to 4) feat = feat.join(shifted(s"ILI_lag$k", -k), Seq("state_abbrev", "ws"), "left")
for (h <- horizons) feat = feat.join(shifted(s"target_h$h", h), Seq("state_abbrev", "ws"), "left")
feat = feat.withColumn("ILI_trend", col("ILI_lag1") - col("ILI_lag2"))
  .na.drop(Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4")).cache()

// ------------------------------------------------------------------ climate at lead L (weather_state_week reaches 2000)
val wx = spark.read.parquet(s"$base/weather_state_week")
  .select(col("state").as("state_abbrev"), weekStart(col("mmwr_year"), col("mmwr_week")).as("ws"), col("mmwr_year").as("wyear"),
          col("TMAX"), col("TMIN"), col("PRCP"), col("DIURNAL_RANGE"))
// within-state z-score statistics from training years only (<= 2021)
val wStats = wx.filter(col("wyear") <= 2021).groupBy("state_abbrev")
  .agg(avg("TMAX").as("TMAX_m"), (stddev("TMAX").as("TMAX_s") +:
       climate.tail.flatMap(f => Seq(avg(f).as(s"${f}_m"), stddev(f).as(s"${f}_s")))): _*)   // agg(first, rest: _*)
val wxz = wx.join(wStats, "state_abbrev")
  .select((Seq(col("state_abbrev"), col("ws")) ++ climate.map(col) ++ climate.map(f => ((col(f) - col(s"${f}_m")) / col(s"${f}_s")).as(s"${f}_z"))): _*)
def climateAtLead(L: Int): DataFrame =                            // climate of week t - L, attached to week t
  wxz.select((Seq(col("state_abbrev"), date_add(col("ws"), 7 * L).as("ws")) ++
              climate.flatMap(f => Seq(col(f).as(s"${f}_L$L"), col(s"${f}_z").as(s"${f}_zL$L")))): _*)
var scanBase = feat
for (L <- leads) scanBase = scanBase.join(climateAtLead(L), Seq("state_abbrev", "ws"), "left")
scanBase = scanBase.cache()

// ------------------------------------------------------------------ 2. lag scan on YEAR <= 2021
val scanRows = scala.collection.mutable.ArrayBuffer[String]()
val chosen = scala.collection.mutable.Map[(String, Int), Int]()
val perState = scala.collection.mutable.ArrayBuffer[String]()
for (h <- horizons) {
  val tr = scanBase.filter(col("YEAR") <= 2021 && col(s"target_h$h").isNotNull)
  val asm = new VectorAssembler().setInputCols(lagCols.toArray).setOutputCol("x")
  val lr = new LinearRegression().setFeaturesCol("x").setLabelCol(s"target_h$h").setRegParam(0.0).fit(asm.transform(tr))
  val res = lr.transform(asm.transform(tr)).withColumn("resid", col(s"target_h$h") - col("prediction")).cache()
  for (f <- climate) {
    val cs = leads.map(L => L -> res.na.drop(Seq(s"${f}_zL$L")).stat.corr("resid", s"${f}_zL$L"))   // complete pairs only
    val best = cs.maxBy(t => math.abs(t._2))._1
    chosen((f, h)) = best
    scanRows += s"""{"horizon": $h, "feature": ${j(f)}, "corr_by_lead": [${cs.map(t => "%.5f".format(t._2)).mkString(", ")}], "chosen_lead": $best}"""
    // analysis only: per-state best lead (not used by any model)
    val corrCols = leads.map(L => corr("resid", s"${f}_zL$L").as(s"c$L"))
    val ps = res.groupBy("state_abbrev").agg(corrCols.head, corrCols.tail: _*).collect()
    val bestLeads = ps.map(r => leads.maxBy(L => { val v = r.getAs[Double](s"c$L"); if (v.isNaN) -1.0 else math.abs(v) }))
    val hist = leads.map(L => bestLeads.count(_ == L))
    perState += s"""{"horizon": $h, "feature": ${j(f)}, "states_by_best_lead": [${hist.mkString(", ")}]}"""
  }
  println(s"V3 | h=$h ILI-lag OLS R2=${"%.4f".format(lr.summary.r2)} | chosen leads: " + climate.map(f => s"$f=${chosen((f, h))}").mkString(" "))
  res.unpersist()
}

// ------------------------------------------------------------------ 3. feature tables per horizon (HDFS + local copy)
val counts = scala.collection.mutable.ArrayBuffer[String]()
for (h <- horizons) {
  val aligned = climate.map(f => col(s"${f}_L${chosen((f, h))}").as(s"${f}_al"))
  val t = scanBase.filter(col(s"target_h$h").isNotNull)
    .select((Seq(col("state_abbrev"), col("YEAR"), col("WEEK"), col("ws").cast("string").as("week_start")) ++ climate.map(col) ++
             Seq(col("ILI_lag1"), col("ILI_lag2"), col("ILI_lag3"), col("ILI_lag4"), col("ILI_trend"), col(s"target_h$h").as("target")) ++
             aligned :+ lit(h).as("horizon")): _*)
    .na.drop(climate.map(f => s"${f}_al"))
    .orderBy("YEAR", "WEEK", "state_abbrev")
  t.write.mode("overwrite").parquet(s"$base/v3_features/h$h")
  spark.read.parquet(s"$base/v3_features/h$h").coalesce(1).write.mode("overwrite").parquet(s"file:///opt/results/v3/features/h$h")
  val n = spark.read.parquet(s"$base/v3_features/h$h")
  counts += s""""h$h": {"rows": ${n.count()}, "test_rows": ${n.filter(col("YEAR") >= 2023).count()}}"""
}

// ------------------------------------------------------------------ 4. check against the v2 (Python) feature table, h = 1
val v2 = spark.read.parquet("file:///opt/results/rigour/feature_table.parquet")
val s1 = scanBase.filter(col("target_h1").isNotNull)
val k = Seq("state_abbrev", "YEAR", "WEEK")
val joined = v2.join(s1, k, "full_outer")
val onlyV2 = joined.filter(col("ws").isNull).count()
val onlyV3 = joined.filter(v2("target").isNull).count()
val diffs = Seq("ILI_lag1", "ILI_lag2", "ILI_lag3", "ILI_lag4").map(c => abs(v2(c) - s1(c))) :+ abs(v2("target") - s1("target_h1"))
val maxDiff = v2.join(s1, k).select(diffs.reduce((a, b) => greatest(a, b)).as("d")).agg(max("d")).first().getDouble(0)
println(s"V3 | v2 consistency h=1: v2 rows=${v2.count()} spark rows=${s1.count()} only-v2=$onlyV2 only-spark=$onlyV3 max|diff|=$maxDiff")

java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/v3/lag_scan.json"), s"""{
  "generated_at": ${j(java.time.Instant.now().toString)},
  "app_id": ${j(sc.applicationId)},
  "selection_years": "YEAR <= 2021",
  "leads_weeks": [${leads.mkString(", ")}],
  "scan": [
    ${scanRows.mkString(",\n    ")}
  ],
  "chosen_leads": {${horizons.map(h => s""""h$h": {${climate.map(f => s"${j(f)}: ${chosen((f, h))}").mkString(", ")}}""").mkString(", ")}},
  "per_state_best_lead_analysis": [
    ${perState.mkString(",\n    ")}
  ],
  "feature_tables": {${counts.mkString(", ")}},
  "v2_consistency_h1": {"v2_rows": ${v2.count()}, "spark_rows": ${s1.count()}, "only_in_v2": $onlyV2, "only_in_spark": $onlyV3, "max_abs_diff": $maxDiff,
                        "identical": ${onlyV2 == 0 && onlyV3 == 0 && maxDiff < 1e-9}}
}
""".getBytes("UTF-8"))
println("V3 | wrote results/v3/lag_scan.json and HDFS processed/v3_features/h1..h4")
System.exit(0)
