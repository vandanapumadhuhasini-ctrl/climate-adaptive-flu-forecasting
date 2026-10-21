:paste
// DataFrame-only cross-engine consistency + verification of the DataFrame rewrite (professor: DataFrame API only).
//   A. element x year (count, sum) over the 26 raw CSVs        == Hadoop MapReduce MR1 == Spark/pandas benchmark (26 years)
//   B. state x year x element via a broadcast join with the stations file (same rule as MR2) == Hadoop MapReduce MR2
//   C. rewritten/merged jobs (Preprocess, BuildModelingTable) run with --conf spark.flu.outputSuffix=_dfcheck must reproduce the
//      existing HDFS tables exactly (schema names/types, row count, and no rows in either direction of exceptAll)
// Outputs: /opt/results/cross_engine_consistency.json, /opt/results/spark_api_evidence.txt (DataFrame physical plans)
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.security.MessageDigest

val base     = "hdfs://namenode:9000/flu_project"
val rawGlob  = s"$base/raw/weather/*.csv"
val evidence = new StringBuilder
def sha256(lines: Seq[String]): String =
  MessageDigest.getInstance("SHA-256").digest(lines.sorted.mkString("\n").getBytes("UTF-8")).map("%02x".format(_)).mkString
def timed[T](f: => T): (T, Double) = { val t = System.nanoTime(); val r = f; (r, (System.nanoTime() - t) / 1e9) }
def j(s: String) = "\"" + s + "\""

// ------------------------------------------------------------------ kept observations (same filter as MapReduce)
val schema = StructType(Seq("id", "date", "element", "value", "mflag", "qflag", "sflag", "obstime").map(StructField(_, StringType)))
val kept = spark.read.option("header", "true").schema(schema).csv(rawGlob)
  .filter(col("id").startsWith("US") && col("element").isin("TMAX", "TMIN", "PRCP") &&
          (col("qflag").isNull || col("qflag") === "") && length(col("date")) >= 4)
  .withColumn("v", col("value").cast("long")).filter(col("v").isNotNull)
  .select(col("id"), substring(col("date"), 1, 4).as("year"), col("element"), col("v"))

// A. element x year
sc.setJobDescription("DataFrame consistency A: element x year aggregate over raw GHCN")
val aggA = kept.groupBy("element", "year").agg(count(lit(1)).as("n"), sum("v").as("s"))
evidence ++= "=== A. element x year (DataFrame API): physical plan ===\n" +
  aggA.queryExecution.explainString(org.apache.spark.sql.execution.FormattedMode) + "\n\n"
val (rowsA, secsA) = timed(aggA.collect())
val linesA = rowsA.map(r => s"${r.getString(0)}|${r.getString(1)}|${r.getLong(2)}|${r.getLong(3)}").toSeq

// B. state x year x element: stations parsed from the fixed-width file with column functions, broadcast join
sc.setJobDescription("DataFrame consistency B: broadcast join with stations, state x year x element")
val stations = spark.read.text(s"$base/raw/weather/ghcnd-stations.txt")
  .filter(length(col("value")) >= 40 && col("value").startsWith("US"))
  .select(trim(substring(col("value"), 1, 11)).as("id"), trim(substring(col("value"), 39, 2)).as("state"))
  .filter(length(col("state")) > 0)
val aggB = kept.join(broadcast(stations), "id").groupBy("state", "year", "element").agg(count(lit(1)).as("n"), sum("v").as("s"))
evidence ++= "=== B. state x year x element (DataFrame API, broadcast join): physical plan ===\n" +
  aggB.queryExecution.explainString(org.apache.spark.sql.execution.FormattedMode) + "\n\n"
val (rowsB, secsB) = timed(aggB.collect())
val linesB = rowsB.map(r => s"${r.getString(0)}|${r.getString(1)}|${r.getString(2)}|${r.getLong(3)}|${r.getLong(4)}").toSeq

// MapReduce outputs (read as DataFrames) and the benchmark reference
def mrLines(dir: String): Seq[String] = spark.read.text(s"$base/mapreduce/$dir/part-*")
  .select(regexp_replace(col("value"), "\t", "|")).collect().map(_.getString(0)).toSeq
val mr1 = mrLines("element_year"); val mr2 = mrLines("state_year_element")
val benchRef = spark.read.json(s"$base/results/benchmark/runs.jsonl")
  .filter(col("years") === 26 && col("label") === "spark-hdfs-local[*]" && col("status") === "ok")
  .select("result_sha256").distinct().collect().map(_.getString(0)).toSeq
val shaA = sha256(linesA); val shaMR1 = sha256(mr1); val shaB = sha256(linesB); val shaMR2 = sha256(mr2)
val okA = shaA == shaMR1 && benchRef == Seq(shaA)
val okB = shaB == shaMR2
println(s"DFCHECK | A element x year: DataFrame=$shaA MR1=$shaMR1 benchmark=${benchRef.mkString(",")} -> ${if (okA) "MATCH" else "MISMATCH"}")
println(s"DFCHECK | B state x year x element: DataFrame=$shaB MR2=$shaMR2 -> ${if (okB) "MATCH" else "MISMATCH"}")

// C. rewrite verification: _dfcheck outputs of the rewritten jobs vs the tables already in HDFS
def same(name: String): String = {
  val a = spark.read.parquet(s"$base/processed/$name"); val b = spark.read.parquet(s"$base/processed/${name}_dfcheck")
  val schemaSame = a.schema.map(f => (f.name, f.dataType)) == b.schema.map(f => (f.name, f.dataType))
  val (na, nb) = (a.count(), b.count())
  val onlyA = if (schemaSame) a.exceptAll(b).count() else -1L
  val onlyB = if (schemaSame) b.exceptAll(a).count() else -1L
  val ok = schemaSame && na == nb && onlyA == 0 && onlyB == 0
  println(f"DFCHECK | C $name%-38s schema=$schemaSame rows=$na/$nb only-old=$onlyA only-new=$onlyB -> ${if (ok) "IDENTICAL" else "DIFFERENT"}")
  s"""${j(name)}: {"schema_names_types_equal": $schemaSame, "rows_existing": $na, "rows_dataframe_rewrite": $nb, "only_in_existing": $onlyA, "only_in_rewrite": $onlyB, "identical": $ok}"""
}
val tablesC = Seq("us_stations", "ILINet", "ICL_NREVSS_Clinical_Labs", "ICL_NREVSS_Public_Health_Labs", "ICL_NREVSS_Combined_prior_to_2015_16",
                  "weather_state_week", "modeling_table")
val resC = tablesC.map(same)
val okC = !resC.exists(_.contains("\"identical\": false"))

java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/cross_engine_consistency.json"), s"""{
  "generated_at": ${j(java.time.Instant.now().toString)},
  "spark_app_id": ${j(sc.applicationId)},
  "spark_api": "DataFrame API only",
  "input": "$rawGlob (26 yearly CSVs, 34.57 GB)",
  "element_year": {
    "description": "US stations, TMAX/TMIN/PRCP, empty quality flag; count and sum(value) per (element, year)",
    "groups": ${linesA.size}, "rows_kept": ${rowsA.map(_.getLong(2)).sum},
    "sha256": {"spark_dataframe": "$shaA", "hadoop_mapreduce_mr1": "$shaMR1", "spark_dataframe_and_pandas_benchmark": "${benchRef.mkString(",")}"},
    "spark_dataframe_seconds": ${"%.1f".format(secsA)}, "all_match": $okA
  },
  "state_year_element": {
    "description": "same filter, broadcast-joined with ghcnd-stations.txt (US id, non-blank STATE); count and sum per (state, year, element)",
    "groups": ${linesB.size}, "rows_joined": ${rowsB.map(_.getLong(3)).sum},
    "sha256": {"spark_dataframe": "$shaB", "hadoop_mapreduce_mr2": "$shaMR2"},
    "spark_dataframe_seconds": ${"%.1f".format(secsB)}, "all_match": $okB
  },
  "dataframe_rewrite_equivalence": {
    ${resC.mkString(",\n    ")},
    "all_identical": $okC
  }
}
""".getBytes("UTF-8"))
java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/spark_api_evidence.txt"), evidence.toString.getBytes("UTF-8"))
println(s"DFCHECK | A=$okA B=$okB C=$okC; wrote cross_engine_consistency.json and spark_api_evidence.txt")
System.exit(0)
