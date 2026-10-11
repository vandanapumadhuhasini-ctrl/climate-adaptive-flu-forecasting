:paste
// Step 3b: the same aggregates with the Spark RDD API and the typed Dataset API, cross-checked against the
// Hadoop MapReduce outputs (mapreduce/) and the Spark DataFrame / pandas benchmark checksum.
//   RDD     : (element, year) -> (count, sum)            == MR1 ElementYearAggregate == benchmark (26 years)
//   Dataset : (state, year, element) -> (count, sum)     == MR2 StateYearCoverage (map-side join with stations)
// Outputs: /opt/results/cross_engine_consistency.json, /opt/results/spark_api_evidence.txt (lineage + plans)


import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types._

val base     = "hdfs://namenode:9000/flu_project"
val rawGlob  = s"$base/raw/weather/*.csv"
val stations = s"$base/raw/weather/ghcnd-stations.txt"
val Elements = Set("TMAX", "TMIN", "PRCP")
val evidence = new StringBuilder

def sha256(lines: Seq[String]): String =
  java.security.MessageDigest.getInstance("SHA-256").digest(lines.sorted.mkString("\n").getBytes("UTF-8")).map("%02x".format(_)).mkString
def timed[T](f: => T): (T, Double) = { val t = System.nanoTime(); val r = f; (r, (System.nanoTime() - t) / 1e9) }

// ------------------------------------------------------------------ RDD API
sc.setJobDescription("Step 3b RDD: element x year aggregate over raw GHCN (textFile -> reduceByKey)")
val rddAgg = sc.textFile(rawGlob)
  .map(_.split(",", -1))
  .filter(f => f.length >= 6 && f(0).startsWith("US") && Elements(f(2)) && f(5).isEmpty && f(1).length >= 4)
  .flatMap(f => scala.util.Try(f(3).toLong).toOption.map(v => ((f(2), f(1).substring(0, 4)), (1L, v))))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
evidence ++= "=== RDD lineage (toDebugString), printed before any action ===\n" + rddAgg.toDebugString + "\n\n"
val (rddRows, rddSecs) = timed(rddAgg.collect())
val rddLines = rddRows.map { case ((el, y), (n, s)) => s"$el|$y|$n|$s" }.toSeq
println(f"AUDIT | RDD     groups=${rddLines.size} rows=${rddRows.map(_._2._1).sum}%,d time=$rddSecs%.1fs partitions=${rddAgg.getNumPartitions}")

// ------------------------------------------------------------------ typed Dataset API
// Case classes Obs, Station, StateObs, CountSum and the typed Aggregator CountSumAgg (map-side partial merge) come from
// scala/lib/ClimaFluTypes.scala, preloaded by:  .\scripts\run_scala.ps1 RddDatasetAudit -Preload lib/ClimaFluTypes.scala

sc.setJobDescription("Step 3b Dataset: typed join with stations + typed Aggregator (state x year x element)")
val obsSchema = StructType(Seq(StructField("id", StringType), StructField("date", StringType), StructField("element", StringType),
  StructField("value", LongType), StructField("mflag", StringType), StructField("qflag", StringType),
  StructField("sflag", StringType), StructField("obstime", StringType)))
val obs = spark.read.option("header", "true").schema(obsSchema).csv(rawGlob).drop("mflag", "sflag", "obstime").as[Obs]
  .filter(o => o.id.startsWith("US") && Elements(o.element) && o.qflag.forall(_.isEmpty) && o.value.isDefined && o.date.length >= 4)
val stationDs = spark.read.textFile(stations)
  .flatMap(l => if (l.length >= 40 && l.startsWith("US") && l.substring(38, 40).trim.nonEmpty)
                  Some(Station(l.substring(0, 11).trim, l.substring(38, 40).trim)) else None)
val joined = obs.joinWith(broadcast(stationDs), obs("id") === stationDs("id"))
  .map { case (o, st) => StateObs(st.state, o.date.substring(0, 4), o.element, o.value.get) }
val dsAgg = joined.groupByKey(r => s"${r.state}|${r.year}|${r.element}").agg(CountSumAgg.toColumn.name("cs"))
evidence ++= "=== Typed Dataset physical plan (explain formatted) ===\n" + dsAgg.queryExecution.explainString(org.apache.spark.sql.execution.FormattedMode) + "\n\n"
val (dsRows, dsSecs) = timed(dsAgg.collect())
val dsLines = dsRows.map { case (k, cs) => s"$k|${cs.n}|${cs.s}" }.toSeq
println(f"AUDIT | Dataset groups=${dsLines.size} rows=${dsRows.map(_._2.n).sum}%,d time=$dsSecs%.1fs stations=${stationDs.count()}")

// ------------------------------------------------------------------ MapReduce outputs (HDFS) + benchmark reference
def mrLines(dir: String): Seq[String] = spark.read.textFile(s"$base/mapreduce/$dir/part-r-*").collect().toSeq
  .map(_.split("\t")).map(a => s"${a(0)}|${a(1)}|${a(2)}")
val mr1 = mrLines("element_year")
val mr2 = mrLines("state_year_element")
val benchRef = scala.io.Source.fromFile("/opt/results/benchmark/runs.jsonl").getLines()
  .filter(l => l.contains("\"years\":26") && l.contains("\"label\":\"spark-hdfs-local[*]\""))
  .map(l => "\"result_sha256\":\"([0-9a-f]+)\"".r.findFirstMatchIn(l).get.group(1)).toSeq.distinct

val rddSha = sha256(rddLines); val mr1Sha = sha256(mr1); val dsSha = sha256(dsLines); val mr2Sha = sha256(mr2)
val check1 = rddSha == mr1Sha && benchRef == Seq(rddSha)
val check2 = dsSha == mr2Sha
println(s"AUDIT | element x year : RDD=$rddSha MR1=$mr1Sha benchmark=${benchRef.mkString(",")} -> ${if (check1) "MATCH" else "MISMATCH"}")
println(s"AUDIT | state x year x element : Dataset=$dsSha MR2=$mr2Sha -> ${if (check2) "MATCH" else "MISMATCH"}")

val json = s"""{
  "generated_at": "${java.time.Instant.now()}",
  "spark_app_id": "${sc.applicationId}",
  "input": "$rawGlob (26 yearly CSVs, 34.57 GB)",
  "element_year": {
    "description": "US stations, TMAX/TMIN/PRCP, empty quality flag; count and sum(value) per (element, year)",
    "groups": ${rddLines.size}, "rows_kept": ${rddRows.map(_._2._1).sum},
    "sha256": {"spark_rdd": "$rddSha", "hadoop_mapreduce_mr1": "$mr1Sha", "spark_dataframe_and_pandas_benchmark": "${benchRef.mkString(",")}"},
    "spark_rdd_seconds": ${"%.1f".format(rddSecs)},
    "all_match": $check1
  },
  "state_year_element": {
    "description": "same filter, joined with ghcnd-stations.txt (US id, non-blank STATE); count and sum per (state, year, element)",
    "groups": ${dsLines.size}, "rows_joined": ${dsRows.map(_._2.n).sum},
    "sha256": {"spark_typed_dataset": "$dsSha", "hadoop_mapreduce_mr2": "$mr2Sha"},
    "spark_dataset_seconds": ${"%.1f".format(dsSecs)},
    "all_match": $check2
  }
}
"""
java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/cross_engine_consistency.json"), json.getBytes("UTF-8"))
java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/spark_api_evidence.txt"), evidence.toString.getBytes("UTF-8"))
println("AUDIT | wrote /opt/results/cross_engine_consistency.json and spark_api_evidence.txt")
System.exit(0)
