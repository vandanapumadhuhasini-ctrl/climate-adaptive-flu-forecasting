:paste
// Independent audit, pass 2: recompute headline numbers in Spark from the copies stored in HDFS (not the local files):
// MapReduce output SHA-256, forecast RMSEs and seed wins from published prediction Parquet, benchmark and streaming medians
// from published JSON-lines, v3 feature row counts. Output: /opt/results/audit/pass2.json
import org.apache.spark.sql.functions._
import java.security.MessageDigest

val R = "hdfs://namenode:9000/flu_project/results"
val out = scala.collection.mutable.ArrayBuffer[String]()
def rec(name: String, claimed: String, measured: String, ok: Boolean): Unit = {
  out += s"""{"check": "$name", "claimed": "$claimed", "measured": "$measured", "pass": $ok}"""
  println(f"AUDIT2 | ${if (ok) "PASS" else "FAIL"} | $name%-50s | claimed $claimed%-22s | measured $measured")
}
def sha(lines: Seq[String]) = MessageDigest.getInstance("SHA-256").digest(lines.sorted.mkString("\n").getBytes("UTF-8")).map("%02x".format(_)).mkString
def rmse(df: org.apache.spark.sql.DataFrame) = math.sqrt(df.selectExpr("avg(pow(pred - target, 2))").first().getDouble(0))

// MapReduce outputs straight from HDFS
val mr1 = spark.read.textFile("hdfs://namenode:9000/flu_project/mapreduce/element_year/part-r-*").collect().map(_.split("\t")).map(a => s"${a(0)}|${a(1)}|${a(2)}")
val mr2 = spark.read.textFile("hdfs://namenode:9000/flu_project/mapreduce/state_year_element/part-r-*").collect().map(_.split("\t")).map(a => s"${a(0)}|${a(1)}|${a(2)}")
rec("MR1 SHA from HDFS part files", "a5c81104c454", sha(mr1).take(12), sha(mr1).startsWith("a5c81104c454"))
rec("MR2 SHA from HDFS part files", "eb02de0659da", sha(mr2).take(12), sha(mr2).startsWith("eb02de0659da"))

// v2 forecasts from the HDFS copies of the prediction files
val g = (0 to 9).map(s => rmse(spark.read.parquet(s"$R/rigour/predictions/GNN-K8_s$s.parquet")))
val gm = g.sum / g.size; val gsd = math.sqrt(g.map(x => (x - gm) * (x - gm)).sum / (g.size - 1))
rec("v2 GNN-K8 RMSE mean (HDFS)", "0.9236", "%.4f".format(gm), math.abs(gm - 0.9236) < 5e-5)
rec("v2 GNN-K8 RMSE sd (HDFS)", "0.0159", "%.4f".format(gsd), math.abs(gsd - 0.0159) < 5e-5)
val gbt2 = rmse(spark.read.parquet(s"$R/rigour/predictions_spark/GBT-lags"))
rec("v2 Spark GBT-lags RMSE (HDFS)", "0.9401", "%.4f".format(gbt2), math.abs(gbt2 - 0.9401) < 5e-5)

// v3 h=3 from the HDFS copies
val g3 = (0 to 9).map(s => rmse(spark.read.parquet(s"$R/v3/predictions/GNN-lagK8_h3_s$s.parquet")))
val gbt3 = rmse(spark.read.parquet(s"$R/v3/predictions_spark/GBT-lags_h3"))
rec("v3 h=3 GNN-lagK8 mean RMSE (HDFS)", "1.2507", "%.4f".format(g3.sum / 10), math.abs(g3.sum / 10 - 1.2507) < 5e-5)
rec("v3 h=3 GBT-lags RMSE (HDFS)", "1.2891", "%.4f".format(gbt3), math.abs(gbt3 - 1.2891) < 5e-5)
rec("v3 h=3 seeds beating GBT (HDFS)", "9", g3.count(_ < gbt3).toString, g3.count(_ < gbt3) == 9)
val f3 = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/v3_features/h3")
rec("v3 h=3 test rows in HDFS feature table", "7591", f3.filter(col("YEAR") >= 2023).count().toString, f3.filter(col("YEAR") >= 2023).count() == 7591)

// benchmark + streaming medians from the published JSON-lines in HDFS
val b = spark.read.json(s"$R/benchmark/runs.jsonl").filter(col("status") === "ok" && col("experiment") === "A" && col("years") === 26)
def median(label: String) = { val v = b.filter(col("label") === label).select("elapsed_s").collect().map(_.getDouble(0)).sorted; if (v.length % 2 == 1) v(v.length / 2) else (v(v.length / 2 - 1) + v(v.length / 2)) / 2 }
val sp = median("spark-hdfs-local[*]"); val pd = median("pandas-chunked")
rec("benchmark 26y Spark median (HDFS)", "90.1", "%.3f".format(sp), math.abs(sp - 90.1) < 0.05)
rec("benchmark speed-up (HDFS)", "18.0 (17.96)", "%.2f".format(pd / sp), math.abs(pd / sp - 17.96) < 0.01)
val drip = spark.read.json(s"$R/streaming_v2/drip.jsonl")
val bat = spark.read.json(s"$R/streaming_v2/batches.jsonl").select(explode(col("files")).as("file"), col("end_ms"))
val lat = drip.join(bat, "file").select(((col("end_ms") - col("arrival_ms")) / 1000).as("l")).collect().map(_.getDouble(0)).sorted
val lm = if (lat.length % 2 == 1) lat(lat.length / 2) else (lat(lat.length / 2 - 1) + lat(lat.length / 2)) / 2
rec("streaming median latency (HDFS)", "4.7", "%.2f".format(lm), math.abs(lm - 4.7) < 0.05)
rec("streaming files matched to batches (HDFS)", "60", lat.length.toString, lat.length == 60)

java.nio.file.Files.createDirectories(java.nio.file.Paths.get("/opt/results/audit"))
java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/audit/pass2.json"),
  s"""{"app_id": "${sc.applicationId}", "source": "HDFS copies", "passed": ${out.count(_.contains("\"pass\": true"))}, "total": ${out.size},
  "checks": [\n    ${out.mkString(",\n    ")}\n  ]}\n""".getBytes("UTF-8"))
println(s"AUDIT2 | ${out.count(_.contains("\"pass\": true"))} / ${out.size} passed")
System.exit(0)
