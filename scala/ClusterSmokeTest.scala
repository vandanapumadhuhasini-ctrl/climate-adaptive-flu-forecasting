:paste
// Step 1 verification: Spark reads every processed HDFS table and records its row count.
// Output: results/cluster_smoke_test.json (host) = /opt/results/cluster_smoke_test.json (container)
val hdfsBase = "hdfs://namenode:9000/flu_project/processed"
val tables = Seq("weather_cleaned", "us_stations", "weather_state_week", "modeling_table",
                 "graph_edges_knn", "baseline_predictions", "ILINet")

val t0 = System.currentTimeMillis()
val counts = tables.map { t =>
  val s = System.currentTimeMillis()
  val n = spark.read.parquet(s"$hdfsBase/$t").count()
  val ms = System.currentTimeMillis() - s
  println(f"SMOKE | $t%-22s | rows=$n%,d | ${ms}ms")
  (t, n, ms)
}
val totalMs = System.currentTimeMillis() - t0

val json = counts.map { case (t, n, ms) => s"""    "$t": {"rows": $n, "count_ms": $ms}""" }.mkString(",\n")
val out = s"""{
  "generated_at": "${java.time.Instant.now()}",
  "spark_version": "${spark.version}",
  "master": "${sc.master}",
  "app_id": "${sc.applicationId}",
  "hdfs_base": "$hdfsBase",
  "tables": {
$json
  },
  "total_ms": $totalMs
}
"""
java.nio.file.Files.write(java.nio.file.Paths.get("/opt/results/cluster_smoke_test.json"), out.getBytes("UTF-8"))
println("SMOKE | wrote /opt/results/cluster_smoke_test.json")
System.exit(0)
