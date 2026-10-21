:paste
// Step 6 evidence: Veracity (data-quality facts re-measured from HDFS), Variety (raw format inventory) and a
// Spark DataFrame physical plan. Outputs (container -> host results/):
//   /opt/results/veracity_audit.json  /opt/results/variety_inventory.json  /opt/results/spark_dataframe_plan.txt
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.nio.file.{Files, Paths}

val base = "hdfs://namenode:9000/flu_project"
def write(name: String, s: String): Unit = Files.write(Paths.get(s"/opt/results/$name"), s.getBytes("UTF-8"))
def j(s: String) = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

// ---------------------------------------------------------------- ILINet raw (first line is a title, then CSV)
def fluViewCsv(path: String): DataFrame = {               // DataFrame API only (same approach as Preprocess.scala)
  val lines = spark.read.text(path)
  val title = lines.first().getString(0)
  val tmp   = s"$base/tmp/veracity/" + path.split("/").last.stripSuffix(".csv")
  lines.filter(col("value") =!= title).coalesce(1).write.mode("overwrite").text(tmp)
  spark.read.option("header", "true").csv(tmp)
}
val iliRaw = fluViewCsv(s"$base/raw/ILINet.csv")
val iliRows = iliRaw.count()
val states = iliRaw.filter(col("`REGION TYPE`") === "States")
val stateRows = states.count()
def xCount(c: String) = states.filter(col(s"`$c`") === "X" || col(s"`$c`").isNull).count()
val wNull = xCount("% WEIGHTED ILI"); val uNull = xCount("%UNWEIGHTED ILI")
val iliTotNull = xCount("ILITOTAL"); val patNull = xCount("TOTAL PATIENTS")
val nyDup = states.filter(col("REGION").isin("New York", "New York City"))
  .groupBy("YEAR", "WEEK").agg(countDistinct("REGION").as("n")).filter(col("n") === 2).count()
// the same count after FinalJoin.scala's filter (ILITOTAL present, TOTAL PATIENTS > 0) = the rows that were actually merged
val nyDupMerged = states.filter(col("REGION").isin("New York", "New York City") && col("ILITOTAL") =!= "X" &&
    col("`TOTAL PATIENTS`") =!= "X" && col("`TOTAL PATIENTS`").cast("double") > 0)
  .groupBy("YEAR", "WEEK").agg(countDistinct("REGION").as("n")).filter(col("n") === 2).count()
val regionTypes = iliRaw.groupBy("`REGION TYPE`").count().collect().map(r => s"${j(r.getString(0))}: ${r.getLong(1)}").mkString(", ")

// ---------------------------------------------------------------- modeling table
val mt = spark.read.parquet(s"$base/processed/modeling_table")
val mtRows = mt.count()
val keyCols = Seq("state_abbrev", "YEAR", "WEEK")
val nullCols = Seq("UNWEIGHTED_ILI", "TMAX", "TMIN", "PRCP", "DIURNAL_RANGE")
val nulls = nullCols.map(c => c -> mt.filter(col(c).isNull).count())
val anyNull = mt.filter(nullCols.map(c => col(c).isNull).reduce(_ || _)).count()
val dupKeys = mt.groupBy(keyCols.map(col): _*).count().filter(col("count") > 1).count()
val tmaxLtTmin = mt.filter(col("TMAX") < col("TMIN")).count()
val prcpNeg = mt.filter(col("PRCP") < 0).count()
val iliOut = mt.filter(col("UNWEIGHTED_ILI") < 0 || col("UNWEIGHTED_ILI") > 100).count()

// ---------------------------------------------------------------- weather_cleaned ranges (303.6 M rows; values in degC / mm)
val wc = spark.read.parquet(s"$base/processed/weather_cleaned")
val ranges = wc.groupBy("element").agg(count(lit(1)).as("n"), min("value_scaled").as("min"), max("value_scaled").as("max"),
    sum(when(col("element") =!= "PRCP" && (col("value_scaled") > 60 || col("value_scaled") < -70), 1).otherwise(0)).as("temp_implausible"),
    sum(when(col("element") === "PRCP" && col("value_scaled") < 0, 1).otherwise(0)).as("prcp_negative"),
    min("obs_date").as("first_date"), max("obs_date").as("last_date"))
  .orderBy("element").collect()
val rangeJson = ranges.map(r => s"""    ${j(r.getString(0))}: {"rows": ${r.getLong(1)}, "min": ${r.getDouble(2)}, "max": ${r.getDouble(3)}, "temp_outside_-70_60C": ${r.getLong(4)}, "prcp_negative": ${r.getLong(5)}, "first_date": ${j(r.get(6).toString)}, "last_date": ${j(r.get(7).toString)}}""").mkString(",\n")

write("veracity_audit.json", s"""{
  "generated_at": ${j(java.time.Instant.now().toString)},
  "app_id": ${j(sc.applicationId)},
  "ilinet_raw": {"rows": $iliRows, "rows_by_region_type": {$regionTypes}, "state_rows": $stateRows,
    "state_missing_X": {"weighted_ili": $wNull, "unweighted_ili": $uNull, "ilitotal": $iliTotNull, "total_patients": $patNull},
    "weighted_ili_missing_fraction": ${wNull.toDouble / stateRows}, "unweighted_ili_missing_fraction": ${uNull.toDouble / stateRows},
    "decision": "target = UNWEIGHTED_ILI (WEIGHTED_ILI is almost always missing at state level)",
    "new_york_plus_nyc_duplicate_state_weeks_raw": $nyDup,
    "new_york_plus_nyc_duplicate_state_weeks_after_finaljoin_filter": $nyDupMerged,
    "nyc_decision": "NY and NYC merged by summing ILITOTAL and TOTAL_PATIENTS, then recomputing the rate (bigdata_pipeline/spark_etl/FinalJoin.scala)"},
  "modeling_table": {"rows": $mtRows, "null_counts": {${nulls.map { case (c, n) => s"${j(c)}: $n" }.mkString(", ")}},
    "rows_with_any_null": $anyNull, "rows_after_null_drop": ${mtRows - anyNull}, "duplicate_state_week_keys": $dupKeys,
    "tmax_lt_tmin": $tmaxLtTmin, "prcp_negative": $prcpNeg, "ili_outside_0_100": $iliOut},
  "weather_cleaned_ranges": {
$rangeJson
  }
}
""")
println(s"VERACITY | ILINet state rows=$stateRows weighted missing=$wNull (${"%.4f".format(wNull.toDouble / stateRows)}) NY+NYC dup weeks raw=$nyDup merged=$nyDupMerged | modeling rows=$mtRows anyNull=$anyNull dupKeys=$dupKeys tmax<tmin=$tmaxLtTmin")

// ---------------------------------------------------------------- Variety: raw inventory straight from the HDFS namespace
val fs = FileSystem.get(new java.net.URI("hdfs://namenode:9000"), sc.hadoopConfiguration)
def files(dir: String) = fs.listStatus(new Path(dir)).filter(_.isFile)
val raw = files(s"$base/raw") ++ files(s"$base/raw/weather")
def kind(n: String) = if (n.matches("\\d{4}\\.csv")) "GHCN-Daily yearly observations: CSV with header (8 columns)"
  else if (n.endsWith(".txt")) "GHCN station metadata: fixed-width text (no delimiter)"
  else if (n.startsWith("ILINet")) "CDC ILINet: CSV with a title line before the header"
  else "CDC NREVSS lab surveillance: CSV with a title line before the header"
val inv = raw.groupBy(f => kind(f.getPath.getName)).toSeq.sortBy(_._1).map { case (k, fsx) =>
  s"""    {"format": ${j(k)}, "files": ${fsx.length}, "bytes": ${fsx.map(_.getLen).sum}, "replication": ${fsx.map(_.getReplication).max}}""" }
val stationLines = spark.read.text(s"$base/raw/weather/ghcnd-stations.txt").count()
val parquetDirs = fs.listStatus(new Path(s"$base/processed")).filter(_.isDirectory).map(_.getPath.getName).sorted
write("variety_inventory.json", s"""{
  "generated_at": ${j(java.time.Instant.now().toString)},
  "raw_formats": [
${inv.mkString(",\n")}
  ],
  "station_file_lines": $stationLines, "ilinet_data_rows": $iliRows,
  "processed_parquet_tables": [${parquetDirs.map(j).mkString(", ")}],
  "distinct_raw_format_families": ${inv.length}
}
""")

// ---------------------------------------------------------------- DataFrame physical plan (Catalyst) of a real query
val q = mt.na.drop(nullCols).groupBy("state_abbrev").agg(avg("UNWEIGHTED_ILI").as("mean_ili"), corr("TMAX", "UNWEIGHTED_ILI").as("corr_tmax_ili"))
  .join(spark.read.parquet(s"$base/processed/us_stations").groupBy("state").count().withColumnRenamed("state", "state_abbrev"), "state_abbrev")
  .orderBy(desc("mean_ili"))
write("spark_dataframe_plan.txt", "=== DataFrame physical plan (explain formatted): per-state mean ILI and TMAX-ILI correlation joined with station counts ===\n" +
  q.queryExecution.explainString(org.apache.spark.sql.execution.FormattedMode) + "\n=== top 5 rows ===\n" + q.limit(5).collect().mkString("\n") + "\n")
println("VERACITY | wrote veracity_audit.json, variety_inventory.json, spark_dataframe_plan.txt")
System.exit(0)
