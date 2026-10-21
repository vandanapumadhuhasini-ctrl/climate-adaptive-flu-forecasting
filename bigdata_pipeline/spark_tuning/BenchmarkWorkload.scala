:paste
// Part 06 benchmark workload (Spark side). Identical logic to bigdata_pipeline/spark_tuning/benchmark_pandas.py:
//   read N yearly GHCN CSVs (2000..2000+N-1) -> keep US stations, TMAX/TMIN/PRCP, no quality flag
//   -> aggregate by (element, year): count, sum(value)  -> sha256 of the sorted result for cross-engine equality.
// Parameters (spark-shell --conf):  spark.bench.years=N  spark.bench.source=hdfs|local  spark.bench.label=<text>
// Prints exactly one line starting with BENCHJSON.
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val years  = spark.conf.get("spark.bench.years").toInt
val source = spark.conf.get("spark.bench.source", "hdfs")
val label  = spark.conf.get("spark.bench.label", s"spark-$source")
val paths  = (2000 until 2000 + years).map { y =>
  if (source == "hdfs") s"hdfs://namenode:9000/flu_project/raw/weather/$y.csv" else s"file:///rawdata/$y.csv"
}

val schema = StructType(Seq(
  StructField("id", StringType), StructField("date", StringType), StructField("element", StringType),
  StructField("value", LongType), StructField("mflag", StringType), StructField("qflag", StringType),
  StructField("sflag", StringType), StructField("obstime", StringType)))

def procStatus(key: String): Long = scala.io.Source.fromFile("/proc/self/status").getLines()
  .find(_.startsWith(key)).map(_.replaceAll("[^0-9]", "").toLong * 1024L).getOrElse(-1L)
def cgroupPeak(): Long = try { scala.io.Source.fromFile("/sys/fs/cgroup/memory.peak").mkString.trim.toLong } catch { case _: Throwable => -1L }

val t0 = System.nanoTime()
val agg = spark.read.option("header", "true").schema(schema).csv(paths: _*)
  .filter(col("id").startsWith("US"))
  .filter(col("element").isin("TMAX", "TMIN", "PRCP"))
  .filter(col("qflag").isNull || col("qflag") === "")
  .groupBy(col("element"), substring(col("date"), 1, 4).as("year"))
  .agg(count(lit(1)).as("n"), sum("value").as("s"))
  .collect()
val elapsed = (System.nanoTime() - t0) / 1e9

val lines = agg.map(r => s"${r.getString(0)}|${r.getString(1)}|${r.getLong(2)}|${r.getLong(3)}").sorted
val sha = java.security.MessageDigest.getInstance("SHA-256").digest(lines.mkString("\n").getBytes("UTF-8")).map("%02x".format(_)).mkString
val rows = agg.map(_.getLong(2)).sum

println(s"""BENCHJSON {"engine": "spark", "label": "$label", "source": "$source", "master": "${sc.master}", "cores": ${sc.defaultParallelism}, "years": $years, "rows_kept": $rows, "groups": ${lines.length}, "result_sha256": "$sha", "elapsed_s": ${"%.3f".format(elapsed)}, "peak_rss_bytes": ${procStatus("VmHWM")}, "cgroup_peak_bytes": ${cgroupPeak()}, "status": "ok"}""")
System.exit(0)
