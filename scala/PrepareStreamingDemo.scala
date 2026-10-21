:paste
val hdfsBase = "hdfs://namenode:9000"
val rawDir = s"$hdfsBase/flu_project/raw"
val suffix = spark.conf.get("spark.flu.outputSuffix", "")   // _dfcheck = verification run, originals untouched
val streamingBase = s"$hdfsBase/flu_project/streaming_source$suffix"

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
// streaming v1 (superseded by StreamingV2.scala); DataFrame API only (same title-line approach as Preprocess.scala)
def readFluViewCsv(path: String): DataFrame = {
  val rawLines = spark.read.text(path)
  val title    = rawLines.first().getString(0)
  val tmp      = s"$hdfsBase/flu_project/tmp/streaming_v1$suffix/" + path.split("/").last.stripSuffix(".csv")
  rawLines.filter(col("value") =!= title).coalesce(1).write.mode("overwrite").text(tmp)
  spark.read.option("header", "true").option("inferSchema", "true").csv(tmp)
}

val iliRaw = readFluViewCsv(s"$rawDir/ILINet.csv")

// Pick the 5 most recent (YEAR, WEEK) pairs to simulate "the next 5 weekly CDC drops"
val recentWeeks = iliRaw.select("YEAR", "WEEK").distinct()
  .orderBy(col("YEAR").desc, col("WEEK").desc)
  .limit(5)
  .collect()
  .map(r => (r.getInt(0), r.getInt(1)))
  .sortBy { case (y, w) => (y, w) }

println(s"Simulated weekly drops, chronological order: ${recentWeeks.mkString(", ")}")

recentWeeks.zipWithIndex.foreach { case ((year, week), idx) =>
  val n = idx + 1
  val weekData = iliRaw.filter(col("YEAR") === year && col("WEEK") === week)
  weekData.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"$streamingBase/week_$n")
  println(s"Staged week_$n <- YEAR=$year WEEK=$week (${weekData.count()} rows)")
}