:paste
// Step 3c: controlled performance-tuning experiment (partitioning x persistence) on the project's own feature step.
// Base: weather_cleaned (303,631,696 rows) joined with us_stations -> repartition(P, state) -> optional persist,
// then three downstream queries that all reuse the base (the situation where caching should pay off):
//   Q1 state x year x element mean value      Q2 distinct reporting stations per state x year
//   Q3 state x month mean TMAX
// Parameters (--conf): spark.tune.partitions=P  spark.tune.persist=none|memory_and_disk  spark.tune.repeats=R
//                      spark.tune.label=<text>  (shuffle partitions and AQE are set by the caller)
// Prints one TUNEJSON line per measured repeat (after one un-recorded warm-up iteration); a failed iteration is
// recorded with status "failed" and its error class instead of aborting silently.
// v2 (2026-10-10): shuffle files of finished iterations are released (GC -> ContextCleaner) before the next one;
// v1 kept all 12 shuffles of a configuration on disk and filled the Docker disk (see docs/TUNING_RESULTS.md).
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

val P       = spark.conf.get("spark.tune.partitions").toInt
val persist = spark.conf.get("spark.tune.persist", "none")
val repeats = spark.conf.get("spark.tune.repeats", "3").toInt
val label   = spark.conf.get("spark.tune.label", s"p$P-$persist")
val aqe     = spark.conf.get("spark.sql.adaptive.enabled")
val shufP   = spark.conf.get("spark.sql.shuffle.partitions")
val base    = "hdfs://namenode:9000/flu_project/processed"

class Metrics extends SparkListener {
  @volatile var shuffleWrite = 0L; @volatile var shuffleRead = 0L; @volatile var spill = 0L; @volatile var tasks = 0L
  override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized {
    val m = e.taskMetrics
    if (m != null) { shuffleWrite += m.shuffleWriteMetrics.bytesWritten; shuffleRead += m.shuffleReadMetrics.totalBytesRead
                     spill += m.diskBytesSpilled; tasks += 1 }
  }
  def reset(): Unit = synchronized { shuffleWrite = 0; shuffleRead = 0; spill = 0; tasks = 0 }
}
val metrics = new Metrics
sc.addSparkListener(metrics)

def secs[T](f: => T): (T, Double) = { val t = System.nanoTime(); val r = f; (r, (System.nanoTime() - t) / 1e9) }
def dirBytes(f: java.io.File): Long = if (f.isFile) f.length else Option(f.listFiles).map(_.map(dirBytes).sum).getOrElse(0L)
def localTmpBytes(): Long = dirBytes(new java.io.File("/tmp"))

/** Drop references to finished queries and let the ContextCleaner delete their shuffle files. */
def releaseShuffles(): Long = {
  for (_ <- 1 to 3) { System.gc(); Thread.sleep(2000) }
  localTmpBytes()
}

def iteration(): (Double, Double, Seq[Double], Long) = {
  val joined = spark.read.parquet(s"$base/weather_cleaned")
    .join(broadcast(spark.read.parquet(s"$base/us_stations").select("stationId", "state")), "stationId")
    .filter(col("state") =!= "UM")
    .withColumn("year", year(col("obs_date"))).withColumn("month", month(col("obs_date")))
    .repartition(P, col("state"))
  val baseDf = if (persist == "memory_and_disk") joined.persist(StorageLevel.MEMORY_AND_DISK) else joined
  try {
    // persisted: materialise once (counted separately); unpersisted: nothing to materialise
    val (rows, matS) = if (persist != "none") secs(baseDf.count()) else (-1L, 0.0)
    val q = Seq(
      () => baseDf.groupBy("state", "year", "element").agg(avg("value_scaled")).collect().length,
      () => baseDf.groupBy("state", "year").agg(countDistinct("stationId")).collect().length,
      () => baseDf.filter(col("element") === "TMAX").groupBy("state", "month").agg(avg("value_scaled")).collect().length)
    val qs = q.map(f => secs(f())._2)
    (matS + qs.sum, matS, qs, rows)
  } finally {
    if (persist != "none") baseDf.unpersist(blocking = true)
  }
}

def emit(r: Int, status: String, body: String): Unit =
  println(s"""TUNEJSON {"label": "$label", "partitions": $P, "persist": "$persist", "aqe": $aqe, "shuffle_partitions": $shufP, "repeat": $r, "status": "$status", $body, "app_id": "${sc.applicationId}"}""")

def errorBody(e: Throwable): String = {
  val root = Iterator.iterate(e)(_.getCause).takeWhile(_ != null).toSeq.last
  val msg = Option(root.getMessage).getOrElse("").replaceAll("[\"\\\\\\n\\r]", " ").take(180)
  s""""error_class": "${root.getClass.getName}", "error": "$msg", "tmp_bytes_after_cleanup": ${releaseShuffles()}"""
}

sc.setJobDescription(s"Tuning warm-up $label")
val warm = scala.util.Try(iteration())
val tmpAfterWarm = releaseShuffles()
warm match {
  case scala.util.Failure(e) => emit(0, "failed", errorBody(e))     // warm-up failed: the configuration does not run
  case scala.util.Success(_) =>
    var r = 1
    var stop = false
    while (r <= repeats && !stop) {
      sc.setJobDescription(s"Tuning $label repeat $r")
      metrics.reset()
      scala.util.Try(iteration()) match {
        case scala.util.Success((total, mat, qs, rows)) =>
          Thread.sleep(1500)                                   // let the listener bus drain
          val tmpAfter = releaseShuffles()
          emit(r, "ok", s""""total_s": ${"%.3f".format(total)}, "materialize_s": ${"%.3f".format(mat)}, "q1_s": ${"%.3f".format(qs(0))}, "q2_s": ${"%.3f".format(qs(1))}, "q3_s": ${"%.3f".format(qs(2))}, "base_rows": $rows, "tasks": ${metrics.tasks}, "shuffle_write_bytes": ${metrics.shuffleWrite}, "shuffle_read_bytes": ${metrics.shuffleRead}, "disk_spill_bytes": ${metrics.spill}, "tmp_bytes_after_cleanup": $tmpAfter""")
        case scala.util.Failure(e) => emit(r, "failed", errorBody(e)); stop = true
      }
      r += 1
    }
}
System.exit(0)
