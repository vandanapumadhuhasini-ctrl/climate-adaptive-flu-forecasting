:paste
// Streaming v2 (Velocity evidence): a live GHCN climate feed. One raw CSV per day is dripped into
// HDFS /flu_project/streaming_v2/incoming (atomic HDFS rename, run_pipeline.ps1 streaming). Every 5 s micro-batch:
//   clean (US station, TMAX/TMIN/PRCP, empty QC flag) -> broadcast-join stations -> state x day x element mean + count
//   -> append Parquet to HDFS /flu_project/streaming_v2/output
// Evidence written to /opt/results/streaming_v2/: progress.jsonl (StreamingQueryProgress per trigger, via a listener),
// batches.jsonl (files, rows in/kept/out, start/end time of each non-empty micro-batch). Stops 40 s after the feed goes quiet.
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{StreamingQueryListener, Trigger}
import java.nio.file.{Files, Paths, StandardOpenOption}

val base = "hdfs://namenode:9000/flu_project"
val outDir = "/opt/results/streaming_v2"
Files.createDirectories(Paths.get(outDir))
def appendLine(file: String, line: String): Unit = synchronized {
  Files.write(Paths.get(s"$outDir/$file"), (line + "\n").getBytes("UTF-8"), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
}

spark.streams.addListener(new StreamingQueryListener {
  override def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit = ()
  override def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = appendLine("progress.jsonl", e.progress.json)
  override def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
})

val stations = broadcast(spark.read.parquet(s"$base/processed/us_stations").select(col("stationId").as("id"), col("state")).cache())
val schema = StructType(Seq("id", "date", "element", "value", "mflag", "qflag", "sflag", "obstime").map(StructField(_, StringType)))
val feed = spark.readStream.option("header", "true").option("maxFilesPerTrigger", 4).schema(schema)
  .csv(s"$base/streaming_v2/incoming").withColumn("src_file", input_file_name())

val process = (batch: DataFrame, batchId: Long) => {
  val t0 = System.currentTimeMillis()
  batch.persist()
  val rowsIn = batch.count()
  if (rowsIn > 0) {
    val kept = batch.filter(col("id").startsWith("US") && col("element").isin("TMAX", "TMIN", "PRCP") &&
                            (col("qflag").isNull || col("qflag") === ""))
                    .join(stations, "id")
    val agg = kept.groupBy("state", "date", "element")
                  .agg(avg(col("value").cast("double") / 10).as("mean_value"), count(lit(1)).as("n_obs"))
                  .withColumn("batch_id", lit(batchId))
    agg.persist()
    val groups = agg.count()
    val rowsKept = agg.agg(sum("n_obs")).first().getLong(0)
    agg.write.mode("append").parquet(s"$base/streaming_v2/output")
    agg.unpersist()
    val files = batch.select("src_file").distinct().collect().map(_.getString(0).split("/").last).sorted
    val t1 = System.currentTimeMillis()
    appendLine("batches.jsonl", s"""{"batch_id": $batchId, "files": [${files.map(f => "\"" + f + "\"").mkString(", ")}], "rows_in": $rowsIn, "rows_kept": $rowsKept, "groups_out": $groups, "start_ms": $t0, "end_ms": $t1}""")
    println(s"STREAM | batch $batchId files=${files.length} rows_in=$rowsIn kept=$rowsKept groups=$groups ${t1 - t0} ms")
  }
  batch.unpersist()
  ()
}

val query = feed.writeStream.foreachBatch(process)
  .option("checkpointLocation", s"$base/streaming_v2/checkpoint")
  .trigger(Trigger.ProcessingTime("5 seconds"))
  .start()
println(s"STREAM | started ${query.id}; waiting for the feed")

// stop 40 s after the last batch with data (or after 20 minutes in total)
val started = System.currentTimeMillis()
var lastData = System.currentTimeMillis()
var sawData = false
while (query.isActive && System.currentTimeMillis() - started < 20 * 60 * 1000 &&
       !(sawData && System.currentTimeMillis() - lastData > 40000)) {
  query.awaitTermination(2000)
  val p = query.lastProgress
  if (p != null && p.numInputRows > 0 && p.timestamp != null) { sawData = true; lastData = System.currentTimeMillis() }
}
query.stop()
val total = spark.read.parquet(s"$base/streaming_v2/output")
println(s"STREAM | stopped; output groups=${total.count()} observations=${total.agg(sum("n_obs")).first().getLong(0)}")
System.exit(0)
