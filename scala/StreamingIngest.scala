:paste
val hdfsBase = "hdfs://namenode:9000"

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

val rawSchema = StructType(Seq(
  StructField("REGION TYPE", StringType),
  StructField("REGION", StringType),
  StructField("YEAR", StringType),
  StructField("WEEK", StringType),
  StructField("% WEIGHTED ILI", StringType),
  StructField("%UNWEIGHTED ILI", StringType),
  StructField("AGE 0-4", StringType),
  StructField("AGE 25-49", StringType),
  StructField("AGE 25-64", StringType),
  StructField("AGE 5-24", StringType),
  StructField("AGE 50-64", StringType),
  StructField("AGE 65", StringType),
  StructField("ILITOTAL", StringType),
  StructField("NUM. OF PROVIDERS", StringType),
  StructField("TOTAL PATIENTS", StringType)
))

val incomingStream = spark.readStream
  .option("header", "true")
  .schema(rawSchema)
  .csv(s"$hdfsBase/flu_project/streaming_incoming")

def cleanStreamingBatch(df: org.apache.spark.sql.DataFrame) = {
  val nullified = df.columns.foldLeft(df) { (acc, c) =>
    acc.withColumn(c, when(col(s"`$c`") === "X" || col(s"`$c`") === "", null).otherwise(col(s"`$c`")))
  }
  nullified
    .withColumnRenamed("REGION TYPE", "REGION_TYPE")
    .withColumnRenamed("% WEIGHTED ILI", "WEIGHTED_ILI")
    .withColumnRenamed("%UNWEIGHTED ILI", "UNWEIGHTED_ILI")
    .withColumnRenamed("NUM. OF PROVIDERS", "NUM_PROVIDERS")
    .withColumn("YEAR", col("YEAR").cast("int"))
    .withColumn("WEEK", col("WEEK").cast("int"))
    .withColumn("WEIGHTED_ILI", col("WEIGHTED_ILI").cast("double"))
    .withColumn("UNWEIGHTED_ILI", col("UNWEIGHTED_ILI").cast("double"))
    .withColumn("ILITOTAL", col("ILITOTAL").cast("double"))
    .withColumn("TOTAL_PATIENTS", col("`TOTAL PATIENTS`").cast("double"))
    .filter(col("REGION").isNotNull)
}

val cleanedStream = cleanStreamingBatch(incomingStream)

val query = cleanedStream.writeStream
  .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
    val cnt = batchDF.count()
    if (cnt > 0) {
      println(s"=== Micro-batch $batchId: $cnt new rows arrived at ${java.time.LocalDateTime.now()} ===")
      batchDF.select("REGION", "YEAR", "WEEK", "UNWEIGHTED_ILI").show(5, truncate = false)
      batchDF.write.mode("append").parquet(s"$hdfsBase/flu_project/streaming_output/ILINet_streamed")
    } else {
      println(s"=== Micro-batch $batchId: no new files yet (heartbeat) ===")
    }
  }
  .option("checkpointLocation", s"$hdfsBase/flu_project/streaming_checkpoint")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()

println("Streaming query started — watching /flu_project/streaming_incoming")
println("Running for up to 3 minutes, then stopping automatically.")

query.awaitTermination(180000)
query.stop()
println("Streaming query stopped.")

val outputExists = try { spark.read.parquet(s"$hdfsBase/flu_project/streaming_output/ILINet_streamed"); true } catch { case _: Exception => false }
if (outputExists) {
  val finalCount = spark.read.parquet(s"$hdfsBase/flu_project/streaming_output/ILINet_streamed").count()
  println(s"Total rows ingested via streaming: $finalCount")
} else {
  println("No data written — the drip script likely wasn't run during this window.")
}