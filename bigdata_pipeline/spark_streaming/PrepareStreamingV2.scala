:paste
// Streaming v2 data prep: split raw GHCN-Daily 2025 (all stations, unfiltered = what a live feed would deliver) into one
// CSV per day for 1 Jan - 1 Mar 2025 under HDFS /flu_project/streaming_v2/source/day=YYYYMMDD/part-*.csv.
// The drip script (run_pipeline.ps1 streaming) then moves one day at a time into /flu_project/streaming_v2/incoming.
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val base = "hdfs://namenode:9000/flu_project"
val schema = StructType(Seq("id", "date", "element", "value", "mflag", "qflag", "sflag", "obstime").map(StructField(_, StringType)))
val raw = spark.read.option("header", "true").schema(schema).csv(s"$base/raw/weather/2025.csv")
val days = raw.filter(col("date").between("20250101", "20250301"))
// partition on a copy of the date: partitionBy drops the partition column from the file contents, and the stream needs it
days.withColumn("day", col("date")).repartition(col("day")).write.mode("overwrite").partitionBy("day").option("header", "true").csv(s"$base/streaming_v2/source")
val perDay = spark.read.option("header", "true").csv(s"$base/streaming_v2/source").groupBy("day").count().orderBy("day").collect()
println(s"STREAMPREP | days=${perDay.length} rows=${perDay.map(_.getLong(1)).sum} min/day=${perDay.map(_.getLong(1)).min} max/day=${perDay.map(_.getLong(1)).max}")
System.exit(0)
