:paste
// =========================================================
// WeatherPreprocess.scala — Big Data Analytics (23AID302), Group 04
// Parse the yearly GHCN CSVs (DataFrame API), keep US TMAX/TMIN/PRCP without QC flags, scale values -> processed/weather_cleaned
// (303.6 M rows). The state-week table is built from it by BuildModelingTable.scala. Run: .\run_pipeline.ps1 job WeatherPreprocess -DriverMemory 3g -Conf spark.local.dir=/opt/spark-local
// =========================================================

val hdfsBase     = "hdfs://namenode:9000"
val rawDir       = s"$hdfsBase/flu_project/raw/weather"
val processedDir = s"$hdfsBase/flu_project/processed"
// ---------------------------------------------------------
// Step 2: parse yearly weather CSVs, filter to TMAX/TMIN/PRCP,
// drop QC-flagged records, scale values, keep US stations only
// ---------------------------------------------------------
println("=== Processing yearly weather CSVs ===")

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col => c, to_date}
import org.apache.spark.storage.StorageLevel

val weatherSchema = StructType(Seq(
  StructField("id",      StringType),
  StructField("date",    StringType),
  StructField("element", StringType),
  StructField("value",   IntegerType),
  StructField("mflag",   StringType),
  StructField("qflag",   StringType),
  StructField("sflag",   StringType),
  StructField("obstime", StringType)
))

val rawWeather = spark.read.schema(weatherSchema).csv(s"$rawDir/*.csv")

val cleanedWeather = rawWeather
  .filter(c("id").startsWith("US"))
  .filter(c("element").isin("TMAX", "TMIN", "PRCP"))
  .filter(c("qflag").isNull || c("qflag") === "")
  .withColumn("obs_date", to_date(c("date"), "yyyyMMdd"))
  .withColumn("value_scaled", c("value") / 10.0)
  .select(
    c("id").as("stationId"),
    c("obs_date"),
    c("element"),
    c("value_scaled")
  )
  .persist(StorageLevel.DISK_ONLY)   // materialize once, spill to disk instead of holding in the 1g heap

val cleanedCount = cleanedWeather.count()   // this triggers the actual read+filter, once
println(s"Records after US-filter + element-filter + QC-flag drop: $cleanedCount")

cleanedWeather.show(10, truncate = false)

cleanedWeather.write.mode("overwrite").partitionBy("element").parquet(s"$processedDir/weather_cleaned")
println(s"Written to $processedDir/weather_cleaned")

cleanedWeather.unpersist()
System.exit(0)
