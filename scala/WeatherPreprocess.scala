:paste
// =========================================================
// WeatherPreprocess.scala — Big Data Analytics (23AID302), Group 04
// Parse the yearly GHCN CSVs (DataFrame API), keep US TMAX/TMIN/PRCP without QC flags, scale values, then build
// the state-week table. US station metadata comes from processed/us_stations, written by StationsPreprocess.scala
// (run that first). Run: .\scripts\run_scala.ps1 WeatherPreprocess -DriverMemory 3g -Conf spark.local.dir=/opt/spark-local
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
// =========================================================
// Part 01, Step 3a — aggregate cleaned weather to
// (state, MMWR year, MMWR week), matching CDC's actual
// epi-week convention (Sunday-start, week 1 contains Jan 4).
// =========================================================

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.functions.{udf, avg, col => c}

def mmwrWeek1Start(year: Int): LocalDate = {
  val jan4 = LocalDate.of(year, 1, 4)
  val dow = jan4.getDayOfWeek.getValue // Monday=1 ... Sunday=7
  jan4.minusDays(dow % 7)              // back up to that week's Sunday
}

def computeMmwr(dateStr: String): (Int, Int) = {
  val d = LocalDate.parse(dateStr)
  val year = d.getYear
  val wk1ThisYear = mmwrWeek1Start(year)
  val (mmwrYear, wk1Start) =
    if (d.isBefore(wk1ThisYear)) {
      val y = year - 1
      (y, mmwrWeek1Start(y))
    } else {
      val wk1NextYear = mmwrWeek1Start(year + 1)
      if (!d.isBefore(wk1NextYear)) (year + 1, wk1NextYear)
      else (year, wk1ThisYear)
    }
  val weekNum = (ChronoUnit.DAYS.between(wk1Start, d) / 7).toInt + 1
  (mmwrYear, weekNum)
}

val mmwrYearUdf = udf((dateStr: String) => computeMmwr(dateStr)._1)
val mmwrWeekUdf = udf((dateStr: String) => computeMmwr(dateStr)._2)

val weatherCleaned = spark.read.parquet(s"$processedDir/weather_cleaned")
val usStations     = spark.read.parquet(s"$processedDir/us_stations")

val weatherWithState = weatherCleaned
  .join(usStations.select(c("stationId"), c("state")), Seq("stationId"))
  .filter(c("state") =!= "UM")   // drop the 1-station non-US territory code

val weatherWithWeek = weatherWithState
  .withColumn("date_str", c("obs_date").cast("string"))
  .withColumn("mmwr_year", mmwrYearUdf(c("date_str")))
  .withColumn("mmwr_week", mmwrWeekUdf(c("date_str")))

val stateWeekAgg = weatherWithWeek
  .groupBy("state", "mmwr_year", "mmwr_week")
  .pivot("element", Seq("TMAX", "TMIN", "PRCP"))
  .agg(avg("value_scaled"))
  .withColumn("DIURNAL_RANGE", c("TMAX") - c("TMIN"))

val aggCount = stateWeekAgg.count()
println(s"State-week aggregated rows: $aggCount")
stateWeekAgg.orderBy("state", "mmwr_year", "mmwr_week").show(15, truncate = false)

stateWeekAgg.write.mode("overwrite").parquet(s"$processedDir/weather_state_week")
println(s"Written to $processedDir/weather_state_week")
