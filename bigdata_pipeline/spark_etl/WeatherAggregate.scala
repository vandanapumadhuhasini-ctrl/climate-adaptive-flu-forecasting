:paste
val hdfsBase     = "hdfs://namenode:9000"
val processedDir = s"$hdfsBase/flu_project/processed"

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.functions.{udf, avg, col => c}

def mmwrWeek1Start(year: Int): LocalDate = {
  val jan4 = LocalDate.of(year, 1, 4)
  val dow = jan4.getDayOfWeek.getValue
  jan4.minusDays(dow % 7)
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
  .filter(c("state") =!= "UM")

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