:paste
// =========================================================
// BuildModelingTable.scala — Big Data Analytics (23AID302), Group 04
//   1. weather_cleaned (303.6 M daily rows) + us_stations -> MMWR state-week climate means -> processed/weather_state_week
//   2. ILINet state rows (New York + New York City merged by summing counts) JOIN state-week climate -> processed/modeling_table
// Optional: --conf spark.flu.outputSuffix=_dfcheck writes <table>_dfcheck instead (verification runs).
// =========================================================
val hdfsBase     = "hdfs://namenode:9000"
val processedDir = s"$hdfsBase/flu_project/processed"
val suffix       = spark.conf.get("spark.flu.outputSuffix", "")

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.functions.{udf, avg, col => c, sum => sparkSum}

// ---------------------------------------------------------------- 1. state-week climate
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
  .cache()   // 68,833 rows: computed once from 303.6 M rows, then reused by count, show and write

val aggCount = stateWeekAgg.count()
println(s"State-week aggregated rows: $aggCount")
stateWeekAgg.orderBy("state", "mmwr_year", "mmwr_week").show(15, truncate = false)

stateWeekAgg.write.mode("overwrite").parquet(s"$processedDir/weather_state_week$suffix")
println(s"Written to $processedDir/weather_state_week$suffix")

// ---------------------------------------------------------------- 2. join with ILINet
val stateAbbrev = Map(
  "Alabama" -> "AL", "Alaska" -> "AK", "Arizona" -> "AZ", "Arkansas" -> "AR",
  "California" -> "CA", "Colorado" -> "CO", "Connecticut" -> "CT", "Delaware" -> "DE",
  "District of Columbia" -> "DC", "Florida" -> "FL", "Georgia" -> "GA", "Hawaii" -> "HI",
  "Idaho" -> "ID", "Illinois" -> "IL", "Indiana" -> "IN", "Iowa" -> "IA", "Kansas" -> "KS",
  "Kentucky" -> "KY", "Louisiana" -> "LA", "Maine" -> "ME", "Maryland" -> "MD",
  "Massachusetts" -> "MA", "Michigan" -> "MI", "Minnesota" -> "MN", "Mississippi" -> "MS",
  "Missouri" -> "MO", "Montana" -> "MT", "Nebraska" -> "NE", "Nevada" -> "NV",
  "New Hampshire" -> "NH", "New Jersey" -> "NJ", "New Mexico" -> "NM", "New York" -> "NY",
  "New York City" -> "NY",
  "North Carolina" -> "NC", "North Dakota" -> "ND", "Ohio" -> "OH", "Oklahoma" -> "OK",
  "Oregon" -> "OR", "Pennsylvania" -> "PA", "Rhode Island" -> "RI", "South Carolina" -> "SC",
  "South Dakota" -> "SD", "Tennessee" -> "TN", "Texas" -> "TX", "Utah" -> "UT",
  "Vermont" -> "VT", "Virginia" -> "VA", "Washington" -> "WA", "West Virginia" -> "WV",
  "Wisconsin" -> "WI", "Wyoming" -> "WY"
)
val stateAbbrevUdf = spark.udf.register("stateAbbrevUdf", (name: String) => stateAbbrev.getOrElse(name, null))

val iliRaw = spark.read.parquet(s"$processedDir/ILINet")
  .withColumn("state_abbrev", stateAbbrevUdf(c("REGION")))
  .filter(c("state_abbrev").isNotNull)
  .filter(c("ILITOTAL").isNotNull && c("TOTAL_PATIENTS").isNotNull && c("TOTAL_PATIENTS") > 0)

// Merge any regions sharing a state code (e.g. "New York" + "New York City")
// by summing raw counts, then recomputing the rate — not averaging percentages.
val iliMerged = iliRaw
  .groupBy("state_abbrev", "YEAR", "WEEK")
  .agg(
    sparkSum("ILITOTAL").as("ILITOTAL_sum"),
    sparkSum("TOTAL_PATIENTS").as("TOTAL_PATIENTS_sum")
  )
  .withColumn("UNWEIGHTED_ILI", c("ILITOTAL_sum") / c("TOTAL_PATIENTS_sum") * 100.0)

val weather = spark.read.parquet(s"$processedDir/weather_state_week$suffix")

val modelingTable = iliMerged.join(
  weather,
  iliMerged("state_abbrev") === weather("state") &&
  iliMerged("YEAR") === weather("mmwr_year") &&
  iliMerged("WEEK") === weather("mmwr_week"),
  "inner"
)

val mergedCount = iliMerged.count()
val joinCount    = modelingTable.count()
println(s"Merged ILI (state, year, week) rows: $mergedCount")
println(s"Rows after join to weather:          $joinCount")

// Verify: should now be zero
val remainingDupes = modelingTable.groupBy("state_abbrev", "YEAR", "WEEK").count().filter(c("count") > 1).count()
println(s"Remaining duplicate (state, year, week) after merge: $remainingDupes")

modelingTable.select(
  c("state_abbrev"), c("YEAR"), c("WEEK"), c("UNWEIGHTED_ILI"),
  c("TMAX"), c("TMIN"), c("PRCP"), c("DIURNAL_RANGE")
).orderBy(c("state_abbrev"), c("YEAR"), c("WEEK")).show(15, truncate = false)

modelingTable.write.mode("overwrite").parquet(s"$processedDir/modeling_table$suffix")
println(s"Written to $processedDir/modeling_table$suffix")
System.exit(0)
