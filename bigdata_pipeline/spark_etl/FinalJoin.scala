:paste
val hdfsBase     = "hdfs://namenode:9000"
val processedDir = s"$hdfsBase/flu_project/processed"

import org.apache.spark.sql.functions.{col => c, sum => sparkSum}

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

val weather = spark.read.parquet(s"$processedDir/weather_state_week")

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

modelingTable.write.mode("overwrite").parquet(s"$processedDir/modeling_table")
println(s"Written to $processedDir/modeling_table")