:paste
// =========================================================
// Preprocess.scala — Big Data Analytics (23AID302), Group 04
// Reference data preprocessing (DataFrame API only):
//   1. GHCN station metadata (fixed-width ghcnd-stations.txt) -> processed/us_stations
//   2. CDC FluView: ILINet + 3 NREVSS lab tables (CSV with a title line)  -> processed/<table>
// Optional: --conf spark.flu.outputSuffix=_dfcheck writes processed/<table>_dfcheck instead (verification runs).
// Run before WeatherPreprocess (which joins observations to us_stations).
// =========================================================

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

val hdfsBase      = "hdfs://namenode:9000"
val rawDir        = s"$hdfsBase/flu_project/raw"
val processedDir  = s"$hdfsBase/flu_project/processed"
val suffix        = spark.conf.get("spark.flu.outputSuffix", "")    // e.g. _dfcheck for verification runs

// FluView CSVs start with a free-text title line before the real header. DataFrame API only: drop the title line as
// text, write the remaining lines to a temporary HDFS folder, and read that back with the normal CSV reader
// (same parser and schema inference as reading the cleaned lines directly).
def readFluViewCsv(path: String): DataFrame = {
  val rawLines = spark.read.text(path)
  val title    = rawLines.first().getString(0)
  val tmp      = s"$hdfsBase/flu_project/tmp/fluview$suffix/" + path.split("/").last.stripSuffix(".csv")
  rawLines.filter(col("value") =!= title).coalesce(1).write.mode("overwrite").text(tmp)
  spark.read.option("header", "true").option("inferSchema", "true").csv(tmp)
}

def cleanDataset(df: DataFrame): DataFrame = {
  def safeCol(name: String) = col(s"`$name`")
  val trimmed = df.columns.foldLeft(df) { (acc, c) =>
    val dtype = acc.schema(c).dataType.typeName
    if (dtype == "string") acc.withColumn(c, trim(safeCol(c))) else acc
  }
  val nullified = trimmed.columns.foldLeft(trimmed) { (acc, c) =>
    val dtype = acc.schema(c).dataType.typeName
    if (dtype == "string") acc.withColumn(c, when(safeCol(c) === "X" || safeCol(c) === "", null).otherwise(safeCol(c)))
    else acc
  }
  nullified.dropDuplicates()
}

// ---------------------------------------------------------------- 1. stations (fixed-width)
// Columns (1-based): ID 1-11, LATITUDE 13-20, LONGITUDE 22-30, STATE 39-40, NAME 42-71.
println("=== Processing ghcnd-stations.txt ===")
val stationLines = spark.read.text(s"$rawDir/weather/ghcnd-stations.txt")
val stations = stationLines
  .filter(length(col("value")) >= 71)                                   // a line must reach the end of NAME
  .select(
    trim(substring(col("value"), 1, 11)).as("stationId"),
    trim(substring(col("value"), 39, 2)).as("state"),
    trim(substring(col("value"), 13, 8)).cast("double").as("lat"),
    trim(substring(col("value"), 22, 9)).cast("double").as("lon"),
    trim(substring(col("value"), 42, 30)).as("name"))
  .na.drop(Seq("lat", "lon"))                                           // unparseable coordinates are dropped
val usStations = stations.filter(col("stationId").startsWith("US") && length(col("state")) > 0)
println(s"Total station records parsed: ${stations.count()}; US stations retained: ${usStations.count()}")
usStations.write.mode("overwrite").parquet(s"$processedDir/us_stations$suffix")

// ---------------------------------------------------------------- 2. FluView tables
println("=== Processing ILINet.csv ===")
val iliRaw = readFluViewCsv(s"$rawDir/ILINet.csv")
val ili = cleanDataset(iliRaw)
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

println(s"ILINet rows after cleaning: ${ili.count()}")
ili.show(5, truncate = false)
ili.write.mode("overwrite").parquet(s"$processedDir/ILINet$suffix")

println("=== Processing ICL_NREVSS_Clinical_Labs.csv ===")
val clinicalRaw = readFluViewCsv(s"$rawDir/ICL_NREVSS_Clinical_Labs.csv")
val clinical = cleanDataset(clinicalRaw)
  .withColumnRenamed("REGION TYPE", "REGION_TYPE")
  .withColumn("YEAR", col("YEAR").cast("int"))
  .withColumn("WEEK", col("WEEK").cast("int"))
  .withColumn("TOTAL_SPECIMENS", col("`TOTAL SPECIMENS`").cast("double"))
  .withColumn("PERCENT_POSITIVE", col("`PERCENT POSITIVE`").cast("double"))
  .filter(col("REGION").isNotNull)
clinical.write.mode("overwrite").parquet(s"$processedDir/ICL_NREVSS_Clinical_Labs$suffix")
println(s"Clinical Labs rows after cleaning: ${clinical.count()}")

println("=== Processing ICL_NREVSS_Public_Health_Labs.csv ===")
val publicRaw = readFluViewCsv(s"$rawDir/ICL_NREVSS_Public_Health_Labs.csv")
val publicHealth = cleanDataset(publicRaw)
  .withColumnRenamed("REGION TYPE", "REGION_TYPE")
  .withColumn("TOTAL_SPECIMENS", col("`TOTAL SPECIMENS`").cast("double"))
  .filter(col("REGION").isNotNull)
publicHealth.write.mode("overwrite").parquet(s"$processedDir/ICL_NREVSS_Public_Health_Labs$suffix")
println(s"Public Health Labs rows after cleaning: ${publicHealth.count()}")

println("=== Processing ICL_NREVSS_Combined_prior_to_2015_16.csv ===")
val combinedRaw = readFluViewCsv(s"$rawDir/ICL_NREVSS_Combined_prior_to_2015_16.csv")
val combined = cleanDataset(combinedRaw)
  .withColumnRenamed("REGION TYPE", "REGION_TYPE")
  .withColumn("YEAR", col("YEAR").cast("int"))
  .withColumn("WEEK", col("WEEK").cast("int"))
  .withColumn("TOTAL_SPECIMENS", col("`TOTAL SPECIMENS`").cast("double"))
  .withColumn("PERCENT_POSITIVE", col("`PERCENT POSITIVE`").cast("double"))
  .filter(col("REGION").isNotNull)
combined.write.mode("overwrite").parquet(s"$processedDir/ICL_NREVSS_Combined_prior_to_2015_16$suffix")
println(s"Combined (pre-2015-16) rows after cleaning: ${combined.count()}")

println("=== ALL DATASETS PREPROCESSED AND WRITTEN TO HDFS ===")
// remove the temporary text copies of the CDC files (all reads above are finished)
org.apache.hadoop.fs.FileSystem.get(new java.net.URI(s"$hdfsBase/flu_project/tmp/fluview$suffix"), sc.hadoopConfiguration)
  .delete(new org.apache.hadoop.fs.Path(s"$hdfsBase/flu_project/tmp/fluview$suffix"), true)
System.exit(0)
