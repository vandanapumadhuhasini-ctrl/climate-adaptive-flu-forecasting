:paste
// =========================================================
// StationsPreprocess.scala — Big Data Analytics (23AID302), Group 04
// Parses the fixed-width GHCN station metadata file into processed/us_stations using the DataFrame API only.
// (Split out of WeatherPreprocess.scala; previously parsed with an RDD + case class.)
// Fixed-width columns (1-based): ID 1-11, LATITUDE 13-20, LONGITUDE 22-30, STATE 39-40, NAME 42-71.
// Optional: --conf spark.flu.outputSuffix=_dfcheck writes processed/us_stations_dfcheck instead (verification runs).
// =========================================================
import org.apache.spark.sql.functions._

val hdfsBase     = "hdfs://namenode:9000"
val rawDir       = s"$hdfsBase/flu_project/raw/weather"
val processedDir = s"$hdfsBase/flu_project/processed"
val suffix       = spark.conf.get("spark.flu.outputSuffix", "")

val lines = spark.read.text(s"$rawDir/ghcnd-stations.txt")
val parsed = lines
  .filter(length(col("value")) >= 71)                                   // a line must reach the end of NAME
  .select(
    trim(substring(col("value"), 1, 11)).as("stationId"),
    trim(substring(col("value"), 39, 2)).as("state"),
    trim(substring(col("value"), 13, 8)).cast("double").as("lat"),
    trim(substring(col("value"), 22, 9)).cast("double").as("lon"),
    trim(substring(col("value"), 42, 30)).as("name"))
  .na.drop(Seq("lat", "lon"))                                           // unparseable coordinates are dropped
val usStations = parsed.filter(col("stationId").startsWith("US") && length(col("state")) > 0)

println(s"Total station records parsed: ${parsed.count()}")
println(s"US stations retained: ${usStations.count()}")
usStations.groupBy("state").count().orderBy("state").show(60)

usStations.write.mode("overwrite").parquet(s"$processedDir/us_stations$suffix")
println(s"Written to $processedDir/us_stations$suffix")
System.exit(0)
