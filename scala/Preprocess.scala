:paste
// =========================================================
// Preprocess.scala
// Big Data Analytics (23AID302) — Group 04
// FluView / ILINet preprocessing job
// =========================================================

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import spark.implicits._

val hdfsBase      = "hdfs://namenode:9000"
val rawDir        = s"$hdfsBase/flu_project/raw"
val processedDir  = s"$hdfsBase/flu_project/processed"

def readFluViewCsv(path: String): DataFrame = {
  val rawLines  = spark.read.text(path)
  val firstLine = rawLines.first().getString(0)
  val dataLines = rawLines.filter(col("value") =!= firstLine)
  spark.read.option("header", "true").option("inferSchema", "true").csv(dataLines.as[String])
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
ili.write.mode("overwrite").parquet(s"$processedDir/ILINet")

println("=== Processing ICL_NREVSS_Clinical_Labs.csv ===")
val clinicalRaw = readFluViewCsv(s"$rawDir/ICL_NREVSS_Clinical_Labs.csv")
val clinical = cleanDataset(clinicalRaw)
  .withColumnRenamed("REGION TYPE", "REGION_TYPE")
  .withColumn("YEAR", col("YEAR").cast("int"))
  .withColumn("WEEK", col("WEEK").cast("int"))
  .withColumn("TOTAL_SPECIMENS", col("`TOTAL SPECIMENS`").cast("double"))
  .withColumn("PERCENT_POSITIVE", col("`PERCENT POSITIVE`").cast("double"))
  .filter(col("REGION").isNotNull)
clinical.write.mode("overwrite").parquet(s"$processedDir/ICL_NREVSS_Clinical_Labs")
println(s"Clinical Labs rows after cleaning: ${clinical.count()}")

println("=== Processing ICL_NREVSS_Public_Health_Labs.csv ===")
val publicRaw = readFluViewCsv(s"$rawDir/ICL_NREVSS_Public_Health_Labs.csv")
val publicHealth = cleanDataset(publicRaw)
  .withColumnRenamed("REGION TYPE", "REGION_TYPE")
  .withColumn("TOTAL_SPECIMENS", col("`TOTAL SPECIMENS`").cast("double"))
  .filter(col("REGION").isNotNull)
publicHealth.write.mode("overwrite").parquet(s"$processedDir/ICL_NREVSS_Public_Health_Labs")
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
combined.write.mode("overwrite").parquet(s"$processedDir/ICL_NREVSS_Combined_prior_to_2015_16")
println(s"Combined (pre-2015-16) rows after cleaning: ${combined.count()}")

println("=== ALL DATASETS PREPROCESSED AND WRITTEN TO HDFS ===")