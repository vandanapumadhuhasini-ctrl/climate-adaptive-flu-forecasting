:paste
// Runs a real aggregation over the 303M-row cleaned weather table so the Spark UI (port 4040) has jobs,
// stages and a SQL plan to show, then keeps the driver alive for 120 s for screenshots.
import org.apache.spark.sql.functions._
spark.sparkContext.setJobDescription("ClimaFlu demo: observations per element and year (weather_cleaned, 303M rows)")
val w = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/weather_cleaned")
w.printSchema()
val dateCol = w.columns.find(c => c.toLowerCase.contains("date")).getOrElse("date")
val byYear = w.groupBy(col("element"), substring(col(dateCol).cast("string"), 1, 4).as("year")).count()
  .orderBy("element", "year")
byYear.explain("formatted")
byYear.show(12, truncate = false)
println(s"SPARKUI | rows aggregated = ${byYear.agg(sum("count")).first().getLong(0)}")
println("SPARKUI | UI ready on http://localhost:4040 for 120 s")
Thread.sleep(120000)
System.exit(0)
