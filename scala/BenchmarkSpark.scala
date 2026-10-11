:paste
val hdfsBase = "hdfs://namenode:9000"
val rawDir = s"$hdfsBase/flu_project/raw/weather"

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col => c}

val weatherSchema = StructType(Seq(
  StructField("id", StringType), StructField("date", StringType), StructField("element", StringType),
  StructField("value", IntegerType), StructField("mflag", StringType), StructField("qflag", StringType),
  StructField("sflag", StringType), StructField("obstime", StringType)
))

val scales = Seq(1, 5, 10, 15, 20, 26)
val results = scala.collection.mutable.ArrayBuffer[(Int, Long, Long)]()

scales.foreach { n =>
  val years = (2000 until 2000 + n).map(y => s"$rawDir/$y.csv")
  val t0 = System.currentTimeMillis()
  val df = spark.read.schema(weatherSchema).csv(years: _*)
  val filtered = df.filter(c("id").startsWith("US"))
    .filter(c("element").isin("TMAX", "TMIN", "PRCP"))
    .filter(c("qflag").isNull || c("qflag") === "")
  val cnt = filtered.count()
  val t1 = System.currentTimeMillis()
  val elapsed = t1 - t0
  println(f"Spark | years=$n%2d | rows=$cnt%,d | time=${elapsed}ms")
  results += ((n, cnt, elapsed))
}

println("\n=== SPARK BENCHMARK SUMMARY ===")
println(f"${"Years"}%6s ${"Rows"}%15s ${"Time(ms)"}%12s")
results.foreach { case (n, cnt, t) => println(f"$n%6d ${cnt}%15d ${t}%12d") }