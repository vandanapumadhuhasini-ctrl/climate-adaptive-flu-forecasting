:paste
val modelingTable = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/modeling_table")

val dupes = modelingTable
  .groupBy("state_abbrev", "YEAR", "WEEK")
  .count()
  .filter(org.apache.spark.sql.functions.col("count") > 1)

val dupeCount = dupes.count()
println(s"Duplicate (state, year, week) combinations: $dupeCount")
dupes.orderBy(org.apache.spark.sql.functions.col("count").desc).show(20)

if (dupeCount > 0) {
  modelingTable.filter(
    modelingTable("state_abbrev") === "AK" &&
    modelingTable("YEAR") === 2010 &&
    modelingTable("WEEK") === 40
  ).show(truncate = false)
}