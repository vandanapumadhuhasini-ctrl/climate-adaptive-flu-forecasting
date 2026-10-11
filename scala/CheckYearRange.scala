:paste
val modelingTable = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/modeling_table")

modelingTable.groupBy("YEAR").count().orderBy("YEAR").show(30)

val minYear = modelingTable.agg(org.apache.spark.sql.functions.min("YEAR")).first().getInt(0)
val maxYear = modelingTable.agg(org.apache.spark.sql.functions.max("YEAR")).first().getInt(0)
println(s"Year range in modeling_table: $minYear to $maxYear")