:paste
val ili = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/ILINet")
ili.filter(ili("REGION").isin("New York", "New York City") && ili("YEAR") === 2010 && ili("WEEK") === 40)
   .select("REGION", "YEAR", "WEEK", "ILITOTAL", "TOTAL_PATIENTS")
   .show(truncate = false)

val modelingTable = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/modeling_table")
modelingTable.filter(modelingTable("state_abbrev") === "NY" && modelingTable("YEAR") === 2010 && modelingTable("WEEK") === 40)
   .select("state_abbrev", "YEAR", "WEEK", "UNWEIGHTED_ILI")
   .show(truncate = false)