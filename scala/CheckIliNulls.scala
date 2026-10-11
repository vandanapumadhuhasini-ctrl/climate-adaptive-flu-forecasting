:paste
val ili = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/ILINet")
val total = ili.count()
val weightedNulls = ili.filter(ili("WEIGHTED_ILI").isNull).count()
val unweightedNulls = ili.filter(ili("UNWEIGHTED_ILI").isNull).count()
println(s"Total: $total | WEIGHTED_ILI nulls: $weightedNulls (${100.0*weightedNulls/total}%) | UNWEIGHTED_ILI nulls: $unweightedNulls (${100.0*unweightedNulls/total}%)")