val df = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/ILINet")
println("Rows: " + df.count())
df.printSchema()
df.show(5, truncate = false)
