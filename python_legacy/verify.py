from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Verify_ILINet") \
    .getOrCreate()

path = "hdfs://namenode:9000/flu_project/processed/ILINet"

df = spark.read.parquet(path)

print("NUMBER OF ROWS:")
print(df.count())

print("COLUMNS:")
print(df.columns)

print("SAMPLE DATA:")
df.show(5, truncate=False)

spark.stop()
