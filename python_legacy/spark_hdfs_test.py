from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFS_Test") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

df = spark.read.option("header", "true").csv(
    "hdfs://namenode:9000/flu_project/raw/ILINet.csv"
)

print("=== SPARK HDFS TEST ===")
print("Rows:", df.count())
print("Columns:", len(df.columns))
print("Columns:", df.columns)

df.show(5, truncate=False)

spark.stop()
