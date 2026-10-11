from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, regexp_replace

spark = SparkSession.builder \
    .appName("FluView_Preprocessing") \
    .getOrCreate()

hdfs = "hdfs://namenode:9000"

ili_path = hdfs + "/flu_project/raw/ILINet.csv"
clinical_path = hdfs + "/flu_project/raw/ICL_NREVSS_Clinical_Labs.csv"

ili = spark.read.option("header", True).option("inferSchema", True).csv(ili_path)
clinical = spark.read.option("header", True).option("inferSchema", True).csv(clinical_path)

print("=== ILINET SCHEMA ===")
ili.printSchema()

print("=== CLINICAL SCHEMA ===")
clinical.printSchema()

print("ILI rows:", ili.count())
print("Clinical rows:", clinical.count())

print("=== ILI COLUMNS ===")
print(ili.columns)

print("=== CLINICAL COLUMNS ===")
print(clinical.columns)

spark.stop()
