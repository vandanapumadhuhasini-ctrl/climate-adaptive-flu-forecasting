from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("FluView_Preprocessing") \
    .getOrCreate()

input_path = "hdfs://namenode:9000/flu_project/raw/ILINet.csv"
output_path = "hdfs://namenode:9000/flu_project/processed/ILINet"

# Read the file as raw lines
raw = spark.read.text(input_path)

# Remove the title line
data = raw.filter(
    ~raw.value.startswith(
        "PERCENTAGE OF VISITS FOR INFLUENZA-LIKE-ILLNESS"
    )
)

# Read remaining lines as CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(data.rdd.map(lambda row: row.value))

print("COLUMNS:")
print(df.columns)

print("SAMPLE DATA:")
df.show(5, truncate=False)

# Save processed data as Parquet
df.write \
    .mode("overwrite") \
    .parquet(output_path)

spark.stop()
