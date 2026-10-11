#!/usr/bin/env bash
# Verifies the processed datasets landed in HDFS and can be read back.
set -e

echo "Processed datasets in HDFS:"
docker exec -it namenode hdfs dfs -ls -h /flu_project/processed

echo ""
echo "Reading back ILINet parquet output to confirm row counts..."
cat > /tmp/verify_snippet.scala <<'EOF'
val df = spark.read.parquet("hdfs://namenode:9000/flu_project/processed/ILINet")
println("Rows: " + df.count())
df.printSchema()
df.show(5, truncate = false)
EOF
docker cp /tmp/verify_snippet.scala spark:/opt/scala/verify_snippet.scala
docker exec -it spark /opt/spark/bin/spark-shell \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  -i /opt/scala/verify_snippet.scala
