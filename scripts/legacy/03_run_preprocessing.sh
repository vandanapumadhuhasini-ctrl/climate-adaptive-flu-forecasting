#!/usr/bin/env bash
# Runs scala/Preprocess.scala inside the spark container via spark-shell -i.
# The docker-compose "spark" service already mounts ./scala -> /opt/scala
set -e

echo "Running Scala preprocessing job on the Spark cluster..."
docker exec -it spark /opt/spark/bin/spark-shell \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  -i /opt/scala/Preprocess.scala
