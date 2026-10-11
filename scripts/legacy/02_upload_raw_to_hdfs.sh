#!/usr/bin/env bash
# Uploads the raw CDC FluView CSVs into HDFS under /flu_project/raw
set -e

echo "Creating HDFS directory structure..."
docker exec -it namenode hdfs dfs -mkdir -p /flu_project/raw
docker exec -it namenode hdfs dfs -mkdir -p /flu_project/processed

echo "Copying local datasets/ into the namenode container..."
docker cp datasets/ILINet.csv namenode:/tmp/ILINet.csv
docker cp datasets/ICL_NREVSS_Clinical_Labs.csv namenode:/tmp/ICL_NREVSS_Clinical_Labs.csv
docker cp datasets/ICL_NREVSS_Public_Health_Labs.csv namenode:/tmp/ICL_NREVSS_Public_Health_Labs.csv
docker cp datasets/ICL_NREVSS_Combined_prior_to_2015_16.csv namenode:/tmp/ICL_NREVSS_Combined_prior_to_2015_16.csv

echo "Putting files into HDFS..."
docker exec -it namenode hdfs dfs -put -f /tmp/ILINet.csv /flu_project/raw/
docker exec -it namenode hdfs dfs -put -f /tmp/ICL_NREVSS_Clinical_Labs.csv /flu_project/raw/
docker exec -it namenode hdfs dfs -put -f /tmp/ICL_NREVSS_Public_Health_Labs.csv /flu_project/raw/
docker exec -it namenode hdfs dfs -put -f /tmp/ICL_NREVSS_Combined_prior_to_2015_16.csv /flu_project/raw/

echo ""
echo "Raw files now in HDFS:"
docker exec -it namenode hdfs dfs -ls -h /flu_project/raw
