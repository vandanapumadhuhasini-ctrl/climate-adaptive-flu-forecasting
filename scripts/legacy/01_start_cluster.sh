#!/usr/bin/env bash
# Starts the Hadoop (NameNode + 3 DataNodes + YARN) and Spark cluster.
set -e

echo "Starting Hadoop + Spark cluster..."
docker compose up -d

echo ""
echo "Waiting 30s for NameNode/DataNodes to finish bootstrapping..."
sleep 30

echo ""
echo "Container status:"
docker compose ps

echo ""
echo "Checking HDFS DataNode report (should show 3 live nodes):"
docker exec -it namenode hdfs dfsadmin -report | grep -A2 "Live datanodes"

echo ""
echo "NameNode Web UI:        http://localhost:9870"
echo "ResourceManager UI:     http://localhost:8088"
