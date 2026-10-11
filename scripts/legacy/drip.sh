for i in 1 2 3 4 5; do
  hdfs dfs -cp /flu_project/streaming_source/week_$i/part-* /flu_project/streaming_incoming/week_$i.csv
  echo "Dropped week_$i at $(date)"
  sleep 15
done