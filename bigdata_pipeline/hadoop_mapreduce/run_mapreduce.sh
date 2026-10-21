#!/bin/bash
# Run both ClimaFlu MapReduce jobs (climaflu_mapreduce.py) on YARN with Hadoop Streaming. Runs INSIDE flu-namenode
# (the MapReduce client); the Python mapper/reducer run in the NodeManager's task containers:
#   docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh            (both jobs)
#   docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh mr1|mr2
# Container sizing: the NodeManager offers 4096 MB; AM 1024 MB + 4 maps x 768 MB fit side by side.
set -euo pipefail
cd /opt/mapreduce
WHICH="${1:-all}"
IN='/flu_project/raw/weather/*.csv'          # the 26 yearly CSVs (excludes ghcnd-stations.txt in the same folder)
STATIONS='hdfs://namenode:9000/flu_project/raw/weather/ghcnd-stations.txt'
OUT=/flu_project/mapreduce
STREAMING=$(ls /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-*.jar)
OPTS="-D mapreduce.map.memory.mb=768 -D mapreduce.map.java.opts=-Xmx400m -D mapreduce.reduce.memory.mb=1024"
PY="python3 climaflu_mapreduce.py"
mkdir -p output
hdfs dfs -mkdir -p "$OUT"

sha_of() {   # sorted "key|count|sum" lines joined by \n, no trailing newline (same as the Spark / pandas jobs)
  hdfs dfs -cat "$1/part-*" | awk -F'\t' '{print $1"|"$2"|"$3}' | LC_ALL=C sort > "output/$2.txt"
  printf '%s' "$(cat "output/$2.txt")" | sha256sum | cut -d' ' -f1
}

if [[ "$WHICH" == all || "$WHICH" == mr1 ]]; then
  echo "== MR1 start $(date -u +%FT%TZ)"
  hdfs dfs -rm -r -f -skipTrash "$OUT/element_year" >/dev/null
  t0=$(date +%s)
  hadoop jar "$STREAMING" $OPTS -D mapreduce.job.name="ClimaFlu MR1: element x year aggregate (Python, combiner)" \
    -files climaflu_mapreduce.py \
    -mapper "$PY map_element_year" -combiner "$PY reduce" -reducer "$PY reduce" -numReduceTasks 2 \
    -input "$IN" -output "$OUT/element_year"
  echo "MR1_SECONDS $(( $(date +%s) - t0 ))"
  echo "MR1_SHA256 $(sha_of "$OUT/element_year" mr1_element_year)"
  echo "MR1_GROUPS $(wc -l < output/mr1_element_year.txt)"
fi

if [[ "$WHICH" == all || "$WHICH" == mr2 ]]; then
  echo "== MR2 start $(date -u +%FT%TZ)"
  hdfs dfs -rm -r -f -skipTrash "$OUT/state_year_element" >/dev/null
  t0=$(date +%s)
  hadoop jar "$STREAMING" $OPTS -D mapreduce.job.name="ClimaFlu MR2: state x year x element (Python, map-side join)" \
    -files "climaflu_mapreduce.py,$STATIONS#stations" \
    -mapper "$PY map_state_year" -combiner "$PY reduce" -reducer "$PY reduce" -numReduceTasks 2 \
    -input "$IN" -output "$OUT/state_year_element"
  echo "MR2_SECONDS $(( $(date +%s) - t0 ))"
  echo "MR2_SHA256 $(sha_of "$OUT/state_year_element" mr2_state_year_element)"
  echo "MR2_GROUPS $(wc -l < output/mr2_state_year_element.txt)"
fi
echo "== done $(date -u +%FT%TZ)"
