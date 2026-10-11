#!/bin/bash
# Build the ClimaFlu MapReduce jar and run both jobs on YARN. Runs INSIDE flu-namenode (the MapReduce client):
#   docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh            (both jobs)
#   docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh mr1|mr2
# Container sizing: the NodeManager offers 4096 MB; AM 1024 MB + 4 maps x 768 MB fit side by side.
set -euo pipefail
cd /opt/mapreduce
WHICH="${1:-all}"
IN='/flu_project/raw/weather/*.csv'          # the 26 yearly CSVs (excludes ghcnd-stations.txt in the same folder)
STATIONS='hdfs://namenode:9000/flu_project/raw/weather/ghcnd-stations.txt'
OUT=/flu_project/mapreduce
OPTS="-Dmapreduce.map.memory.mb=768 -Dmapreduce.map.java.opts=-Xmx600m -Dmapreduce.reduce.memory.mb=1024"

echo "== build $(date -u +%FT%TZ)"
rm -rf build && mkdir -p build/classes output
javac -source 8 -target 8 -nowarn -cp "$(hadoop classpath)" -d build/classes src/climaflu/mr/*.java
jar cf build/climaflu-mr.jar -C build/classes .
hdfs dfs -mkdir -p "$OUT"

sha_of() {   # sorted "key|count|sum" lines joined by \n, no trailing newline (same as the Spark / pandas jobs)
  hdfs dfs -cat "$1/part-r-*" | awk -F'\t' '{print $1"|"$2"|"$3}' | LC_ALL=C sort > "output/$2.txt"
  printf '%s' "$(cat "output/$2.txt")" | sha256sum | cut -d' ' -f1
}

if [[ "$WHICH" == all || "$WHICH" == mr1 ]]; then
  echo "== MR1 start $(date -u +%FT%TZ)"
  t0=$(date +%s)
  hadoop jar build/climaflu-mr.jar climaflu.mr.ElementYearAggregate $OPTS "$IN" "$OUT/element_year" 2
  echo "MR1_SECONDS $(( $(date +%s) - t0 ))"
  echo "MR1_SHA256 $(sha_of "$OUT/element_year" mr1_element_year)"
  echo "MR1_GROUPS $(wc -l < output/mr1_element_year.txt)"
fi

if [[ "$WHICH" == all || "$WHICH" == mr2 ]]; then
  echo "== MR2 start $(date -u +%FT%TZ)"
  t0=$(date +%s)
  hadoop jar build/climaflu-mr.jar climaflu.mr.StateYearCoverage $OPTS "$IN" "$STATIONS" "$OUT/state_year_element" 2
  echo "MR2_SECONDS $(( $(date +%s) - t0 ))"
  echo "MR2_SHA256 $(sha_of "$OUT/state_year_element" mr2_state_year_element)"
  echo "MR2_GROUPS $(wc -l < output/mr2_state_year_element.txt)"
fi
echo "== done $(date -u +%FT%TZ)"
