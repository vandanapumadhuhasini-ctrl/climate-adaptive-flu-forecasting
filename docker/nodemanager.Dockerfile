# YARN NodeManager + Python 3, needed by the Python MapReduce jobs (Hadoop Streaming runs the mapper/reducer
# scripts inside the NodeManager's task containers). Debian 9 (stretch) packages now live on archive.debian.org.
FROM bde2020/hadoop-nodemanager:2.0.0-hadoop3.2.1-java8
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && apt-get install -y --no-install-recommends python3-minimal \
 && rm -rf /var/lib/apt/lists/* \
 && python3 --version
