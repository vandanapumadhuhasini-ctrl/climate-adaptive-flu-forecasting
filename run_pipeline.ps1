# ClimaFlu-GNN - ONE entry point for the whole project.
#
#   .\run_pipeline.ps1 <stage>              run one stage (stages below, in pipeline order)
#   .\run_pipeline.ps1 all                  run every stage in order (many hours: GNN seeds, benchmark, tuning)
#   .\run_pipeline.ps1 job <Name> [-Conf k=v ...] [-DriverMemory 3g]   run one Spark (Scala) job by name
#
# Stage        What it does                                                     Main output
# up           start the Docker cluster, wait for HDFS                          containers running
# ingest       copy the raw datasets (34.6 GB GHCN + 4 CDC FluView CSVs) to HDFS  /flu_project/raw
# etl          Spark ETL: stations + FluView, weather cleaning, weekly join, graph /flu_project/processed
# mapreduce    2 Python MapReduce jobs (Hadoop Streaming) on YARN + counters     results/mapreduce_jobs.json
# gnn          rigour pass: features, Python + Spark GBT baselines, 120 GNN runs results/rigour, docs/RIGOUR_RESULTS.md
# v3           lag-aware multi-horizon features (Spark) + GBT + GNN, evaluation   results/v3, docs/V3_RESULTS.md
# mllib        Spark MLlib comparison: Linear Regression, Random Forest, GBT      results/mllib, docs/MLLIB_RESULTS.md
# streaming    live GHCN feed: Structured Streaming job + drip feed + analysis    results/streaming_summary.json
# performance  Spark vs pandas benchmark + partition/caching tuning + analysis    docs/BENCHMARK_RESULTS.md, docs/TUNING_RESULTS.md
# checks       smoke test + veracity audit, MapReduce = Spark DataFrame check     results/*.json
# report       cluster verification, dashboard data, evidence docs, screenshots   docs/COURSE_COMPLIANCE_MATRIX.md
# publish      copy every result file into HDFS with a SHA-256 manifest           /flu_project/results
#
# Run from the project folder in PowerShell. Spark jobs run in the 'spark' container (local[*]) against HDFS; their logs
# go to logs\<Job>_<time>.log and their event logs to results\spark-events (Spark History Server: http://localhost:18080).
param(
    [Parameter(Position = 0)][string]$Stage = "help",
    [Parameter(Position = 1)][string]$Name = "",
    [string]$DriverMemory = "3g",       # 3g: a bigger driver can be OOM-killed while the Hadoop containers run (8 GB VM)
    [string[]]$Conf = @()               # extra Spark settings, e.g. -Conf spark.flu.outputSuffix=_dfcheck
)
$ErrorActionPreference = "Continue"
$root = $PSScriptRoot
$utf8 = New-Object System.Text.UTF8Encoding $false
$py   = "docker run --rm -v `"${root}:/proj`" climaflu-pytorch:1.0 python"     # one-off Python run on the project files
New-Item -ItemType Directory -Force (Join-Path $root "logs"), (Join-Path $root "results\spark-events") | Out-Null

function Say([string]$msg) { Write-Host "`n=== $msg" -ForegroundColor Cyan }
function Hdfs([string]$cmd) { docker exec flu-namenode bash -c $cmd }

# Run one Scala job with spark-shell. The file is fed through cmd.exe '<' because a PowerShell pipe adds a BOM,
# which breaks the ':paste' on line 1 of every job.
function Job([string]$job, [string[]]$extra = @(), [string]$mem = $DriverMemory) {
    $src = Get-ChildItem (Join-Path $root "bigdata_pipeline") -Recurse -Filter "$job.scala" | Select-Object -First 1
    if (-not $src) { Write-Error "No Scala job named $job under bigdata_pipeline\"; return }
    $log  = Join-Path $root ("logs\{0}_{1}.log" -f $job, (Get-Date -Format "yyyyMMdd_HHmmss"))
    $opts = (($Conf + $extra) | ForEach-Object { "--conf $_ " }) -join ""
    $cmd  = "docker exec -i spark /opt/spark/bin/spark-shell --name $job --master local[*] --driver-memory $mem " +
            "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 " +
            "--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///opt/results/spark-events " +
            "$opts< `"$($src.FullName)`" > `"$log`" 2>&1"
    Say "Spark job $job  ($($src.Directory.Name))"
    $t0 = Get-Date
    cmd /c $cmd
    Get-Content $log -Encoding UTF8 | Select-String -NotMatch "^\s*$|WARN|^\[Stage" | Select-Object -Last 15 | ForEach-Object { $_.Line }
    Write-Host ("[job] {0} finished in {1:N1} s; log: {2}" -f $job, ((Get-Date) - $t0).TotalSeconds, $log)
    if (Select-String -Path $log -Pattern "error:|Exception" -Quiet) { Write-Warning "[job] $job : errors in the log - check it before trusting outputs" }
}

# ------------------------------------------------------------------------------------------------ stages
function Stage-Up {
    Say "Starting the cluster (docker compose up -d)"
    docker compose -f (Join-Path $root "docker-compose.yml") up -d
    for ($i = 0; $i -lt 60; $i++) { if ((Hdfs "hdfs dfsadmin -safemode get 2>/dev/null") -match "OFF") { break }; Start-Sleep 5 }
    Hdfs "hdfs dfsadmin -safemode get; hdfs dfsadmin -report 2>/dev/null | grep 'Live datanodes'"
}

function Stage-Ingest {
    Say "Raw data -> HDFS /flu_project/raw (files already in HDFS are skipped)"
    Hdfs "hdfs dfs -mkdir -p /flu_project/raw/weather && hdfs dfs -put -d /mnt/datasets/weather_raw/*.csv /mnt/datasets/weather_raw/ghcnd-stations.txt /flu_project/raw/weather/ 2>&1 | grep -v 'File exists'; hdfs dfs -put -f /mnt/datasets/*.csv /flu_project/raw/"
    Hdfs "hdfs dfs -du -s -h /flu_project/raw /flu_project/raw/weather"
}

function Stage-Etl {
    foreach ($j in "Preprocess", "WeatherPreprocess", "BuildModelingTable", "GraphConstruction") {
        if ($j -eq "WeatherPreprocess") { Job $j @("spark.local.dir=/opt/spark-local") }
        elseif ($j -eq "BuildModelingTable") { Job $j @("spark.local.dir=/opt/spark-local") "2g" }   # 2g: 3g was OOM-killed next to Hadoop
        else { Job $j }
    }
    Say "Export the modelling tables for the Python GNN (HDFS -> exports\)"
    foreach ($t in "modeling_table", "graph_edges_knn", "us_stations") {
        Hdfs "rm -rf /export/$t && hdfs dfs -get /flu_project/processed/$t /export/$t"
    }
}

function Stage-MapReduce {
    Say "Hadoop MapReduce on YARN (Python, Hadoop Streaming): MR1 element x year, MR2 state x year x element"
    $log = Join-Path $root "logs\mapreduce_mr1_mr2.log"
    cmd /c "docker exec flu-namenode bash /opt/mapreduce/run_mapreduce.sh > `"$log`" 2>&1"
    # parse the job log -> results\mapreduce_jobs.json (YARN app id, wall time, result SHA-256, Hadoop counters)
    $text  = Get-Content $log -Raw
    $parts = $text -split '== MR2 start'
    $wanted = 'Launched map tasks', 'Launched reduce tasks', 'Data-local map tasks', 'Rack-local map tasks', 'Map input records',
              'Map output records', 'Combine input records', 'Combine output records', 'Reduce input records', 'Reduce output records',
              'HDFS: Number of bytes read', 'Total time spent by all map tasks \(ms\)', 'Total megabyte-milliseconds taken by all map tasks',
              'KEPT', 'HEADER', 'MALFORMED', 'NOT_US', 'OTHER_ELEMENT', 'QC_FLAGGED', 'JOINED', 'NO_STATE', 'STATIONS_LOADED_PER_MAPPER', 'GC time elapsed \(ms\)'
    function Parse-Job([string]$chunk, [string]$tag, [string]$name) {
        $c = [ordered]@{}
        foreach ($w in $wanted) { if ($chunk -match "(?m)^\s*$w=(\d+)") { $c[($w -replace '\\', '')] = [int64]$Matches[1] } }
        [ordered]@{
            job           = $name
            yarn_app_id   = if ($chunk -match 'Submitted application (application_\S+)') { $Matches[1] } else { $null }
            mr_job_id     = if ($chunk -match 'Running job: (job_\S+)') { $Matches[1] } else { $null }
            succeeded     = $chunk -match 'completed successfully'
            input_splits  = if ($chunk -match 'number of splits:(\d+)') { [int]$Matches[1] } else { $null }
            wall_seconds  = if ($chunk -match "${tag}_SECONDS (\d+)") { [int]$Matches[1] } else { $null }
            result_sha256 = if ($chunk -match "${tag}_SHA256 ([0-9a-f]{64})") { $Matches[1] } else { $null }
            result_groups = if ($chunk -match "${tag}_GROUPS (\d+)") { [int]$Matches[1] } else { $null }
            counters      = $c
        }
    }
    $result = [ordered]@{
        generated_at     = (Get-Date).ToUniversalTime().ToString("o")
        input            = "hdfs:///flu_project/raw/weather/*.csv (26 yearly GHCN-Daily CSVs)"
        container_sizing = "AM 1024 MB, map 768 MB (-Xmx600m), reduce 1024 MB on one NodeManager with 4096 MB / 4 vcores"
        jobs = @((Parse-Job $parts[0] "MR1" "MR1 element x year (Python streaming, combiner)"),
                 (Parse-Job $(if ($parts.Count -gt 1) { $parts[1] } else { "" }) "MR2" "MR2 state x year x element (Python streaming, map-side join via distributed cache)"))
    }
    [IO.File]::WriteAllText((Join-Path $root "results\mapreduce_jobs.json"), ($result | ConvertTo-Json -Depth 5), $utf8)
    $result.jobs | ForEach-Object { "{0}: app={1} ok={2} {3}s sha={4}" -f $_.job, $_.yarn_app_id, $_.succeeded, $_.wall_seconds, $_.result_sha256 }
}

function Stage-Gnn {
    Say "GNN rigour pass (docs/RESEARCH_PROTOCOL.md)"
    docker exec -w /workspace/gnn pytorch python prepare_v2.py           # feature table + Persistence / Ridge baselines
    Job "BaselineGBT" @("spark.flu.gbt=v2")
    docker exec -w /workspace/gnn pytorch python run_all.py --version v2 --workers 6
    cmd /c "$py /proj/src/gnn/evaluate_v2.py"
}

function Stage-V3 {
    Job "LagAwareFeaturesV3"
    Job "BaselineGBT" @("spark.flu.gbt=v3")
    docker exec -w /workspace/gnn pytorch python run_all.py --version v3 --workers 6
    cmd /c "$py /proj/src/gnn/evaluate_v3.py"
}

function Stage-MLlib {
    Job "MLlibComparison"
    cmd /c "$py /proj/src/gnn/evaluate_mllib.py"
}

function Stage-Streaming {
    Say "Structured Streaming on a live GHCN feed (60 daily files dripped into HDFS every 3 s)"
    Job "StreamingV2" @("spark.flu.streaming=prepare")
    Hdfs "hdfs dfs -rm -r -f -skipTrash /flu_project/streaming_v2/incoming /flu_project/streaming_v2/output /flu_project/streaming_v2/checkpoint; hdfs dfs -mkdir -p /flu_project/streaming_v2/incoming"
    $stream = Start-Job -ScriptBlock { param($r) & powershell -ExecutionPolicy Bypass -File (Join-Path $r "run_pipeline.ps1") job StreamingV2 } -ArgumentList $root
    Start-Sleep 60   # let the streaming query start before the first file arrives
    $drip = Join-Path $root "results\streaming_v2\drip.jsonl"
    New-Item -ItemType Directory -Force (Split-Path $drip) | Out-Null
    cmd /c "docker exec -i flu-namenode bash -s 3 < `"$(Join-Path $root 'bigdata_pipeline\spark_bigdata_pipeline\spark_streaming\drip_feed.sh')`" > `"$drip`""
    Receive-Job $stream -Wait -AutoRemoveJob
    cmd /c "$py /proj/src/reporting/analyze_performance.py streaming"
}

function Stage-Performance {
    Say "Spark vs pandas benchmark (resumable) + partitioning/caching tuning (resumable, ~8 h)"
    & (Join-Path $root "scripts\run_benchmark.ps1")
    & (Join-Path $root "scripts\run_tuning.ps1")
    cmd /c "$py /proj/src/reporting/analyze_performance.py benchmark tuning"
}

function Stage-Checks {
    Job "DataChecks"
    # DataFrame rewrite check: re-run the parsers into *_dfcheck tables, then compare with MapReduce and with the existing tables
    Job "Preprocess" @("spark.flu.outputSuffix=_dfcheck")
    Job "BuildModelingTable" @("spark.flu.outputSuffix=_dfcheck", "spark.local.dir=/opt/spark-local") "2g"
    Job "DataFrameConsistency" @() "2g"
}

function Stage-Report {
    Say "Cluster verification -> results\cluster_verification.json"
    function Grab([string]$text, [string]$pattern) { if ($text -match $pattern) { $Matches[1].Trim() } else { $null } }
    $report = (Hdfs "hdfs dfsadmin -report 2>/dev/null") -join "`n"
    $fsck   = (Hdfs "hdfs fsck /flu_project 2>/dev/null") -join "`n"
    $du     = (Hdfs "hdfs dfs -du -s /flu_project/raw /flu_project/processed /flu_project 2>/dev/null")
    $sizes = @{}; foreach ($l in $du) { $f = $l -split '\s+'; if ($f.Count -ge 3) { $sizes[$f[2]] = @{ logical_bytes = [int64]$f[0]; with_replicas_bytes = [int64]$f[1] } } }
    $yarn = try { (Invoke-RestMethod -TimeoutSec 10 "http://localhost:8088/ws/v1/cluster/metrics").clusterMetrics } catch { $null }
    $uis = [ordered]@{ namenode_9870 = "http://localhost:9870/dfshealth.html"; datanode1_9864 = "http://localhost:9864/datanode.html"
        datanode2_9865 = "http://localhost:9865/datanode.html"; datanode3_9866 = "http://localhost:9866/datanode.html"
        yarn_rm_8088 = "http://localhost:8088/cluster"; yarn_timeline_8188 = "http://localhost:8188/applicationhistory"
        spark_history_18080 = "http://localhost:18080/"; dashboard_8501 = "http://localhost:8501/" }
    $ui = [ordered]@{}; foreach ($k in $uis.Keys) { $ui[$k] = try { (Invoke-WebRequest -UseBasicParsing -TimeoutSec 8 $uis[$k]).StatusCode } catch { "down" } }
    $raw = if ($sizes['/flu_project/raw']) { $sizes['/flu_project/raw'].logical_bytes } else { 0 }
    $cv = [ordered]@{
        generated_at = (Get-Date).ToUniversalTime().ToString("o")
        containers   = @(docker compose -f (Join-Path $root "docker-compose.yml") ps --format "{{.Name}}|{{.State}}|{{.Status}}" |
                         ForEach-Object { $p = $_ -split '\|'; [ordered]@{ name = $p[0]; state = $p[1]; status = $p[2] } })
        hdfs = [ordered]@{
            safemode = ((Hdfs "hdfs dfsadmin -safemode get 2>/dev/null") -join "")
            live_datanodes = [int](Grab $report 'Live datanodes \((\d+)\)'); dead_datanodes = [int](Grab $report 'Dead datanodes \((\d+)\)')
            dfs_used = Grab $report 'DFS Used: (.+)'; under_replicated = [int](Grab $report 'Under replicated blocks: (\d+)')
            corrupt_replicas = [int](Grab $report 'Blocks with corrupt replicas: (\d+)'); missing_blocks = [int](Grab $report 'Missing blocks: (\d+)')
            fsck_status = Grab $fsck "The filesystem under path '/flu_project' is (\w+)"; fsck_total_files = [int](Grab $fsck 'Total files:\s+(\d+)')
            fsck_total_blocks = Grab $fsck 'Total blocks \(validated\):\s+(\d+)'; fsck_avg_replication = Grab $fsck 'Average block replication:\s+([\d.]+)'
            sizes = $sizes; raw_logical_GB = [math]::Round($raw / 1e9, 2); volume_requirement_25GB_met = $raw -ge 25e9
            raw_weather_yearly_csv_files = [int]((Hdfs "hdfs dfs -ls '/flu_project/raw/weather/*.csv' 2>/dev/null | grep -c '\.csv$'") -join "")
        }
        yarn = if ($yarn) { [ordered]@{ active_nodes = $yarn.activeNodes; total_mb = $yarn.totalMB; total_vcores = $yarn.totalVirtualCores
                                        apps_completed = $yarn.appsCompleted; apps_failed = $yarn.appsFailed } } else { "unreachable" }
        web_ui_http_status = $ui
        versions = [ordered]@{
            hadoop  = Grab ((Hdfs "hadoop version") -join "`n") 'Hadoop (\S+)'
            spark   = Grab ((docker exec spark /opt/spark/bin/spark-submit --version 2>&1) -join "`n") 'version (\d+\.\d+\.\d+)'
            pytorch = ((docker exec pytorch python -c "import torch, torch_geometric; print(torch.__version__, torch_geometric.__version__)" 2>$null) -join "")
        }
    }
    [IO.File]::WriteAllText((Join-Path $root "results\cluster_verification.json"), ($cv | ConvertTo-Json -Depth 6), $utf8)
    "HDFS {0} | live DataNodes {1} | fsck {2} | raw {3} GB | UIs: {4}" -f $cv.hdfs.safemode, $cv.hdfs.live_datanodes, $cv.hdfs.fsck_status,
        $cv.hdfs.raw_logical_GB, (($ui.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join " ")

    docker exec -w /workspace/reporting pytorch python dashboard_data.py
    & (Join-Path $root "scripts\capture_screenshots.ps1") -Only hdfs_overview
    cmd /c "$py /proj/src/reporting/build_reports.py"
}

function Stage-Publish { Job "PublishResultsToHdfs" }

# ------------------------------------------------------------------------------------------------ dispatch
$stages = [ordered]@{ up = { Stage-Up }; ingest = { Stage-Ingest }; etl = { Stage-Etl }; mapreduce = { Stage-MapReduce }
    gnn = { Stage-Gnn }; v3 = { Stage-V3 }; mllib = { Stage-MLlib }; streaming = { Stage-Streaming }
    performance = { Stage-Performance }; checks = { Stage-Checks }; report = { Stage-Report }; publish = { Stage-Publish } }

if ($Stage -eq "job") { Job $Name }
elseif ($Stage -eq "all") { foreach ($s in $stages.Keys) { & $stages[$s] } }
elseif ($stages.Contains($Stage)) { & $stages[$Stage] }
else { Get-Content $PSCommandPath -TotalCount 19 | ForEach-Object { $_ -replace '^# ?', '' } }
