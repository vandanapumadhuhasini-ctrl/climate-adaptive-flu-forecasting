# One-shot cluster verification -> results/cluster_verification.json (+ console summary).
# Usage:  .\scripts\verify_cluster.ps1
# Checks: containers, HDFS safe mode / DataNodes / fsck / logical sizes, YARN nodes, web UI HTTP status, versions.
$ErrorActionPreference = "Continue"
$root = Split-Path -Parent $PSScriptRoot
$outFile = Join-Path $root "results\cluster_verification.json"

function Exec([string]$container, [string]$cmd) { (docker exec $container bash -c $cmd 2>$null) -join "`n" }
function Grab([string]$text, [string]$pattern) { if ($text -match $pattern) { $Matches[1].Trim() } else { $null } }

$containers = docker compose -f (Join-Path $root "docker-compose.yml") ps --format "{{.Name}}|{{.State}}|{{.Status}}" |
    ForEach-Object { $p = $_ -split '\|'; [ordered]@{ name = $p[0]; state = $p[1]; status = $p[2] } }

$safemode = Exec "flu-namenode" "hdfs dfsadmin -safemode get"
$report   = Exec "flu-namenode" "hdfs dfsadmin -report"
$fsck     = Exec "flu-namenode" "hdfs fsck /flu_project"
$du       = Exec "flu-namenode" "hdfs dfs -du -s /flu_project/raw /flu_project/processed /flu_project"
$duRows = @{}
foreach ($line in ($du -split "`n")) { $f = $line -split '\s+'; if ($f.Count -ge 3) { $duRows[$f[2]] = @{ logical_bytes = [int64]$f[0]; with_replicas_bytes = [int64]$f[1] } } }

$yarn = $null
try { $yarn = (Invoke-RestMethod -TimeoutSec 10 "http://localhost:8088/ws/v1/cluster/metrics").clusterMetrics } catch {}

$uis = [ordered]@{
    namenode_9870 = "http://localhost:9870/dfshealth.html"; datanode1_9864 = "http://localhost:9864/datanode.html"
    datanode2_9865 = "http://localhost:9865/datanode.html"; datanode3_9866 = "http://localhost:9866/datanode.html"
    yarn_rm_8088 = "http://localhost:8088/cluster"; yarn_timeline_8188 = "http://localhost:8188/applicationhistory"
    spark_ui_4040 = "http://localhost:4040/jobs/"
}
$uiStatus = [ordered]@{}
foreach ($k in $uis.Keys) {
    try { $uiStatus[$k] = (Invoke-WebRequest -UseBasicParsing -TimeoutSec 8 $uis[$k]).StatusCode }
    catch { $uiStatus[$k] = "down" + $(if ($k -eq "spark_ui_4040") { " (only while a Spark job runs)" } else { "" }) }
}

$result = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    containers   = @($containers)
    hdfs = [ordered]@{
        safemode               = $safemode
        live_datanodes         = [int](Grab $report 'Live datanodes \((\d+)\)')
        dead_datanodes         = [int](Grab $report 'Dead datanodes \((\d+)\)')
        dfs_used               = Grab $report 'DFS Used: (.+)'
        under_replicated       = [int](Grab $report 'Under replicated blocks: (\d+)')
        corrupt_replicas       = [int](Grab $report 'Blocks with corrupt replicas: (\d+)')
        missing_blocks         = [int](Grab $report 'Missing blocks: (\d+)')
        fsck_status            = Grab $fsck "The filesystem under path '/flu_project' is (\w+)"
        fsck_total_files       = [int](Grab $fsck 'Total files:\s+(\d+)')
        fsck_total_blocks      = Grab $fsck 'Total blocks \(validated\):\s+(\d+)'
        fsck_avg_replication   = Grab $fsck 'Average block replication:\s+([\d.]+)'
        sizes                  = $duRows
        raw_logical_GB         = if ($duRows['/flu_project/raw']) { [math]::Round($duRows['/flu_project/raw'].logical_bytes / 1e9, 2) } else { $null }
        volume_requirement_25GB_met = if ($duRows['/flu_project/raw']) { $duRows['/flu_project/raw'].logical_bytes -ge 25e9 } else { $false }
    }
    yarn = if ($yarn) { [ordered]@{ active_nodes = $yarn.activeNodes; total_mb = $yarn.totalMB; total_vcores = $yarn.totalVirtualCores
                                    apps_completed = $yarn.appsCompleted; apps_failed = $yarn.appsFailed } } else { "unreachable" }
    web_ui_http_status = $uiStatus
    versions = [ordered]@{
        hadoop  = Grab (Exec "flu-namenode" "hadoop version") 'Hadoop (\S+)'
        spark   = Grab ((docker exec spark /opt/spark/bin/spark-submit --version 2>&1) -join "`n") 'version (\d+\.\d+\.\d+)'
        pytorch = ((docker exec pytorch python -c "import torch, torch_geometric; print(torch.__version__, torch_geometric.__version__)" 2>$null) -join "")
    }
}
$json = $result | ConvertTo-Json -Depth 6
[System.IO.File]::WriteAllText($outFile, $json, (New-Object System.Text.UTF8Encoding $false))

"HDFS : {0} | live DataNodes {1} | fsck {2} | under-replicated {3} | raw {4} GB (>=25 GB: {5})" -f `
    $result.hdfs.safemode, $result.hdfs.live_datanodes, $result.hdfs.fsck_status, $result.hdfs.under_replicated,
    $result.hdfs.raw_logical_GB, $result.hdfs.volume_requirement_25GB_met
if ($yarn) { "YARN : active nodes {0} | {1} MB | {2} vcores | apps completed {3}, failed {4}" -f $yarn.activeNodes, $yarn.totalMB, $yarn.totalVirtualCores, $yarn.appsCompleted, $yarn.appsFailed }
"UIs  : " + (($uiStatus.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join "  ")
"Saved: $outFile"
