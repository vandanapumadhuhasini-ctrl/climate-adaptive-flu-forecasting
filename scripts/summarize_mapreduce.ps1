# Parse logs/mapreduce_mr1_mr2.log (output of mapreduce/run_mapreduce.sh) -> results/mapreduce_jobs.json
# Records per job: YARN application id, wall seconds, result SHA-256, group count and the Hadoop counters.
param([string]$Log = "logs\mapreduce_mr1_mr2.log")
$root = Split-Path -Parent $PSScriptRoot
$text = Get-Content (Join-Path $root $Log) -Raw
$parts = $text -split '== MR2 start'
$wanted = 'Launched map tasks', 'Launched reduce tasks', 'Data-local map tasks', 'Rack-local map tasks', 'Map input records', 'Map output records',
          'Combine input records', 'Combine output records', 'Reduce input records', 'Reduce output records',
          'HDFS: Number of bytes read', 'Total time spent by all map tasks \(ms\)', 'Total megabyte-milliseconds taken by all map tasks',
          'KEPT', 'HEADER', 'MALFORMED', 'NOT_US', 'OTHER_ELEMENT', 'QC_FLAGGED', 'JOINED', 'NO_STATE', 'STATIONS_LOADED_PER_MAPPER', 'GC time elapsed \(ms\)'

function Parse-Job([string]$chunk, [string]$tag, [string]$name) {
    $c = [ordered]@{}
    foreach ($w in $wanted) { if ($chunk -match "(?m)^\s*$w=(\d+)") { $c[($w -replace '\\', '')] = [int64]$Matches[1] } }
    [ordered]@{
        job            = $name
        yarn_app_id    = if ($chunk -match 'Submitted application (application_\S+)') { $Matches[1] } else { $null }
        mr_job_id      = if ($chunk -match 'Running job: (job_\S+)') { $Matches[1] } else { $null }
        succeeded      = $chunk -match 'completed successfully'
        input_splits   = if ($chunk -match 'number of splits:(\d+)') { [int]$Matches[1] } else { $null }
        wall_seconds   = if ($chunk -match "${tag}_SECONDS (\d+)") { [int]$Matches[1] } else { $null }
        result_sha256  = if ($chunk -match "${tag}_SHA256 ([0-9a-f]{64})") { $Matches[1] } else { $null }
        result_groups  = if ($chunk -match "${tag}_GROUPS (\d+)") { [int]$Matches[1] } else { $null }
        counters       = $c
    }
}
$result = [ordered]@{
    generated_at = (Get-Date).ToUniversalTime().ToString("o")
    input        = "hdfs:///flu_project/raw/weather/*.csv (26 yearly GHCN-Daily CSVs)"
    container_sizing = "AM 1024 MB, map 768 MB (-Xmx600m), reduce 1024 MB on one NodeManager with 4096 MB / 4 vcores"
    jobs = @(
        (Parse-Job $parts[0] "MR1" "ElementYearAggregate (combiner + custom Writable)"),
        (Parse-Job $(if ($parts.Count -gt 1) { $parts[1] } else { "" }) "MR2" "StateYearCoverage (map-side join via distributed cache)")
    )
}
$out = Join-Path $root "results\mapreduce_jobs.json"
[System.IO.File]::WriteAllText($out, ($result | ConvertTo-Json -Depth 5), (New-Object System.Text.UTF8Encoding $false))
$result.jobs | ForEach-Object { "{0}: app={1} ok={2} {3}s sha={4} groups={5}" -f $_.job, $_.yarn_app_id, $_.succeeded, $_.wall_seconds, $_.result_sha256, $_.result_groups }
"Saved $out"
