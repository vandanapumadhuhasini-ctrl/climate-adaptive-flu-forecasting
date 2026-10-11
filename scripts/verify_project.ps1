# Final atomic project verification -> results\verification.json (+ docs\screenshots\hdfs_overview_final.png)
# One run, one timestamp: cluster health (HDFS size, fsck, DataNodes, YARN, web UIs), NameNode UI screenshot,
# HDFS raw-data counts and an inventory of every evidence file used by docs\COURSE_COMPLIANCE_MATRIX.md.
# Usage: .\scripts\verify_project.ps1     then: docker run --rm -v "C:/Project/flu-hadoop-project:/proj" climaflu-pytorch:1.0 python /proj/python/build_compliance_matrix.py
$root = Split-Path -Parent $PSScriptRoot
$t0 = (Get-Date).ToUniversalTime().ToString("o")
Write-Host "[verify] snapshot $t0"

& powershell -ExecutionPolicy Bypass -File (Join-Path $root "scripts\verify_cluster.ps1") | ForEach-Object { Write-Host "  $_" }
& powershell -ExecutionPolicy Bypass -File (Join-Path $root "scripts\capture_ui_screenshots.ps1") -Only hdfs_overview | Out-Null
$shot = Join-Path $root "docs\screenshots\hdfs_overview_final.png"
Copy-Item (Join-Path $root "docs\screenshots\hdfs_overview.png") $shot -Force

$count = (docker exec flu-namenode bash -c "hdfs dfs -count /flu_project/raw/weather 2>/dev/null") -split '\s+' | Where-Object { $_ }
$csvCount = (docker exec flu-namenode bash -c "hdfs dfs -ls '/flu_project/raw/weather/*.csv' 2>/dev/null | grep -c '\.csv$'")

$evidence = @(
    "results/cluster_verification.json", "results/cluster_smoke_test.json", "results/mapreduce_jobs.json",
    "results/cross_engine_consistency.json", "results/spark_api_evidence.txt", "results/spark_dataframe_plan.txt",
    "results/spark_event_log_summary.json", "results/veracity_audit.json", "results/variety_inventory.json",
    "results/streaming_summary.json", "results/benchmark_summary.json", "results/benchmark/kernel_oom_log.txt",
    "results/tuning_summary.json", "results/rigour/summary.json", "results/rigour/prepare_summary.json",
    "results/v3/summary.json", "results/v3/lag_scan.json", "results/hdfs_publish_manifest.json", "docs/V3_RESULTS.md", "docs/BASE_PAPER.md",
    "docs/BENCHMARK_PROTOCOL.md", "docs/RESEARCH_PROTOCOL.md", "docs/RIGOUR_RESULTS.md", "docs/TUNING_RESULTS.md",
    "docs/HADOOP_SPARK_API_EVIDENCE.md", "docs/screenshots/hdfs_overview_final.png", "docs/screenshots/hdfs_datanodes.png",
    "docs/screenshots/yarn_nodes.png", "docs/screenshots/yarn_timeline.png", "docs/screenshots/yarn_timeline_mr1.png",
    "docs/screenshots/spark_ui.png", "docs/screenshots/spark_sql.png", "docs/screenshots/dashboard_overview.png",
    "docs/screenshots/dashboard_forecast.png", "docs/screenshots/dashboard_streaming.png"
)
$inv = foreach ($e in $evidence) {
    $p = Join-Path $root $e
    if (Test-Path $p) { $i = Get-Item $p; [ordered]@{ path = $e; exists = $true; bytes = $i.Length; modified_utc = $i.LastWriteTimeUtc.ToString("o") } }
    else { [ordered]@{ path = $e; exists = $false } }
}
$missing = @($inv | Where-Object { -not $_.exists }).Count
$dash = try { (Invoke-WebRequest -UseBasicParsing -TimeoutSec 10 "http://localhost:8501/").StatusCode } catch { "down" }

$result = [ordered]@{
    snapshot_utc = $t0
    finished_utc = (Get-Date).ToUniversalTime().ToString("o")
    hdfs_raw_weather = [ordered]@{ dirs = [int]$count[0]; files = [int]$count[1]; logical_bytes = [int64]$count[2]; yearly_csv_files = [int]$csvCount }
    hdfs_overview_screenshot = "docs/screenshots/hdfs_overview_final.png"
    dashboard_http = $dash
    evidence_files = @($inv)
    evidence_missing = $missing
}
[IO.File]::WriteAllText((Join-Path $root "results\verification.json"), ($result | ConvertTo-Json -Depth 5), (New-Object Text.UTF8Encoding $false))
Write-Host ("[verify] raw weather: {0} yearly CSVs + stations, {1:N0} bytes | dashboard HTTP {2} | evidence files missing: {3}" -f $csvCount, [int64]$count[2], $dash, $missing)
