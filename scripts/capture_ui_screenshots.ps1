# Capture the Hadoop / YARN / Spark web UIs as PNGs for the report (docs/screenshots/).
# Usage:  .\scripts\capture_ui_screenshots.ps1            (all pages)
#         .\scripts\capture_ui_screenshots.ps1 -Only spark_ui
# Uses headless Microsoft Edge with a throw-away profile; the user's browser profile is not touched.
param([string]$Only = "")

$root = Split-Path -Parent $PSScriptRoot
$out  = Join-Path $root "docs\screenshots"
New-Item -ItemType Directory -Force $out | Out-Null
$edge = "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe"
$profileDir = Join-Path $env:TEMP ("climaflu-edge-" + [guid]::NewGuid().ToString("N").Substring(0, 8))   # fresh profile: a reused one can stay locked and hang Edge

$pages = [ordered]@{
    "hdfs_overview"      = "http://localhost:9870/dfshealth.html#tab-overview"
    "hdfs_datanodes"     = "http://localhost:9870/dfshealth.html#tab-datanode"
    "hdfs_browse_raw"    = "http://localhost:9870/explorer.html#/flu_project/raw/weather"
    "hdfs_browse_proc"   = "http://localhost:9870/explorer.html#/flu_project/processed"
    "datanode1"          = "http://localhost:9864/datanode.html"
    "datanode2"          = "http://localhost:9865/datanode.html"
    "datanode3"          = "http://localhost:9866/datanode.html"
    "yarn_cluster"       = "http://localhost:8088/cluster"
    "yarn_nodes"         = "http://localhost:8088/cluster/nodes"
    "yarn_apps"          = "http://localhost:8088/cluster/apps"
    "yarn_timeline"      = "http://localhost:8188/applicationhistory"
    "yarn_timeline_mr1"  = "http://localhost:8188/applicationhistory/app/application_1791573873798_0002"
    # dashboard pages (Streamlit, http://localhost:8501); tall window so long pages fit
    "dashboard_overview"  = "http://localhost:8501/?page=overview"
    "dashboard_cluster"   = "http://localhost:8501/?page=cluster"
    "dashboard_batch"     = "http://localhost:8501/?page=batch"
    "dashboard_streaming" = "http://localhost:8501/?page=streaming"
    "dashboard_forecast"  = "http://localhost:8501/?page=forecast"
    "dashboard_graph"     = "http://localhost:8501/?page=graph"
    # Spark UI exists only while a job runs (e.g. scala/SparkUiDemo.scala keeps it open 120 s)
    "spark_ui"           = "http://localhost:4040/jobs/"
    "spark_stages"       = "http://localhost:4040/stages/"
    "spark_sql"          = "http://localhost:4040/SQL/execution/?id=1"
    "spark_executors"    = "http://localhost:4040/executors/"
}

foreach ($name in $pages.Keys) {
    if ($Only -and $name -ne $Only) { continue }
    $url  = $pages[$name]
    $file = Join-Path $out "$name.png"
    & $edge --headless=new --disable-gpu --hide-scrollbars --no-first-run --user-data-dir="$profileDir" `
        --window-size=$(if ($name -like "dashboard_*") { "1440,2200" } else { "1440,1000" }) $(if ($name -like "dashboard_*") { "--timeout=25000" } else { "--virtual-time-budget=10000" }) --screenshot="$file" $url 2>$null | Out-Null
    Start-Sleep -Milliseconds 500
    if (Test-Path $file) { "{0,-18} {1,8:N0} bytes  {2}" -f $name, (Get-Item $file).Length, $url }
    else { "{0,-18} FAILED  {1}" -f $name, $url }
}
