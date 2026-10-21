# Run a Scala job in the spark container and log it.
# Usage:  .\scripts\run_scala.ps1 ClusterSmokeTest   [-DriverMemory 3g] [-Conf key=value ...]
# Why: piping with Get-Content in PowerShell prepends a BOM, which breaks the ':paste' on line 1 and makes
# the REPL execute statements one by one. cmd.exe '<' redirection passes the file bytes unchanged.
# -Preload: optional file (relative to scala/) loaded line by line with spark-shell -i before the job (not used now:
# the project uses the DataFrame API only).
param(
    [Parameter(Mandatory = $true)][string]$Job,
    [string]$DriverMemory = "4g",
    [string]$Preload = "",
    [string[]]$Conf = @()        # extra Spark settings, e.g. -Conf spark.demo.holdSeconds=600
)
$root = Split-Path -Parent $PSScriptRoot
$src  = Join-Path $root "scala\$Job.scala"
if (-not (Test-Path $src)) { Write-Error "Not found: $src"; exit 1 }
New-Item -ItemType Directory -Force (Join-Path $root "logs"), (Join-Path $root "results\spark-events") | Out-Null
$log  = Join-Path $root ("logs\{0}_{1}.log" -f $Job, (Get-Date -Format "yyyyMMdd_HHmmss"))
$pre  = if ($Preload) { "-i /opt/scala/$Preload " } else { "" }
$extra = ($Conf | ForEach-Object { "--conf $_ " }) -join ""

# Output goes straight to the log file through cmd.exe (plain bytes, UTF-8), then is echoed to the console.
$cmd = "docker exec -i spark /opt/spark/bin/spark-shell --master local[*] --driver-memory $DriverMemory " +
       "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 " +
       "--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///opt/results/spark-events " +
       "$extra$pre< `"$src`" > `"$log`" 2>&1"
$start = Get-Date
cmd /c $cmd
$secs = [math]::Round(((Get-Date) - $start).TotalSeconds, 1)
Get-Content $log -Encoding UTF8
Write-Host "`n[run_scala] $Job finished in $secs s; log: $log"
if (Select-String -Path $log -Pattern "error:|Exception" -Quiet) {
    Write-Warning "[run_scala] errors or exceptions found in the log - check it before trusting outputs"
}
