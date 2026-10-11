# Step 3c - Spark performance-tuning experiment (partitions x persistence), resumable and disk-safe.
# Usage: .\benchmarks\run_tuning.ps1      -> results/tuning/runs.jsonl, logs/tuning/*.log, results/spark-events/
# Grid: partitions {8, 24, 96, 384} x persistence {none, memory_and_disk}, AQE off (so it cannot override P),
#       spark.sql.shuffle.partitions = P. Reference: Spark defaults (AQE on, 200 partitions, no persist).
# Each configuration: fresh spark-shell JVM, 1 warm-up iteration + 3 measured repeats (scala/TuningExperiment.scala v2).
#
# Disk safety (v1 of this experiment filled E: through the Docker VHDX, which never shrinks, and crashed WSL on 2026-10-10):
#   * shuffle/spill go to spark.local.dir = /opt/spark-local = .\.spark-local on C: (bind mount: deleted files free real space)
#   * a configuration starts only if C: (shuffle drive) has >= $MinFreeGB GB and E: (Docker disk) >= $MinDockerGB GB free
#   * a watchdog polls both every 5 s and kills the Spark job if C: < $KillFreeGB GB or E: < $KillDockerGB GB;
#     a watchdog kill is an infrastructure abort: nothing is recorded and the grid stops (re-run resumes)
#   * Spark temp files are removed between configurations
param([int]$MinFreeGB = 15, [int]$KillFreeGB = 8, [int]$MinDockerGB = 5, [int]$KillDockerGB = 3, [string]$OnlyPersist = "")

$root   = Split-Path -Parent $PSScriptRoot
$outDir = Join-Path $root "results\tuning"; $logDir = Join-Path $root "logs\tuning"
New-Item -ItemType Directory -Force $outDir, $logDir, (Join-Path $root "results\spark-events") | Out-Null
$runs   = Join-Path $outDir "runs.jsonl"
$scala  = Join-Path $root "scala\TuningExperiment.scala"
$utf8   = New-Object System.Text.UTF8Encoding $false
function FreeGB([string]$d) { [math]::Round((Get-PSDrive $d).Free / 1GB, 1) }

$grid = @()
foreach ($p in 8, 24, 96, 384) { foreach ($m in "none", "memory_and_disk") { $grid += @{ P = $p; persist = $m; aqe = "false"; label = "p$p-$m" } } }
$grid += @{ P = 200; persist = "none"; aqe = "true"; label = "spark-defaults-aqe-p200-none" }

# Resume: a configuration is finished when it has 3 ok repeats or a recorded failure. Partial ones are re-run from scratch.
$byLabel = @{}
if (Test-Path $runs) { Get-Content $runs | ForEach-Object { try { $o = $_ | ConvertFrom-Json; if (-not $byLabel[$o.label]) { $byLabel[$o.label] = @() }; $byLabel[$o.label] += $o } catch {} } }
$finished = @{}
foreach ($k in $byLabel.Keys) {
    $recs = $byLabel[$k]
    if ((@($recs | Where-Object { $_.status -eq "ok" }).Count -ge 3) -or (@($recs | Where-Object { $_.status -ne "ok" }).Count -gt 0)) { $finished[$k] = $true }
}
if (Test-Path $runs) {   # drop partial configurations so they are not double-counted after the re-run
    $keep = Get-Content $runs | Where-Object { try { $finished[($_ | ConvertFrom-Json).label] } catch { $false } }
    [System.IO.File]::WriteAllLines($runs, [string[]]@($keep), $utf8)
}

foreach ($g in $grid) {
    if ($finished[$g.label]) { Write-Host "skip $($g.label) (finished)"; continue }
    if ($OnlyPersist -and $g.persist -ne $OnlyPersist) { Write-Host "skip $($g.label) (-OnlyPersist $OnlyPersist)"; continue }
    docker exec -u root spark sh -c 'rm -rf /tmp/blockmgr-* /tmp/spark-* /opt/spark-local/*' | Out-Null
    $cFree = FreeGB C; $eFree = FreeGB E
    if ($cFree -lt $MinFreeGB -or $eFree -lt $MinDockerGB) { Write-Host "STOP: C: $cFree GB (need $MinFreeGB), E: $eFree GB (need $MinDockerGB). Free space, then re-run (resumes)."; break }

    $log = Join-Path $logDir "$($g.label).log"
    # driver 3g: the 8 GB Docker VM also hosts the Hadoop cluster (4g was killed by the VM OOM killer)
    $cmd = "docker exec -i spark /opt/spark/bin/spark-shell --master local[*] --driver-memory 3g " +
           "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 --conf spark.ui.enabled=false " +
           "--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///opt/results/spark-events " +
           "--conf spark.cleaner.referenceTracking.blocking.shuffle=true --conf spark.local.dir=/opt/spark-local " +
           "--conf spark.sql.adaptive.enabled=$($g.aqe) --conf spark.sql.shuffle.partitions=$($g.P) " +
           "--conf spark.tune.partitions=$($g.P) --conf spark.tune.persist=$($g.persist) --conf spark.tune.repeats=3 " +
           "--conf spark.tune.label=$($g.label) < `"$scala`" > `"$log`" 2>&1"

    $watch = Start-Job -ArgumentList $KillFreeGB, $KillDockerGB -ScriptBlock {
        param($cLimit, $eLimit)
        while ($true) {
            $c = [math]::Round((Get-PSDrive C).Free / 1GB, 1); $e = [math]::Round((Get-PSDrive E).Free / 1GB, 1)
            if ($c -lt $cLimit -or $e -lt $eLimit) {
                docker exec -u root spark sh -c 'pkill -f org.apache.spark.deploy.SparkSubmit' | Out-Null
                return "killed (C: $c GB, E: $e GB)"
            }
            Start-Sleep 5
        }
    }
    $t0 = Get-Date
    cmd /c $cmd
    $rc = $LASTEXITCODE
    $wd = if ($watch.State -eq "Completed") { Receive-Job $watch } else { "ok" }
    Stop-Job $watch -ErrorAction SilentlyContinue; Remove-Job $watch -Force -ErrorAction SilentlyContinue

    if ($wd -ne "ok") { Write-Host "STOP: watchdog aborted $($g.label): $wd. Nothing recorded for it; free space and re-run (resumes)."; break }
    $lines = @(Select-String -Path $log -Pattern '^TUNEJSON (.*)$' | ForEach-Object { $_.Matches[0].Groups[1].Value })
    if ($lines.Count -eq 0) {   # crashed before printing anything: record that, do not lose it
        $reason = "no TUNEJSON output (exit $rc); see $($log.Replace($root + '\', ''))"
        $lines = @((@{ label = $g.label; partitions = $g.P; persist = $g.persist; aqe = [bool]::Parse($g.aqe); repeat = 0
                       status = "failed"; error_class = "runner"; error = $reason } | ConvertTo-Json -Compress))
    }
    foreach ($l in $lines) { [System.IO.File]::AppendAllText($runs, $l + "`n", $utf8) }
    $ok = @($lines | Where-Object { $_ -match '"status": ?"ok"' }).Count
    Write-Host ("{0}  {1,-32} ok={2} recorded={3}  {4:N0} s  C: {5} GB  E: {6} GB" -f (Get-Date -Format "HH:mm:ss"), $g.label, $ok, $lines.Count, ((Get-Date) - $t0).TotalSeconds, (FreeGB C), (FreeGB E))
}
docker exec -u root spark sh -c 'rm -rf /tmp/blockmgr-* /tmp/spark-* /opt/spark-local/*' | Out-Null
Write-Host "Done. $runs"
