# Part 06 - Spark vs pandas scalability benchmark (fair, resumable).
# Usage:  .\benchmarks\run_benchmark.ps1                 (full plan; already-finished runs are skipped)
#         .\benchmarks\run_benchmark.ps1 -Smoke          (1-year sanity run of every configuration)
#
# Fairness rules (see docs/BENCHMARK_PROTOCOL.md):
#   * identical workload + identical result checksum (scala/BenchmarkWorkload.scala == python/benchmark_workload.py)
#   * every run in a FRESH container with the same memory cap (Experiment A: 4 GB; B: 6 GB), swap disabled
#   * peak memory measured the same way for both engines (process VmHWM + cgroup memory.peak)
#   * Spark is run on HDFS AND on the same local files pandas reads, to separate I/O from compute
#   * Spark core count is explicit (local[1] with --cpus 1, local[4] with --cpus 4, local[*] = all 24)
# Output: results/benchmark/runs.jsonl (one JSON object per run, including failures), logs/benchmark/*.log
param([switch]$Smoke)

$root    = Split-Path -Parent $PSScriptRoot
$outDir  = Join-Path $root "results\benchmark"
$logDir  = Join-Path $root "logs\benchmark"
New-Item -ItemType Directory -Force $outDir, $logDir | Out-Null
$runsFile = Join-Path $outDir ($(if ($Smoke) { "smoke_runs.jsonl" } else { "runs.jsonl" }))
$network = "flu-hadoop-project_flu_network"
$raw     = "C:/Project/flu-hadoop-project/datasets/weather_raw"
$pyDir   = "C:/Project/flu-hadoop-project/python"
$scala   = Join-Path $root "scala\BenchmarkWorkload.scala"
$utf8    = New-Object System.Text.UTF8Encoding $false

# ---------------- plan ----------------
# Each entry: experiment, label, engine, extra params, sizes, repeats(size) -> int
$all   = @(1, 3, 5, 10, 15, 20, 26)
$rep3  = { param($n) if ($n -le 5) { 3 } else { 1 } }
$plan = @(
  @{ exp = "A"; label = "spark-hdfs-local[*]";  engine = "spark";  source = "hdfs";  master = "local[*]"; cpus = $null; mem = "4g"; sizes = $all; reps = { param($n) 3 } }
  @{ exp = "A"; label = "spark-localfs-local[*]"; engine = "spark"; source = "local"; master = "local[*]"; cpus = $null; mem = "4g"; sizes = $all; reps = { param($n) 3 } }
  @{ exp = "A"; label = "spark-hdfs-local[4]";  engine = "spark";  source = "hdfs";  master = "local[4]"; cpus = 4;     mem = "4g"; sizes = $all; reps = $rep3 }
  @{ exp = "A"; label = "spark-hdfs-local[1]";  engine = "spark";  source = "hdfs";  master = "local[1]"; cpus = 1;     mem = "4g"; sizes = @(1, 3, 5); reps = { param($n) 1 } }
  @{ exp = "A"; label = "pandas-naive";         engine = "pandas"; variant = "naive";     mem = "4g"; sizes = $all; reps = $rep3 }
  @{ exp = "A"; label = "pandas-optimized";     engine = "pandas"; variant = "optimized"; mem = "4g"; sizes = $all; reps = $rep3 }
  @{ exp = "A"; label = "pandas-pyarrow";       engine = "pandas"; variant = "pyarrow";   mem = "4g"; sizes = $all; reps = $rep3 }
  @{ exp = "A"; label = "pandas-chunked";       engine = "pandas"; variant = "chunked";   mem = "4g"; sizes = @(1, 3, 5, 10, 26); reps = { param($n) if ($n -eq 1) { 3 } else { 1 } } }
  @{ exp = "B"; label = "pandas-optimized";     engine = "pandas"; variant = "optimized"; mem = "6g"; sizes = @(1, 3, 5, 10); reps = { param($n) 1 } }
  @{ exp = "B"; label = "pandas-pyarrow";       engine = "pandas"; variant = "pyarrow";   mem = "6g"; sizes = @(1, 3, 5, 10); reps = { param($n) 1 } }
)
if ($Smoke) { foreach ($p in $plan) { $p.sizes = @(1); $p.reps = { param($n) 1 } } }

# ---------------- resume support ----------------
$done = @{}
if (Test-Path $runsFile) {
    Get-Content $runsFile | ForEach-Object { try { $o = $_ | ConvertFrom-Json; $done[$o.run_id] = $o.status } catch {} }
}

function Invoke-Run($p, [int]$n, [int]$r) {
    $runId = "{0}|{1}|{2}|y{3}|r{4}" -f $p.exp, $p.label, $p.mem, $n, $r
    if ($done.ContainsKey($runId)) { return $done[$runId] }
    $safe = ($runId -replace '[^A-Za-z0-9]+', '_')
    $log  = Join-Path $logDir "$safe.log"
    $memArgs = "--memory=$($p.mem) --memory-swap=$($p.mem)"
    if ($p.engine -eq "spark") {
        $cpuArg = if ($p.cpus) { "--cpus=$($p.cpus)" } else { "" }
        $heap = if ($p.mem -eq "4g") { "3g" } else { "5g" }
        $cmd = "docker run --rm -i $memArgs $cpuArg --network $network -v `"${raw}:/rawdata:ro`" apache/spark:3.5.1 " +
               "/opt/spark/bin/spark-shell --master $($p.master) --driver-memory $heap " +
               "--conf spark.ui.enabled=false --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 " +
               "--conf spark.bench.years=$n --conf spark.bench.source=$($p.source) --conf spark.bench.label=$($p.label) " +
               "< `"$scala`" > `"$log`" 2>&1"
    } else {
        $cmd = "docker run --rm $memArgs -v `"${pyDir}:/workspace:ro`" -v `"${raw}:/rawdata:ro`" climaflu-pytorch:1.0 " +
               "python /workspace/benchmark_workload.py $($p.variant) $n > `"$log`" 2>&1"
    }
    $t0 = Get-Date
    cmd /c $cmd
    $rc = $LASTEXITCODE
    $wall = [math]::Round(((Get-Date) - $t0).TotalSeconds, 2)
    $line = Select-String -Path $log -Pattern '^BENCHJSON (.*)$' | Select-Object -Last 1
    $rec = [ordered]@{}
    if ($line) { ($line.Matches[0].Groups[1].Value | ConvertFrom-Json).PSObject.Properties | ForEach-Object { $rec[$_.Name] = $_.Value } }
    else {
        $rec.engine = $p.engine; $rec.label = $p.label; $rec.years = $n
        $rec.status = if ($rc -eq 137) { "oom_killed" } else { "failed" }
    }
    $rec.run_id = $runId; $rec.experiment = $p.exp; $rec.mem_limit = $p.mem; $rec.cpus_limit = $p.cpus
    $rec.repeat = $r; $rec.exit_code = $rc; $rec.wall_s_incl_startup = $wall
    $rec.finished_at = (Get-Date).ToUniversalTime().ToString("o"); $rec.log = $log.Replace($root + "\", "")
    [System.IO.File]::AppendAllText($runsFile, (($rec | ConvertTo-Json -Compress) + "`n"), $utf8)
    Write-Host ("{0}  {1,-44} -> {2,-10} {3,8} s  (exit {4})" -f (Get-Date -Format "HH:mm:ss"), $runId, $rec.status, $rec.elapsed_s, $rc)
    return $rec.status
}

foreach ($p in $plan) {
    foreach ($n in $p.sizes) {
        $failed = $false
        $reps = & $p.reps $n
        for ($r = 1; $r -le $reps; $r++) {
            $st = Invoke-Run $p $n $r
            if ($st -ne "ok") { $failed = $true; break }
        }
        if ($failed) { Write-Host "          $($p.exp) $($p.label): stopped at $n years (larger sizes skipped - they need more memory)"; break }
    }
}
"Done. Runs file: $runsFile"
