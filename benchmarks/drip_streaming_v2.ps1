# Streaming v2 feed driver: runs streaming/drip_feed.sh inside flu-namenode (fed through stdin with cmd.exe '<', so the
# script bytes arrive unchanged - PowerShell 5 mangles quotes in native arguments and adds a BOM when piping).
# Usage: .\benchmarks\drip_streaming_v2.ps1 [-IntervalSec 3]   -> results\streaming_v2\drip.jsonl
param([double]$IntervalSec = 3)
$root = Split-Path -Parent $PSScriptRoot
$out = Join-Path $root "results\streaming_v2\drip.jsonl"
New-Item -ItemType Directory -Force (Split-Path $out) | Out-Null
$sh = Join-Path $root "streaming\drip_feed.sh"
cmd /c "docker exec -i flu-namenode bash -s $IntervalSec < `"$sh`" > `"$out`""
"dripped $((Get-Content $out | Measure-Object).Count) files -> $out"
