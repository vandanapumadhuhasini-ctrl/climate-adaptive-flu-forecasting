# Screenshots of the web UIs for the report -> docs\screenshots\<name>.png
# Usage:  .\scripts\capture_screenshots.ps1                  (Hadoop, YARN, Spark History and dashboard pages)
#         .\scripts\capture_screenshots.ps1 -Only hdfs_overview
# Hadoop / YARN / Spark pages: headless Edge --screenshot. Dashboard pages: Streamlit renders over a WebSocket, so they
# are captured through the Chrome DevTools Protocol (navigate, wait, measure the page height, capture).
# Every run uses a fresh throw-away Edge profile (a reused one can stay locked and hang Edge); the user's profile is untouched.
param([string]$Only = "", [int]$WaitSec = 12, [int]$Port = 9333, [int]$Width = 1440)
$root = Split-Path -Parent $PSScriptRoot
$out  = Join-Path $root "docs\screenshots"
New-Item -ItemType Directory -Force $out | Out-Null
$edge = "${env:ProgramFiles(x86)}\Microsoft\Edge\Application\msedge.exe"
function NewProfile { Join-Path $env:TEMP ("climaflu-edge-" + [guid]::NewGuid().ToString("N").Substring(0, 8)) }

$pages = [ordered]@{
    "hdfs_overview"     = "http://localhost:9870/dfshealth.html#tab-overview"
    "hdfs_datanodes"    = "http://localhost:9870/dfshealth.html#tab-datanode"
    "hdfs_browse_raw"   = "http://localhost:9870/explorer.html#/flu_project/raw/weather"
    "hdfs_browse_proc"  = "http://localhost:9870/explorer.html#/flu_project/processed"
    "yarn_cluster"      = "http://localhost:8088/cluster"
    "yarn_nodes"        = "http://localhost:8088/cluster/nodes"
    "yarn_timeline"     = "http://localhost:8188/applicationhistory"
    "yarn_timeline_mr1" = "http://localhost:8188/applicationhistory/app/application_1791573873798_0002"
    "spark_history"     = "http://localhost:18080/?showIncomplete=false"
}
foreach ($name in $pages.Keys) {
    if ($Only -and $name -ne $Only) { continue }
    $file = Join-Path $out "$name.png"
    $p = Start-Process $edge -PassThru -ArgumentList "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
        "--user-data-dir=$(NewProfile)", "--window-size=1440,1000", "--virtual-time-budget=10000", "--screenshot=$file", $pages[$name]
    if (-not $p.WaitForExit(60000)) { $p | Stop-Process -Force; "{0,-18} TIMEOUT" -f $name; continue }
    "{0,-18} {1,8:N0} bytes  {2}" -f $name, (Get-Item $file).Length, $pages[$name]
}

# ---------------------------------------------------------------- dashboard pages (http://localhost:8501)
$dash = "overview", "cluster", "batch", "batch&view=benchmark", "batch&view=tuning", "streaming", "forecast", "graph"
$dash = @($dash | Where-Object { -not $Only -or "dashboard_$($_ -replace '&view=', '_')" -eq $Only })
if (-not $dash) { return }
$prof = NewProfile
$proc = Start-Process $edge -PassThru -ArgumentList "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
    "--remote-debugging-port=$Port", "--user-data-dir=$prof", "--window-size=$Width,1000", "about:blank"
try {
    $target = $null
    for ($i = 0; $i -lt 30 -and -not $target; $i++) {
        Start-Sleep -Milliseconds 500
        try { $target = (Invoke-RestMethod "http://127.0.0.1:$Port/json/list") | Where-Object { $_.type -eq "page" } | Select-Object -First 1 } catch {}
    }
    if (-not $target) { throw "Edge DevTools endpoint did not come up on port $Port" }
    $ws = New-Object System.Net.WebSockets.ClientWebSocket
    $ws.Options.KeepAliveInterval = [TimeSpan]::FromSeconds(20)
    $ws.ConnectAsync([Uri]$target.webSocketDebuggerUrl, [Threading.CancellationToken]::None).Wait()
    $script:id = 0
    function Send-Cdp([string]$method, $params = @{}) {
        $script:id++
        $msg = @{ id = $script:id; method = $method; params = $params } | ConvertTo-Json -Depth 6 -Compress
        $bytes = [Text.Encoding]::UTF8.GetBytes($msg)
        $ws.SendAsync([ArraySegment[byte]]::new($bytes), [Net.WebSockets.WebSocketMessageType]::Text, $true, [Threading.CancellationToken]::None).Wait()
        while ($true) {   # read frames until the response with our id arrives (events are skipped)
            $sb = New-Object Text.StringBuilder
            do {
                $buf = New-Object byte[] 1048576
                $r = $ws.ReceiveAsync([ArraySegment[byte]]::new($buf), [Threading.CancellationToken]::None).Result
                [void]$sb.Append([Text.Encoding]::UTF8.GetString($buf, 0, $r.Count))
            } until ($r.EndOfMessage)
            $o = $sb.ToString() | ConvertFrom-Json
            if ($o.id -eq $script:id) { return $o }
        }
    }
    $height = 'Math.max(...["[data-testid=\"stMain\"]","section.main","[data-testid=\"stAppViewContainer\"]"].map(s => document.querySelector(s)).filter(e => e).map(e => e.scrollHeight), document.body.scrollHeight)'
    foreach ($p in $dash) {
        [void](Send-Cdp "Emulation.setDeviceMetricsOverride" @{ width = $Width; height = 1000; deviceScaleFactor = 1; mobile = $false })
        [void](Send-Cdp "Page.navigate" @{ url = "http://localhost:8501/?page=$p&shot=1" })
        Start-Sleep -Seconds $WaitSec
        $h = [int][math]::Min(7000, (Send-Cdp "Runtime.evaluate" @{ expression = $height; returnByValue = $true }).result.result.value)
        [void](Send-Cdp "Emulation.setDeviceMetricsOverride" @{ width = $Width; height = [math]::Max($h, 1000); deviceScaleFactor = 1; mobile = $false })
        Start-Sleep -Seconds 3   # let Plotly re-layout to the new viewport
        $png = (Send-Cdp "Page.captureScreenshot" @{ format = "png"; captureBeyondViewport = $true }).result.data
        $file = Join-Path $out ("dashboard_" + ($p -replace "&view=", "_") + ".png")
        [IO.File]::WriteAllBytes($file, [Convert]::FromBase64String($png))
        "{0,-22} {1,9:N0} bytes  {2}x{3}" -f "dashboard_$p", (Get-Item $file).Length, $Width, [math]::Max($h, 1000)
    }
    $ws.Dispose()
} finally {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    Get-CimInstance Win32_Process -Filter "Name='msedge.exe'" | Where-Object { $_.CommandLine -like "*$prof*" } |
        ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
}
