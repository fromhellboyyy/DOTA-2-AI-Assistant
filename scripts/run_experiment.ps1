$TAGS = @(
    @{ tag = "baseline_stopped"; size = "100000"; scale = 1; phase = "stopped" },
    @{ tag = "bp_stopped";       size = "20";     scale = 1; phase = "stopped" },
    @{ tag = "bp_live_1w";       size = "20";     scale = 1; phase = "live" },
    @{ tag = "bp_live_2w";       size = "20";     scale = 2; phase = "live" }
)

$EXCLUDE_SERVICES = @("worker", "producer", "app")

function Invoke-Docker {
    param([string[]]$DArgs, [switch]$IgnoreExit)

    $prev = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"

    try {
        $out = docker @DArgs 2>&1
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $prev
    }

    $global:DockerExitCode = $code

    if (-not $IgnoreExit -and $code -ne 0) {
        throw "docker $($DArgs -join ' ') exited $code : $($out -join ' ')"
    }

    return $out
}

function Get-ComposeServices {
    $all = Invoke-Docker @("compose", "config", "--services") -IgnoreExit |
        ForEach-Object { "$_".Trim() } |
        Where-Object { $_ }

    return @($all)
}

function Get-InfraServices {
    $all = Get-ComposeServices
    return @($all | Where-Object { $EXCLUDE_SERVICES -notcontains $_ })
}

function Build-Experiment-Images {
    Write-Host "building worker and producer images without cache"
    Invoke-Docker @("compose", "build", "--no-cache", "worker", "producer") | Out-Null
}

function Wait-MlflowReady {
    $services = Get-ComposeServices

    if ($services -notcontains "mlflow") {
        Write-Host "mlflow service not found, skipping readiness wait"
        return
    }

    $pycheck = "import socket; s=socket.create_connection(('127.0.0.1',5000),timeout=2); s.close()"

    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 2

        Invoke-Docker @(
            "compose", "exec", "-T", "mlflow",
            "python", "-c", $pycheck
        ) -IgnoreExit | Out-Null

        if ($global:DockerExitCode -eq 0) {
            Start-Sleep -Seconds 3
            return
        }
    }

    Write-Host "--- mlflow status ---"
    Invoke-Docker @("compose", "ps", "-a", "mlflow") -IgnoreExit | ForEach-Object { Write-Host "$_" }

    Write-Host "--- mlflow logs ---"
    Invoke-Docker @("compose", "logs", "--tail=120", "mlflow") -IgnoreExit | ForEach-Object { Write-Host "$_" }

    $st = Invoke-Docker @("compose", "ps", "-a", "--format", "{{.Status}}", "mlflow") -IgnoreExit
    $stText = ($st | ForEach-Object { "$_" }) -join " "

    if ($stText -match "Up|Running|healthy") {
        Write-Warning "MLflow port check did not succeed, but container appears up; relying on worker model-load retries."
        return
    }

    throw "MLflow did not become ready and container is not running"
}

function Kill-Producer {
    $ids = Invoke-Docker @("ps", "-q", "--filter", "name=producer") -IgnoreExit
    foreach ($id in $ids) {
        $x = "$id".Trim()
        if ($x) {
            Invoke-Docker @("rm", "-f", $x) -IgnoreExit | Out-Null
        }
    }
}

function Reset-Cold-Rabbit-Warm-MLflow {
    Kill-Producer
    Invoke-Docker @("compose", "down", "--remove-orphans") -IgnoreExit | Out-Null

    $vols = Invoke-Docker @("volume", "ls", "-q", "--filter", "name=rabbitmq") -IgnoreExit
    foreach ($v in $vols) {
        $name = "$v".Trim()
        if ($name) {
            Invoke-Docker @("volume", "rm", $name) -IgnoreExit | Out-Null
        }
    }
}

function Print-Diagnostics {
    Write-Host "--- docker compose ps -a ---"
    Invoke-Docker @("compose", "ps", "-a") -IgnoreExit | ForEach-Object { Write-Host "$_" }

    Write-Host "--- rabbitmq list_queues ---"
    Invoke-Docker @(
        "compose", "exec", "-T", "rabbitmq",
        "rabbitmqctl", "list_queues",
        "name", "messages", "messages_ready", "messages_unacknowledged", "consumers", "auto_delete"
    ) -IgnoreExit | ForEach-Object { Write-Host "$_" }

    Write-Host "--- rabbitmq list_policies ---"
    Invoke-Docker @(
        "compose", "exec", "-T", "rabbitmq",
        "rabbitmqctl", "list_policies",
        "name", "pattern", "definition", "priority"
    ) -IgnoreExit | ForEach-Object { Write-Host "$_" }

    Write-Host "--- rabbitmq logs tail ---"
    Invoke-Docker @("compose", "logs", "--tail=80", "rabbitmq") -IgnoreExit | ForEach-Object { Write-Host "$_" }
}

function Print-WorkerDiagnostics {
    Write-Host "--- worker env prefetch ---"

    $ids = Invoke-Docker @("compose", "ps", "-q", "worker") -IgnoreExit

    foreach ($id in $ids) {
        $x = "$id".Trim()

        if ($x) {
            $py = "import os; print('PREFETCH_COUNT=' + os.getenv('PREFETCH_COUNT', 'unset')); print('CONCURRENCY_LIMIT=' + os.getenv('CONCURRENCY_LIMIT', 'unset'))"
            Invoke-Docker @("exec", "-T", $x, "python", "-c", $py) -IgnoreExit | ForEach-Object { Write-Host "$_" }
        }
    }

    Write-Host "--- worker logs qos/listening ---"
    Invoke-Docker @("compose", "logs", "worker", "--tail=120") -IgnoreExit |
        Select-String "worker_listening|aio_pika_channel_qos" |
        ForEach-Object { Write-Host "$_" }

    Write-Host "--- rabbitmq channels prefetch ---"
    Invoke-Docker @(
        "compose", "exec", "-T", "rabbitmq",
        "rabbitmqctl", "list_channels", "name", "prefetch_count"
    ) -IgnoreExit | ForEach-Object { Write-Host "$_" }
}

function Set-QueuePolicy($size) {
    & uv run python scripts/set_policy.py --size $size
    if ($LASTEXITCODE -ne 0) {
        throw "set_policy failed"
    }

    Print-Diagnostics
}

function Ensure-Queue {
    & uv run --extra dev python scripts/ensure_queue.py
    if ($LASTEXITCODE -ne 0) {
        throw "ensure_queue failed"
    }

    Print-Diagnostics
}

function Wait-WorkerListening {
    for ($i = 0; $i -lt 100; $i++) {
        Start-Sleep -Seconds 3

        $st = Invoke-Docker @("compose", "ps", "-a", "--format", "{{.Status}}", "worker")
        $stText = ($st | ForEach-Object { "$_" }) -join " "

        if ($stText -match "Exited") {
            Write-Host "--- worker exited, logs ---"
            Invoke-Docker @("compose", "logs", "worker", "--tail=60") | ForEach-Object { Write-Host "$_" }
            Write-Host "--- mlflow logs tail ---"
            Invoke-Docker @("compose", "logs", "--tail=40", "mlflow") -IgnoreExit | ForEach-Object { Write-Host "$_" }
            throw "Worker exited during startup (see logs above)."
        }

        $log = Invoke-Docker @("compose", "logs", "worker", "--tail=40")
        $logText = ($log | ForEach-Object { "$_" }) -join "`n"

        if ($logText -match "worker_listening") {
            return
        }

        if ($logText -match "RESOURCE_DOES_NOT_EXIST|PRECONDITION_FAILED|inequivalent|Traceback") {
            $log | ForEach-Object { Write-Host "$_" }
            throw "Worker crashed during startup (see logs above)."
        }
    }

    Write-Host "--- worker status ---"
    Invoke-Docker @("compose", "ps", "-a", "worker") | ForEach-Object { Write-Host "$_" }
    Write-Host "--- worker logs (last 60) ---"
    Invoke-Docker @("compose", "logs", "worker", "--tail=60") | ForEach-Object { Write-Host "$_" }
    Print-Diagnostics
    throw "Worker did not reach worker_listening in 300s."
}

function Read-FinalDepth($tag) {
    $p = "results\$tag.final"
    if (Test-Path $p) {
        $raw = Get-Content $p -Raw -ErrorAction SilentlyContinue
        if ($null -ne $raw) {
            $txt = $raw.Trim()
            if ($txt) {
                return [int]$txt
            }
        }
    }
    return -1
}

function Read-Drain($tag) {
    $p = "results\$tag.drain"
    if (Test-Path $p) {
        $raw = Get-Content $p -Raw -ErrorAction SilentlyContinue
        if ($null -ne $raw) {
            $txt = $raw.Trim()
            if ($txt) {
                return [double]$txt
            }
        }
    }
    return $null
}

function Count-Nacks($logPath) {
    if (-not (Test-Path $logPath)) {
        return 0
    }
    return @(Get-Content $logPath | Select-String "backpressure_nack").Count
}

function Last-Published($logPath) {
    if (-not (Test-Path $logPath)) {
        return 0
    }

    $m = Get-Content $logPath | Select-String "published=(\d+)" | Select-Object -Last 1
    if ($m -match "published=(\d+)") {
        return [int]$Matches[1]
    }

    return 0
}

Build-Experiment-Images

foreach ($cfg in $TAGS) {
    $tag = $cfg.tag

    Write-Host "=== $tag (size=$($cfg.size), scale=$($cfg.scale), phase=$($cfg.phase)) ==="

    $env:MAX_QUEUE_SIZE = $cfg.size

    Reset-Cold-Rabbit-Warm-MLflow

    $infra = Get-InfraServices

    if ($infra.Count -gt 0) {
        Write-Host "[$tag] starting infra services: $($infra -join ', ')"
        Invoke-Docker (@("compose", "up", "-d") + $infra) | Out-Null
    }

    Wait-MlflowReady

    Set-QueuePolicy $cfg.size

    Invoke-Docker @(
        "compose", "up", "-d",
        "--scale", "worker=$($cfg.scale)",
        "worker"
    ) | Out-Null

    Wait-WorkerListening

    if ($cfg.phase -eq "live") {
        Print-WorkerDiagnostics
    }

    $cap = "30"
    $drainFlag = $false

    if ($cfg.phase -eq "stopped") {
        Invoke-Docker @("compose", "stop", "worker") | Out-Null
        Write-Host "[$tag] workers stopped"
        Ensure-Queue
        $cap = "30"
        $drainFlag = $false
    } else {
        Write-Host "[$tag] workers live"
        $cap = "180"
        $drainFlag = $true
    }

    $logOut = "logs_$tag.txt"
    $logErr = "logs_$tag.err"

    Start-Process docker `
        -ArgumentList "compose", "run", "--rm", "-e", "MAX_QUEUE_SIZE=$($cfg.size)", "producer" `
        -RedirectStandardOutput $logOut `
        -RedirectStandardError $logErr `
        -NoNewWindow

    $measureArgs = @(
        "run", "python", "scripts/measure.py",
        "--tag", $tag,
        "--fast", "1.0",
        "--slow", "5.0",
        "--tail", "3",
        "--cap", $cap
    )

    if ($drainFlag) {
        $measureArgs += "--drain"
    }

    & uv @measureArgs
    if ($LASTEXITCODE -ne 0) {
        throw "[$tag] measure failed"
    }

    Kill-Producer

    & uv run python scripts/extract_rate.py --tag $tag
    if ($LASTEXITCODE -ne 0) {
        throw "[$tag] extract_rate failed"
    }

    $final = Read-FinalDepth $tag
    $drain = Read-Drain $tag
    $nacks = Count-Nacks $logOut
    $published = Last-Published $logOut

    if ($final -lt 0) {
        Write-Host "--- diagnostics on missing final ---"
        Print-Diagnostics
        Write-Host "--- producer log tail ---"
        Get-Content $logOut -Tail 60 | ForEach-Object { Write-Host "$_" }
        throw "[$tag] no final depth from rabbitmqctl, abort."
    }

    if ($cfg.phase -eq "stopped" -and $cfg.size -eq "100000") {
        if ($final -lt 21 -or $published -ne 88) {
            Write-Host "--- diagnostics on baseline stopped failure ---"
            Print-Diagnostics
            Write-Host "--- producer log tail ---"
            Get-Content $logOut -Tail 60 | ForEach-Object { Write-Host "$_" }
            throw "[$tag] baseline stopped must reach full packet (got final=$final published=$published)."
        }
    }

    if ($cfg.phase -eq "stopped" -and $cfg.size -eq "20") {
        if ($final -lt 1 -or $final -gt 20 -or $nacks -lt 1) {
            Write-Host "--- diagnostics on bp stopped failure ---"
            Print-Diagnostics
            Write-Host "--- producer log tail ---"
            Get-Content $logOut -Tail 60 | ForEach-Object { Write-Host "$_" }
            throw "[$tag] bp stopped must clamp at 20 with rejections (got final=$final nacks=$nacks published=$published)."
        }
    }

    if ($cfg.phase -eq "live") {
        if ($null -eq $drain -or $drain -le 0) {
            Write-Host "--- diagnostics on live drain failure ---"
            Print-Diagnostics
            Write-Host "--- producer log tail ---"
            Get-Content $logOut -Tail 80 | ForEach-Object { Write-Host "$_" }
            throw "[$tag] live drain was not reached within cap=$cap."
        }

        if ($final -ne 0) {
            Write-Host "--- diagnostics on live drain final failure ---"
            Print-Diagnostics
            Write-Host "--- producer log tail ---"
            Get-Content $logOut -Tail 80 | ForEach-Object { Write-Host "$_" }
            throw "[$tag] live final depth must be zero after drain (got final=$final)."
        }

        if ($published -ne 88) {
            Write-Host "--- diagnostics on live publication failure ---"
            Print-Diagnostics
            Write-Host "--- producer log tail ---"
            Get-Content $logOut -Tail 80 | ForEach-Object { Write-Host "$_" }
            throw "[$tag] live producer did not publish all 88 tasks (got published=$published)."
        }
    }

    Write-Host "[$tag] final_depth=$final drain=$drain nacks=$nacks published=$published phase=$($cfg.phase)"
}

Remove-Item Env:MAX_QUEUE_SIZE -ErrorAction SilentlyContinue

& uv run python scripts/report.py
if ($LASTEXITCODE -ne 0) {
    throw "report failed"
}

& uv run python scripts/update_readme.py
if ($LASTEXITCODE -ne 0) {
    throw "update_readme failed"
}