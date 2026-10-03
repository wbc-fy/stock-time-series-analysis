param(
    [int]$ReadyTimeoutSeconds = 120
)

$ErrorActionPreference = 'Stop'
$systemRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $systemRoot 'infrastructure/docker-compose.yml'
$javaServices = Join-Path $systemRoot 'java-services'
$jarOnHost = Join-Path $javaServices 'flink-realtime-job/target/flink-realtime-job-0.3.0-SNAPSHOT-all.jar'
$jarInContainer = '/opt/flink/usrlib/flink-realtime-job-0.3.0-SNAPSHOT-all.jar'

function Get-Setting([string]$name, [string]$fallback) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ([string]::IsNullOrWhiteSpace($value)) { return $fallback }
    return $value
}

$jdkHome = Get-Setting 'FLINK_JAVA_HOME' (Get-Setting 'JAVA_HOME' '')
if ([string]::IsNullOrWhiteSpace($jdkHome)) {
    throw 'Set FLINK_JAVA_HOME or JAVA_HOME to a JDK 21 installation before submitting.'
}
$javaExe = Join-Path $jdkHome 'bin/java.exe'
if (-not (Test-Path $javaExe)) { throw "JDK executable not found: $javaExe" }
$previousErrorActionPreference = $ErrorActionPreference
try {
    # Windows PowerShell 5.1 promotes native stderr to a terminating error under Stop.
    $ErrorActionPreference = 'Continue'
    $javaVersion = (& $javaExe -version 2>&1 | Out-String)
    $versionExitCode = $LASTEXITCODE
}
finally {
    $ErrorActionPreference = $previousErrorActionPreference
}
if ($versionExitCode -ne 0 -or $javaVersion -notmatch '(?:openjdk|java) version "21(?:\.|\")') {
    throw "JDK 21 is required; found: $javaVersion"
}
$env:JAVA_HOME = $jdkHome
$env:PATH = "$(Join-Path $jdkHome 'bin');$env:PATH"

Push-Location $javaServices
try {
    & mvn -q -pl flink-realtime-job -am package
    if ($LASTEXITCODE -ne 0) { throw 'Maven package failed.' }
}
finally {
    Pop-Location
}
if (-not (Test-Path $jarOnHost)) { throw "Shaded JAR not found: $jarOnHost" }

$deadline = (Get-Date).AddSeconds($ReadyTimeoutSeconds)
do {
    try {
        $null = Invoke-RestMethod -Uri 'http://localhost:8082/overview' -TimeoutSec 5
        $taskmanagers = Invoke-RestMethod -Uri 'http://localhost:8082/taskmanagers' -TimeoutSec 5
        if ($null -ne $taskmanagers.taskmanagers -and $taskmanagers.taskmanagers.Count -eq 1) { break }
    }
    catch {
        # The Session Cluster may still be starting.
    }
    if ((Get-Date) -ge $deadline) {
        throw 'Flink REST API did not become ready with exactly one TaskManager.'
    }
    Start-Sleep -Seconds 5
} while ($true)

$running = & docker compose -f $composeFile exec -T flink-jobmanager flink list -r
if ($LASTEXITCODE -ne 0) { throw 'Could not list running Flink jobs.' }
if ($running -match 'stock-daily-indicator-v1') {
    Write-Host 'stock-daily-indicator-v1 is already running; no duplicate submitted.'
    return
}

# These options match FlinkJobConfig; environment values override its Docker defaults.
$jobArgs = @(
    '--bootstrap-servers', (Get-Setting 'FLINK_KAFKA_BOOTSTRAP_SERVERS' 'kafka:29092'),
    '--input-topic', (Get-Setting 'FLINK_INPUT_TOPIC' 'stock.ods.daily.v1'),
    '--output-topic', (Get-Setting 'FLINK_OUTPUT_TOPIC' 'stock.dws.daily-indicator.v1'),
    '--late-topic', (Get-Setting 'FLINK_LATE_TOPIC' 'stock.late.daily.v1'),
    '--dead-letter-topic', (Get-Setting 'FLINK_DLT_TOPIC' 'stock.flink.dead-letter.v1'),
    '--group-id', (Get-Setting 'FLINK_CONSUMER_GROUP' 'stock-flink-daily-indicator-v1'),
    '--checkpoint-uri', (Get-Setting 'FLINK_CHECKPOINT_URI' 'file:///opt/flink/checkpoints'),
    '--checkpoint-interval-ms', (Get-Setting 'FLINK_CHECKPOINT_INTERVAL_MS' '10000'),
    '--checkpoint-timeout-ms', (Get-Setting 'FLINK_CHECKPOINT_TIMEOUT_MS' '60000'),
    '--checkpoint-min-pause-ms', (Get-Setting 'FLINK_CHECKPOINT_MIN_PAUSE_MS' '5000'),
    '--kafka-transaction-timeout-ms', (Get-Setting 'FLINK_KAFKA_TRANSACTION_TIMEOUT_MS' '600000'),
    '--parallelism', (Get-Setting 'FLINK_PARALLELISM' '3'),
    '--schema-version', (Get-Setting 'FLINK_SCHEMA_VERSION' '1'),
    '--deployment-namespace', (Get-Setting 'FLINK_DEPLOYMENT_NAMESPACE' 'stock-flink')
)
& docker compose -f $composeFile exec -T flink-jobmanager flink run -d -m flink-jobmanager:8081 -c com.stock.flink.DailyIndicatorJob $jarInContainer @jobArgs
if ($LASTEXITCODE -ne 0) { throw 'Flink job submission failed.' }
