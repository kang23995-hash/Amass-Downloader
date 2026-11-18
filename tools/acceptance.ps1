# Run a real, isolated, full-file acceptance test. Requires the publisher's hash.
param(
    [Parameter(Mandatory=$true)][string]$Url,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-fA-F]{64}$')][string]$Sha256,
    [ValidateRange(1,10)][int]$Runs = 1
)
$ErrorActionPreference = 'Stop'
$amassRoot = Split-Path $PSScriptRoot -Parent
$amassExe = Join-Path $amassRoot 'target\release\rd-cli.exe'
if (-not (Test-Path -LiteralPath $amassExe)) { $amassExe = Join-Path $amassRoot 'bin\rd-cli.exe' }
if (-not (Test-Path -LiteralPath $amassExe)) { throw 'Build first: cargo build --release --workspace' }
$amassStamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$amassRunRoot = Join-Path $amassRoot "diagnostics\acceptance-$amassStamp"
New-Item -ItemType Directory -Path $amassRunRoot | Out-Null
for ($amassRun = 1; $amassRun -le $Runs; $amassRun++) {
    $amassDir = Join-Path $amassRunRoot "run-$amassRun"
    New-Item -ItemType Directory -Path $amassDir | Out-Null
    $amassDb = Join-Path $amassDir 'tasks.db'
    $amassLog = Join-Path $amassDir 'download.log'
    & $amassExe $Url --db $amassDb -o $amassDir --sha256 $Sha256 2>&1 | Tee-Object -FilePath $amassLog
    if ($LASTEXITCODE -ne 0) { throw "Run $amassRun failed; see $amassLog" }
}
Write-Output "Verified downloads and logs: $amassRunRoot"
