# Run from this project or an unpacked release archive.
$ErrorActionPreference = 'Stop'
$amassExecutable = Join-Path $PSScriptRoot 'bin\Amass.exe'
if (-not (Test-Path -LiteralPath $amassExecutable)) {
    $amassExecutable = Join-Path $PSScriptRoot 'target\release\amass.exe'
}
if (-not (Test-Path -LiteralPath $amassExecutable)) {
    $amassExecutable = Join-Path $env:LOCALAPPDATA 'Amass\bin\Amass.exe'
}
if (-not (Test-Path -LiteralPath $amassExecutable)) {
    throw 'Build first: cargo build --release --workspace'
}
& $amassExecutable @args
exit $LASTEXITCODE
