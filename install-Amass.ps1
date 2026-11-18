param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
$amassRoot = $PSScriptRoot
$amassSource = Join-Path $amassRoot 'bin\Amass.exe'
if (-not (Test-Path -LiteralPath $amassSource)) {
    if (-not $SkipBuild) {
        Push-Location $amassRoot
        try {
            cargo build --release -p amass -j 2
            if ($LASTEXITCODE -ne 0) { throw 'Amass build failed' }
        } finally { Pop-Location }
    }
    $amassSource = Join-Path $amassRoot 'target\release\amass.exe'
}
if (-not (Test-Path -LiteralPath $amassSource)) { throw 'Amass.exe is missing; build the project first' }
$amassBin = Join-Path $env:LOCALAPPDATA 'Amass\bin'
New-Item -ItemType Directory -Force -Path $amassBin | Out-Null
Copy-Item -LiteralPath $amassSource -Destination (Join-Path $amassBin 'Amass.exe') -Force
$amassUserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
$amassEntries = @($amassUserPath -split ';' | Where-Object { $_ })
$amassPresent = $amassEntries | Where-Object { [Environment]::ExpandEnvironmentVariables($_).TrimEnd('\') -ieq $amassBin.TrimEnd('\') }
if (-not $amassPresent) {
    $amassNextPath = (@($amassEntries) + $amassBin) -join ';'
    [Environment]::SetEnvironmentVariable('Path', $amassNextPath, 'User')
}
$env:Path = $amassBin + ';' + $env:Path
Write-Host "Installed: $amassBin\Amass.exe"
Write-Host 'Open a new terminal and type: Amass'
Write-Host 'To start in the current terminal, use the absolute path above.'
