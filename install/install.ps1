# Lymon Agent installer (Windows). Downloads the agent, writes its config,
# and registers a startup task. Azure-Arc-style one-liner — params can be
# passed directly (preferred) or via LYMON_* env vars:
#
#   & ([scriptblock]::Create((irm https://get.lymon.io/agent.ps1))) `
#       -EnrollCode "<CODE>" -EnrollUrl "https://host/api/agent/enroll" `
#       -ModbusHost "10.0.0.10" -Datasource "planta-1"
#
# Installs per-user by default (no admin needed). Add -System from an elevated
# PowerShell for an all-users install that starts at boot.
param(
  [string]$EnrollCode = $env:LYMON_ENROLL_CODE,
  [string]$EnrollUrl  = $env:LYMON_ENROLL_URL,
  [string]$Datasource = $env:LYMON_DATASOURCE_ID,
  [string]$ModbusHost = $env:LYMON_MODBUS_HOST,
  [string]$ModbusPort = $env:LYMON_MODBUS_PORT,
  [string]$Version    = $env:LYMON_AGENT_VERSION,
  [string]$Repo       = $env:LYMON_AGENT_REPO,
  [switch]$System
)
$ErrorActionPreference = 'Stop'

$repo    = if ($Repo) { $Repo } else { 'lybits/lymon-agent' }
$version = if ($Version) { $Version } else { 'latest' }
$code    = $EnrollCode
$url     = $EnrollUrl
if (-not $code -or -not $url) { throw 'Provide -EnrollCode and -EnrollUrl (or set $env:LYMON_ENROLL_CODE / _URL).' }

# Install location. Per-user by default (always writable, no admin). -System
# uses Program Files + a boot task and requires an elevated shell.
if ($System) {
  $dir = Join-Path $env:ProgramFiles 'LymonAgent'
  $bufferDir = Join-Path $env:ProgramData 'LymonAgent'
} else {
  $base = $env:LOCALAPPDATA
  if (-not $base) { $base = Join-Path $env:USERPROFILE 'AppData\Local' }
  if (-not $base) { $base = $env:TEMP }
  $dir = Join-Path $base 'LymonAgent'
  $bufferDir = $dir
}
Write-Host "Install dir: $dir"
New-Item -ItemType Directory -Force -Path $dir | Out-Null
New-Item -ItemType Directory -Force -Path $bufferDir | Out-Null
$exe = Join-Path $dir 'lymon-agent.exe'

$asset = 'lymon-agent-windows-x86_64.exe'
$dl = if ($version -eq 'latest') {
  "https://github.com/$repo/releases/latest/download/$asset"
} else {
  "https://github.com/$repo/releases/download/$version/$asset"
}
Write-Host "Downloading lymon-agent ($asset) ..."
Invoke-WebRequest -Uri $dl -OutFile $exe -UseBasicParsing

# Default helper (Windows PowerShell 5.1 has no ?? operator).
function Def($v, $d) { if ([string]::IsNullOrEmpty($v)) { $d } else { $v } }
$datasource = Def $Datasource 'default-source'
$modbusHost = Def $ModbusHost 'CHANGE_ME'
$modbusPort = Def $ModbusPort '502'
$bufferPath = Join-Path $bufferDir 'buffer.db'

# Self-contained launcher the task runs: sets the config in-process then starts
# the agent (more reliable than relying on persisted env reaching the task).
$launcher = Join-Path $dir 'start-lymon-agent.cmd'
@"
@echo off
set "LYMON_ENROLL_CODE=$code"
set "LYMON_ENROLL_URL=$url"
set "LYMON_DATASOURCE_ID=$datasource"
set "LYMON_MODBUS_HOST=$modbusHost"
set "LYMON_MODBUS_PORT=$modbusPort"
set "LYMON_BUFFER_PATH=$bufferPath"
"%~dp0lymon-agent.exe"
"@ | Set-Content -Path $launcher -Encoding ASCII

# Keep the agent running via a scheduled task (a native service wrapper ships
# in agent v0.3). -System → SYSTEM at boot; default → current user at logon.
$action   = New-ScheduledTaskAction -Execute $launcher
$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
if ($System) {
  $trigger   = New-ScheduledTaskTrigger -AtStartup
  $principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
} else {
  $trigger   = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive
}
Register-ScheduledTask -TaskName 'LymonAgent' -Action $action -Trigger $trigger `
  -Principal $principal -Settings $settings -Force | Out-Null
Start-ScheduledTask -TaskName 'LymonAgent'

Write-Host "OK - lymon-agent installed in $dir and started (Scheduled Task: LymonAgent)."
Write-Host "  Runs in the background; verify ingestion in the Lymon portal."
if ($modbusHost -eq 'CHANGE_ME') {
  Write-Host "  WARNING: no -ModbusHost given; set your PLC/source IP and re-run."
}
