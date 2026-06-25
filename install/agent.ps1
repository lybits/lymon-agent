# Lymon Agent installer (Windows) — installs the agent as a NATIVE WINDOWS
# SERVICE (LocalSystem, auto-start). Survives reboots with no interactive
# logon and shows no console window (unlike the old scheduled-task model).
#
# Requires an ELEVATED PowerShell. Azure-Arc-style one-liner:
#
#   & ([scriptblock]::Create((irm https://get.lymon.io/agent.ps1))) `
#       -EnrollCode "<CODE>" -EnrollUrl "https://host/api/agent/enroll" `
#       -Datasource "planta-1"
#
# The agent self-enrolls with the one-time code on first start; the permanent
# token never travels in the command.
param(
  [string]$EnrollCode = $env:LYMON_ENROLL_CODE,
  [string]$EnrollUrl  = $env:LYMON_ENROLL_URL,
  [string]$Datasource = $env:LYMON_DATASOURCE_ID,
  [string]$ModbusHost = $env:LYMON_MODBUS_HOST,
  [string]$ModbusPort = $env:LYMON_MODBUS_PORT,
  [string]$Version    = $env:LYMON_AGENT_VERSION,
  [string]$Repo       = $env:LYMON_AGENT_REPO
)
$ErrorActionPreference = 'Stop'

# A Windows service + LocalSystem need admin. Fail early with a clear hint.
$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) { throw 'Run this in an ELEVATED PowerShell (Run as administrator) — the agent installs as a Windows service.' }

$repo    = if ($Repo) { $Repo } else { 'lybits/lymon-agent' }
$version = if ($Version) { $Version } else { 'latest' }
if (-not $EnrollCode -or -not $EnrollUrl) { throw 'Provide -EnrollCode and -EnrollUrl (or set $env:LYMON_ENROLL_CODE / _URL).' }

# Migrate from the OLD scheduled-task model (per-user tasks + %LOCALAPPDATA%
# install): remove its tasks and per-user files so we don't run two agents.
# Safe no-op on a fresh box.
foreach ($t in 'LymonAgent', 'LymonAgentUpdate') {
  if (Get-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $t -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed old scheduled task '$t'."
  }
}
$oldUserDir = Join-Path $env:LOCALAPPDATA 'LymonAgent'
if (Test-Path $oldUserDir) {
  Get-Process -Name 'lymon-agent' -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
  Start-Sleep -Milliseconds 500
  Remove-Item -Recurse -Force $oldUserDir -ErrorAction SilentlyContinue
  Write-Host "Removed old per-user install ($oldUserDir)."
}

# Binaries + plugins under Program Files; mutable state (buffer, credentials,
# update trigger, logs) under ProgramData — both readable by the LocalSystem
# service and stable across reboots.
$svc = 'LymonAgent'
$dir = Join-Path $env:ProgramFiles 'LymonAgent'
$dataDir = Join-Path $env:ProgramData 'LymonAgent'
New-Item -ItemType Directory -Force -Path $dir, $dataDir | Out-Null
$exe = Join-Path $dir 'lymon-agent.exe'
$pluginsDir = Join-Path $dir 'plugins'
$bufferPath = Join-Path $dataDir 'buffer.db'

# ── Download + unpack the platform bundle (agent + plugins) ────────────────
$bundle = 'lymon-agent-windows-x86_64'
$asset  = "$bundle.zip"
$dl = if ($version -eq 'latest') {
  "https://github.com/$repo/releases/latest/download/$asset"
} else {
  "https://github.com/$repo/releases/download/$version/$asset"
}
Write-Host "Downloading lymon-agent bundle ($asset) ..."
$zip = Join-Path $env:TEMP $asset
Invoke-WebRequest -Uri $dl -OutFile $zip -UseBasicParsing
$extract = Join-Path $env:TEMP 'lymon-agent-extract'
if (Test-Path $extract) { Remove-Item -Recurse -Force $extract }
Expand-Archive -Path $zip -DestinationPath $extract -Force

# Stop an existing service before overwriting its files.
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
  Stop-Service -Name $svc -Force -ErrorAction SilentlyContinue
  Start-Sleep -Seconds 2
}
Copy-Item -Force (Join-Path $extract "$bundle\lymon-agent.exe") $exe
if (Test-Path $pluginsDir) { Remove-Item -Recurse -Force $pluginsDir }
Copy-Item -Recurse -Force (Join-Path $extract "$bundle\plugins") $pluginsDir
Remove-Item $zip -Force -ErrorAction SilentlyContinue
Remove-Item $extract -Recurse -Force -ErrorAction SilentlyContinue

# ── Config (PowerShell 5.1 has no ?? operator) ─────────────────────────────
function Def($v, $d) { if ([string]::IsNullOrEmpty($v)) { $d } else { $v } }
$datasource = Def $Datasource 'default-source'
$modbusHost = Def $ModbusHost 'CHANGE_ME'
$modbusPort = Def $ModbusPort '502'

# ── Register the service ───────────────────────────────────────────────────
# Recreate cleanly so re-running the installer is idempotent.
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
  sc.exe delete $svc | Out-Null
  Start-Sleep -Seconds 2
}
$bin = '"' + $exe + '" --service'
New-Service -Name $svc -BinaryPathName $bin -DisplayName 'Lymon Agent' `
  -Description 'Lymon edge data collection agent' -StartupType Automatic | Out-Null
# Auto-restart on crash.
& sc.exe failure $svc reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null

# Service config: the agent reads LYMON_* from its environment, and a service
# does NOT inherit a shell's env — set it via the service's Environment value
# (REG_MULTI_SZ), read by the SCM when the service starts.
$envLines = @(
  "LYMON_ENROLL_CODE=$EnrollCode",
  "LYMON_ENROLL_URL=$EnrollUrl",
  "LYMON_DATASOURCE_ID=$datasource",
  "LYMON_MODBUS_HOST=$modbusHost",
  "LYMON_MODBUS_PORT=$modbusPort",
  "LYMON_BUFFER_PATH=$bufferPath",
  "LYMON_PLUGINS_DIR=$pluginsDir"
)
$svcKey = "HKLM:\SYSTEM\CurrentControlSet\Services\$svc"
New-ItemProperty -Path $svcKey -Name 'Environment' -PropertyType MultiString -Value $envLines -Force | Out-Null

Start-Service -Name $svc

# ── Self-update ────────────────────────────────────────────────────────────
# A SYSTEM scheduled task (session 0 → no visible console) polls the trigger
# the agent drops on a cloud "update" command, swaps the whole bundle and
# stops/starts the SERVICE. Writes update.log under the data dir.
$updScript = @'
$ErrorActionPreference = 'Stop'
$dir = '__DIR__'
$dataDir = '__DATADIR__'
$trigger = Join-Path $dataDir 'update-request.json'
$log = Join-Path $dataDir 'update.log'
function Log($m) { "$(Get-Date -Format o)  $m" | Out-File -FilePath $log -Append -Encoding UTF8 }
if (-not (Test-Path $trigger)) { return }
$stopped = $false
Log "update-request found"
try {
  $req = Get-Content $trigger -Raw | ConvertFrom-Json
  $version = "$($req.version)"
  $repo = if ($req.repo) { "$($req.repo)" } else { 'lybits/lymon-agent' }
  if ($version -notmatch '^v?[0-9A-Za-z._-]+$') { throw "bad version '$version'" }
  if ($repo -notmatch '^[0-9A-Za-z._-]+/[0-9A-Za-z._-]+$') { throw "bad repo '$repo'" }
  $bundle = 'lymon-agent-windows-x86_64'
  $tag = if ($version -like 'v*') { $version } else { "v$version" }
  $url = "https://github.com/$repo/releases/download/$tag/$bundle.zip"
  Log "downloading $url"
  $zip = Join-Path $env:TEMP "$bundle.zip"
  Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
  $extract = Join-Path $env:TEMP 'lymon-agent-update'
  if (Test-Path $extract) { Remove-Item -Recurse -Force $extract }
  Expand-Archive -Path $zip -DestinationPath $extract -Force
  Log "stopping service"
  Stop-Service -Name 'LymonAgent' -Force -ErrorAction SilentlyContinue
  $stopped = $true
  for ($i = 0; $i -lt 30 -and (Get-Process -Name 'lymon-agent' -ErrorAction SilentlyContinue); $i++) { Start-Sleep -Milliseconds 500 }
  Get-Process -Name 'lymon-agent' -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
  Start-Sleep -Milliseconds 500
  Log "swapping binary + plugins"
  $exe = Join-Path $dir 'lymon-agent.exe'
  if (Test-Path $exe) { Move-Item -Force $exe "$exe.old" -ErrorAction SilentlyContinue }
  Copy-Item -Force (Join-Path $extract "$bundle\lymon-agent.exe") $exe
  $pluginsDir = Join-Path $dir 'plugins'
  if (Test-Path $pluginsDir) { Remove-Item -Recurse -Force $pluginsDir }
  Copy-Item -Recurse -Force (Join-Path $extract "$bundle\plugins") $pluginsDir
  Remove-Item $trigger -Force
  Remove-Item "$exe.old" -Force -ErrorAction SilentlyContinue
  Log "updated to $tag OK"
} catch {
  Log "ERROR: $($_.Exception.Message)"
  Remove-Item $trigger -Force -ErrorAction SilentlyContinue
} finally {
  if ($stopped) { Start-Service -Name 'LymonAgent' -ErrorAction SilentlyContinue; Log "service (re)started" }
}
'@
$updScript = $updScript.Replace('__DIR__', $dir).Replace('__DATADIR__', $dataDir)
$updPath = Join-Path $dir 'update.ps1'
Set-Content -Path $updPath -Value $updScript -Encoding UTF8
$updAction  = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$updPath`""
# Two triggers, both repeating every 2 min indefinitely:
#  • -Once now: starts polling right after install (no reboot needed).
#  • -AtStartup: re-arms the polling after every boot — so it keeps working
#    even if nobody logs in (the SYSTEM principal already runs without logon;
#    this guarantees the schedule resumes post-reboot regardless).
$tNow = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 2)
$tNow.Repetition.Duration = ''
$tBoot = New-ScheduledTaskTrigger -AtStartup
$tBoot.Repetition.Interval = 'PT2M'
$tBoot.Repetition.Duration = ''
$updPrincipal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
# StartWhenAvailable: run as soon as possible if a scheduled start was missed.
$updSettings  = New-ScheduledTaskSettingsSet -StartWhenAvailable
Register-ScheduledTask -TaskName 'LymonAgentUpdate' -Action $updAction -Trigger @($tNow, $tBoot) `
  -Principal $updPrincipal -Settings $updSettings -Force | Out-Null

Write-Host "OK - Lymon Agent installed as a Windows service ('$svc') and started."
Write-Host "  Status:  Get-Service $svc"
Write-Host "  Updates: scheduled task 'LymonAgentUpdate' (runs as SYSTEM; log in $dataDir\update.log)"
if ($modbusHost -eq 'CHANGE_ME') {
  Write-Host "  Note: configure the agent's sources from the Lymon portal."
}
