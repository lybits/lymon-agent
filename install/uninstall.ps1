# Lymon Agent uninstaller (Windows) — removes the agent installed by agent.ps1:
# the Windows service, the self-update scheduled task, the binaries/plugins
# under Program Files and the mutable state under ProgramData. Also cleans up
# the LEGACY per-user scheduled-task install (%LOCALAPPDATA%\LymonAgent) so a
# box upgraded across models ends up fully clean.
#
# Requires an ELEVATED PowerShell. One-liner the portal generates:
#
#   & ([scriptblock]::Create((irm https://host/install/uninstall.ps1)))
#
# -KeepData leaves ProgramData\LymonAgent (buffer, logs) in place.
param(
  [switch]$KeepData
)
$ErrorActionPreference = 'Continue'

$isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) { throw 'Run this in an ELEVATED PowerShell (Run as administrator) — removing a Windows service needs admin.' }

$svc = 'LymonAgent'

# ── Stop + remove the self-update scheduled task ───────────────────────────
foreach ($t in 'LymonAgentUpdate', 'LymonAgent') {
  if (Get-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue) {
    Stop-ScheduledTask -TaskName $t -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $t -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed scheduled task '$t'."
  }
}

# ── Stop + delete the Windows service ──────────────────────────────────────
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) {
  Stop-Service -Name $svc -Force -ErrorAction SilentlyContinue
  Start-Sleep -Seconds 2
  & sc.exe delete $svc | Out-Null
  Write-Host "Removed Windows service '$svc'."
}

# ── Make sure no stray agent process keeps the files locked ────────────────
Get-Process -Name 'lymon-agent' -ErrorAction SilentlyContinue | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Milliseconds 500

# ── Remove binaries + plugins (Program Files) and legacy per-user install ──
$dir = Join-Path $env:ProgramFiles 'LymonAgent'
$oldUserDir = Join-Path $env:LOCALAPPDATA 'LymonAgent'
foreach ($d in @($dir, $oldUserDir)) {
  if (Test-Path $d) {
    Remove-Item -Recurse -Force $d -ErrorAction SilentlyContinue
    Write-Host "Removed $d."
  }
}

# ── Mutable state (buffer, update trigger, logs) under ProgramData ─────────
$dataDir = Join-Path $env:ProgramData 'LymonAgent'
if (Test-Path $dataDir) {
  if ($KeepData) {
    Write-Host "Kept data dir ($dataDir) — remove it manually to wipe the buffer/logs."
  } else {
    Remove-Item -Recurse -Force $dataDir -ErrorAction SilentlyContinue
    Write-Host "Removed data dir ($dataDir)."
  }
}

Write-Host "OK - Lymon Agent uninstalled."
