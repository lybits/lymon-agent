# Lymon Agent uninstaller (Windows). Reverses install.ps1: stops + removes the
# scheduled task and deletes the install + buffer directories. Mirrors the
# install's per-user (default) vs -System layout. One-liner from the portal:
#
#   & ([scriptblock]::Create((irm https://get.lymon.io/uninstall.ps1)))
#   # all-users install (elevated): add -System
#
# Removing the agent here does NOT remove it from the Lymon portal — delete it
# from the Agents page so it stops showing as enrolled/offline.
param([switch]$System)
$ErrorActionPreference = 'SilentlyContinue'

# Stop + unregister the scheduled task.
Stop-ScheduledTask -TaskName 'LymonAgent'
Unregister-ScheduledTask -TaskName 'LymonAgent' -Confirm:$false

# Remove install + buffer dirs (same locations install.ps1 used).
if ($System) {
  Remove-Item -Recurse -Force (Join-Path $env:ProgramFiles 'LymonAgent')
  Remove-Item -Recurse -Force (Join-Path $env:ProgramData 'LymonAgent')
} else {
  $base = $env:LOCALAPPDATA
  if (-not $base) { $base = Join-Path $env:USERPROFILE 'AppData\Local' }
  Remove-Item -Recurse -Force (Join-Path $base 'LymonAgent')
}

Write-Host "OK - lymon-agent uninstalled (scheduled task + files removed)."
Write-Host "  Remember to delete the agent from the Lymon portal -> Agents."
