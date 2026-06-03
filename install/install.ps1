# Lymon Agent installer (Windows). Downloads the agent, writes its config,
# and registers a startup task. Driven by the portal one-liner:
#
#   $env:LYMON_ENROLL_CODE="<CODE>"; $env:LYMON_ENROLL_URL="https://host/api/agent/enroll"; `
#     iwr https://get.lymon.io/agent.ps1 -UseBasicParsing | iex
#
# Optional env before running: LYMON_DATASOURCE_ID, LYMON_MODBUS_HOST,
# LYMON_MODBUS_PORT, LYMON_AGENT_VERSION.
$ErrorActionPreference = 'Stop'

$repo    = if ($env:LYMON_AGENT_REPO) { $env:LYMON_AGENT_REPO } else { 'lybits/lymon-agent' }
$version = if ($env:LYMON_AGENT_VERSION) { $env:LYMON_AGENT_VERSION } else { 'latest' }
$code    = $env:LYMON_ENROLL_CODE
$url     = $env:LYMON_ENROLL_URL
if (-not $code -or -not $url) { throw 'Set $env:LYMON_ENROLL_CODE and $env:LYMON_ENROLL_URL first.' }

$dir = 'C:\Program Files\LymonAgent'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$exe = Join-Path $dir 'lymon-agent.exe'

$asset = 'lymon-agent-windows-x86_64.exe'
$dl = if ($version -eq 'latest') {
  "https://github.com/$repo/releases/latest/download/$asset"
} else {
  "https://github.com/$repo/releases/download/$version/$asset"
}
Write-Host "Downloading lymon-agent ($asset) …"
Invoke-WebRequest -Uri $dl -OutFile $exe -UseBasicParsing

# Persist config as machine env vars (read by the agent + the startup task).
[Environment]::SetEnvironmentVariable('LYMON_ENROLL_CODE', $code, 'Machine')
[Environment]::SetEnvironmentVariable('LYMON_ENROLL_URL',  $url,  'Machine')
[Environment]::SetEnvironmentVariable('LYMON_DATASOURCE_ID', ($env:LYMON_DATASOURCE_ID  ?? 'default-source'), 'Machine')
[Environment]::SetEnvironmentVariable('LYMON_MODBUS_HOST',   ($env:LYMON_MODBUS_HOST     ?? 'CHANGE_ME'),     'Machine')
[Environment]::SetEnvironmentVariable('LYMON_MODBUS_PORT',   ($env:LYMON_MODBUS_PORT     ?? '502'),          'Machine')
[Environment]::SetEnvironmentVariable('LYMON_BUFFER_PATH',   (Join-Path $env:ProgramData 'LymonAgent\buffer.db'), 'Machine')

# Run at startup as SYSTEM (native Windows Service wrapper ships in agent v0.3;
# a scheduled task keeps it running 24/7 without a service-aware binary).
$action  = New-ScheduledTaskAction -Execute $exe
$trigger = New-ScheduledTaskTrigger -AtStartup
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1)
Register-ScheduledTask -TaskName 'LymonAgent' -Action $action -Trigger $trigger `
  -Principal $principal -Settings $settings -Force | Out-Null

Start-ScheduledTask -TaskName 'LymonAgent'
Write-Host '✓ lymon-agent installed and started (Task Scheduler: LymonAgent).'
if (($env:LYMON_MODBUS_HOST ?? 'CHANGE_ME') -eq 'CHANGE_ME') {
  Write-Host '  Set LYMON_MODBUS_HOST (machine env) to your PLC/source IP, then restart the task.'
}
