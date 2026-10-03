# ONE-TIME, from an ADMINISTRATOR PowerShell. Registers the nightly Postgres maintenance task:
# retention (drop 45d+ landed partitions) plus offline vhdx compaction, at 21:00, retrying once
# an hour later if the run exits non-zero. Safe to re-run (replaces the task).
#
#   powershell.exe -ExecutionPolicy Bypass -File "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows\setup-pg-maintenance-task.ps1"

$ErrorActionPreference = "Stop"
$Repo  = "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows"
$Local = "C:\Users\Doug\scripts"

New-Item -ItemType Directory -Force $Local | Out-Null
Copy-Item "$Repo\pg-maintenance.ps1" $Local -Force

$action    = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Local\pg-maintenance.ps1`""
$trigger   = New-ScheduledTaskTrigger -Daily -At 21:00
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
# Retry once, 1 hour after a failed run; allow up to 2h of runtime for the compaction.
$settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew `
                -RestartInterval (New-TimeSpan -Hours 1) -RestartCount 1 `
                -ExecutionTimeLimit (New-TimeSpan -Hours 2)
Register-ScheduledTask -TaskName "PgMaintenance-Nightly" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Task PgMaintenance-Nightly registered (daily 21:00, retry +1h on failure). Done."
