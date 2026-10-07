# ONE-TIME, from a PowerShell prompt: registers the scheduled task that keeps the WSL2 VM warm at
# logon and every 5 minutes, so the Postgres/MinIO mount tasks stop cold-booting the VM and flashing
# the systemd boot console on the desktop. No elevation needed at runtime (it only runs wsl.exe), so
# this registers a Limited task for the current user. Safe to re-run.
#
#   powershell.exe -ExecutionPolicy Bypass -File "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows\setup-wsl-keepalive.ps1"

$ErrorActionPreference = "Stop"
$Repo  = "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows"
$Local = "C:\Users\Doug\scripts"

New-Item -ItemType Directory -Force $Local | Out-Null
Copy-Item "$Repo\wsl-keepalive.ps1" $Local -Force

# Run through `conhost --headless` so neither PowerShell nor the child wsl.exe calls flash a console
# window (same wrapper the mount tasks use). This covers every tick where the VM is already up; the
# only run that can still show a console is the rare one that has to cold-boot a stopped VM, which is
# exactly what this task then prevents for all subsequent ticks.
$action    = New-ScheduledTaskAction -Execute "conhost.exe" -Argument "--headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Local\wsl-keepalive.ps1`""
$trigger   = New-ScheduledTaskTrigger -AtLogOn
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)).Repetition
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
# ExecutionTimeLimit = zero (PT0S) disables the limit: the action blocks forever on `sleep infinity`
# to hold the session open, so it must not be killed on a timeout. IgnoreNew keeps a single holder.
$settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName "KeepAliveWSL" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Task KeepAliveWSL registered (logon + every 5 minutes). Done."
