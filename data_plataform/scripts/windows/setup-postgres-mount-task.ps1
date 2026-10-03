# ONE-TIME, from an ADMINISTRATOR PowerShell: registers the scheduled task that re-mounts the Postgres disk at
# logon and every 5 minutes, replacing the two older tasks ("WSL Mount Postgres SSD", logon-only, and
# "MountPostgresSSD-WSL", which pointed at a script that no longer exists). Safe to re-run.
#
#   powershell.exe -ExecutionPolicy Bypass -File "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows\setup-postgres-mount-task.ps1"

$ErrorActionPreference = "Stop"
$Repo  = "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows"
$Local = "C:\Users\Doug\scripts"

New-Item -ItemType Directory -Force $Local | Out-Null
Copy-Item "$Repo\mount-postgres-ssd.ps1" $Local -Force

foreach ($old in "WSL Mount Postgres SSD", "MountPostgresSSD-WSL") {
    Unregister-ScheduledTask -TaskName $old -Confirm:$false -ErrorAction SilentlyContinue
}

$action    = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Local\mount-postgres-ssd.ps1`""
$trigger   = New-ScheduledTaskTrigger -AtLogOn
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)).Repetition
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
$settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2)
Register-ScheduledTask -TaskName "MountPostgresSSD-WSL" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Task MountPostgresSSD-WSL registered (logon + every 5 minutes). Done."
