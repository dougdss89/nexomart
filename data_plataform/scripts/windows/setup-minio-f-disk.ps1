# ONE-TIME setup, run from an ADMINISTRATOR PowerShell (wsl --mount and New-VHD need elevation).
# Creates F:\WSLDisks\minio-data.vhdx (ext4, label minio-f) if missing, mounts it, and registers the
# scheduled task that re-mounts it at every logon and every 5 minutes. Safe to re-run.
#
#   Set-ExecutionPolicy -Scope Process Bypass
#   \\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows\setup-minio-f-disk.ps1

$ErrorActionPreference = "Stop"
$Vhd    = "F:\WSLDisks\minio-data.vhdx"
$Distro = "Ubuntu-22.04"
$Repo   = "\\wsl.localhost\$Distro\home\doug\project\data_plataform\scripts\windows"
$Local  = "C:\Users\Doug\scripts"

New-Item -ItemType Directory -Force (Split-Path $Vhd) | Out-Null
New-Item -ItemType Directory -Force $Local | Out-Null
Copy-Item "$Repo\mount-minio-f.ps1" $Local -Force

if (-not (Test-Path $Vhd)) {
    New-VHD -Path $Vhd -SizeBytes 600GB -Dynamic | Out-Null
}

# Format only when the disk has no minio-f filesystem yet (also covers a vhdx created but never formatted).
& wsl.exe -d $Distro -u root -- mountpoint -q /mnt/wsl/minio-f
$mounted = ($LASTEXITCODE -eq 0)
$hasFs   = (& wsl.exe -d $Distro -u root -- blkid -L minio-f)
if (-not $mounted -and -not $hasFs) {
    $before = (& wsl.exe -d $Distro -u root -- lsblk -dnro NAME) -split "`n"
    wsl.exe --mount --vhd $Vhd --bare
    $dev = ((& wsl.exe -d $Distro -u root -- lsblk -dnro NAME) -split "`n" | Where-Object { $_ -and $_ -notin $before }) | Select-Object -First 1
    if (-not $dev) { throw "Could not identify the new device after wsl --mount (already attached? run wsl --unmount $Vhd and retry)." }
    Write-Output "Formatting /dev/$dev as ext4 (label minio-f)..."
    wsl.exe -d $Distro -u root -- mkfs.ext4 -F -L minio-f "/dev/$dev"
    wsl.exe --unmount $Vhd
}

& "$Local\mount-minio-f.ps1"

# Run through `conhost --headless` so neither PowerShell nor the child wsl.exe calls
# flash a console window on the desktop every 5 minutes (-WindowStyle Hidden only hides
# PowerShell, not wsl.exe, which allocates its own console).
$action    = New-ScheduledTaskAction -Execute "conhost.exe" -Argument "--headless powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Local\mount-minio-f.ps1`""
$trigger   = New-ScheduledTaskTrigger -AtLogOn
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)).Repetition
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
$settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2)
Register-ScheduledTask -TaskName "MountMinioF-WSL" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Task MountMinioF-WSL registered. Done."
