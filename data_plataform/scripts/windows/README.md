# Windows helpers

**First time (or after the disk is lost):** run `setup-minio-f-disk.ps1` from an administrator PowerShell. It creates
and formats `F:\WSLDisks\minio-data.vhdx`, mounts it, and registers the logon/5-minute task below. The same disk
holds MinIO (`data/`) and the shared Docker volumes (`containers/<volume>`, bound by `make init`).

## mount-minio-f.ps1

Re-attaches the MinIO data disk (`F:\WSLDisks\minio-data.vhdx`) to WSL2 and mounts it at
`/mnt/wsl/minio-f`, which `minio/docker-compose.yml` bind-mounts (`MINIO_DATA_DIR`) into `minio`.
`wsl --mount` does not survive a WSL VM restart (`wsl --shutdown`, a Docker Desktop restart, the VM's
idle shutdown, a Windows reboot), so the script has to run again each time. It is safe to re-run: it
exits without doing anything when the disk is already mounted.

A logon-only trigger is not enough: if the VM restarts after logon, nothing remounts the disk. Run the
script on logon and every 5 minutes. Copy it to a Windows path first, so the task does not depend on WSL
being up to read it (re-copy after changing the script):

```powershell
# Administrator PowerShell
New-Item -ItemType Directory -Force C:\Users\Doug\scripts | Out-Null
Copy-Item "\\wsl.localhost\Ubuntu-22.04\home\doug\project\data_plataform\scripts\windows\mount-minio-f.ps1" C:\Users\Doug\scripts\

Unregister-ScheduledTask -TaskName "WSL Mount MinIO F" -Confirm:$false -ErrorAction SilentlyContinue  # old duplicate

$action    = New-ScheduledTaskAction -Execute "powershell.exe" -Argument '-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File "C:\Users\Doug\scripts\mount-minio-f.ps1"'
$trigger   = New-ScheduledTaskTrigger -AtLogOn
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 5)).Repetition
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Highest
$settings  = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 2)
Register-ScheduledTask -TaskName "MountMinioF-WSL" -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force
```

`RunLevel Highest` matters: `wsl --mount` needs administrator rights, and a `Limited` task cannot do it.
Side effect: the check runs `wsl.exe`, which starts the Ubuntu-22.04 distro every 5 minutes, so the WSL VM
no longer idles off while the task is enabled.

Check it: `Get-ScheduledTaskInfo -TaskName "MountMinioF-WSL"` (LastTaskResult 0), and in WSL
`mountpoint /mnt/wsl/minio-f`.

## One-time disk creation (MinIO)

Create the vhdx once, format it as ext4 with label `minio-f`, from an administrator PowerShell:

```powershell
New-Item -ItemType Directory -Force F:\WSLDisks | Out-Null
New-VHD -Path F:\WSLDisks\minio-data.vhdx -SizeBytes 300GB -Dynamic   # needs the Hyper-V PowerShell module
wsl --mount --vhd F:\WSLDisks\minio-data.vhdx --bare
wsl -d Ubuntu-22.04 -u root -- lsblk -f      # find the new, unformatted device (e.g. /dev/sdf)
wsl -d Ubuntu-22.04 -u root -- mkfs.ext4 -L minio-f /dev/sdf   # CONFIRM the device name first
wsl --unmount F:\WSLDisks\minio-data.vhdx
```

`setup-minio-f-disk.ps1` automates all of this. Postgres is not on this disk: it has its own
`L:\WSLDisks\postgres-data.vhdx`, attached by the scheduled task `MountPostgresSSD-WSL` (`mount-postgres-ssd.ps1`, registered by `setup-postgres-mount-task.ps1`; see
`configs/postgresql/README.md`).
