# Re-attaches the Postgres data disk (L:\WSLDisks\postgres-data.vhdx) to WSL2; `--name postgres-ssd` makes WSL
# mount it at /mnt/wsl/postgres-ssd, which postgres/docker-compose.yml bind-mounts (${POSTGRES_DATA_DIR}).
# `wsl --mount` does not survive a WSL VM restart, so this runs at logon and every 5 minutes
# (registered by setup-postgres-mount-task.ps1). Safe to re-run: exits when already mounted.
#
# The scheduled task runs this through `conhost --headless`, which swallows both the output and the
# exit code (the task always reports Last Result 0), so every mount attempt is logged to $Log. The
# already-mounted fast path is not logged, since it runs every 5 minutes.

$ErrorActionPreference = "Stop"
# wsl.exe writes UTF-16 by default; force UTF-8 so its messages are readable in the log.
$env:WSL_UTF8 = "1"

$VhdPath    = "L:\WSLDisks\postgres-data.vhdx"
$Distro     = "Ubuntu-22.04"
$MountPoint = "/mnt/wsl/postgres-ssd"
$Name       = "postgres-ssd"
$Log        = "C:\Users\Doug\scripts\mount-postgres-ssd.log"

function Log($m) { "{0}  {1}" -f (Get-Date -Format "u"), $m | Out-File -FilePath $Log -Append -Encoding utf8 }

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "$Name already mounted at $MountPoint - nothing to do."
    exit 0
}

if (-not (Test-Path $VhdPath)) {
    Log "ERROR: VHD not found at $VhdPath"
    Write-Error "VHD not found at $VhdPath - check the drive letter/path."
    exit 1
}

Log "not mounted -> attaching $VhdPath"
Write-Output "Attaching $VhdPath to WSL2..."
# Continue around the native call: under Stop, PowerShell 5.1 turns any wsl.exe stderr line
# into a terminating error before its exit code and message could be logged.
$ErrorActionPreference = "Continue"
$out  = & wsl.exe --mount --vhd $VhdPath --name $Name 2>&1 | ForEach-Object { "$_".Trim() } | Where-Object { $_ }
$code = $LASTEXITCODE
$ErrorActionPreference = "Stop"
# Non-fatal if it was already attached (the mount below is what matters).
Log ("wsl --mount exit {0}: {1}" -f $code, ($out -join " | "))

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Log "mounted $Name at $MountPoint"
    Write-Output "Mounted $Name at $MountPoint successfully."
} else {
    Log "ERROR: attach finished but $MountPoint is not mounted"
    Write-Error "Attach finished but $MountPoint is not mounted - check manually."
    exit 1
}
