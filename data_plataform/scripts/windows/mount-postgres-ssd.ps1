# Re-attaches the Postgres data disk (L:\WSLDisks\postgres-data.vhdx) to WSL2; `--name postgres-ssd` makes WSL
# mount it at /mnt/wsl/postgres-ssd, which postgres/docker-compose.yml bind-mounts (${POSTGRES_DATA_DIR}).
# `wsl --mount` does not survive a WSL VM restart, so this runs at logon and every 5 minutes
# (registered by setup-postgres-mount-task.ps1). Safe to re-run: exits when already mounted.

$ErrorActionPreference = "Stop"

$VhdPath    = "L:\WSLDisks\postgres-data.vhdx"
$Distro     = "Ubuntu-22.04"
$MountPoint = "/mnt/wsl/postgres-ssd"
$Name       = "postgres-ssd"

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "$Name already mounted at $MountPoint - nothing to do."
    exit 0
}

if (-not (Test-Path $VhdPath)) {
    Write-Error "VHD not found at $VhdPath - check the drive letter/path."
    exit 1
}

Write-Output "Attaching $VhdPath to WSL2..."
& wsl.exe --mount --vhd $VhdPath --name $Name
# Non-fatal if it was already attached (the mount below is what matters).

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "Mounted $Name at $MountPoint successfully."
} else {
    Write-Error "Attach finished but $MountPoint is not mounted - check manually."
    exit 1
}
