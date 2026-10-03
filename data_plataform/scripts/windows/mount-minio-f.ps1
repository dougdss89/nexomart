# Re-attaches the dedicated data disk (MinIO + shared container volumes) (F:\WSLDisks\minio-data.vhdx) to WSL2 and
# mounts it inside Ubuntu-22.04 at /mnt/wsl/minio-f, which minio/docker-compose.yml
# bind-mounts (${MINIO_DATA_DIR}) into the minio container. `wsl --mount` does not persist across
# `wsl --shutdown` / a Windows restart, so this must re-run on every boot/logon.
#
# Registered as a Scheduled Task (run at logon) - see scripts/windows/README.md.

$ErrorActionPreference = "Stop"

$VhdPath    = "F:\WSLDisks\minio-data.vhdx"
$Distro     = "Ubuntu-22.04"
$MountPoint = "/mnt/wsl/minio-f"
$Label      = "minio-f"

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "minio-f already mounted at $MountPoint - nothing to do."
    exit 0
}

if (-not (Test-Path $VhdPath)) {
    Write-Error "VHD not found at $VhdPath - check the drive letter/path (was F: reassigned?)."
    exit 1
}

Write-Output "Attaching $VhdPath to WSL2..."
& wsl.exe --mount --vhd $VhdPath --bare
# Non-fatal: if it's already attached (e.g. re-run), wsl --mount errors but we can still proceed to mount.

& wsl.exe -d $Distro -u root -- mkdir -p $MountPoint
& wsl.exe -d $Distro -u root -- mount -t ext4 LABEL=$Label $MountPoint
& wsl.exe -d $Distro -u root -- mkdir -p "$MountPoint/data"
# Shared volumes of the other containers (CONTAINERS_DATA_DIR in configs/secrets.env); the Makefile
# `init` target binds docker volumes to these. World-writable because containers run as different uids.
foreach ($v in "dockervol", "spark-logs", "spark-ivy-cache", "uc-data") {
    & wsl.exe -d $Distro -u root -- mkdir -p "$MountPoint/containers/$v"
}
& wsl.exe -d $Distro -u root -- chmod 777 "$MountPoint/containers"

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "Mounted $Label at $MountPoint successfully."
} else {
    Write-Error "Mount attempt finished but $MountPoint is not mounted - check manually."
    exit 1
}
