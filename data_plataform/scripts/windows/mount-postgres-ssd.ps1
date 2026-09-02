# Re-attaches the dedicated Postgres data disk (L:\WSLDisks\postgres-data.vhdx) to WSL2 and
# mounts it inside Ubuntu-22.04 at /mnt/wsl/postgres-ssd, which postgres/docker-compose.yml
# bind-mounts into the pgprojeto container. `wsl --mount` does not persist across
# `wsl --shutdown` / a Windows restart, so this must re-run on every boot/logon.
#
# Registered as a Scheduled Task (run at logon) — see scripts/windows/README.md.

$ErrorActionPreference = "Stop"

$VhdPath    = "L:\WSLDisks\postgres-data.vhdx"
$Distro     = "Ubuntu-22.04"
$MountPoint = "/mnt/wsl/postgres-ssd"

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "postgres-ssd already mounted at $MountPoint - nothing to do."
    exit 0
}

if (-not (Test-Path $VhdPath)) {
    Write-Error "VHD not found at $VhdPath - check the drive letter/path (was L: reassigned?)."
    exit 1
}

Write-Output "Attaching $VhdPath to WSL2..."
& wsl.exe --mount --vhd $VhdPath --bare
# Non-fatal: if it's already attached (e.g. re-run), wsl --mount errors but we can still proceed to mount.

& wsl.exe -d $Distro -u root -- mkdir -p $MountPoint
& wsl.exe -d $Distro -u root -- mount -t ext4 LABEL=postgres-ssd $MountPoint

& wsl.exe -d $Distro -u root -- mountpoint -q $MountPoint
if ($LASTEXITCODE -eq 0) {
    Write-Output "Mounted postgres-ssd at $MountPoint successfully."
} else {
    Write-Error "Mount attempt finished but $MountPoint is not mounted - check manually."
    exit 1
}
