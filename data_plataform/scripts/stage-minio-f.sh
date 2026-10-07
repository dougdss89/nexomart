#!/usr/bin/env bash
#
# Recreate Docker Desktop's WSL bind-mount staging dirs for minio-f-backed volumes.
#
# Background: Docker Desktop mounts a WSL-backed bind volume by first re-exporting
# the source path into
#     /mnt/wsl/docker-desktop-bind-mounts/<distro>/<sha256(source_path)>
# (rshared, so it propagates into the docker-desktop VM), then bind-mounting that
# staging dir onto the volume's _data. It only creates those re-exports on its own
# for disks that were present when its WSL integration initialised. The minio-f
# disk is attached late (scripts/windows/mount-minio-f.ps1, wsl --mount), so after
# the WSL VM or Docker Desktop restarts the re-exports are missing and every mount
# of a minio-f-backed volume fails with:
#     error while mounting volume '.../_data': failed to mount local volume:
#     mount .../docker-desktop-bind-mounts/<distro>/<hash>:..., flags: 0x1000:
#     no such file or directory
# Restarting Docker Desktop does NOT fix it; the re-exports must be recreated by
# hand. The staging hash is deterministic (sha256 of the source path), so we can
# rebuild each one. Idempotent: an existing staging mountpoint is left untouched.
#
# Must run as root (mount(2)). Source paths are passed as arguments by the Makefile
# so the list of minio-f-backed volumes stays single-sourced there.
#
# Usage: sudo bash scripts/stage-minio-f.sh <source_path> [<source_path> ...]

set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "stage-minio-f: must run as root (needs mount)." >&2
  exit 1
fi

distro="${WSL_DISTRO_NAME:-Ubuntu-22.04}"
stage_root="/mnt/wsl/docker-desktop-bind-mounts/${distro}"

if [ "$#" -eq 0 ]; then
  echo "stage-minio-f: no source paths given; nothing to stage." >&2
  exit 0
fi

mkdir -p "$stage_root"

for src in "$@"; do
  if [ ! -d "$src" ]; then
    # The disk guard (check-minio-disk) runs first, so a missing source here means
    # a misconfigured path rather than an unmounted disk. Warn and keep going.
    echo "stage-minio-f: WARN source does not exist, skipping: $src" >&2
    continue
  fi

  hash="$(printf '%s' "$src" | sha256sum | cut -d' ' -f1)"
  stage="${stage_root}/${hash}"

  if mountpoint -q "$stage"; then
    echo "stage-minio-f: already staged: $src"
    continue
  fi

  mkdir -p "$stage"
  mount --bind "$src" "$stage"
  mount --make-rshared "$stage"
  echo "stage-minio-f: staged: $src -> $stage"
done
