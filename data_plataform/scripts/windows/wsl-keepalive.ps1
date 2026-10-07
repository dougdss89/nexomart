# Keeps the WSL2 utility VM warm so the Postgres/MinIO mount tasks never have to cold-boot it.
# A cold boot dumps the full systemd init to a console window that `conhost --headless` cannot
# suppress (WSL allocates that console itself), which is the popup that kept appearing.
#
# WSL2 tears a distro down shortly after the last wsl.exe/interop session attached to it exits, and
# it kills that distro's background processes in the process - so a detached `sleep` does NOT
# survive. The only reliable keeper is an attached session that stays open. This script is exactly
# that: it blocks on a foreground `sleep infinity` inside the distro, holding one live session, so
# WSL2 never hits its idle timeout and the mount ticks always find the VM already running.
#
# The scheduled task (setup-wsl-keepalive.ps1) runs this with NO execution time limit and
# MultipleInstances=IgnoreNew, so exactly one holder exists. If the VM is ever torn down (e.g. a
# manual `wsl --shutdown`), this wsl.exe returns, the task instance ends, and the next tick starts a
# fresh holder.

$ErrorActionPreference = "Stop"

$Distro = "Ubuntu-22.04"

# Blocks here for the lifetime of the VM, holding the session open. Do not background this.
& wsl.exe -d $Distro -- sleep infinity
