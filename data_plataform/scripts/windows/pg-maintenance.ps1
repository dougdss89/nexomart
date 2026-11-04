# Nightly Postgres maintenance: 45-day partition retention (Docker up) + offline vhdx compaction.
# Registered at 21:00 with a +1h retry by setup-pg-maintenance-task.ps1. Exits non-zero on failure
# so Task Scheduler retries. Every step logs to C:\Users\Doug\scripts\pg-maintenance.log.
#
# Works under both Windows PowerShell 5.1 and PowerShell 7: ErrorActionPreference is "Continue"
# (so native-command stderr warnings, e.g. "DOCKER_INSECURE_NO_IPTABLES_RAW is set", never turn
# into a terminating error on 5.1), and native exit codes are checked by hand via Native(). The
# cmdlets that must fail the job (the VHD compaction) carry -ErrorAction Stop explicitly.
#
# Sequence: ensure Docker + pgprojeto up -> run retention.sql -> fstrim the ext4 (release freed
# blocks) -> stop pgprojeto (graceful checkpoint) -> wsl --unmount the vhd -> Optimize-VHD (compact)
# -> remount + start pgprojeto. The catch block always tries to remount the disk and restart
# pgprojeto so a mid-run failure never leaves the stack down.
#
# Guards (added after the 2026-10-24 run, where `wsl --unmount` returned E_ACCESSDENIED, the
# compaction ran anyway, the remount failed, pgprojeto was recreated over the empty mountpoint and
# the run still logged "done OK"): compaction is skipped unless the unmount verifiably succeeded;
# pgprojeto is only recreated once the real disk (sentinel file) is back; and the run exits 1
# whenever Postgres did not come back, so the +1h retry fires and the log says so.

$ErrorActionPreference = "Continue"
# wsl.exe writes UTF-16 by default, which ends up interleaved with the UTF-8 lines in the log.
$env:WSL_UTF8 = "1"
$Distro = "Ubuntu-22.04"
$Vhd    = "L:\WSLDisks\postgres-data.vhdx"
$Mount  = "/mnt/wsl/postgres-ssd"
$Repo   = "\\wsl.localhost\$Distro\home\doug\project\data_plataform"
$Docker = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
$Log    = "C:\Users\Doug\scripts\pg-maintenance.log"

function Log($m) { "{0}  {1}" -f (Get-Date -Format "u"), $m | Tee-Object -FilePath $Log -Append }
function Native($msg) { if ($LASTEXITCODE -ne 0) { throw $msg } }

# True only when the real Postgres disk is mounted. The mountpoint check alone is not enough: the
# sentinel file lives on the disk itself, so it is absent from the empty shadow dir underneath.
function Test-RealDisk {
    & wsl.exe -d $Distro -u root -- bash -c "mountpoint -q $Mount && test -f $Mount/pgdata/.nexomart_real_disk" 2>&1 | Out-Null
    return ($LASTEXITCODE -eq 0)
}

# Set by Remount-And-Start: whether the disk and pgprojeto actually came back.
$script:RemountOk = $false

function Remount-And-Start {
    $script:RemountOk = $false
    try { & "$Repo\scripts\windows\mount-postgres-ssd.ps1" | Out-Null } catch { Log "remount failed: $($_.Exception.Message)" }
    # Re-enable the 5-minute remount task that was disabled around the compaction (runs on every
    # path, so the task is never left disabled and can still remount the disk later on its own).
    Enable-ScheduledTask -TaskName "MountPostgresSSD-WSL" -ErrorAction SilentlyContinue | Out-Null
    # Without the real disk, recreating pgprojeto would bind its volume to the empty shadow dir. The
    # entrypoint sentinel refuses to initdb there, but the container would still be pointed at the
    # wrong place. Leave it stopped; `make start-dev` re-checks the disk and stages the bind mount.
    if (-not (Test-RealDisk)) {
        Log "real Postgres disk is not mounted -> leaving pgprojeto stopped (fix the mount, then make start-dev)"
        return
    }
    # Recreate pgprojeto + its named volume so the data bind re-resolves to the freshly remounted
    # disk. A plain `docker start` reuses a stale WSL bind-mount staging that still points at the
    # pre-unmount (empty) dir, which the entrypoint sentinel guard then correctly refuses -> pg would
    # stay down until a manual recreate. Run inside WSL so compose and its relative paths are native.
    & wsl.exe -d $Distro -- bash -lc "cd /home/doug/project/data_plataform && docker rm -f pgprojeto >/dev/null 2>&1; docker volume rm postgres_pg-data >/dev/null 2>&1; docker compose --env-file configs/secrets.env -f postgres/docker-compose.yml up -d" 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $Log -Append
    if ($LASTEXITCODE -ne 0) { Log "pgprojeto recreate failed (exit $LASTEXITCODE)"; return }
    $script:RemountOk = $true
}

try {
    Log "=== maintenance start ==="

    # 1. Docker engine must answer; start Docker Desktop and wait if not. (docker info prints a
    #    harmless stderr warning on this host; under Continue it does not throw, and info exits 0.)
    & docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Log "Docker not ready -> launching Docker Desktop"
        if (Test-Path $Docker) { Start-Process $Docker | Out-Null }
        $ok = $false
        for ($i = 0; $i -lt 60; $i++) { Start-Sleep 5; & docker info 2>&1 | Out-Null; if ($LASTEXITCODE -eq 0) { $ok = $true; break } }
        if (-not $ok) { throw "Docker did not become ready within 5 minutes" }
    }
    Log "Docker ready"

    # 2. The data disk and pgprojeto must be up to run retention.
    & wsl.exe -d $Distro -u root -- mountpoint -q $Mount 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) { Log "disk not mounted -> mounting"; & "$Repo\scripts\windows\mount-postgres-ssd.ps1" | Out-Null }
    if ((& docker ps --filter "name=^pgprojeto$" --format "{{.Names}}") -ne "pgprojeto") {
        Log "pgprojeto not running -> starting"; & docker start pgprojeto 2>&1 | Out-Null; Start-Sleep 10
    }

    # 3. Retention: drop partitions older than 45d and already landed.
    Log "running retention.sql"
    $user = (& wsl.exe -d $Distro -- bash -lc "grep '^POSTGRES_USER=' /home/doug/project/data_plataform/configs/secrets.env | cut -d= -f2").Trim()
    $sql  = Get-Content "$Repo\configs\postgresql\retention.sql" -Raw -ErrorAction Stop
    # psql writes its RAISE NOTICE lines to stderr; stringify each line so PowerShell logs them as
    # plain text instead of decorating the first one as a NativeCommandError (cosmetic only).
    $sql | & docker exec -i pgprojeto psql -U $user -d nexomarket_db -v ON_ERROR_STOP=1 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $Log -Append
    Native "retention.sql failed (exit $LASTEXITCODE)"

    # 4. fstrim so freed ext4 blocks are released to the vhd (non-fatal: if the device has no
    #    discard support, compaction simply reclaims less).
    Log "fstrim $Mount"
    & wsl.exe -d $Distro -u root -- fstrim -v $Mount 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $Log -Append

    # 5. Quiesce writes and detach the vhd from WSL for an offline compaction. Disable the 5-minute
    #    MountPostgresSSD-WSL task first so it cannot re-mount the vhd mid-compaction (Remount-And-Start
    #    re-enables it). Only pgprojeto uses this disk, so the rest of Docker keeps running untouched.
    Log "disabling MountPostgresSSD-WSL + stopping pgprojeto for offline compaction"
    Disable-ScheduledTask -TaskName "MountPostgresSSD-WSL" -ErrorAction SilentlyContinue | Out-Null
    & docker stop pgprojeto 2>&1 | Out-Null
    & wsl.exe --unmount $Vhd 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $Log -Append
    $unmountExit = $LASTEXITCODE
    # Never compact a disk WSL may still hold. A non-zero exit is fatal even if the kernel did
    # detach: it means the WSL service state is out of sync, and the remount will fail too.
    & wsl.exe -d $Distro -u root -- mountpoint -q $Mount 2>&1 | Out-Null
    $stillMounted = ($LASTEXITCODE -eq 0)
    if ($unmountExit -ne 0 -or $stillMounted) {
        throw "wsl --unmount failed (exit $unmountExit, still mounted: $stillMounted) -> compaction skipped"
    }

    # 6. Compact. Prefer Hyper-V's Optimize-VHD; fall back to diskpart if that module is absent.
    $sizeBefore = (Get-Item $Vhd -ErrorAction Stop).Length
    Log ("vhd size before: {0:N1} GB" -f ($sizeBefore / 1GB))
    if (Get-Command Optimize-VHD -ErrorAction SilentlyContinue) {
        Mount-VHD -Path $Vhd -ReadOnly -NoDriveLetter -ErrorAction Stop
        try { Optimize-VHD -Path $Vhd -Mode Full -ErrorAction Stop } finally { Dismount-VHD -Path $Vhd -ErrorAction SilentlyContinue }
    } else {
        Log "Optimize-VHD unavailable -> diskpart compact"
        "select vdisk file=`"$Vhd`"`r`nattach vdisk readonly`r`ncompact vdisk`r`ndetach vdisk`r`nexit" | diskpart 2>&1 | ForEach-Object { "$_" } | Tee-Object -FilePath $Log -Append
    }
    $sizeAfter = (Get-Item $Vhd -ErrorAction Stop).Length
    Log ("vhd size after:  {0:N1} GB  (reclaimed {1:N1} GB)" -f ($sizeAfter / 1GB), (($sizeBefore - $sizeAfter) / 1GB))

    # 7. Bring the disk and Postgres back.
    Remount-And-Start
    if (-not $script:RemountOk) {
        Log "=== maintenance FAILED: compaction done but Postgres did not come back ==="
        exit 1
    }
    Log "=== maintenance done OK ==="
    exit 0
}
catch {
    Log "ERROR: $($_.Exception.Message)"
    Remount-And-Start
    Log ("=== maintenance FAILED (Postgres back: {0}) ===" -f $script:RemountOk)
    exit 1
}
