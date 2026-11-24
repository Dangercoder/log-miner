# Which of the miner's statements take the one connection of a SINGLE_USER database, so that a DBA's
#   ALTER DATABASE x SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE x ...
# is refused (3101 / 924 / 5064)? And does staying out while user_access = SINGLE_USER, checked in the same batch, prevent it?
param([int]$Rounds = 25)
$ErrorActionPreference = 'Stop'
$cs = 'Server=lpc:.;Integrated Security=true;TrustServerCertificate=true;Encrypt=true'
Add-Type -AssemblyName System.Data
function Invoke-Sql([string]$sql, [string]$db = 'master') {
    $c = New-Object System.Data.SqlClient.SqlConnection "$cs;Database=$db;Pooling=false"; $c.Open()
    try { $k = $c.CreateCommand(); $k.CommandText = $sql; $k.CommandTimeout = 60; return $k.ExecuteScalar() } finally { $c.Dispose() }
}
Invoke-Sql "IF DB_ID('lm_su') IS NOT NULL BEGIN ALTER DATABASE lm_su SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_su; END; CREATE DATABASE lm_su; ALTER DATABASE lm_su SET RECOVERY FULL;" | Out-Null
Invoke-Sql "CREATE TABLE dbo.t (id int PRIMARY KEY); INSERT dbo.t VALUES (1); SELECT 1" 'lm_su' | Out-Null
Invoke-Sql "BACKUP DATABASE lm_su TO DISK = N'lm_su.bak' WITH INIT;" | Out-Null

$guard = "IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'lm_su' AND user_access = 1) RETURN; "
$kinds = [ordered]@{
    'nothing'                          = $null
    'fn_full_dblog from master'        = "DECLARE @n int, @d int = DB_ID(N'lm_su'); SELECT @n = COUNT(*) FROM sys.fn_full_dblog(NULL, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
    'dm_db_log_stats from master'      = "DECLARE @n nvarchar(46); SELECT @n = log_end_lsn FROM sys.dm_db_log_stats(DB_ID(N'lm_su'));"
    'database_recovery_status'         = "DECLARE @n uniqueidentifier; SELECT @n = recovery_fork_guid FROM sys.database_recovery_status WHERE database_id = DB_ID(N'lm_su');"
    'sp_executesql inside'             = "EXEC lm_su.sys.sp_executesql N'DECLARE @n int; SELECT @n = COUNT(*) FROM dbo.t';"
    'guard + fn_full_dblog'            = $guard + "DECLARE @n int, @d int = DB_ID(N'lm_su'); SELECT @n = COUNT(*) FROM sys.fn_full_dblog(NULL, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);"
    'guard + sp_executesql inside'     = $guard + "EXEC lm_su.sys.sp_executesql N'DECLARE @n int; SELECT @n = COUNT(*) FROM dbo.t';"
}
$loop = {
    param($cs, $sql, $stop)
    Add-Type -AssemblyName System.Data
    $n = 0; $err = @{}
    while (-not (Test-Path $stop)) {
        $c = New-Object System.Data.SqlClient.SqlConnection "$cs;Database=master;Pooling=false"
        try { $c.Open(); while (-not (Test-Path $stop)) { $k = $c.CreateCommand(); $k.CommandText = $sql; [void]$k.ExecuteNonQuery(); $n++ } }
        catch { $e = $_.Exception; while ($e.InnerException) { $e = $e.InnerException }; $key = "$($e.Number)"; $err[$key] = 1 + [int]$err[$key] }
        finally { $c.Dispose() }
    }
    "$n statements; errors " + (($err.GetEnumerator() | Sort-Object Name | ForEach-Object { "$($_.Name)x$($_.Value)" }) -join ' ')
}
foreach ($kind in $kinds.Keys) {
    $stop = [System.IO.Path]::GetTempFileName(); Remove-Item $stop
    $jobs = @()
    if ($kinds[$kind]) { $jobs = 1..2 | ForEach-Object { Start-Job -ScriptBlock $loop -ArgumentList $cs, $kinds[$kind], $stop }; Start-Sleep -Seconds 3 }
    $ok = 0; $refused = @{}
    for ($i = 0; $i -lt $Rounds; $i++) {
        try { Invoke-Sql "ALTER DATABASE lm_su SET SINGLE_USER WITH ROLLBACK IMMEDIATE; RESTORE DATABASE lm_su FROM DISK = N'lm_su.bak' WITH REPLACE; ALTER DATABASE lm_su SET MULTI_USER;" | Out-Null; $ok++ }
        catch {
            $e = $_.Exception; while ($e.InnerException) { $e = $e.InnerException }
            $refused["$($e.Number)"] = 1 + [int]$refused["$($e.Number)"]
            for ($j = 0; $j -lt 200; $j++) { try { Invoke-Sql "ALTER DATABASE lm_su SET MULTI_USER WITH ROLLBACK IMMEDIATE;" | Out-Null; break } catch { Start-Sleep -Milliseconds 20 } }
        }
    }
    New-Item $stop -ItemType File | Out-Null
    $out = ''
    if ($jobs) { $out = ($jobs | Wait-Job -Timeout 60 | Receive-Job) -join ' | '; $jobs | Remove-Job -Force }
    Remove-Item $stop
    '{0,-32} restores ok {1,2} of {2}, refused: {3}   [{4}]' -f $kind, $ok, $Rounds, (($refused.GetEnumerator() | ForEach-Object { "$($_.Name) x$($_.Value)" }) -join ', '), $out
}
Invoke-Sql "ALTER DATABASE lm_su SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_su; EXEC msdb.dbo.sp_delete_database_backuphistory N'lm_su'; DECLARE @p nvarchar(500) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultBackupPath')) + N'\lm_su.bak'; EXEC master.sys.xp_delete_files @p;" | Out-Null
