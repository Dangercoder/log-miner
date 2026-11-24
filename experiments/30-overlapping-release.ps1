# Overlapping replicated transactions, sp_repldone and truncation:
#  A begins first (older), stays open. B begins and commits, is the bookmark (held) or is released. Then A commits.
#  - Is A's beginning kept by the log until A itself is released, although B (later begin, earlier commit) was released and log was backed up?
#  - What does DBCC OPENTRAN report as oldest non-distributed, i.e. may "oldest > bookmark.begin" happen with nobody else releasing anything?
$cs = 'Data Source=lpc:.;Integrated Security=true;TrustServerCertificate=true;Pooling=false'
Add-Type -AssemblyName System.Data
function Open($db = 'master') { $c = New-Object System.Data.SqlClient.SqlConnection("$cs;Initial Catalog=$db"); $c.Open(); $c }
function Run($c, $sql) { $k = $c.CreateCommand(); $k.CommandTimeout = 60; $k.CommandText = $sql; [void]$k.ExecuteNonQuery() }
function Rows($c, $sql) { $k = $c.CreateCommand(); $k.CommandText = $sql; $r = $k.ExecuteReader(); $out = @(); while ($r.Read()) { $o = @(); for ($i = 0; $i -lt $r.FieldCount; $i++) { $o += [string]$r.GetValue($i) }; $out += ,($o) }; $r.Close(); ,$out }
$m = Open
Run $m "IF DB_ID(N'lm_ov') IS NOT NULL BEGIN ALTER DATABASE lm_ov SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_ov; END; CREATE DATABASE lm_ov; ALTER DATABASE lm_ov SET RECOVERY FULL;"
$x = Open 'lm_ov'
Run $x "EXEC sys.sp_cdc_enable_db;"
Run $x "CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int); CREATE TABLE dbo.noise (id int IDENTITY PRIMARY KEY, pad char(4000) NOT NULL);"
Run $x "EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL; EXEC sys.sp_cdc_drop_job N'capture'; EXEC sys.sp_cdc_drop_job N'cleanup';"
Run $m "BACKUP DATABASE lm_ov TO DISK = 'NUL';"
function Oldest() { $r = Rows $x "DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS"; $o = ($r | Where-Object { $_[0] -eq 'REPL_NONDIST_OLD_LSN' }); $d = ($r | Where-Object { $_[0] -eq 'REPL_DIST_OLD_LSN' }); $f = { param($p) if ($p) { $v = $p[1].Trim('()').Split(':'); '{0:X8}:{1:X8}:{2:X4}' -f [int]$v[0], [int]$v[1], [int]$v[2] } else { '-' } }; "nondist $(& $f $o) dist $(& $f $d)" }
function Stats() { $r = Rows $m "SELECT log_min_lsn, log_end_lsn, (SELECT log_reuse_wait_desc FROM sys.databases WHERE name = 'lm_ov') FROM sys.dm_db_log_stats(DB_ID('lm_ov'))"; "min $($r[0][0]) end $($r[0][1]) wait $($r[0][2])" }
function Noise() { for ($i = 0; $i -lt 4; $i++) { Run $x "INSERT dbo.noise (pad) SELECT TOP (1500) 'x' FROM sys.all_objects a CROSS JOIN sys.all_objects b; TRUNCATE TABLE dbo.noise; CHECKPOINT;"; Run $m "BACKUP LOG lm_ov TO DISK = 'NUL';" } }
function Txns() { Rows $m "DECLARE @s nvarchar(46) = (SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID('lm_ov'))), @id int = DB_ID('lm_ov'); SELECT b.[Transaction Name], b.[Current LSN], c.[Current LSN], c.[Transaction Begin], c.[Replicated Records] FROM sys.fn_full_dblog('0x' + @s, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) c LEFT JOIN sys.fn_full_dblog('0x' + @s, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) b ON b.[Transaction ID] = c.[Transaction ID] AND b.Operation = N'LOP_BEGIN_XACT' WHERE c.Operation = N'LOP_COMMIT_XACT' AND CONVERT(int, c.[Flag Bits]) & 4 = 4 ORDER BY c.[Current LSN]" }
function Release($begin, $commit) { $h = { param($l) $p = $l.Split(':'); '0x' + $p[0] + $p[1] + $p[2] }; Run $x "EXEC sys.sp_repldone @xactid = $(& $h $begin), @xact_seqno = $(& $h $commit), @numtrans = 0, @time = 0, @reset = 0; EXEC sys.sp_replflush;" }

$t = Txns; foreach ($q in $t) { Release $q[3] $q[2] }   # the enabling transactions
"after releasing the CDC set-up:          $(Oldest)   $(Stats)"
$a = Open 'lm_ov'; Run $a "BEGIN TRAN A; INSERT dbo.t VALUES (1, 1);"
Run $x "BEGIN TRAN B; INSERT dbo.t VALUES (2, 2); COMMIT;"
Run $x "BEGIN TRAN C; INSERT dbo.t VALUES (3, 3); COMMIT;"
"A open, B and C committed:                $(Oldest)   $(Stats)"
$t = Txns; $b = $t | Where-Object { $_[0] -eq 'B' }; $c = $t | Where-Object { $_[0] -eq 'C' }
"B begin $($b[1]) commit $($b[2])   C begin $($c[1]) commit $($c[2])"
Release $b[3] $b[2]
"B released (C is the bookmark):           $(Oldest)   $(Stats)"
Noise
"... and noise, log backups:               $(Oldest)   $(Stats)"
Run $a "INSERT dbo.t VALUES (4, 4); COMMIT;"
"A committed:                              $(Oldest)   $(Stats)"
Noise
"... and noise, log backups:               $(Oldest)   $(Stats)"
foreach ($q in (Txns)) { "   in the log: $($q -join ' | ')" }
Release $c[3] $c[2]
"C released (A is the newest now):         $(Oldest)   $(Stats)"
Noise
"... and noise, log backups:               $(Oldest)   $(Stats)"
foreach ($q in (Txns)) { "   in the log: $($q -join ' | ')" }

""
"== second case: D begins first, stays open; E commits and is released completely (nothing replicated is unreleased); then D commits"
foreach ($q in (Txns)) { Release $q[3] $q[2] }
$d = Open 'lm_ov'; Run $d "BEGIN TRAN D; INSERT dbo.t VALUES (11, 1);"
Run $x "BEGIN TRAN E; INSERT dbo.t VALUES (12, 2); COMMIT;"
$e = (Txns) | Where-Object { $_[0] -eq 'E' }; Release $e[3] $e[2]
"E released, D open:                       $(Oldest)   $(Stats)"
Noise
Run $d "INSERT dbo.t VALUES (14, 4); COMMIT;"
"D committed:                              $(Oldest)   $(Stats)"
Noise
"... and noise, log backups:               $(Oldest)   $(Stats)"
foreach ($q in (Txns)) { "   in the log: $($q -join ' | ')" }

""
"== third case: a replicated transaction whose changes were all undone is the newest; everything before it is released"
foreach ($q in (Txns)) { Release $q[3] $q[2] }
Run $x "BEGIN TRAN U; SAVE TRAN s; INSERT dbo.t VALUES (20, 1); ROLLBACK TRAN s; COMMIT;"
"U committed (nothing left of it):         $(Oldest)   $(Stats)"
Noise
"... and noise, log backups:               $(Oldest)   $(Stats)"
$u = (Txns) | Where-Object { $_[0] -eq 'U' }; if ($u) { Release $u[3] $u[2] }
"U released:                               $(Oldest)   $(Stats)"
Noise
"... and noise, log backups:               $(Oldest)   $(Stats)"

""
"== fourth case: nothing held, caught up; ordinary activity and log backups: does log_min_lsn pass the position of a caught-up reader (block markers :0001)?"
for ($i = 0; $i -lt 5; $i++) { $s = Stats; Run $x "INSERT dbo.noise (pad) VALUES ('y'); CHECKPOINT;"; Run $m "BACKUP LOG lm_ov TO DISK = 'NUL';"; "before: $s   after: $(Stats)" }
foreach ($k in $a, $d, $x) { $k.Dispose() }
Run $m "ALTER DATABASE lm_ov SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_ov;"
