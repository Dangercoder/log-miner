-- Could log that has left the active log be read from log backups (a fallback for gaps)? On a TDE database: is it decrypted, are the replicated
-- records complete, how do compressed and striped backups behave, and does the function leak threads (reported in 2013)?
SET NOCOUNT ON; USE master;
IF DB_ID(N'lm_bak') IS NOT NULL BEGIN ALTER DATABASE lm_bak SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_bak; END;
CREATE DATABASE lm_bak; ALTER DATABASE lm_bak SET RECOVERY FULL;
GO
USE lm_bak;
CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = AES_256 ENCRYPTION BY SERVER CERTIFICATE LogMinerTdeCert;
EXEC sys.sp_cdc_enable_db; CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v varchar(60) NULL); CREATE TABLE dbo.noise (id int IDENTITY PRIMARY KEY, pad char(4000) NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL;
EXEC sys.sp_cdc_drop_job N'capture'; EXEC sys.sp_cdc_drop_job N'cleanup';
ALTER DATABASE lm_bak SET ENCRYPTION ON;
GO
DECLARE @i int = 0; WHILE @i < 600 AND NOT EXISTS (SELECT 1 FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(N'lm_bak') AND encryption_state = 3) BEGIN WAITFOR DELAY '00:00:00.1'; SET @i += 1; END
SELECT 'encryption_state' AS what, encryption_state FROM sys.dm_database_encryption_keys WHERE database_id = DB_ID(N'lm_bak');
BACKUP DATABASE lm_bak TO DISK = N'lm_bak_full.bak' WITH INIT;
USE lm_bak;
INSERT dbo.t VALUES (1, 'NEEDLE-ONE-plaintext'); UPDATE dbo.t SET v = 'NEEDLE-TWO-plaintext' WHERE id = 1; BEGIN TRAN; INSERT dbo.t VALUES (2, 'x'), (3, 'y'); DELETE dbo.t WHERE id = 2; COMMIT;
BACKUP LOG lm_bak TO DISK = N'lm_bak_plain.trn' WITH INIT, NO_COMPRESSION;
INSERT dbo.t VALUES (11, 'NEEDLE-COMPRESSED');
BACKUP LOG lm_bak TO DISK = N'lm_bak_comp.trn' WITH INIT, COMPRESSION;
INSERT dbo.t VALUES (21, 'NEEDLE-STRIPED');
BACKUP LOG lm_bak TO DISK = N'lm_bak_s1.trn', DISK = N'lm_bak_s2.trn' WITH INIT, NO_COMPRESSION;
GO
USE master;
DECLARE @p nvarchar(400) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultBackupPath')) + N'\';
DECLARE @rest nvarchar(max) = REPLICATE(CONVERT(nvarchar(max), N', DEFAULT'), 63), @sql nvarchar(max), @what nvarchar(40), @f1 nvarchar(400), @f2 nvarchar(400);
DECLARE c CURSOR LOCAL FOR SELECT * FROM (VALUES (N'plain', N'lm_bak_plain.trn', NULL), (N'compressed', N'lm_bak_comp.trn', NULL), (N'striped, both', N'lm_bak_s1.trn', N'lm_bak_s2.trn'), (N'striped, one', N'lm_bak_s1.trn', NULL)) v (what, f1, f2);
OPEN c; FETCH NEXT FROM c INTO @what, @f1, @f2;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT ''' + @what + N''' AS backup_kind, COUNT(*) AS records, SUM(CASE WHEN CONVERT(int, [Flag Bits]) & 4 = 4 AND Operation <> N''LOP_COMMIT_XACT'' THEN 1 ELSE 0 END) AS replicated, SUM(CASE WHEN Operation = N''LOP_COMMIT_XACT'' THEN [Replicated Records] END) AS counted_at_commits,
        SUM(CASE WHEN CHARINDEX(CONVERT(varbinary(20), ''NEEDLE''), [RowLog Contents 0]) > 0 OR CHARINDEX(CONVERT(varbinary(20), ''NEEDLE''), [RowLog Contents 1]) > 0 THEN 1 ELSE 0 END) AS rows_in_plaintext, MIN([Current LSN]) AS first_lsn, MAX([Current LSN]) AS last_lsn
        FROM sys.fn_dump_dblog(NULL, NULL, N''DISK'', 1, N''' + @p + @f1 + N'''' + CASE WHEN @f2 IS NULL THEN N', DEFAULT' ELSE N', N''' + @p + @f2 + N'''' END + REPLICATE(CONVERT(nvarchar(max), N', DEFAULT'), 62) + N') WHERE [Current LSN] IS NOT NULL';
    BEGIN TRY EXEC (@sql); END TRY BEGIN CATCH SELECT @what AS backup_kind, ERROR_NUMBER() AS error, LEFT(ERROR_MESSAGE(), 200) AS message; END CATCH
    FETCH NEXT FROM c INTO @what, @f1, @f2;
END
CLOSE c; DEALLOCATE c;
GO
PRINT '== threads and hidden schedulers before and after 25 calls';
DECLARE @p nvarchar(400) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultBackupPath')) + N'\';
DECLARE @sql nvarchar(max) = N'SELECT @n = COUNT(*) FROM sys.fn_dump_dblog(NULL, NULL, N''DISK'', 1, N''' + @p + N'lm_bak_plain.trn''' + REPLICATE(CONVERT(nvarchar(max), N', DEFAULT'), 63) + N')', @i int = 0, @n int, @t datetime2 = SYSDATETIME();
SELECT 'before' AS at, (SELECT COUNT(*) FROM sys.dm_os_threads) AS threads, (SELECT COUNT(*) FROM sys.dm_os_workers) AS workers, (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status LIKE N'HIDDEN%') AS hidden_schedulers, (SELECT COUNT(*) FROM sys.dm_os_schedulers) AS schedulers;
WHILE @i < 25 BEGIN EXEC sp_executesql @sql, N'@n int OUTPUT', @n OUTPUT; SET @i += 1; END
SELECT 'after 25 calls' AS at, (SELECT COUNT(*) FROM sys.dm_os_threads) AS threads, (SELECT COUNT(*) FROM sys.dm_os_workers) AS workers, (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status LIKE N'HIDDEN%') AS hidden_schedulers, (SELECT COUNT(*) FROM sys.dm_os_schedulers) AS schedulers, DATEDIFF(millisecond, @t, SYSDATETIME()) / 25.0 AS ms_per_call;
GO
WAITFOR DELAY '00:00:15';
SELECT '15 s later' AS at, (SELECT COUNT(*) FROM sys.dm_os_threads) AS threads, (SELECT COUNT(*) FROM sys.dm_os_workers) AS workers, (SELECT COUNT(*) FROM sys.dm_os_schedulers WHERE status LIKE N'HIDDEN%') AS hidden_schedulers, (SELECT COUNT(*) FROM sys.dm_os_schedulers) AS schedulers;
GO
ALTER DATABASE lm_bak SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_bak;
EXEC msdb.dbo.sp_delete_database_backuphistory N'lm_bak';
DECLARE @p nvarchar(400) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultBackupPath')) + N'\lm_bak_'; DECLARE @f nvarchar(500);
SET @f = @p + N'full.bak'; EXEC master.sys.xp_delete_files @f; SET @f = @p + N'plain.trn'; EXEC master.sys.xp_delete_files @f; SET @f = @p + N'comp.trn'; EXEC master.sys.xp_delete_files @f; SET @f = @p + N's1.trn'; EXEC master.sys.xp_delete_files @f; SET @f = @p + N's2.trn'; EXEC master.sys.xp_delete_files @f;
GO
