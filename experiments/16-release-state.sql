-- What DBCC OPENTRAN says about the replication release point: normal release, foreign release, CDC disabled and re-enabled.
IF DB_ID('lab_gap') IS NOT NULL BEGIN ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap; END
CREATE DATABASE lab_gap; ALTER DATABASE lab_gap SET RECOVERY FULL;
GO
USE lab_gap;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);
EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
BACKUP DATABASE lab_gap TO DISK = 'NUL';
SELECT '0 enabled, no changes' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
INSERT dbo.t VALUES (1, 1); INSERT dbo.t VALUES (2, 1); INSERT dbo.t VALUES (3, 1);
SELECT [Current LSN] AS commit_lsn, [Transaction Begin] AS begin_lsn FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0;
SELECT '1 three changes, none released' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
GO
USE lab_gap;
DECLARE @b varchar(22), @c varchar(22);
SELECT TOP (1) @c = [Current LSN], @b = [Transaction Begin] FROM (SELECT TOP (2) * FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0 ORDER BY [Current LSN]) x ORDER BY [Current LSN] DESC;
DECLARE @x binary(10) = CONVERT(binary(10), REPLACE(@b, ':', ''), 2), @s binary(10) = CONVERT(binary(10), REPLACE(@c, ':', ''), 2);
EXEC sys.sp_repldone @xactid = @x, @xact_seqno = @s, @numtrans = 0, @time = 0, @reset = 0; EXEC sys.sp_replflush;
SELECT '2 released through the second' AS step, @c AS released_commit, @b AS released_begin; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
GO
USE lab_gap;
EXEC sys.sp_repldone @xactid = NULL, @xact_seqno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sys.sp_replflush;
SELECT '3 foreign sp_repldone @reset = 1' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
INSERT dbo.t VALUES (4, 1);
SELECT '4 one more change' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
GO
USE lab_gap;
EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N't', @capture_instance=N'all';
SELECT '5 cdc disabled on the table' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
INSERT dbo.t VALUES (5, 1);
BACKUP LOG lab_gap TO DISK = 'NUL';
SELECT '5b after log backup' AS step, log_min_lsn, log_end_lsn, log_truncation_holdup_reason FROM sys.dm_db_log_stats(DB_ID());
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'capture') EXEC sys.sp_cdc_drop_job @job_type = N'capture';
IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup') EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
INSERT dbo.t VALUES (6, 1);
SELECT '6 re-enabled, one change' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
SELECT capture_instance, start_lsn, create_date FROM cdc.change_tables;
GO
USE lab_gap;
EXEC sys.sp_cdc_disable_db;
SELECT '7 cdc disabled on the database' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
INSERT dbo.t VALUES (7, 1);
BACKUP LOG lab_gap TO DISK = 'NUL'; BACKUP LOG lab_gap TO DISK = 'NUL';
SELECT '7b after log backups' AS step, log_min_lsn, log_end_lsn, log_truncation_holdup_reason FROM sys.dm_db_log_stats(DB_ID());
EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
INSERT dbo.t VALUES (8, 1);
SELECT '8 cdc enabled again, one change' AS step; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
SELECT capture_instance, start_lsn, create_date FROM cdc.change_tables;
SELECT log_min_lsn FROM sys.dm_db_log_stats(DB_ID());
GO
USE master; ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap;
