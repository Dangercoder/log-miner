-- Where does SQL Server expose "distributed through LSN x" (to detect that someone else released the log)?
IF DB_ID('lab_gap') IS NOT NULL BEGIN ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap; END
CREATE DATABASE lab_gap; ALTER DATABASE lab_gap SET RECOVERY FULL;
GO
USE lab_gap;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);
EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
BACKUP DATABASE lab_gap TO DISK = 'NUL';
INSERT dbo.t VALUES (1, 1); INSERT dbo.t VALUES (2, 1); INSERT dbo.t VALUES (3, 1);
GO
USE lab_gap;
DECLARE @b varchar(22), @c varchar(22);
SELECT TOP (1) @c = [Current LSN], @b = [Transaction Begin] FROM (SELECT TOP (2) * FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0 ORDER BY [Current LSN]) x ORDER BY [Current LSN] DESC;
DECLARE @x binary(10) = CONVERT(binary(10), REPLACE(@b, ':', ''), 2), @s binary(10) = CONVERT(binary(10), REPLACE(@c, ':', ''), 2);
EXEC sys.sp_repldone @xactid = @x, @xact_seqno = @s, @numtrans = 0, @time = 0, @reset = 0; EXEC sys.sp_replflush;
SELECT 'released through' AS step, @c AS commit_lsn, @b AS begin_lsn;
SELECT 'replcounters' AS step; EXEC sp_replcounters;
CHECKPOINT;
SELECT [Current LSN], [Oldest Replicated Begin LSN], [Next Replicated End LSN], [Last Distributed Backup End LSN], [Last Distributed End LSN], [Repl Min Hold LSN] FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_CKPT' ORDER BY 1 DESC;
SELECT 'dbinfo' AS step; DBCC DBINFO WITH TABLERESULTS, NO_INFOMSGS;
SELECT 'repl_traninfo' AS step; SELECT * FROM sys.dm_repl_traninfo;
GO
USE lab_gap;
EXEC sys.sp_repldone @xactid = NULL, @xact_seqno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sys.sp_replflush;
SELECT 'after reset' AS step; EXEC sp_replcounters;
CHECKPOINT;
SELECT TOP (1) [Current LSN], [Oldest Replicated Begin LSN], [Next Replicated End LSN], [Last Distributed Backup End LSN], [Last Distributed End LSN], [Repl Min Hold LSN] FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_CKPT' ORDER BY 1 DESC;
EXEC sys.sp_cdc_disable_db;
CHECKPOINT;
SELECT TOP (1) 'after cdc disable_db' AS step, [Current LSN], [Oldest Replicated Begin LSN], [Next Replicated End LSN], [Last Distributed Backup End LSN], [Last Distributed End LSN], [Repl Min Hold LSN] FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_CKPT' ORDER BY 2 DESC;
EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
INSERT dbo.t VALUES (9, 1);
CHECKPOINT;
SELECT TOP (1) 'after re-enable' AS step, [Current LSN], [Oldest Replicated Begin LSN], [Next Replicated End LSN], [Last Distributed Backup End LSN], [Last Distributed End LSN], [Repl Min Hold LSN] FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_CKPT' ORDER BY 2 DESC;
SELECT capture_instance, start_lsn FROM cdc.change_tables;
GO
USE master; ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap;
