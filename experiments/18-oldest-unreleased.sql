-- (CDC refuses databases with delayed durability, error 22891: a captured commit is always hardened before it is visible.)
-- Is REPL_NONDIST_OLD_LSN the exact begin LSN, or rounded to the log block?
IF DB_ID('lab_gap') IS NOT NULL BEGIN ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap; END
CREATE DATABASE lab_gap; ALTER DATABASE lab_gap SET RECOVERY FULL;
GO
USE lab_gap;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL); CREATE TABLE dbo.noise (id int IDENTITY PRIMARY KEY, pad char(100));
EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
BACKUP DATABASE lab_gap TO DISK = 'NUL';
INSERT dbo.t VALUES (1, 1);
EXEC sys.sp_repldone @xactid = NULL, @xact_seqno = NULL, @numtrans = 0, @time = 0, @reset = 1; EXEC sys.sp_replflush;
DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
-- unflushed activity first, so that the tracked transactions begin in the middle of a block
BEGIN TRAN; INSERT dbo.noise (pad) VALUES ('a'); ROLLBACK;
INSERT dbo.t VALUES (2, 1);
BEGIN TRAN; INSERT dbo.noise (pad) VALUES ('b'); ROLLBACK;
INSERT dbo.t VALUES (3, 1);
BEGIN TRAN; INSERT dbo.noise (pad) VALUES ('c'); ROLLBACK; BEGIN TRAN; INSERT dbo.noise (pad) VALUES ('d'); ROLLBACK;
INSERT dbo.t VALUES (4, 1);
SELECT [Current LSN] AS commit_lsn, [Transaction Begin] AS begin_lsn FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0 ORDER BY 1;
DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
GO
USE lab_gap;
DECLARE @b varchar(22), @c varchar(22);
SELECT TOP (1) @c = [Current LSN], @b = [Transaction Begin] FROM (SELECT TOP (2) * FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0 ORDER BY [Current LSN] DESC) x ORDER BY [Current LSN];
DECLARE @x binary(10) = CONVERT(binary(10), REPLACE(@b, ':', ''), 2), @s binary(10) = CONVERT(binary(10), REPLACE(@c, ':', ''), 2);
EXEC sys.sp_repldone @xactid = @x, @xact_seqno = @s, @numtrans = 0, @time = 0, @reset = 0; EXEC sys.sp_replflush;
SELECT 'released all but the newest' AS step, @c AS released_commit; DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;
GO
USE master; ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap;
