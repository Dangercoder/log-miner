-- fn_full_dblog with a @start that is not the LSN of a record: inside an existing block, in a block that does not exist, truncated, at and
-- beyond the end. Is it refused, or moved silently?
SET NOCOUNT ON; USE master;
IF DB_ID(N'lm_st') IS NOT NULL BEGIN ALTER DATABASE lm_st SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_st; END;
CREATE DATABASE lm_st; ALTER DATABASE lm_st SET RECOVERY FULL; BACKUP DATABASE lm_st TO DISK = 'NUL';
GO
USE lm_st; CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, pad char(4000) NOT NULL); 
DECLARE @i int = 0; WHILE @i < 50 BEGIN INSERT dbo.t VALUES ('x'); SET @i += 1; END
BACKUP LOG lm_st TO DISK = 'NUL'; CHECKPOINT; BACKUP LOG lm_st TO DISK = 'NUL'; 
SET @i = 0; WHILE @i < 20 BEGIN INSERT dbo.t VALUES ('x'); SET @i += 1; END
GO
USE master;
GO
CREATE OR ALTER PROCEDURE #read @what varchar(60), @start nvarchar(46) AS
BEGIN
    DECLARE @id int = DB_ID('lm_st'), @n int, @first nvarchar(46), @last nvarchar(46);
    BEGIN TRY
        SELECT @n = COUNT(*), @first = MIN([Current LSN]), @last = MAX([Current LSN]) FROM sys.fn_full_dblog('0x' + @start, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
        SELECT @what AS what, @start AS start, @n AS records, @first AS first_record, @last AS last_record, CASE WHEN @first = @start THEN 'exact' WHEN @first IS NULL THEN 'nothing' ELSE 'MOVED' END AS verdict;
    END TRY
    BEGIN CATCH SELECT @what AS what, @start AS start, ERROR_NUMBER() AS error, LEFT(ERROR_MESSAGE(), 100) AS message, ERROR_SEVERITY() AS severity; END CATCH
END
GO
DECLARE @id int = DB_ID('lm_st'), @min nvarchar(46), @end nvarchar(46);
SELECT @min = log_min_lsn, @end = log_end_lsn FROM sys.dm_db_log_stats(@id);
SELECT @min AS log_min_lsn, @end AS log_end_lsn;
SELECT [Current LSN] AS lsn, ROW_NUMBER() OVER (ORDER BY [Current LSN]) AS rn INTO #l FROM sys.fn_full_dblog('0x' + @min, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
DECLARE @x nvarchar(46), @y nvarchar(46);
EXEC #read 'log_min_lsn', @min;
SELECT @x = MAX(lsn) FROM #l; EXEC #read 'the last record', @x;
SELECT TOP (1) @x = lsn FROM #l WHERE rn = 20; EXEC #read 'a record in the middle', @x;
SELECT @y = LEFT(@x, 18) + N'0FFF'; EXEC #read 'existing block, no such slot', @y;
SELECT @y = LEFT(@x, 18) + N'0000'; EXEC #read 'existing block, slot 0', @y;
SELECT @y = LEFT(@x, 9) + RIGHT(N'0000000' + CONVERT(nvarchar(8), CONVERT(varbinary(4), CONVERT(int, CONVERT(varbinary(4), SUBSTRING(@x, 10, 8), 2)) + 1), 2), 8) + N':0001'; EXEC #read 'no such block (middle of one)', @y;
EXEC #read 'log_end_lsn', @end;
SELECT @y = LEFT(@end, 9) + N'0FFFFFFF:0001'; EXEC #read 'far beyond the end, same VLF', @y;
EXEC #read 'a VLF that does not exist yet', N'0FFFFFFF:00000010:0001';
EXEC #read 'a VLF before the log', N'00000001:00000010:0001';
SELECT TOP (1) @x = lsn FROM #l ORDER BY lsn; 
SELECT @y = LEFT(@x, 9) + N'00000010:0001'; EXEC #read 'start of the first VLF (truncated part?)', @y;
DECLARE @first_active nvarchar(46) = (SELECT MIN(lsn) FROM #l); SELECT @first_active AS first_readable_from_min;
GO
ALTER DATABASE lm_st SET AUTO_CLOSE ON;
GO
USE master;
GO
WAITFOR DELAY '00:00:02';
SELECT 'AUTO_CLOSE, closed' AS what, d.state_desc, d.is_auto_close_on, DATABASEPROPERTYEX('lm_st', 'Collation') AS collation_if_started, CONVERT(char(36), r.recovery_fork_guid) AS fork, r.last_log_backup_lsn FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id = d.database_id WHERE d.name = 'lm_st';
SELECT 'stats' AS what, s.log_min_lsn, s.log_end_lsn FROM sys.dm_db_log_stats(DB_ID('lm_st')) s;
BEGIN TRY
    DECLARE @id int = DB_ID('lm_st'); SELECT 'read by id' AS what, COUNT(*) AS records FROM sys.fn_full_dblog(NULL, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
END TRY BEGIN CATCH SELECT 'read by id' AS what, ERROR_NUMBER() AS error, ERROR_MESSAGE() AS message; END CATCH
SELECT 'after the read' AS what, CONVERT(char(36), r.recovery_fork_guid) AS fork FROM sys.database_recovery_status r WHERE r.database_id = DB_ID('lm_st');
EXEC lm_st.sys.sp_executesql N'SELECT ''opened'' AS what, CONVERT(char(36), r.recovery_fork_guid) AS fork FROM sys.database_recovery_status r WHERE r.database_id = DB_ID()';
GO
ALTER DATABASE lm_st SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_st;
GO
