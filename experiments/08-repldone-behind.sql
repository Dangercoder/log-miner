-- Is sp_repldone with a transaction BEHIND the current distributed point an error or a no-op?
-- (A "Latest" start's first durable checkpoint names the last skipped - possibly already distributed - transaction.)
SET NOCOUNT ON;
IF DB_ID(N'lm_exp08') IS NOT NULL BEGIN ALTER DATABASE lm_exp08 SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_exp08; END;
CREATE DATABASE lm_exp08;
ALTER DATABASE lm_exp08 SET RECOVERY FULL;
GO
USE lm_exp08;
EXEC sys.sp_cdc_enable_db;
CREATE TABLE dbo.t (id int PRIMARY KEY, v int);
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture';
EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
GO
BACKUP DATABASE lm_exp08 TO DISK = N'NUL';
INSERT dbo.t VALUES (1, 1);   -- transaction A
INSERT dbo.t VALUES (2, 2);   -- transaction B
GO
DECLARE @a_begin binary(10), @a_commit binary(10), @b_begin binary(10), @b_commit binary(10);
WITH x AS (
    SELECT [Transaction ID] AS xid, MIN(CASE WHEN Operation = N'LOP_BEGIN_XACT' THEN [Current LSN] END) AS b,
           MIN(CASE WHEN Operation = N'LOP_COMMIT_XACT' THEN [Current LSN] END) AS c
    FROM sys.fn_dblog(NULL, NULL)
    WHERE [Transaction ID] IN (SELECT [Transaction ID] FROM sys.fn_dblog(NULL, NULL) WHERE Operation = N'LOP_INSERT_ROWS' AND AllocUnitName LIKE N'dbo.t%')
    GROUP BY [Transaction ID])
SELECT IDENTITY(int) AS n, b, c INTO #x FROM x ORDER BY c;
DECLARE @t TABLE (n int, b nvarchar(30), c nvarchar(30));
INSERT @t SELECT n, b, c FROM #x;
SELECT n, b, c FROM @t;
DECLARE @bA nvarchar(30), @cA nvarchar(30), @bB nvarchar(30), @cB nvarchar(30);
SELECT @bA = b, @cA = c FROM @t WHERE n = 1;
SELECT @bB = b, @cB = c FROM @t WHERE n = 2;
-- LSN text 0000002a:000001b0:0003 -> binary(10) big-endian: vlf(4) block(4) slot(2)
SET @a_begin  = CONVERT(binary(10), REPLACE(@bA, ':', ''), 2);
SET @a_commit = CONVERT(binary(10), REPLACE(@cA, ':', ''), 2);
SET @b_begin  = CONVERT(binary(10), REPLACE(@bB, ':', ''), 2);
SET @b_commit = CONVERT(binary(10), REPLACE(@cB, ':', ''), 2);
PRINT 'opentran before:';
DBCC OPENTRAN WITH TABLERESULTS;
PRINT 'repldone(B)';
EXEC sp_repldone @xactid = @b_begin, @xact_seqno = @b_commit, @numtrans = 0, @time = 0, @reset = 0;
EXEC sp_replflush;
PRINT 'opentran after B:';
DBCC OPENTRAN WITH TABLERESULTS;
PRINT 'repldone(A) - behind the distributed point';
BEGIN TRY
    EXEC sp_repldone @xactid = @a_begin, @xact_seqno = @a_commit, @numtrans = 0, @time = 0, @reset = 0;
    EXEC sp_replflush;
    PRINT 'repldone(A): no error';
END TRY
BEGIN CATCH
    PRINT CONCAT('repldone(A): error ', ERROR_NUMBER(), ' severity ', ERROR_SEVERITY(), ': ', ERROR_MESSAGE());
    EXEC sp_replflush;
END CATCH;
PRINT 'opentran after A:';
DBCC OPENTRAN WITH TABLERESULTS;
GO
USE master;
ALTER DATABASE lm_exp08 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE lm_exp08;
