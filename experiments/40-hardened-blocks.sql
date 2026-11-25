-- 40: no record is ever added to a log block that a read up to log_end_lsn has returned (writers, then this reader, in parallel sessions).
-- Lab, CU28, three writers with small transactions, rollbacks and delayed durability for 25 s: 907 positions, 0 of them with a later record in the same block.
-- (Measured up to MAX([Current LSN]) instead - what the function shows beyond log_end_lsn - 1 of 299 had one.)
SET NOCOUNT ON; USE master; IF DB_ID('lm_blk') IS NOT NULL BEGIN ALTER DATABASE lm_blk SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_blk; END; CREATE DATABASE lm_blk; ALTER DATABASE lm_blk SET RECOVERY FULL; ALTER DATABASE lm_blk SET DELAYED_DURABILITY = ALLOWED;
GO
USE lm_blk; CREATE TABLE dbo.t (id int IDENTITY PRIMARY KEY, v int, p char(300)); CREATE TABLE dbo.r (n int IDENTITY PRIMARY KEY, last_lsn varchar(50), end_lsn varchar(50));
GO -- writers (three sessions)
SET NOCOUNT ON; USE lm_blk; DECLARE @t datetime2 = DATEADD(SECOND, 25, SYSDATETIME()), @i int = 0;
WHILE SYSDATETIME() < @t
BEGIN
    BEGIN TRAN; INSERT dbo.t (v) SELECT TOP (1 + @i % 5) @i FROM sys.all_objects;
    IF @i % 7 = 0 ROLLBACK ELSE IF @i % 3 = 0 COMMIT WITH (DELAYED_DURABILITY = ON) ELSE COMMIT;
    IF @i % 40 = 0 WAITFOR DELAY '00:00:00.013';
    SET @i += 1;
END
GO -- reader
SET NOCOUNT ON; USE master; DECLARE @t datetime2 = DATEADD(SECOND, 24, SYSDATETIME()), @d int = DB_ID('lm_blk'), @end nvarchar(46), @from nvarchar(46), @last nvarchar(46);
DECLARE @r TABLE (n int IDENTITY PRIMARY KEY, last_lsn varchar(50), end_lsn varchar(50));
WHILE SYSDATETIME() < @t
BEGIN
    SELECT @end = log_end_lsn FROM sys.dm_db_log_stats(@d);
    SET @from = ISNULL((SELECT TOP (1) '0x' + last_lsn FROM @r ORDER BY n DESC), NULL);
    SELECT @last = MAX([Current LSN]) FROM sys.fn_full_dblog(@from, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) WHERE [Current LSN] < @end;
    IF @last IS NOT NULL INSERT @r VALUES (@last, @end);
END;
WAITFOR DELAY '00:00:03';
SELECT [Current LSN] AS lsn, LEAD([Current LSN]) OVER (ORDER BY [Current LSN]) AS nxt, Operation, LEAD(Operation) OVER (ORDER BY [Current LSN]) AS nxtop INTO #l FROM sys.fn_full_dblog(NULL, NULL, @d, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT COUNT(*) AS reads, COUNT(DISTINCT r.last_lsn) AS positions, SUM(CASE WHEN LEFT(l.nxt, 17) = LEFT(l.lsn, 17) THEN 1 ELSE 0 END) AS later_record_in_the_same_block, SUM(CASE WHEN l.nxt < r.end_lsn THEN 1 ELSE 0 END) AS not_the_last_before_the_end, (SELECT COUNT(*) FROM #l) AS records
FROM @r r JOIN #l l ON l.lsn = r.last_lsn;
