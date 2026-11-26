-- Cost of one incremental log read (the miner's query) from a recent LSN, for a tenant with real history.
SET NOCOUNT ON;
DECLARE @dbid int = DB_ID('tenant_0100'), @start nvarchar(46), @i int = 0, @t0 datetime2 = SYSDATETIME(), @n int = 200, @rows int = 0;
SELECT TOP (1) @start = [Current LSN] FROM sys.fn_full_dblog(NULL, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' ORDER BY [Current LSN] DESC;
SELECT @start AS last_commit, (SELECT COUNT(*) FROM sys.fn_full_dblog(NULL, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS active_log_records,
       (SELECT log_min_lsn FROM sys.dm_db_log_stats(@dbid)) AS log_min_lsn, (SELECT total_log_size_mb FROM sys.dm_db_log_stats(@dbid)) AS log_mb;
DECLARE @t TABLE (lsn nvarchar(46));
SET @t0 = SYSDATETIME();
WHILE @i < @n
BEGIN
  DELETE @t;
  INSERT @t SELECT [Current LSN] FROM sys.fn_full_dblog('0x' + @start, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
  WHERE (CONVERT(int, [Flag Bits]) & 5) <> 0 OR Operation IN (N'LOP_COMMIT_XACT', N'LOP_ABORT_XACT') OPTION (MAXDOP 1);
  SET @rows += @@ROWCOUNT;
  SET @i += 1;
END
SELECT DATEDIFF(microsecond, @t0, SYSDATETIME()) / 1000.0 / @n AS ms_per_incremental_read, @rows / @n AS rows_per_read;
-- versus reading from the start of the active log
SET @i = 0; SET @t0 = SYSDATETIME();
WHILE @i < 20
BEGIN
  DELETE @t;
  INSERT @t SELECT [Current LSN] FROM sys.fn_full_dblog(NULL, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
  WHERE (CONVERT(int, [Flag Bits]) & 5) <> 0 OR Operation IN (N'LOP_COMMIT_XACT', N'LOP_ABORT_XACT') OPTION (MAXDOP 1);
  SET @i += 1;
END
SELECT DATEDIFF(microsecond, @t0, SYSDATETIME()) / 1000.0 / 20 AS ms_per_full_active_log_read;
