-- What does it cost to check, with every log read, that the database id still means the same database?
SET NOCOUNT ON; USE master;
DECLARE @id int = DB_ID(N'tenant_0500'), @i int, @t datetime2, @x nvarchar(200), @g uniqueidentifier, @n int = 20000, @cpu int, @d datetime;
SET @i = 0; SET @cpu = (SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID); SET @t = SYSDATETIME();
WHILE @i < @n BEGIN SELECT @x = DB_NAME(@id); SET @i += 1; END
SELECT 'DB_NAME' AS what, DATEDIFF(microsecond, @t, SYSDATETIME()) * 1.0 / @n AS us, ((SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID) - @cpu) * 1000.0 / @n AS cpu_us;
SET @i = 0; SET @cpu = (SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID); SET @t = SYSDATETIME();
WHILE @i < @n BEGIN SELECT @g = recovery_fork_guid FROM sys.database_recovery_status WHERE database_id = @id; SET @i += 1; END
SELECT 'database_recovery_status', DATEDIFF(microsecond, @t, SYSDATETIME()) * 1.0 / @n, ((SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID) - @cpu) * 1000.0 / @n;
SET @i = 0; SET @cpu = (SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID); SET @t = SYSDATETIME();
WHILE @i < @n BEGIN SELECT @d = create_date FROM sys.databases WHERE database_id = @id; SET @i += 1; END
SELECT 'sys.databases', DATEDIFF(microsecond, @t, SYSDATETIME()) * 1.0 / @n, ((SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID) - @cpu) * 1000.0 / @n;
SET @i = 0; SET @cpu = (SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID); SET @t = SYSDATETIME();
WHILE @i < @n BEGIN SELECT @x = log_min_lsn FROM sys.dm_db_log_stats(@id); SET @i += 1; END
SELECT 'dm_db_log_stats (already part of every read)', DATEDIFF(microsecond, @t, SYSDATETIME()) * 1.0 / @n, ((SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID) - @cpu) * 1000.0 / @n;
SET @n = 500; SET @i = 0; SET @cpu = (SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID); SET @t = SYSDATETIME();
DECLARE @s nvarchar(46) = (SELECT log_end_lsn FROM sys.dm_db_log_stats(@id));
WHILE @i < @n BEGIN SELECT @x = MAX([Current LSN]) FROM sys.fn_full_dblog('0x' + @s, NULL, @id, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) OPTION (MAXDOP 1); SET @i += 1; END
SELECT 'an empty log read', DATEDIFF(microsecond, @t, SYSDATETIME()) * 1.0 / @n, ((SELECT cpu_time FROM sys.dm_exec_requests WHERE session_id = @@SPID) - @cpu) * 1000.0 / @n;
