-- What makes dm_xe_session_targets expensive, and can it be narrowed?
SET NOCOUNT ON;
SELECT v = OBJECT_DEFINITION(OBJECT_ID('sys.dm_xe_session_targets'));
SELECT v = OBJECT_DEFINITION(OBJECT_ID('sys.dm_xe_sessions'));
SELECT v = OBJECT_DEFINITION(OBJECT_ID('sys.dm_xe_session_events'));
SELECT v = OBJECT_DEFINITION(OBJECT_ID('sys.dm_xe_session_object_columns'));
SELECT name, v = OBJECT_DEFINITION(object_id) FROM sys.system_objects WHERE name LIKE 'dm_xe%' AND type = 'V' ORDER BY name;
SELECT name FROM sys.system_objects WHERE name LIKE '%xe[_]%' AND type NOT IN ('V') ORDER BY name;
GO
CREATE EVENT SESSION logminer_lab_bell ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4) ADD TARGET package0.event_counter
WITH (MAX_MEMORY = 1024 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE);
ALTER EVENT SESSION logminer_lab_bell ON SERVER STATE = START;
GO
SELECT s.name, t.target_name, LEN(t.target_data) AS len, t.execution_count, t.execution_duration_ms FROM sys.dm_xe_session_targets t LEFT JOIN sys.dm_xe_sessions s ON s.address = t.event_session_address;
GO
DECLARE @a varbinary(8) = (SELECT address FROM sys.dm_xe_sessions WHERE name = 'logminer_lab_bell'), @n int, @i int = 0, @c int = @@CPU_BUSY, @t datetime2(7) = SYSDATETIME(), @x bigint;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT @n = COUNT(*) FROM sys.dm_xe_session_targets; SET @i += 1; END; SELECT 'count all' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT TOP (1) @x = execution_count FROM sys.dm_xe_session_targets; SET @i += 1; END; SELECT 'top 1' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT @x = execution_count FROM sys.dm_xe_session_targets WHERE event_session_address = @a; SET @i += 1; END; SELECT 'by address' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT TOP (1) @x = execution_count FROM sys.dm_xe_session_targets WHERE event_session_address = @a OPTION (RECOMPILE); SET @i += 1; END; SELECT 'top 1 by address' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT @n = COUNT(*) FROM sys.dm_xe_session_events; SET @i += 1; END; SELECT 'session_events' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT @n = COUNT(*) FROM sys.dm_xe_session_object_columns; SET @i += 1; END; SELECT 'object_columns' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
SET @i = 0; SET @t = SYSDATETIME(); WHILE @i < 50 BEGIN SELECT @n = COUNT(*) FROM sys.dm_xe_sessions; SET @i += 1; END; SELECT 'sessions' AS what, DATEDIFF_BIG(MICROSECOND, @t, SYSDATETIME()) / 50 AS us;
GO
DROP EVENT SESSION logminer_lab_bell ON SERVER;
