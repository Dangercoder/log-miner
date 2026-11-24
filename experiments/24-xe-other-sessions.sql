-- Does forcing the dispatch of one session also dispatch others (system_health!)? A second session gets events all the time, with a 30 s
-- dispatch latency of its own; its target's execution_count tells how often it was dispatched.
SET NOCOUNT ON;
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'logminer_lab_mine') DROP EVENT SESSION logminer_lab_mine ON SERVER;
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'logminer_lab_other') DROP EVENT SESSION logminer_lab_other ON SERVER;
CREATE EVENT SESSION logminer_lab_mine ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4) ADD TARGET package0.ring_buffer (SET max_memory = 64)
WITH (MAX_MEMORY = 1024 KB, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE);
CREATE EVENT SESSION logminer_lab_other ON SERVER ADD EVENT sqlserver.sp_statement_completed, ADD EVENT sqlserver.sql_statement_completed ADD TARGET package0.ring_buffer (SET max_memory = 256)
WITH (MAX_MEMORY = 4096 KB, MAX_DISPATCH_LATENCY = 30 SECONDS, MEMORY_PARTITION_MODE = NONE);
ALTER EVENT SESSION logminer_lab_mine ON SERVER STATE = START;
ALTER EVENT SESSION logminer_lab_other ON SERVER STATE = START;
GO
DECLARE @mine varbinary(8) = (SELECT address FROM sys.dm_xe_sessions WHERE name = 'logminer_lab_mine'), @other varbinary(8) = (SELECT address FROM sys.dm_xe_sessions WHERE name = 'logminer_lab_other');
DECLARE @i int, @n int, @t datetime2(7), @r nvarchar(400);
DECLARE @out TABLE (what varchar(60), seconds decimal(9, 2), loops int, other_bytes bigint, other_executions bigint);
DECLARE @b0 bigint, @e0 bigint, @b1 bigint, @e1 bigint, @round int = 0;
WHILE @round < 3
BEGIN
    -- baseline: a busy loop of statements (they are the other session's events), no forcing
    SELECT @b0 = total_bytes_generated FROM sys.dm_xe_sessions WHERE address = @other;
    SELECT @e0 = execution_count FROM sys.dm_xe_session_targets WHERE event_session_address = @other;
    SET @t = SYSDATETIME(); SET @i = 0;
    WHILE @i < 3000 BEGIN SELECT @n = COUNT(*) FROM sys.dm_xe_session_events WHERE event_session_address = @mine; SET @i += 1; END;
    SELECT @b1 = total_bytes_generated FROM sys.dm_xe_sessions WHERE address = @other;
    SELECT @e1 = execution_count FROM sys.dm_xe_session_targets WHERE event_session_address = @other;
    INSERT @out VALUES ('not forcing', DATEDIFF(ms, @t, SYSDATETIME()) / 1000.0, @i, @b1 - @b0, @e1 - @e0);
    -- the same, forcing my session
    SELECT @b0 = total_bytes_generated FROM sys.dm_xe_sessions WHERE address = @other;
    SELECT @e0 = execution_count FROM sys.dm_xe_session_targets WHERE event_session_address = @other;
    SET @t = SYSDATETIME(); SET @i = 0;
    WHILE @i < 3000 BEGIN SELECT @n = COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = @mine; SET @i += 1; END;
    SELECT @b1 = total_bytes_generated FROM sys.dm_xe_sessions WHERE address = @other;
    SELECT @e1 = execution_count FROM sys.dm_xe_session_targets WHERE event_session_address = @other;
    INSERT @out VALUES ('forcing only my session', DATEDIFF(ms, @t, SYSDATETIME()) / 1000.0, @i, @b1 - @b0, @e1 - @e0);
    SET @round += 1;
END;
SELECT * FROM @out;
GO
SELECT s.name, t.target_name, t.execution_count, t.execution_duration_ms, s.total_bytes_generated, s.create_time FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_targets t ON t.event_session_address = s.address;
GO
-- what the built-in telemetry session collects
SELECT e.event_name, e.event_fire_count, e.event_fire_average_time, e.event_fire_min_time, e.event_fire_max_time FROM sys.dm_xe_sessions s JOIN sys.dm_xe_session_events e ON e.event_session_address = s.address WHERE s.name = 'telemetry_xevents' AND e.event_fire_count > 0 ORDER BY e.event_fire_count DESC;
GO
DROP EVENT SESSION logminer_lab_mine ON SERVER;
DROP EVENT SESSION logminer_lab_other ON SERVER;
GO
