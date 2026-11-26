-- What does the built-in telemetry session hold (which events, how many, when)?
SET NOCOUNT ON;
DECLARE @x xml = (SELECT CAST(t.target_data AS xml) FROM sys.dm_xe_session_targets t JOIN sys.dm_xe_sessions s ON s.address = t.event_session_address WHERE s.name = 'telemetry_xevents' AND t.target_name = 'ring_buffer');
SELECT @x.value('(/RingBufferTarget/@eventCount)[1]', 'int') AS in_buffer, @x.value('(/RingBufferTarget/@totalEventsProcessed)[1]', 'bigint') AS total, @x.value('(/RingBufferTarget/@droppedCount)[1]', 'bigint') AS dropped;
SELECT event, COUNT(*) AS n, MIN(ts) AS first_ts, MAX(ts) AS last_ts
FROM (SELECT n.value('@name', 'sysname') AS event, n.value('@timestamp', 'varchar(30)') AS ts FROM @x.nodes('/RingBufferTarget/event') x(n)) e GROUP BY event ORDER BY n DESC;
SELECT TOP (3) CONVERT(nvarchar(1500), n.query('.')) AS sample FROM @x.nodes('/RingBufferTarget/event') x(n) ORDER BY n.value('@timestamp', 'varchar(30)') DESC;
SELECT e.name AS event FROM sys.server_event_sessions s JOIN sys.server_event_session_events e ON e.event_session_id = s.event_session_id WHERE s.name = 'telemetry_xevents' ORDER BY e.name;
