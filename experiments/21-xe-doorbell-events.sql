-- Which extended events tell "database X flushed log", what do they carry, and which targets are synchronous?
SET NOCOUNT ON;
SELECT p.name AS package, o.name, LEFT(o.description, 110) AS description
FROM sys.dm_xe_objects o JOIN sys.dm_xe_packages p ON p.guid = o.package_guid
WHERE o.object_type = 'event' AND (o.name LIKE '%log_flush%' OR o.name LIKE '%log_block%' OR o.name IN ('sql_transaction', 'databases_log_flush', 'databases_log_flush_wait', 'file_write_completed', 'file_write_enqueued', 'async_io_completed'))
ORDER BY 1, 2;
SELECT object_name, name, type_name, column_type, column_value
FROM sys.dm_xe_object_columns
WHERE object_name IN ('log_flush_complete', 'log_flush_start', 'log_flush_requested', 'databases_log_flush', 'file_write_completed') AND column_type <> 'readonly'
ORDER BY object_name, column_id;
SELECT p.name AS package, o.name, o.capabilities_desc, LEFT(o.description, 110) AS description
FROM sys.dm_xe_objects o JOIN sys.dm_xe_packages p ON p.guid = o.package_guid
WHERE o.object_type = 'target' ORDER BY 2;
SELECT o.name AS target, c.name, c.type_name, c.column_value, LEFT(c.description, 110) AS description
FROM sys.dm_xe_objects o JOIN sys.dm_xe_object_columns c ON c.object_name = o.name AND c.object_package_guid = o.package_guid
WHERE o.object_type = 'target' AND c.column_type = 'customizable' ORDER BY 1, c.column_id;
SELECT name, OBJECT_ID('sys.fn_MSxe_read_event_stream') AS id FROM sys.system_objects WHERE name LIKE 'fn[_]MSxe%';
SELECT parameter_id, name, TYPE_NAME(system_type_id) AS t, max_length FROM sys.system_parameters WHERE object_id = OBJECT_ID('sys.fn_MSxe_read_event_stream');
SELECT column_id, name, TYPE_NAME(system_type_id) AS t, max_length FROM sys.system_columns WHERE object_id = OBJECT_ID('sys.fn_MSxe_read_event_stream');
SELECT s.name, s.total_buffer_size, s.buffer_policy_desc, s.flag_desc, s.total_regular_buffers, s.regular_buffer_size FROM sys.dm_xe_sessions s;
