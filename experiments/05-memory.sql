SELECT COUNT(*) AS dbs FROM sys.databases;
SELECT TOP (12) type, name, CAST(SUM(pages_kb) / 1024.0 AS decimal(10, 1)) AS mb
FROM sys.dm_os_memory_clerks GROUP BY type, name ORDER BY SUM(pages_kb) DESC;
SELECT physical_memory_in_use_kb / 1024 AS process_mb, memory_utilization_percentage, process_physical_memory_low, process_virtual_memory_low
FROM sys.dm_os_process_memory;
SELECT name, value_in_use FROM sys.configurations WHERE name IN ('max server memory (MB)', 'min server memory (MB)');
SELECT committed_kb / 1024 AS committed_mb, committed_target_kb / 1024 AS target_mb FROM sys.dm_os_sys_info;
