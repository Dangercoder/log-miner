-- 39: what logminer leaves in the plan cache with 1,200 databases (Windows, CU28, lmbench_0001..1200, 200 tx/s, log release on; run while the service runs).
-- Statements in a database's own context (layouts, sp_repldone) have a plan per database. Version ea7a67a / with the identity checked from master,
-- RECOMPILE on what is needed once per start, and DBCC OPENTRAN from master:
--   20 s after the start  306 MB (478 plans)  /  54 MB (118)
--   50 s                  306 MB (375)        /  38 MB  (99)
--   90 s, steady            5 MB   (7)        /   5 MB (141)   (the rest has been evicted)
--   SQL sessions of the service: 31.9 % / 6.5 % of one core; logminer.exe 14.9 % / 14.1 %; issue -> consumed p50 3.8 / 4.0, p99 7.3 / 7.5, p99.9 19.9 / 15.8 ms
-- CACHESTORE_PHDR (bound trees of the catalog views, SQL Server's own) is 550-780 MB for a minute after the start with both, and is evicted.
SET NOCOUNT ON;
WITH p AS (
    SELECT CASE WHEN d.name LIKE 'lmbench[_]%' THEN 'in the databases' WHEN pa.dbid = 1 THEN 'master' ELSE 'other' END AS place,
           CASE WHEN t.text LIKE N'%INSERT dbo.orders%' OR t.text LIKE N'%INSERT INTO dbo.%' THEN 'the application (load)' WHEN t.text LIKE N'%@dbid%' OR t.text LIKE N'%logminer%' OR t.text LIKE N'%@fork%' THEN 'logminer' ELSE 'other' END AS kind,
           cp.size_in_bytes
    FROM sys.dm_exec_cached_plans cp
    CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) t
    CROSS APPLY (SELECT CONVERT(int, value) AS dbid FROM sys.dm_exec_plan_attributes(cp.plan_handle) WHERE attribute = 'dbid') pa
    LEFT JOIN sys.databases d ON d.database_id = pa.dbid
    WHERE cp.cacheobjtype = N'Compiled Plan')
SELECT place, kind, COUNT(*) AS plans, SUM(CONVERT(bigint, size_in_bytes)) / 1024 / 1024 AS mb FROM p GROUP BY place, kind ORDER BY 4 DESC;
SELECT type, SUM(pages_kb) / 1024 AS mb FROM sys.dm_os_memory_clerks WHERE type IN ('CACHESTORE_SQLCP', 'CACHESTORE_PHDR') GROUP BY type;
