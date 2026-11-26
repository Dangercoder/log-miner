-- 39: what logminer leaves in the plan cache with 1,200 databases (Windows, CU28, lmbench_0001..1200, 200 tx/s, log release on; run while the service runs).
-- Statements in a database's own context (layouts, sp_repldone) have a plan per database. Version 51c9240 / with the identity checked from master,
-- RECOMPILE on what is needed once per start, and DBCC OPENTRAN from master:
--   20 s after the start  306 MB (478 plans)  /  54 MB (118)
--   50 s                  306 MB (375)        /  38 MB  (99)
--   90 s, steady            5 MB   (7)        /   5 MB (141)   (the rest has been evicted)
--   SQL sessions of the service 45-105 s after the start, while 51c9240 is still starting: 31.9 % / 6.5 % of one core. In steady state (from 60 s on, 8 runs in
--   turn) they are the same: 4.7-8.8 % / 5.0-5.9 %, logminer.exe 11.5-15.9 % / 12.0-15.7 %; issue -> consumed p50 3.8-3.9 / 3.9-4.1, p99 6.9-14.1 / 7.2-9.2,
--   p99.9 11-22 / 16-19 ms (one run of the new version 113 ms, with an application insert of 208 ms: the host stood still).
--   The start is shorter: 20 s after it 4,466 of 6,000 events are late with 51c9240, 3,916 now; 30 s after it 4,183 / 2,931.
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
