SET NOCOUNT ON; USE master;
DECLARE @i int = 1, @p nvarchar(300); CREATE TABLE #a (au bigint, name sysname);
WHILE @i <= 1200 BEGIN SET @p = N'lmbench_' + RIGHT(N'000' + CONVERT(nvarchar(4), @i), 4) + N'.sys.sp_executesql'; INSERT #a EXEC @p N'SELECT au.allocation_unit_id, o.name FROM sys.all_objects o JOIN sys.partitions p ON p.object_id = o.object_id AND p.index_id = 1 JOIN sys.allocation_units au ON au.container_id = p.hobt_id AND au.type = 1 WHERE o.schema_id = 4 AND o.name = N''sysschobjs'''; SET @i += 100; END
INSERT #a EXEC tenant_0007.sys.sp_executesql N'SELECT au.allocation_unit_id, o.name FROM sys.all_objects o JOIN sys.partitions p ON p.object_id = o.object_id AND p.index_id = 1 JOIN sys.allocation_units au ON au.container_id = p.hobt_id AND au.type = 1 WHERE o.schema_id = 4 AND o.name = N''sysschobjs''';
SELECT au, name, COUNT(*) AS dbs FROM #a GROUP BY au, name;
-- what touches it? on a scratch database: create, CDC on/off, alter, truncate, statistics, inserts, identity
IF DB_ID('lm_cat') IS NOT NULL DROP DATABASE lm_cat; CREATE DATABASE lm_cat; ALTER DATABASE lm_cat SET RECOVERY FULL;
GO
USE lm_cat; EXEC sys.sp_cdc_enable_db; CREATE TABLE dbo.k (id int IDENTITY PRIMARY KEY, v int, w varchar(20)); INSERT dbo.k (v, w) SELECT 1, 'x' FROM sys.all_objects;
CREATE TABLE #m (n int IDENTITY, what varchar(60), lsn nvarchar(46)); INSERT #m SELECT 'start', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
GO
CREATE TABLE dbo.t (a int PRIMARY KEY, b int); INSERT #m SELECT 'create table', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
GO
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL; INSERT #m SELECT 'cdc on', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
GO
INSERT dbo.t SELECT object_id, 1 FROM sys.all_objects; INSERT #m SELECT 'inserts', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
SELECT COUNT(*) FROM dbo.k WHERE v = 1 AND w = 'x'; INSERT #m SELECT 'auto statistics', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
UPDATE STATISTICS dbo.k; INSERT #m SELECT 'update statistics', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
TRUNCATE TABLE dbo.k; INSERT #m SELECT 'truncate', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
ALTER INDEX ALL ON dbo.t REBUILD; INSERT #m SELECT 'rebuild', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
ALTER TABLE dbo.t ADD c int NULL; INSERT #m SELECT 'add column', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
EXEC sys.sp_cdc_disable_table @source_schema = N'dbo', @source_name = N't', @capture_instance = N'all'; INSERT #m SELECT 'cdc off', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
EXEC sp_rename 'dbo.k', 'k2'; INSERT #m SELECT 'rename', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
DROP TABLE dbo.k2; INSERT #m SELECT 'drop', MAX([Current LSN]) FROM sys.fn_dblog(NULL, NULL);
SELECT m.n, m.what, COUNT(l.[Current LSN]) AS sysschobjs_records FROM #m m JOIN #m b ON b.n = m.n - 1 LEFT JOIN sys.fn_dblog(NULL, NULL) l ON l.[Current LSN] > b.lsn AND l.[Current LSN] <= m.lsn AND l.AllocUnitId = 281474978938880 AND l.Operation IN ('LOP_INSERT_ROWS', 'LOP_DELETE_ROWS', 'LOP_MODIFY_ROW', 'LOP_MODIFY_COLUMNS') GROUP BY m.n, m.what ORDER BY m.n;
GO
USE master; ALTER DATABASE lm_cat SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_cat;
