SET NOCOUNT ON; USE master; IF DB_ID('lm_ren') IS NOT NULL DROP DATABASE lm_ren; CREATE DATABASE lm_ren; ALTER DATABASE lm_ren SET RECOVERY FULL;
GO
USE lm_ren; EXEC sys.sp_cdc_enable_db; EXEC ('CREATE SCHEMA other');
GO
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int); CREATE TABLE dbo.t_new (id int NOT NULL PRIMARY KEY, v int);
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
CREATE TABLE #m (n int IDENTITY, what varchar(40), lsn nvarchar(46)); INSERT #m SELECT 'start', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
GO
EXEC sp_rename 'dbo.t','t_old'; INSERT #m SELECT 'rename longer', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
EXEC sp_rename 'dbo.t_old','t_odd'; INSERT #m SELECT 'rename same length', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
ALTER SCHEMA other TRANSFER dbo.t_odd; INSERT #m SELECT 'transfer', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
ALTER TABLE other.t_odd ADD c int NULL; INSERT #m SELECT 'add column', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
EXEC sys.sp_cdc_enable_table @source_schema=N'other', @source_name=N't_odd', @role_name=NULL, @capture_instance='second'; INSERT #m SELECT 'cdc second', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
ALTER INDEX ALL ON other.t_odd REBUILD; INSERT #m SELECT 'rebuild', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
EXEC sp_rename 'other.t_odd.c', 'cc', 'COLUMN'; INSERT #m SELECT 'rename column', MAX([Current LSN]) FROM sys.fn_dblog(NULL,NULL);
DECLARE @id varbinary(4) = CONVERT(binary(4), REVERSE(CONVERT(binary(4), OBJECT_ID('other.t_odd'))));
SELECT m.what, l.Operation, l.Context, l.[Offset in Row] AS ofs, l.[Modify Size] AS sz, CONVERT(varchar(70), l.[RowLog Contents 0], 1) AS c0, CONVERT(varchar(70), l.[RowLog Contents 1], 1) AS c1, CONVERT(varchar(40), l.[RowLog Contents 2], 1) AS c2, CHARINDEX(@id, l.[RowLog Contents 2]) AS idpos
FROM #m m JOIN #m b ON b.n = m.n - 1 JOIN sys.fn_dblog(NULL,NULL) l ON l.[Current LSN] > b.lsn AND l.[Current LSN] <= m.lsn WHERE l.AllocUnitId = 281474978938880 AND (CHARINDEX(@id, l.[RowLog Contents 2]) > 0 OR CHARINDEX(@id, l.[RowLog Contents 0]) > 0) ORDER BY l.[Current LSN];
GO
USE master; ALTER DATABASE lm_ren SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_ren;
