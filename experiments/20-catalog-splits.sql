-- Page splits in the catalog: do the moved rows (logged as inserts of a system transaction) belong to the table being altered?
IF DB_ID('lab_split') IS NOT NULL BEGIN ALTER DATABASE lab_split SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_split; END
CREATE DATABASE lab_split; ALTER DATABASE lab_split SET RECOVERY FULL;
GO
USE lab_split;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);
BACKUP DATABASE lab_split TO DISK = 'NUL';
DECLARE @i int = 0, @sql nvarchar(max);
WHILE @i < 12 BEGIN SELECT @sql = N'ALTER TABLE dbo.t ADD ' + STRING_AGG(CONVERT(nvarchar(max), CONCAT(N'col_', @i, N'_', value, N'_', REPLICATE(N'x', 60), N' int NULL')), N', ') FROM GENERATE_SERIES(1, 20); EXEC (@sql); SET @i += 1; END
DECLARE @oid binary(4) = CONVERT(binary(4), REVERSE(CONVERT(binary(4), OBJECT_ID('dbo.t'))));
DECLARE @rs binary(8) = CONVERT(binary(8), REVERSE(CONVERT(binary(8), (SELECT partition_id FROM sys.partitions WHERE object_id = OBJECT_ID('dbo.t')))));
SELECT l.AllocUnitName, l.Operation, l.Context, b.[Transaction Name], b.[Xact Type], COUNT(*) AS n, MAX(DATALENGTH(l.[RowLog Contents 0])) AS maxlen,
       SUM(CASE WHEN SUBSTRING(l.[RowLog Contents 0], 5, 4) = @oid AND l.AllocUnitName = 'sys.syscolpars.clst' THEN 1 WHEN SUBSTRING(l.[RowLog Contents 0], 5, 8) = @rs AND l.AllocUnitName = 'sys.sysrscols.clst' THEN 1 ELSE 0 END) AS of_t, MIN(l.[Num Elements]) AS min_el, MAX(l.[Num Elements]) AS max_el
FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.syscolpars.clst') AND l.Operation IN ('LOP_INSERT_ROWS', 'LOP_DELETE_ROWS', 'LOP_MODIFY_ROW', 'LOP_DELETE_SPLIT')
GROUP BY l.AllocUnitName, l.Operation, l.Context, b.[Transaction Name], b.[Xact Type] ORDER BY 1, 4, 2;
SELECT TOP (6) l.AllocUnitName, l.[Num Elements], l.[RowLog Contents 0] AS rl0, l.[RowLog Contents 1] AS rl1 FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT' AND b.[Transaction Name] = 'SplitPage'
WHERE l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.syscolpars.clst') AND l.Operation = 'LOP_INSERT_ROWS' AND l.Context = 'LCX_CLUSTERED';
GO
USE master; ALTER DATABASE lab_split SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_split;
