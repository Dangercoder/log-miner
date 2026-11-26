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

-- 38b: which operations the log has on sys.sysschobjs (allocation unit 281474978938880) for every kind of DDL on a table: is a rename ever LOP_MODIFY_COLUMNS?
SET NOCOUNT ON; USE master; IF DB_ID('lm_ren') IS NOT NULL BEGIN ALTER DATABASE lm_ren SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_ren; END; CREATE DATABASE lm_ren; ALTER DATABASE lm_ren SET RECOVERY FULL;
GO
USE lm_ren; EXEC ('CREATE SCHEMA s1'); EXEC ('CREATE SCHEMA s2');
GO
CREATE TABLE dbo.t (id int NOT NULL, v int); CREATE TABLE dbo.p (id int NOT NULL PRIMARY KEY);
GO
CREATE TRIGGER dbo.trg ON dbo.t AFTER INSERT AS RETURN;
GO
DECLARE @i int = 0, @n nvarchar(128), @prev nvarchar(128) = N't', @schema sysname = N'dbo', @sql nvarchar(max), @r int;
WHILE @i < 400
BEGIN
    SET @r = @i % 8;
    SET @n = CASE @r WHEN 0 THEN N't' WHEN 1 THEN N'table_' + REPLICATE(N'x', @i % 100) WHEN 2 THEN N'table_' + REPLICATE(N'x', @i % 90) + N'y' WHEN 3 THEN N'tå ble 表 ' + CONVERT(nvarchar(10), @i)
                     WHEN 4 THEN N'prefix_' + CONVERT(nvarchar(10), @i) + N'_suffix' WHEN 5 THEN N'prefix_suffix' WHEN 6 THEN REPLICATE(N'z', 128) ELSE N'Table_' + CONVERT(nvarchar(10), @i % 7) END;
    IF @n <> @prev COLLATE Latin1_General_BIN2
    BEGIN
        SET @sql = N'EXEC sp_rename ' + QUOTENAME(QUOTENAME(@schema) + N'.' + QUOTENAME(@prev), '''') + N', ' + QUOTENAME(@n, '''');
        IF LEN(@n) >= 110 SET @sql = N'EXEC sp_rename @a, @b';
        DECLARE @full nvarchar(400) = QUOTENAME(@schema) + N'.' + QUOTENAME(@prev);
        EXEC sys.sp_rename @full, @n;
        SET @prev = @n;
    END;
    IF @i % 5 = 0
    BEGIN
        DECLARE @to sysname = CASE @i / 5 % 3 WHEN 0 THEN N's1' WHEN 1 THEN N's2' ELSE N'dbo' END;
        IF @to <> @schema BEGIN SET @sql = N'ALTER SCHEMA ' + @to + N' TRANSFER ' + QUOTENAME(@schema) + N'.' + QUOTENAME(@prev); EXEC (@sql); SET @schema = @to; END;
    END;
    SET @full = QUOTENAME(@schema) + N'.' + QUOTENAME(@prev);
    IF @i % 40 = 7 EXEC (N'ALTER TABLE ' + @full + N' ADD c' + @i + N' int NULL');
    IF @i % 40 = 17 EXEC (N'ALTER TABLE ' + @full + N' ADD CONSTRAINT ck' + @i + N' CHECK (id > -1)');
    IF @i % 40 = 27 EXEC (N'CREATE INDEX ix' + @i + N' ON ' + @full + N' (v); ALTER INDEX ix' + @i + N' ON ' + @full + N' REBUILD');
    IF @i % 40 = 37 EXEC (N'ALTER TABLE ' + @full + N' SET (LOCK_ESCALATION = DISABLE); ALTER TABLE ' + @full + N' DISABLE TRIGGER ALL');
    IF @i % 100 = 50 EXEC (N'BEGIN TRAN; EXEC sys.sp_rename ''' + @full + N''', ''rolledback''; ROLLBACK');
    SET @i += 1;
END;
DECLARE @id varbinary(4) = CONVERT(binary(4), REVERSE(CONVERT(binary(4), OBJECT_ID(QUOTENAME(@schema) + N'.' + QUOTENAME(@prev)))));
SELECT Operation, Context, CASE WHEN [Offset in Row] >= 47 THEN 'name' WHEN [Offset in Row] BETWEEN 8 AND 11 THEN 'nsid' ELSE CONVERT(varchar(10), [Offset in Row]) END AS place, CASE WHEN CONVERT(int, [Flag Bits]) & 1 = 1 THEN 'undo' ELSE '' END AS undo, COUNT(*) AS n,
       MIN([Offset in Row]) AS min_offset, MAX([Offset in Row]) AS max_offset
FROM sys.fn_dblog(NULL, NULL) WHERE AllocUnitId = 281474978938880 AND CHARINDEX(@id, [RowLog Contents 2]) > 0 GROUP BY Operation, Context, CASE WHEN [Offset in Row] >= 47 THEN 'name' WHEN [Offset in Row] BETWEEN 8 AND 11 THEN 'nsid' ELSE CONVERT(varchar(10), [Offset in Row]) END, CASE WHEN CONVERT(int, [Flag Bits]) & 1 = 1 THEN 'undo' ELSE '' END;
SELECT Operation, COUNT(*) AS n FROM sys.fn_dblog(NULL, NULL) WHERE AllocUnitId = 281474978938880 GROUP BY Operation;
-- schema ids are used again
DROP TABLE IF EXISTS dbo.none; EXEC ('CREATE SCHEMA s8'); DECLARE @s8 int = SCHEMA_ID('s8'); EXEC ('DROP SCHEMA s8'); EXEC ('CREATE SCHEMA s9'); SELECT @s8 AS dropped_schema_id, SCHEMA_ID('s9') AS next_schema_id;
GO
USE master; ALTER DATABASE lm_ren SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_ren;

-- Result (lab, CU28): of 403 renames of one table 356 are LOP_MODIFY_ROW (end offset and name in one place, offset 54 = fixed part + 10), 47 LOP_MODIFY_COLUMNS;
-- transfers are LOP_MODIFY_ROW at 8 (nsid); schema ids are used again (7, 7).
-- LOP_MODIFY_COLUMNS of a name, e.g. abcdefghijkYmnop..XyX -> ..XyX_longer: contents 0 = offsets before/after of each place (0x36,0x36)(0x6C,0x6C), contents 1 = the
-- lengths they HAD (1, 0), contents 4 = old and new bytes of each, aligned to four: 6C000000 7A000000 | (nothing) 5F006C006F006E00670065007200. The new lengths
-- follow from the offsets of the next place, the last one from the change of the end offset. 43 of 43 such records of a random chain decoded exactly.
