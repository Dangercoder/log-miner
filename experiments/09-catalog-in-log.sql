-- What the engine logs about a table's physical layout when DDL runs on a CDC table, and whether row records of a dropped
-- rowset still identify it. Basis for "layout as of LSN" (docs/design.md).
IF DB_ID('lab_ddl') IS NOT NULL BEGIN ALTER DATABASE lab_ddl SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_ddl; END
CREATE DATABASE lab_ddl;
ALTER DATABASE lab_ddl SET RECOVERY FULL;
GO
USE lab_ddl;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL, s varchar(40) COLLATE Latin1_General_CI_AS NULL, d decimal(9,2) NULL, n int NULL);
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture';
EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
BACKUP DATABASE lab_ddl TO DISK = 'NUL';
GO
INSERT dbo.t VALUES (1, 10, 'one', 1.5, 7), (2, 20, 'two', 2.5, 8);
BEGIN TRAN z01_rebuild; ALTER INDEX ALL ON dbo.t REBUILD; COMMIT;
INSERT dbo.t VALUES (3, 30, 'three', 3.5, 9);
BEGIN TRAN z02_int_to_bigint; ALTER TABLE dbo.t ALTER COLUMN v bigint NULL; COMMIT;
BEGIN TRAN z03_widen_varchar; ALTER TABLE dbo.t ALTER COLUMN s varchar(200) COLLATE Latin1_General_CI_AS NULL; COMMIT;
BEGIN TRAN z04_collation; ALTER TABLE dbo.t ALTER COLUMN s varchar(200) COLLATE Cyrillic_General_CI_AS NULL; COMMIT;
BEGIN TRAN z05_dec_prec; ALTER TABLE dbo.t ALTER COLUMN d decimal(8,2) NULL; COMMIT;
BEGIN TRAN z06_dec_wider; ALTER TABLE dbo.t ALTER COLUMN d decimal(18,2) NULL; COMMIT;
BEGIN TRAN z07_notnull; ALTER TABLE dbo.t ALTER COLUMN n int NOT NULL; COMMIT;
BEGIN TRAN z08_to_max; ALTER TABLE dbo.t ALTER COLUMN s varchar(max) COLLATE Cyrillic_General_CI_AS NULL; COMMIT;
BEGIN TRAN z09_add_default; ALTER TABLE dbo.t ADD extra int NOT NULL CONSTRAINT df DEFAULT 5 WITH VALUES; COMMIT;
BEGIN TRAN z10_drop_col; ALTER TABLE dbo.t DROP COLUMN d; COMMIT;
GO
BEGIN TRY BEGIN TRAN z11_rename; EXEC sp_rename 'dbo.t.n', 'n2', 'COLUMN'; COMMIT; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK; SELECT 'rename column refused' AS z11, ERROR_NUMBER() AS err, ERROR_MESSAGE() AS msg; END CATCH;
GO
BEGIN TRY BEGIN TRAN z12_rename_table; EXEC sp_rename 'dbo.t', 't2'; COMMIT; SELECT 'rename table allowed' AS z12; EXEC sp_rename 'dbo.t2', 't'; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK; SELECT 'rename table refused' AS z12, ERROR_NUMBER() AS err, ERROR_MESSAGE() AS msg; END CATCH;
GO
BEGIN TRAN z13_rebuild_page; ALTER TABLE dbo.t REBUILD WITH (DATA_COMPRESSION = PAGE); COMMIT;
BEGIN TRAN z14_rebuild_none; ALTER TABLE dbo.t REBUILD WITH (DATA_COMPRESSION = NONE); COMMIT;
BEGIN TRAN z15_aborted; ALTER TABLE dbo.t ADD ghost int NULL; ROLLBACK;
ALTER INDEX ALL ON dbo.t REBUILD WITH (ONLINE = ON);
GO
-- 1. row records of the dropped rowsets: which identifying columns survive?
SELECT l.[Current LSN], l.Operation, l.Context, l.AllocUnitId, l.AllocUnitName, l.[PartitionId], LEFT(l.[Lock Information], 120) AS locks
FROM fn_dblog(NULL, NULL) l
WHERE CONVERT(int, l.[Flag Bits]) & 4 <> 0 AND l.Operation IN ('LOP_INSERT_ROWS', 'LOP_DELETE_ROWS', 'LOP_MODIFY_ROW', 'LOP_MODIFY_COLUMNS')
ORDER BY l.[Current LSN];
GO
-- 2. catalog row changes per DDL transaction (sysrscols 3, sysrowsets 5, sysallocunits 7, syscolpars 41, sysschobjs 34, sysseobjvalues)
DECLARE @lo nvarchar(50) = '0x' + (SELECT MIN([Current LSN]) FROM fn_dblog(NULL, NULL) WHERE [Transaction Name] = 'z01_rebuild');
SELECT ISNULL(b.[Transaction Name], '') AS xname, l.[Current LSN], l.Operation, l.Context, l.AllocUnitName, l.[Flag Bits] AS flags, l.[Offset in Row] AS offs,
       l.[RowLog Contents 0] AS rl0, l.[RowLog Contents 1] AS rl1
FROM fn_dblog(@lo, NULL) l
LEFT JOIN fn_dblog(@lo, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.sysrowsets.clust', 'sys.syscolpars.clst', 'sys.sysallocunits.clust', 'sys.sysschobjs.clst', 'sys.sysseobjvalues.clst', 'sys.sysidxstats.clst', 'sys.sysiscols.clst')
  AND l.Operation IN ('LOP_INSERT_ROWS', 'LOP_DELETE_ROWS', 'LOP_MODIFY_ROW', 'LOP_MODIFY_COLUMNS')
ORDER BY l.[Current LSN];
GO
-- 3. are the system base tables' own layouts and column names visible (so their rows can be decoded by metadata, not by hard-coded offsets)?
SELECT OBJECT_NAME(p.object_id) AS tbl, p.object_id, p.index_id, pc.partition_id, pc.partition_column_id, c.name, pc.system_type_id, pc.max_length, pc.leaf_offset, pc.leaf_null_bit
FROM sys.partitions p JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id
LEFT JOIN sys.all_columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id
WHERE p.object_id IN (3, 5, 7, 34, 41) AND p.index_id = 1 ORDER BY p.object_id, pc.partition_column_id;
SELECT object_id, name FROM sys.all_objects WHERE name IN ('sysrscols', 'sysrowsets', 'sysallocunits', 'sysschobjs', 'syscolpars', 'sysseobjvalues', 'sysidxstats', 'sysiscols');
SELECT OBJECT_DEFINITION(OBJECT_ID('sys.system_internals_partition_columns')) AS def;
SELECT OBJECT_DEFINITION(OBJECT_ID('sys.tables')) AS def;
SELECT name FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.fn_dblog') ORDER BY column_id;
