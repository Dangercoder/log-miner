-- (a) can collation ids be mapped without DAC; (b) how do system transactions (page splits moving catalog rows) look;
-- (c) metadata-only default values in the log; (d) what log_end_lsn points at.
USE lab_ddl;
SELECT TOP (3) name, COLLATIONPROPERTY(name, 'CollationId') AS cid, COLLATIONPROPERTY(name, 'CodePage') AS cp FROM sys.fn_helpcollations() WHERE name IN ('Latin1_General_CI_AS', 'Cyrillic_General_CI_AS', 'Latin1_General_100_CI_AS_SC_UTF8');
SELECT COUNT(*) AS collations, COUNT(DISTINCT CONVERT(int, COLLATIONPROPERTY(name, 'CollationId'))) AS ids FROM sys.fn_helpcollations();
GO
SELECT [Transaction Name], [Xact Type], [Parent Transaction ID], COUNT(*) AS n
FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_XACT' GROUP BY [Transaction Name], [Xact Type], [Parent Transaction ID] ORDER BY 1;
GO
SELECT l.[Current LSN], l.Operation, l.Context, l.AllocUnitName, l.AllocUnitId, l.[RowLog Contents 0] AS rl0
FROM fn_dblog(NULL, NULL) l WHERE l.AllocUnitName LIKE 'sys.sysseobjvalues%' AND l.Operation IN ('LOP_INSERT_ROWS', 'LOP_DELETE_ROWS') ORDER BY 1;
SELECT pc.partition_column_id, c.name, pc.system_type_id, pc.max_length, pc.leaf_offset, pc.leaf_null_bit FROM sys.partitions p JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id
LEFT JOIN sys.all_columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id WHERE p.object_id = 9 AND p.index_id = 1;
GO
SELECT log_end_lsn, (SELECT MAX([Current LSN]) FROM sys.fn_full_dblog(NULL, NULL, DB_ID(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)) AS last_record FROM sys.dm_db_log_stats(DB_ID());
GO
-- sysrscols rows of ALTER COLUMN int -> bigint (z02): how rscolid / hbcolid move
SELECT l.[Current LSN], l.Operation, l.[RowLog Contents 0] AS row_ FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.[Transaction Name] = 'z02_int_to_bigint'
WHERE l.AllocUnitName = 'sys.sysrscols.clst' ORDER BY 1;
