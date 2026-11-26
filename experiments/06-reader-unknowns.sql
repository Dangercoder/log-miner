-- (a) is log_min_lsn a valid @start?  (b) sp_repldone via three-part name from master?
-- (d) partition_column_id vs column_id after ADD/DROP COLUMN and with a uniqueifier
USE master;
DECLARE @dbid int = DB_ID('lab_a'), @min nvarchar(46);
SELECT @min = log_min_lsn FROM sys.dm_db_log_stats(@dbid);
SELECT @min AS log_min_lsn;
SELECT TOP (3) [Current LSN], Operation FROM sys.fn_full_dblog('0x' + @min, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
GO
BEGIN TRAN; INSERT lab_a.dbo.t_cdc (id, a) VALUES (900, 1); COMMIT;
DECLARE @dbid int = DB_ID('lab_a'), @begin nvarchar(46), @commit nvarchar(46);
SELECT TOP (1) @commit = [Current LSN], @begin = [Transaction Begin]
FROM sys.fn_full_dblog(NULL, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0 ORDER BY [Current LSN] DESC;
DECLARE @x binary(10) = CONVERT(binary(10), '0x' + REPLACE(@begin, ':', ''), 1), @s binary(10) = CONVERT(binary(10), '0x' + REPLACE(@commit, ':', ''), 1);
EXEC lab_a.sys.sp_repldone @xactid = @x, @xact_seqno = @s, @numtrans = 0, @time = 0, @reset = 0;
EXEC lab_a.sys.sp_replflush;
SELECT 'repldone via three-part name ok' AS b, @begin AS begin_lsn, @commit AS commit_lsn;
GO
USE lab_a;
IF OBJECT_ID('dbo.t_map') IS NOT NULL DROP TABLE dbo.t_map;
CREATE TABLE dbo.t_map (k int NOT NULL, a int NULL, b varchar(10) NULL, c int NULL);
CREATE CLUSTERED INDEX cx ON dbo.t_map (k);   -- non-unique: uniqueifier
ALTER TABLE dbo.t_map DROP COLUMN a;
ALTER TABLE dbo.t_map ADD d bigint NULL;
SELECT pc.partition_column_id, pc.is_dropped, pc.is_uniqueifier, pc.leaf_offset, pc.leaf_null_bit, pc.key_ordinal, TYPE_NAME(pc.system_type_id) AS type,
       c.column_id, c.name
FROM sys.partitions p
JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id
LEFT JOIN sys.columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id
WHERE p.object_id = OBJECT_ID('dbo.t_map') AND p.index_id IN (0, 1)
ORDER BY pc.partition_column_id;
SELECT column_id, name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.t_map') ORDER BY column_id;
