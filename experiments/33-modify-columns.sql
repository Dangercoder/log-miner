-- LOP_MODIFY_COLUMNS on the catalog's own tables: when does it occur, and what is in the record?
SET NOCOUNT ON; USE master;
IF DB_ID(N'lm_mc') IS NOT NULL BEGIN ALTER DATABASE lm_mc SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_mc; END;
CREATE DATABASE lm_mc; ALTER DATABASE lm_mc SET RECOVERY FULL;
GO
USE lm_mc;
CREATE PARTITION FUNCTION pf (int) AS RANGE RIGHT FOR VALUES (100, 200); CREATE PARTITION SCHEME ps AS PARTITION pf ALL TO ([PRIMARY]);
CREATE TABLE dbo.p (id int NOT NULL PRIMARY KEY, v varchar(20) NULL) ON ps (id);
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v varchar(20) NULL);
CREATE TABLE dbo.r (id int NOT NULL PRIMARY KEY, d decimal(9,4) NULL, s varchar(20) NULL);
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'p', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'r', @role_name = NULL;
EXEC sys.sp_cdc_drop_job N'capture'; EXEC sys.sp_cdc_drop_job N'cleanup';
GO
SELECT [Current LSN] AS lsn, Operation AS op, Context AS ctx, [Transaction ID] AS xid, [Transaction Name] AS xname, AllocUnitName AS au, [Offset in Row] AS ofs, [Modify Size] AS ms, [Num Elements] AS ne, [Log Record Length] AS len,
 [RowLog Contents 0] AS l0, [RowLog Contents 1] AS l1, [RowLog Contents 2] AS l2, [RowLog Contents 3] AS l3, [RowLog Contents 4] AS l4, [RowLog Contents 5] AS l5, [Log Record] AS rec
INTO #l FROM sys.fn_dblog(NULL, NULL);
SELECT xid, MAX(xname) AS name INTO #x FROM #l WHERE op = N'LOP_BEGIN_XACT' GROUP BY xid;
PRINT '== where';
SELECT x.name, l.op, l.ctx, l.au, COUNT(*) AS n FROM #l l LEFT JOIN #x x ON x.xid = l.xid WHERE l.op = N'LOP_MODIFY_COLUMNS' AND l.au LIKE N'sys.sys%' GROUP BY x.name, l.op, l.ctx, l.au ORDER BY n DESC;
PRINT '== the records on sysrscols and sysrowsets';
SELECT TOP (12) l.lsn, x.name, l.au, l.ofs, l.ms, l.ne, l.len, l.l0, l.l1, l.l2, l.l3, l.l4, l.l5 FROM #l l LEFT JOIN #x x ON x.xid = l.xid WHERE l.op = N'LOP_MODIFY_COLUMNS' AND (l.au LIKE N'sys.sysrscols%' OR l.au LIKE N'sys.sysrowsets%') ORDER BY l.lsn;
SELECT TOP (4) l.lsn, l.rec FROM #l l WHERE l.op = N'LOP_MODIFY_COLUMNS' AND l.au LIKE N'sys.sysrscols%' ORDER BY l.lsn;
PRINT '== for comparison: the same kind of change as LOP_MODIFY_ROW';
SELECT TOP (6) l.lsn, x.name, l.au, l.ofs, l.ms, l.ne, l.len, l.l0, l.l1, l.l2, l.l3 FROM #l l LEFT JOIN #x x ON x.xid = l.xid WHERE l.op = N'LOP_MODIFY_ROW' AND l.au LIKE N'sys.sysrscols%' ORDER BY l.lsn DESC;
PRINT '== where the fixed columns of sysrscols lie';
SELECT c.name, pc.leaf_offset, pc.max_inrow_length FROM sys.system_internals_partition_columns pc JOIN sys.partitions p ON p.partition_id = pc.partition_id JOIN sys.all_columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id WHERE p.object_id = OBJECT_ID(N'sys.sysrscols') AND p.index_id = 1 ORDER BY pc.leaf_offset;
GO
PRINT '== a user table, where before and after are known: two separate columns, then three';
CREATE TABLE dbo.u (id int NOT NULL PRIMARY KEY, a int NOT NULL, pad char(40) NOT NULL DEFAULT 'x', b int NOT NULL, pad2 char(40) NOT NULL DEFAULT 'y', c bigint NOT NULL);
INSERT dbo.u (id, a, b, c) VALUES (7, 0x01020304, 0x05060708, 0x1112131415161718);
UPDATE dbo.u SET a = 0x0A0B0C0D, b = 0x0E0F1011;
UPDATE dbo.u SET a = 0x21222324, b = 0x25262728, c = 0x3132333435363738;
UPDATE dbo.u SET a = 0x21222325, c = 0x3132333435363739;
SELECT [Current LSN], Operation, Context, [Offset in Row], [Modify Size], [Num Elements], [Log Record Length], [RowLog Contents 0], [RowLog Contents 1], [RowLog Contents 2], [RowLog Contents 3], [RowLog Contents 4], [RowLog Contents 5] FROM sys.fn_dblog(NULL, NULL) WHERE AllocUnitName LIKE N'dbo.u.%' AND Operation IN (N'LOP_MODIFY_ROW', N'LOP_MODIFY_COLUMNS');
GO
USE master; ALTER DATABASE lm_mc SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_mc;
GO
