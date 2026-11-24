-- What does the log of a captured table say about ALTER TABLE ... SWITCH (rows appear or disappear without row records) and about DDL that
-- converts or adds values of existing rows? Is there a replication marker, and what changes in the catalog?
SET NOCOUNT ON; USE master;
IF DB_ID(N'lm_sw') IS NOT NULL BEGIN ALTER DATABASE lm_sw SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_sw; END;
CREATE DATABASE lm_sw; ALTER DATABASE lm_sw SET RECOVERY FULL;
GO
USE lm_sw;
EXEC sys.sp_cdc_enable_db;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v varchar(20) NULL);
CREATE TABLE dbo.staging (id int NOT NULL PRIMARY KEY, v varchar(20) NULL);
CREATE PARTITION FUNCTION pf (int) AS RANGE RIGHT FOR VALUES (100, 200); CREATE PARTITION SCHEME ps AS PARTITION pf ALL TO ([PRIMARY]);
CREATE TABLE dbo.p (id int NOT NULL PRIMARY KEY, v varchar(20) NULL) ON ps (id);
CREATE TABLE dbo.p1 (id int NOT NULL PRIMARY KEY, v varchar(20) NULL, CHECK (id < 100));
CREATE TABLE dbo.p2 (id int NOT NULL PRIMARY KEY, v varchar(20) NULL, CHECK (id >= 100 AND id < 200));
CREATE TABLE dbo.r (id int NOT NULL PRIMARY KEY, d decimal(9,4) NULL, s varchar(20) NULL);
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N't', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'p', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'r', @role_name = NULL;
EXEC sys.sp_cdc_drop_job N'capture'; EXEC sys.sp_cdc_drop_job N'cleanup';
INSERT dbo.t VALUES (1, 'a'); INSERT dbo.p VALUES (1, 'a'), (150, 'b'); INSERT dbo.r VALUES (1, 1.2345, 'x'); INSERT dbo.p2 VALUES (160, 's');
SELECT source_object_id, OBJECT_NAME(source_object_id) AS t, partition_switch FROM cdc.change_tables;
GO
CREATE OR ALTER PROCEDURE #rowsets @what varchar(40) AS SELECT @what AS step, OBJECT_NAME(object_id) AS t, partition_number AS pn, partition_id, rows FROM sys.partitions WHERE OBJECT_SCHEMA_NAME(object_id) = N'dbo' AND index_id IN (0, 1) AND OBJECT_NAME(object_id) IN (N't', N'staging', N'p', N'p1', N'p2', N'r') ORDER BY t, pn;
GO
EXEC #rowsets 'before';
GO
BEGIN TRAN sw_out_t; ALTER TABLE dbo.t SWITCH TO dbo.staging; COMMIT;
GO
BEGIN TRAN sw_out_p; ALTER TABLE dbo.p SWITCH PARTITION 1 TO dbo.p1; COMMIT;
GO
DELETE dbo.p WHERE id = 150;
BEGIN TRAN sw_in_p; ALTER TABLE dbo.p2 SWITCH TO dbo.p PARTITION 2; COMMIT;
GO
BEGIN TRAN trunc_p; TRUNCATE TABLE dbo.p WITH (PARTITIONS (2)); COMMIT;
GO
IF @@TRANCOUNT > 0 ROLLBACK;
EXEC #rowsets 'after';
GO
BEGIN TRAN alt_dec; ALTER TABLE dbo.r ALTER COLUMN d decimal(9,1) NULL; COMMIT;
BEGIN TRAN add_def; ALTER TABLE dbo.r ADD f int NOT NULL DEFAULT 5; COMMIT;
BEGIN TRAN add_null; ALTER TABLE dbo.r ADD g int NULL; COMMIT;
GO
SELECT [Current LSN] AS lsn, Operation AS op, Context AS ctx, [Transaction ID] AS xid, [Transaction Name] AS xname, CONVERT(int, [Flag Bits]) AS f, AllocUnitName AS au, [Offset in Row] AS ofs, [RowLog Contents 0] AS l0, [RowLog Contents 1] AS l1
INTO #l FROM sys.fn_dblog(NULL, NULL);
SELECT xid, MAX(xname) AS name INTO #x FROM #l WHERE op = N'LOP_BEGIN_XACT' GROUP BY xid;
PRINT '== replication markers per transaction';
SELECT x.name, l.op, l.f, COUNT(*) AS n FROM #l l JOIN #x x ON x.xid = l.xid WHERE x.name IN ('sw_out_t', 'sw_out_p', 'sw_in_p', 'trunc_p', 'alt_dec', 'add_def', 'add_null') AND (l.f & 4 = 4) GROUP BY x.name, l.op, l.f ORDER BY x.name;
PRINT '== catalog changes of the switches (sysrowsets, sysrscols, sysallocunits)';
SELECT x.name, l.op, l.ctx, l.au, l.ofs, DATALENGTH(l.l0) AS len0, CASE WHEN DATALENGTH(l.l0) <= 16 THEN l.l0 END AS l0, CASE WHEN DATALENGTH(l.l1) <= 16 THEN l.l1 END AS l1 FROM #l l JOIN #x x ON x.xid = l.xid
WHERE x.name LIKE 'sw[_]%' AND l.au LIKE N'sys.sys%' ORDER BY l.lsn;
PRINT '== row changes of DDL that rewrites (not replicated)';
SELECT x.name, l.op, l.ctx, l.f, l.au, COUNT(*) AS n FROM #l l JOIN #x x ON x.xid = l.xid WHERE x.name IN ('alt_dec', 'add_def', 'add_null', 'trunc_p') AND l.au LIKE N'dbo.%' GROUP BY x.name, l.op, l.ctx, l.f, l.au;
GO
USE master; ALTER DATABASE lm_sw SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_sw;
GO
