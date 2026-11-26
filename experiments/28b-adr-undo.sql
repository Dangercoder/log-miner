-- How does a partial rollback (failed statement, savepoint) look in the log with accelerated database recovery? Does every undone record get a
-- compensation record, and does the lowest [Previous LSN] still reach back to before the first undone record?
SET NOCOUNT ON;
USE master;
IF DB_ID(N'lm_adr') IS NOT NULL BEGIN ALTER DATABASE lm_adr SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_adr; END;
CREATE DATABASE lm_adr;
ALTER DATABASE lm_adr SET RECOVERY FULL;
ALTER DATABASE lm_adr SET ACCELERATED_DATABASE_RECOVERY = ON WITH ROLLBACK IMMEDIATE;
GO
USE lm_adr;
EXEC sys.sp_cdc_enable_db;
CREATE TABLE dbo.c (id int NOT NULL PRIMARY KEY, v varchar(100) NULL);
CREATE TABLE dbo.ix (id int NOT NULL PRIMARY KEY, a int NOT NULL, b int NOT NULL); CREATE UNIQUE INDEX ux ON dbo.ix (a);
CREATE TABLE dbo.h (id int NOT NULL, a int NOT NULL); CREATE UNIQUE INDEX uh ON dbo.h (a);
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'c', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ix', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'h', @role_name = NULL;
EXEC sys.sp_cdc_drop_job N'capture'; EXEC sys.sp_cdc_drop_job N'cleanup';
INSERT dbo.c VALUES (1, 'one'), (2, 'two'), (3, 'three'); INSERT dbo.ix VALUES (500, 500, 0); INSERT dbo.h VALUES (500, 500);
GO
BEGIN TRAN t1_dup2; BEGIN TRY INSERT dbo.ix VALUES (1, 7, 1), (2, 7, 2); END TRY BEGIN CATCH END CATCH; INSERT dbo.c VALUES (10, 'after'); COMMIT;
BEGIN TRAN t2_dup5; BEGIN TRY INSERT dbo.ix VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4), (5, 500, 5); END TRY BEGIN CATCH END CATCH; COMMIT;
BEGIN TRAN t3_updfail; BEGIN TRY UPDATE dbo.ix SET a = a + 1 WHERE id = 500; UPDATE dbo.c SET v = CONVERT(varchar(100), 1 / (id - 3)); END TRY BEGIN CATCH END CATCH; COMMIT;
BEGIN TRAN t4_save; INSERT dbo.c VALUES (20, 'keep'); SAVE TRAN s; INSERT dbo.c VALUES (21, 'x'); UPDATE dbo.c SET v = 'y' WHERE id = 20; DELETE dbo.c WHERE id = 1; UPDATE dbo.c SET v = 'z' WHERE id = 20; ROLLBACK TRAN s; COMMIT;
BEGIN TRAN t5_save2; SAVE TRAN s; UPDATE dbo.c SET v = 'a' WHERE id = 2; UPDATE dbo.c SET v = 'b' WHERE id = 2; UPDATE dbo.c SET v = 'c' WHERE id = 2; ROLLBACK TRAN s; COMMIT;
BEGIN TRAN t6_heap; BEGIN TRY INSERT dbo.h VALUES (1, 1), (2, 2), (3, 500); END TRY BEGIN CATCH END CATCH; COMMIT;
BEGIN TRAN t7_heapsave; SAVE TRAN s; INSERT dbo.h VALUES (7, 7); UPDATE dbo.h SET id = 8 WHERE id = 7; DELETE dbo.h WHERE id = 8; ROLLBACK TRAN s; COMMIT;
BEGIN TRAN t8_abort; INSERT dbo.c VALUES (30, 'x'), (31, 'y'); UPDATE dbo.c SET v = 'q' WHERE id = 3; ROLLBACK;
BEGIN TRAN t9_wide; SAVE TRAN s; INSERT dbo.c SELECT 1000 + n.n, 'w' FROM (SELECT TOP (300) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns) n; ROLLBACK TRAN s; COMMIT;
BEGIN TRAN t10_bigabort; INSERT dbo.c SELECT 5000 + n.n, 'w' FROM (SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b) n; ROLLBACK;
BEGIN TRAN t11_bigsave; INSERT dbo.c VALUES (60000, 'keep'); SAVE TRAN s; INSERT dbo.c SELECT 5000 + n.n, 'w' FROM (SELECT TOP (20000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS n FROM sys.all_columns a CROSS JOIN sys.all_columns b) n; ROLLBACK TRAN s; INSERT dbo.c VALUES (60001, 'keep'); COMMIT;
BEGIN TRAN t12_nested; INSERT dbo.c VALUES (61000, 'keep'); SAVE TRAN s1; INSERT dbo.c VALUES (61001, 'x'); SAVE TRAN s2; INSERT dbo.c VALUES (61002, 'x'); ROLLBACK TRAN s2; INSERT dbo.c VALUES (61003, 'x'); ROLLBACK TRAN s1; INSERT dbo.c VALUES (61004, 'keep'); SAVE TRAN s3; INSERT dbo.c VALUES (61005, 'x'); ROLLBACK TRAN s3; COMMIT;
GO
SET XACT_ABORT ON;
BEGIN TRY BEGIN TRAN t13_xact; INSERT dbo.c VALUES (62000, 'x'); INSERT dbo.c VALUES (62000, 'dup'); COMMIT; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK; END CATCH;
SET XACT_ABORT OFF;
GO
CHECKPOINT;
BEGIN TRAN t14_lock; INSERT dbo.c VALUES (63000, 'keep'); SAVE TRAN s; SELECT COUNT(*) FROM dbo.ix WITH (TABLOCKX, HOLDLOCK); ROLLBACK TRAN s; INSERT dbo.c VALUES (63001, 'keep'); UPDATE dbo.c SET v = 'k2' WHERE id = 63000; COMMIT;
GO
SELECT [Current LSN] AS lsn, Operation AS op, Context AS ctx, [Transaction ID] AS xid, [Transaction Name] AS xname, CONVERT(int, [Flag Bits]) AS f, [Replicated Records] AS rr, [Previous LSN] AS prev, AllocUnitName AS au
INTO #l FROM sys.fn_dblog(NULL, NULL);
SELECT xid, MAX(xname) AS name INTO #x FROM #l WHERE op = N'LOP_BEGIN_XACT' GROUP BY xid;
SELECT x.name, l.lsn, l.op, l.ctx, l.f, l.prev, l.rr, REPLACE(l.au, 'dbo.', '') AS au FROM #l l JOIN #x x ON x.xid = l.xid WHERE x.name LIKE 't[1-8][_]%' ORDER BY l.lsn;
SELECT l.xid, MIN(l.prev) AS lowest INTO #u FROM #l l WHERE l.f & 1 = 1 GROUP BY l.xid;
PRINT '== per transaction: replicated records, of which above the lowest [Previous LSN] of a compensation record (= undone by the rule), the count at the commit, how it ended';
SELECT x.name, SUM(CASE WHEN l.f & 4 = 4 AND l.op <> N'LOP_COMMIT_XACT' THEN 1 ELSE 0 END) AS replicated, SUM(CASE WHEN l.f & 1 = 1 THEN 1 ELSE 0 END) AS clrs,
       SUM(CASE WHEN l.f & 4 = 4 AND l.op <> N'LOP_COMMIT_XACT' AND l.lsn > u.lowest THEN 1 ELSE 0 END) AS above_lowest_prev, MAX(CASE WHEN l.op = N'LOP_COMMIT_XACT' THEN l.rr END) AS commit_rr,
       STRING_AGG(CONVERT(varchar(max), CASE WHEN l.op IN (N'LOP_COMMIT_XACT', N'LOP_ABORT_XACT', N'LOP_FORGET_XACT', N'LOP_PREP_XACT') THEN l.op + ' ' + l.ctx END), ', ') WITHIN GROUP (ORDER BY l.lsn) AS ended
FROM #l l JOIN #x x ON x.xid = l.xid LEFT JOIN #u u ON u.xid = l.xid WHERE x.name LIKE 't[0-9]%' GROUP BY x.name, l.xid ORDER BY MIN(l.lsn);
PRINT '== compensation-flagged records by operation';
SELECT l.op, l.ctx, l.f, COUNT(*) AS n, SUM(CASE WHEN l.prev = N'00000000:00000000:0000' THEN 1 ELSE 0 END) AS without_prev FROM #l l WHERE l.f & 1 = 1 GROUP BY l.op, l.ctx, l.f ORDER BY 1, 2;
GO
USE master;
ALTER DATABASE lm_adr SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_adr;
GO
