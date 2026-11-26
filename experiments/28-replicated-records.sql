-- (a) Does the commit record's [Replicated Records] say exactly how many replicated records the transaction wrote (so that a reader can prove it saw them all)?
-- (b) Which operations and contexts carry the replicated flag (0x4), in everything that can change a captured row?
-- For: plain, RCSI, ADR.
SET NOCOUNT ON;
USE master;
IF DB_ID(N'lm_rr') IS NOT NULL BEGIN ALTER DATABASE lm_rr SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_rr; END;
CREATE DATABASE lm_rr;
ALTER DATABASE lm_rr SET RECOVERY FULL;
GO
USE lm_rr;
EXEC sys.sp_cdc_enable_db;
CREATE TABLE dbo.c (id int NOT NULL PRIMARY KEY, v varchar(100) NULL, doc nvarchar(max) NULL);
CREATE TABLE dbo.h (id int NOT NULL, v varchar(7000) NULL);
CREATE TABLE dbo.n (id int NOT NULL CONSTRAINT pk_n PRIMARY KEY NONCLUSTERED, g int NOT NULL, v varchar(100) NULL);
CREATE CLUSTERED INDEX cx ON dbo.n (g);
CREATE TABLE dbo.txt (id int NOT NULL PRIMARY KEY, t text NULL);
CREATE TABLE dbo.b1 (id int NOT NULL, v char(50) NOT NULL);
CREATE TABLE dbo.b2 (id int NOT NULL PRIMARY KEY, v char(50) NOT NULL);
CREATE TABLE dbo.casc (id int NOT NULL PRIMARY KEY, c int NULL REFERENCES dbo.c (id) ON DELETE CASCADE);
CREATE TABLE dbo.ix (id int NOT NULL PRIMARY KEY, a int NOT NULL, b int NOT NULL); CREATE UNIQUE INDEX ux ON dbo.ix (a); CREATE INDEX ib ON dbo.ix (b) INCLUDE (a);
CREATE TABLE dbo.plain (id int NOT NULL PRIMARY KEY, v int NULL);
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'c', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'h', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'n', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'txt', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'b1', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'b2', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'casc', @role_name = NULL; EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ix', @role_name = NULL;
EXEC sys.sp_cdc_drop_job N'capture'; EXEC sys.sp_cdc_drop_job N'cleanup';
GO
CREATE OR ALTER PROCEDURE dbo.workload @tag varchar(10), @base int AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @n varchar(32), @big nvarchar(max) = REPLICATE(CONVERT(nvarchar(max), N'x'), 20000);
    SET @n = @tag + '_ins'; BEGIN TRAN @n; INSERT dbo.c (id, v, doc) VALUES (@base + 1, 'a', NULL), (@base + 2, 'b', N'small'); COMMIT;
    SET @n = @tag + '_lob'; BEGIN TRAN @n; INSERT dbo.c (id, v, doc) VALUES (@base + 3, 'a', @big); UPDATE dbo.c SET doc = doc + N'y' WHERE id = @base + 3; COMMIT;
    SET @n = @tag + '_write'; BEGIN TRAN @n; UPDATE dbo.c SET doc.WRITE(N'zz', 5, 2) WHERE id = @base + 3; COMMIT;
    SET @n = @tag + '_append'; BEGIN TRAN @n; UPDATE dbo.c SET doc.WRITE(N'zz', NULL, 0) WHERE id = @base + 3; COMMIT;
    SET @n = @tag + '_key'; BEGIN TRAN @n; UPDATE dbo.c SET id = id + 100 WHERE id IN (@base + 1, @base + 2); COMMIT;
    SET @n = @tag + '_noop'; BEGIN TRAN @n; UPDATE dbo.c SET v = v WHERE id = @base + 101; COMMIT;
    SET @n = @tag + '_savept'; BEGIN TRAN @n; INSERT dbo.c (id, v, doc) VALUES (@base + 10, 'x', NULL); SAVE TRAN s; INSERT dbo.c (id, v, doc) VALUES (@base + 11, 'y', NULL); UPDATE dbo.c SET v = 'q' WHERE id = @base + 10; ROLLBACK TRAN s; COMMIT;
    SET @n = @tag + '_undone'; BEGIN TRAN @n; SAVE TRAN s; INSERT dbo.c (id, v, doc) VALUES (@base + 12, 'x', NULL); ROLLBACK TRAN s; COMMIT;
    SET @n = @tag + '_dupkey'; BEGIN TRAN @n; BEGIN TRY INSERT dbo.ix VALUES (@base + 1, @base, 1), (@base + 2, @base, 2); END TRY BEGIN CATCH END CATCH; IF XACT_STATE() = 1 COMMIT; ELSE ROLLBACK;
    SET @n = @tag + '_ix'; BEGIN TRAN @n; INSERT dbo.ix VALUES (@base + 1, @base + 1, 1), (@base + 2, @base + 2, 2); UPDATE dbo.ix SET a = a + 1000, b = b + 1 WHERE id IN (@base + 1, @base + 2); DELETE dbo.ix WHERE id = @base + 2; COMMIT;
    SET @n = @tag + '_heap'; BEGIN TRAN @n; INSERT dbo.h VALUES (@base + 1, 'a'), (@base + 2, REPLICATE('b', 4000)), (@base + 3, 'c'); UPDATE dbo.h SET v = REPLICATE('a', 6000) WHERE id = @base + 1; UPDATE dbo.h SET v = REPLICATE('a', 7000) WHERE id = @base + 1; UPDATE dbo.h SET v = 'a' WHERE id = @base + 1; DELETE dbo.h WHERE id IN (@base + 1, @base + 3); COMMIT;
    SET @n = @tag + '_nuc'; BEGIN TRAN @n; INSERT dbo.n VALUES (@base + 1, 1, 'a'), (@base + 2, 1, 'b'); UPDATE dbo.n SET g = 2 WHERE id = @base + 1; DELETE dbo.n WHERE id = @base + 2; COMMIT;
    SET @n = @tag + '_merge'; BEGIN TRAN @n; MERGE dbo.n AS t USING (VALUES (@base + 1, 'm'), (@base + 9, 'n')) AS s (id, v) ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v WHEN NOT MATCHED THEN INSERT (id, g, v) VALUES (s.id, 5, s.v); COMMIT;
    SET @n = @tag + '_text'; BEGIN TRAN @n; INSERT dbo.txt VALUES (@base + 1, REPLICATE(CONVERT(varchar(max), 't'), 9000)); COMMIT;
    SET @n = @tag + '_casc'; BEGIN TRAN @n; INSERT dbo.casc VALUES (@base + 1, @base + 101); DELETE dbo.c WHERE id = @base + 101; COMMIT;
    SET @n = @tag + '_mixed'; BEGIN TRAN @n; INSERT dbo.plain VALUES (@base + 1, 1); INSERT dbo.c (id, v, doc) VALUES (@base + 20, 'x', NULL); UPDATE dbo.plain SET v = 2 WHERE id = @base + 1; COMMIT;
    SET @n = @tag + '_plain'; BEGIN TRAN @n; INSERT dbo.plain VALUES (@base + 2, 1); COMMIT;
    SET @n = @tag + '_ddl'; BEGIN TRAN @n; INSERT dbo.c (id, v, doc) VALUES (@base + 21, 'x', NULL); DECLARE @ddl nvarchar(200) = N'ALTER TABLE dbo.c ADD ' + @tag + N' int NULL'; EXEC (@ddl); UPDATE dbo.c SET v = 'after' WHERE id = @base + 21; COMMIT;
    SET @n = @tag + '_abort'; BEGIN TRAN @n; INSERT dbo.c (id, v, doc) VALUES (@base + 30, 'x', NULL); ROLLBACK;
    SET @n = @tag + '_bulkheap'; BEGIN TRAN @n; INSERT dbo.b1 WITH (TABLOCK) SELECT TOP (30000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) + @base, 'x' FROM sys.all_columns a CROSS JOIN sys.all_columns b; COMMIT;
    SET @n = @tag + '_bulkclus'; BEGIN TRAN @n; INSERT dbo.b2 WITH (TABLOCK) SELECT TOP (30000) ROW_NUMBER() OVER (ORDER BY (SELECT 1)) + @base, 'x' FROM sys.all_columns a CROSS JOIN sys.all_columns b; COMMIT;
    SET @n = @tag + '_bulkdel'; BEGIN TRAN @n; DELETE dbo.b1 WHERE id > @base; DELETE dbo.b2 WHERE id > @base; COMMIT;
END;
GO
EXEC dbo.workload 'p', 100000;
GO
PRINT '== (e) UPDATETEXT and WRITETEXT on a captured text column (refused: 7137)';
DECLARE @p varbinary(16) = (SELECT TOP (1) TEXTPTR(t) FROM dbo.txt); UPDATETEXT dbo.txt.t @p 5 3 WITH LOG 'ZZZ';
GO
DECLARE @p varbinary(16) = (SELECT TOP (1) TEXTPTR(t) FROM dbo.txt); WRITETEXT dbo.txt.t @p WITH LOG 'short';
GO
ALTER DATABASE lm_rr SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
ALTER DATABASE lm_rr SET ALLOW_SNAPSHOT_ISOLATION ON;
GO
EXEC dbo.workload 'r', 200000;
GO
ALTER DATABASE lm_rr SET ACCELERATED_DATABASE_RECOVERY = ON WITH ROLLBACK IMMEDIATE;
GO
EXEC dbo.workload 'a', 300000;
GO
ALTER DATABASE lm_rr SET RECOVERY BULK_LOGGED;
GO
EXEC dbo.workload 'k', 400000;
GO
SELECT [Current LSN] AS lsn, Operation AS op, Context AS ctx, [Transaction ID] AS xid, [Transaction Name] AS xname, CONVERT(int, [Flag Bits]) AS f, [Replicated Records] AS rr, [Previous LSN] AS prev,
       DATALENGTH([RowLog Contents 0]) AS l0, DATALENGTH([RowLog Contents 1]) AS l1, AllocUnitName AS au
INTO #l FROM sys.fn_dblog(NULL, NULL);
SELECT xid, MAX(xname) AS name INTO #x FROM #l WHERE op = N'LOP_BEGIN_XACT' GROUP BY xid;
PRINT '== (a) per transaction: the count at the commit, and the replicated records in the log (rows + REPL_COMMAND; all flagged; those without the commit itself)';
SELECT x.name, MAX(CASE WHEN l.op = N'LOP_COMMIT_XACT' THEN l.rr END) AS commit_rr, MAX(CASE WHEN l.op = N'LOP_COMMIT_XACT' THEN l.f END) AS commit_flags,
       SUM(CASE WHEN l.f & 4 = 4 AND l.op <> N'LOP_COMMIT_XACT' THEN 1 ELSE 0 END) AS flagged,
       SUM(CASE WHEN l.f & 4 = 4 AND l.op IN (N'LOP_INSERT_ROWS', N'LOP_DELETE_ROWS', N'LOP_MODIFY_ROW', N'LOP_MODIFY_COLUMNS') AND l.ctx IN (N'LCX_HEAP', N'LCX_CLUSTERED', N'LCX_MARK_AS_GHOST') THEN 1 ELSE 0 END) AS flagged_rows,
       SUM(CASE WHEN l.f & 1 = 1 THEN 1 ELSE 0 END) AS clrs,
       SUM(CASE WHEN l.f & 1 = 1 AND l.prev = N'00000000:00000000:0000' THEN 1 ELSE 0 END) AS clrs_without_prev
FROM #x x JOIN #l l ON l.xid = x.xid WHERE x.name LIKE '[prak][_]%' GROUP BY x.name, x.xid ORDER BY MIN(l.lsn);
PRINT '== (b) kinds of replicated records';
SELECT LEFT(x.name, 1) AS mode, l.op, l.ctx, l.f, COUNT(*) AS n, MIN(l.l0) AS min_l0, MAX(l.l0) AS max_l0, MIN(x.name) AS e_g
FROM #l l LEFT JOIN #x x ON x.xid = l.xid WHERE l.f & 4 = 4 GROUP BY LEFT(x.name, 1), l.op, l.ctx, l.f ORDER BY l.op, l.ctx, mode;
PRINT '== (c) transactions on captured tables whose commit is not flagged, or that logged row changes of captured tables without the flag';
SELECT x.name, l.op, l.ctx, l.f, l.au, COUNT(*) AS n FROM #l l JOIN #x x ON x.xid = l.xid
WHERE x.name LIKE '[prak][_]%' AND l.f & 5 = 0 AND l.op IN (N'LOP_INSERT_ROWS', N'LOP_DELETE_ROWS', N'LOP_MODIFY_ROW', N'LOP_MODIFY_COLUMNS', N'LOP_FORMAT_PAGE', N'LOP_SET_FREE_SPACE') AND l.ctx IN (N'LCX_HEAP', N'LCX_CLUSTERED', N'LCX_MARK_AS_GHOST')
  AND l.au LIKE N'dbo.%' AND l.au NOT LIKE N'dbo.plain%' GROUP BY x.name, l.op, l.ctx, l.f, l.au ORDER BY x.name;
PRINT '== (d) text updates in detail';
SELECT x.name, l.op, l.ctx, l.f, l.l0, l.l1, l.au FROM #l l JOIN #x x ON x.xid = l.xid WHERE x.name IN ('p_updtext', 'p_wrttext', 'p_write', 'p_append') ORDER BY l.lsn;
GO
USE master;
ALTER DATABASE lm_rr SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lm_rr;
GO
