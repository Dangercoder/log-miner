-- Catalog changes when a clustered index is (re)created on a non-unique key and dropped again.
IF DB_ID('lab_hc') IS NOT NULL BEGIN ALTER DATABASE lab_hc SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_hc; END
CREATE DATABASE lab_hc; ALTER DATABASE lab_hc SET RECOVERY FULL;
GO
USE lab_hc;
CREATE TABLE dbo.t (a int NULL, id int NOT NULL, v int NULL, s varchar(40) NULL);
INSERT dbo.t VALUES (1, 1, 1, 'x');
GO
BEGIN TRAN h1; CREATE CLUSTERED INDEX cx ON dbo.t (v, id); COMMIT;
BEGIN TRAN h2; ALTER TABLE dbo.t ADD CONSTRAINT pk PRIMARY KEY NONCLUSTERED (id); COMMIT;
BEGIN TRAN h3; CREATE CLUSTERED INDEX cx ON dbo.t (s) WITH (DROP_EXISTING = ON); COMMIT;
BEGIN TRAN h4; DROP INDEX cx ON dbo.t; COMMIT;
GO
SELECT b.[Transaction Name] AS xname, l.[Current LSN], l.Operation, l.AllocUnitName, l.[Flag Bits] AS flags, l.[Offset in Row] AS offs, l.[RowLog Contents 0] AS rl0, l.[RowLog Contents 1] AS rl1, l.[RowLog Contents 2] AS rl2
FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT' AND b.[Xact Type] <> 3
WHERE l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.sysrowsets.clust') AND l.Context IN ('LCX_CLUSTERED', 'LCX_MARK_AS_GHOST') AND b.[Transaction Name] LIKE 'h_' ORDER BY l.[Current LSN];
GO
USE master; ALTER DATABASE lab_hc SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_hc;
