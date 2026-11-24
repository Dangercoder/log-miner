-- Run after 09 (same database). (1) partial diffs on the catalog tables: which bytes; (2) does every layout-changing DDL carry
-- a LOP_REPL_COMMAND marker; (3) DDL and DML in one transaction; (4) DML during an ONLINE rebuild; (5) rollback of DDL.
USE lab_ddl;
DECLARE @lo nvarchar(50) = '0x' + (SELECT MIN([Current LSN]) FROM fn_dblog(NULL, NULL) WHERE [Transaction Name] = 'z01_rebuild');
SELECT b.[Transaction Name] AS xname, l.Operation, l.AllocUnitName, l.[Offset in Row] AS offs, l.[Modify Size] AS msz, l.[RowLog Contents 0] AS before_, l.[RowLog Contents 1] AS after_, l.[RowLog Contents 2] AS key_
FROM fn_dblog(@lo, NULL) l JOIN fn_dblog(@lo, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.sysrowsets.clust', 'sys.syscolpars.clst') AND l.Operation IN ('LOP_MODIFY_ROW', 'LOP_MODIFY_COLUMNS') AND l.Context = 'LCX_CLUSTERED'
ORDER BY l.[Current LSN];
SELECT b.[Transaction Name] AS xname, SUM(CASE WHEN l.Operation = 'LOP_REPL_COMMAND' THEN 1 ELSE 0 END) AS repl_cmds, SUM(CASE WHEN l.Operation = 'LOP_HOBT_DDL' THEN 1 ELSE 0 END) AS hobt_ddl,
       MAX(CASE WHEN l.Operation IN ('LOP_COMMIT_XACT', 'LOP_ABORT_XACT') THEN l.Operation END) AS ended, MAX(CASE WHEN l.Operation IN ('LOP_COMMIT_XACT', 'LOP_ABORT_XACT') THEN l.[Flag Bits] END) AS end_flags
FROM fn_dblog(@lo, NULL) l JOIN fn_dblog(@lo, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE b.[Transaction Name] LIKE 'z%' OR b.[Transaction Name] LIKE '%INDEX%' OR b.[Transaction Name] LIKE '%nline%'
GROUP BY b.[Transaction Name], b.[Transaction ID] ORDER BY b.[Transaction ID];
GO
BEGIN TRAN y1_same_txn; INSERT dbo.t (id, v, s, n) VALUES (50, 1, 'a', 1); ALTER INDEX ALL ON dbo.t REBUILD; INSERT dbo.t (id, v, s, n) VALUES (51, 1, 'b', 1); ALTER TABLE dbo.t ALTER COLUMN v decimal(19,0) NULL; INSERT dbo.t (id, v, s, n) VALUES (52, 1, 'c', 1); COMMIT;
BEGIN TRAN y2_ddl_savepoint; INSERT dbo.t (id, v, s, n) VALUES (53, 1, 'd', 1); SAVE TRAN sp; ALTER TABLE dbo.t ADD temp1 int NULL; ROLLBACK TRAN sp; INSERT dbo.t (id, v, s, n) VALUES (54, 1, 'e', 1); COMMIT;
GO
DECLARE @lo nvarchar(50) = '0x' + (SELECT MIN([Current LSN]) FROM fn_dblog(NULL, NULL) WHERE [Transaction Name] = 'y1_same_txn');
SELECT b.[Transaction Name] AS xname, l.[Current LSN], l.Operation, l.Context, l.[Flag Bits] AS flags, l.AllocUnitName, l.PartitionId, l.[Offset in Row] AS offs, DATALENGTH(l.[RowLog Contents 0]) AS rl0, DATALENGTH(l.[RowLog Contents 1]) AS rl1
FROM fn_dblog(@lo, NULL) l JOIN fn_dblog(@lo, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE b.[Transaction Name] IN ('y1_same_txn', 'y2_ddl_savepoint')
  AND (CONVERT(int, l.[Flag Bits]) & 5 <> 0 OR l.AllocUnitName IN ('sys.sysrscols.clst', 'sys.sysrowsets.clust', 'sys.syscolpars.clst'))
ORDER BY l.[Current LSN];
GO
