-- Edge cases for a log-based decoder (run after 01, in lab_a).
USE lab_a;
CREATE TABLE dbo.t_lob_cdc (id int NOT NULL PRIMARY KEY, doc nvarchar(max) NULL, bin varbinary(max) NULL);
CREATE TABLE dbo.t_max_cdc (id int NOT NULL PRIMARY KEY, c1 char(4000) NOT NULL, c2 char(4005) NOT NULL);
CREATE TABLE dbo.t_split_cdc (id int NOT NULL PRIMARY KEY, pad char(900) NOT NULL);
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't_lob_cdc', @role_name=NULL;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't_max_cdc', @role_name=NULL;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't_split_cdc', @role_name=NULL;
BEGIN TRY EXEC sys.sp_cdc_drop_job @job_type = N'capture'; END TRY BEGIN CATCH END CATCH;
BEGIN TRY EXEC sys.sp_cdc_drop_job @job_type = N'cleanup'; END TRY BEGIN CATCH END CATCH;
GO
BEGIN TRAN y01_add_col;   ALTER TABLE dbo.t_cdc ADD g int NULL; COMMIT;
GO
BEGIN TRAN y02_ins_newcol; INSERT dbo.t_cdc (id, a, g) VALUES (10, 1, 77); COMMIT;
GO
BEGIN TRAN y03_drop_col;  ALTER TABLE dbo.t_cdc DROP COLUMN g; COMMIT;
GO
BEGIN TRAN y04_ins_after_drop; INSERT dbo.t_cdc (id, a) VALUES (11, 1); COMMIT;
BEGIN TRAN y05_rebuild;   ALTER INDEX ALL ON dbo.t_cdc REBUILD; COMMIT;
BEGIN TRAN y06_rollback;  INSERT dbo.t_cdc (id, a) VALUES (12, 1); ROLLBACK;
BEGIN TRAN y07_savepoint; INSERT dbo.t_cdc (id, a) VALUES (13, 1); SAVE TRAN s1; INSERT dbo.t_cdc (id, a) VALUES (14, 1); ROLLBACK TRAN s1; COMMIT;
BEGIN TRY BEGIN TRAN y08_truncate; TRUNCATE TABLE dbo.t_cdc; COMMIT; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK; SELECT 'truncate refused' AS y08, ERROR_NUMBER() AS err, ERROR_MESSAGE() AS msg; END CATCH;
BEGIN TRAN y09_bulk;      INSERT dbo.t_cdc WITH (TABLOCK) (id, a) SELECT 1000 + value, value FROM GENERATE_SERIES(1, 5); COMMIT;
BEGIN TRAN y10_lob_ins;   INSERT dbo.t_lob_cdc VALUES (1, REPLICATE(CONVERT(nvarchar(max), N'Ã©'), 20000), CONVERT(varbinary(max), REPLICATE(CONVERT(varchar(max), 'z'), 30000))); COMMIT;
BEGIN TRAN y11_lob_small; INSERT dbo.t_lob_cdc VALUES (2, N'small inline', 0x01); COMMIT;
BEGIN TRAN y12_lob_upd;   UPDATE dbo.t_lob_cdc SET doc = REPLICATE(CONVERT(nvarchar(max), N'q'), 15000) WHERE id = 1; COMMIT;
BEGIN TRAN y13_lob_upd_other; UPDATE dbo.t_lob_cdc SET bin = 0x02 WHERE id = 1; COMMIT;
BEGIN TRAN y14_max_row;   INSERT dbo.t_max_cdc VALUES (1, REPLICATE('a', 4000), REPLICATE('b', 4005)); COMMIT;
BEGIN TRAN y15_max_upd;   UPDATE dbo.t_max_cdc SET c1 = REPLICATE('c', 4000) WHERE id = 1; COMMIT;
GO
-- page splits: 9 rows of ~900 bytes per page; insert 40 rows in one transaction
BEGIN TRAN y20_split; INSERT dbo.t_split_cdc SELECT value * 2, 'x' FROM GENERATE_SERIES(1, 40); COMMIT;
BEGIN TRAN y21_split_mid; INSERT dbo.t_split_cdc SELECT value * 2 + 1, 'y' FROM GENERATE_SERIES(1, 20); COMMIT;
GO
SELECT 'commit-begin-link' AS q, b.[Current LSN] AS begin_lsn, c.[Current LSN] AS commit_lsn, c.[Transaction Begin], c.[Xact ID], b.[Transaction Name]
FROM fn_dblog(NULL, NULL) c JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = c.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE c.Operation = 'LOP_COMMIT_XACT' AND b.[Transaction Name] IN ('y02_ins_newcol', 'y07_savepoint');
GO
-- every record of every transaction started by the y-statements, plus nested system transactions in between
DECLARE @lo nvarchar(50) = '0x' + (SELECT MIN([Current LSN]) FROM fn_dblog(NULL, NULL) WHERE [Transaction Name] = 'y01_add_col');
SELECT l.[Current LSN], l.Operation, l.Context, l.[Transaction ID], ISNULL(b.[Transaction Name], '') AS xname, l.[Flag Bits], l.[Replicated Records] AS repl,
       l.AllocUnitId, l.AllocUnitName, l.[Log Record Length] AS len, DATALENGTH(l.[RowLog Contents 0]) AS rl0, DATALENGTH(l.[RowLog Contents 1]) AS rl1,
       l.[Offset in Row] AS offs, l.[Modify Size] AS msz, l.[Description]
FROM fn_dblog(@lo, NULL) l
LEFT JOIN fn_dblog(@lo, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE CONVERT(int, l.[Flag Bits]) & 4 <> 0
   OR l.Operation IN ('LOP_BEGIN_XACT', 'LOP_COMMIT_XACT', 'LOP_ABORT_XACT', 'LOP_HOBT_DDL', 'LOP_HOBT_DELTA')
   OR l.AllocUnitName LIKE 'sys.sys%' OR l.AllocUnitName LIKE 'dbo.t[_]%'
ORDER BY l.[Current LSN];
