-- E: can a plain session release the replication log hold with sp_repldone (LSN form)?
-- F: what does sys.dm_db_log_stats.log_end_lsn mean relative to the last commit LSN?
-- C: which LSN string formats does fn_full_dblog accept for @start, and does it seek?
USE lab_a;
BACKUP LOG lab_a TO DISK = 'NUL';
SELECT 'after log backup' AS step, log_reuse_wait_desc FROM sys.databases WHERE name = 'lab_a';
SELECT log_end_lsn, log_truncation_holdup_reason, log_backup_lsn, log_min_lsn, recovery_vlf_count, total_vlf_count
FROM sys.dm_db_log_stats(DB_ID());
DBCC OPENTRAN WITH TABLERESULTS;
GO
-- F: commit something, compare the commit LSN with log_end_lsn captured right after
BEGIN TRAN f01_marker; INSERT dbo.t_cdc VALUES (100, 1, 'f', N'f', NULL, NULL, NULL); COMMIT;
SELECT 'post-commit' AS step, log_end_lsn FROM sys.dm_db_log_stats(DB_ID());
SELECT TOP (3) [Current LSN], Operation, [Transaction Name], [End Time] FROM fn_dblog(NULL, NULL) ORDER BY [Current LSN] DESC;
GO
-- C: LSN formats for @start
DECLARE @hex nvarchar(50) = (SELECT TOP (1) [Current LSN] FROM fn_dblog(NULL, NULL) WHERE [Transaction Name] = 'f01_marker');
DECLARE @dec nvarchar(50) = CONCAT(CONVERT(bigint, CONVERT(varbinary(4), '0x' + LEFT(@hex, 8), 1)), ':',
                                   CONVERT(bigint, CONVERT(varbinary(4), '0x' + SUBSTRING(@hex, 10, 8), 1)), ':',
                                   CONVERT(int,    CONVERT(varbinary(2), '0x' + RIGHT(@hex, 4), 1)));
SELECT @hex AS hex_lsn, @dec AS dec_lsn;
SELECT 'dec' AS fmt, COUNT(*) AS rows_from_start FROM sys.fn_full_dblog(@dec, NULL, DB_ID(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT 'hex0x' AS fmt, COUNT(*) AS rows_from_start FROM sys.fn_full_dblog('0x' + @hex, NULL, DB_ID(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT 'all' AS fmt, COUNT(*) AS rows_total FROM sys.fn_full_dblog(NULL, NULL, DB_ID(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
GO
-- E: pending replicated transactions, then release up to the last commit with sp_repldone (LSN form)
EXEC sp_repltrans;
GO
DECLARE @commit nvarchar(50), @begin nvarchar(50);
SELECT TOP (1) @commit = c.[Current LSN], @begin = b.[Current LSN]
FROM fn_dblog(NULL, NULL) c JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = c.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE c.Operation = 'LOP_COMMIT_XACT' ORDER BY c.[Current LSN] DESC;
DECLARE @xact_seqno binary(10) = CONVERT(binary(10), '0x' + REPLACE(@commit, ':', ''), 1);
DECLARE @xactid binary(10) = CONVERT(binary(10), '0x' + REPLACE(@begin, ':', ''), 1);
SELECT @begin AS begin_lsn, @xactid AS xactid, @commit AS commit_lsn, @xact_seqno AS xact_seqno;
EXEC sp_repldone @xactid = @xactid, @xact_seqno = @xact_seqno, @numtrans = 0, @time = 0, @reset = 0;
EXEC sp_replflush;
GO
EXEC sp_repltrans;
DBCC OPENTRAN WITH TABLERESULTS;
BACKUP LOG lab_a TO DISK = 'NUL';
SELECT 'after repldone + log backup' AS step, log_reuse_wait_desc FROM sys.databases WHERE name = 'lab_a';
GO
