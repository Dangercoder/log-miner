-- Does a compensation record say exactly what it undoes? ([Previous LSN] = the record before the one it compensates?)
IF DB_ID('lab_clr') IS NOT NULL BEGIN ALTER DATABASE lab_clr SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_clr; END
CREATE DATABASE lab_clr; ALTER DATABASE lab_clr SET RECOVERY FULL;
GO
USE lab_clr;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL, doc nvarchar(max) NULL);
EXEC sys.sp_cdc_enable_db; EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture'; EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
BACKUP DATABASE lab_clr TO DISK = 'NUL';
INSERT dbo.t VALUES (1, 1, N'x');
GO
BEGIN TRAN w1; INSERT dbo.t VALUES (10, 1, N'keep'); SAVE TRAN s1; INSERT dbo.t VALUES (11, 1, N'undo'); UPDATE dbo.t SET v = 5 WHERE id = 1; SAVE TRAN s2; DELETE dbo.t WHERE id = 10; ROLLBACK TRAN s2; INSERT dbo.t VALUES (12, 1, N'undo'); ROLLBACK TRAN s1; INSERT dbo.t VALUES (13, 1, N'keep'); COMMIT;
BEGIN TRAN w2; INSERT dbo.t VALUES (20, 1, N'keep'); SAVE TRAN s1; ALTER TABLE dbo.t ALTER COLUMN v bigint NULL; ROLLBACK TRAN s1; COMMIT;
-- statement-level rollback inside a transaction (duplicate key on the second row)
BEGIN TRAN w3; INSERT dbo.t VALUES (30, 1, N'keep'); BEGIN TRY INSERT dbo.t SELECT v, 1, N'dup' FROM (VALUES (31), (1)) x(v); END TRY BEGIN CATCH END CATCH; INSERT dbo.t VALUES (32, 1, N'keep'); COMMIT;
BEGIN TRAN w4; INSERT dbo.t VALUES (40, 1, REPLICATE(CONVERT(nvarchar(max), N'a'), 20000)); SAVE TRAN s1; UPDATE dbo.t SET doc = REPLICATE(CONVERT(nvarchar(max), N'b'), 30000) WHERE id = 40; ROLLBACK TRAN s1; COMMIT;
GO
SELECT b.[Transaction Name] AS xname, l.[Current LSN], l.[Previous LSN], l.Operation, l.Context, l.[Flag Bits] AS flags, l.AllocUnitName, l.[Slot ID]
FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT'
WHERE b.[Transaction Name] IN ('w1', 'w2', 'w3', 'w4') ORDER BY l.[Current LSN];
GO
USE master; ALTER DATABASE lab_clr SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_clr;
