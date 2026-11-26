-- Experiment A/B: how are UPDATEs logged with and without CDC enabled on the table,
-- and is fn_dblog's [Log Record] complete for a ~7.5 KB row?
IF DB_ID('lab_a') IS NOT NULL BEGIN ALTER DATABASE lab_a SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_a; END
GO
CREATE DATABASE lab_a;
ALTER DATABASE lab_a SET RECOVERY FULL;
GO
USE lab_a;
CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = AES_256 ENCRYPTION BY SERVER CERTIFICATE LogMinerTdeCert;
GO
USE master;
ALTER DATABASE lab_a SET ENCRYPTION ON;
BACKUP DATABASE lab_a TO DISK = 'NUL';
GO
USE lab_a;
CREATE TABLE dbo.t_plain (id int NOT NULL PRIMARY KEY, a int NOT NULL, b varchar(50) NULL, c nvarchar(100) NULL, d datetime2(3) NULL, e decimal(18,4) NULL, f varbinary(100) NULL);
CREATE TABLE dbo.t_cdc   (id int NOT NULL PRIMARY KEY, a int NOT NULL, b varchar(50) NULL, c nvarchar(100) NULL, d datetime2(3) NULL, e decimal(18,4) NULL, f varbinary(100) NULL);
CREATE TABLE dbo.t_heap_cdc (id int NOT NULL, a int NULL, b varchar(50) NULL);
CREATE TABLE dbo.t_wide_cdc (id int NOT NULL PRIMARY KEY, big varchar(8000) NULL);
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't_cdc', @role_name=NULL;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't_heap_cdc', @role_name=NULL;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't_wide_cdc', @role_name=NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture';
EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
GO
BEGIN TRAN x01_ins_plain; INSERT dbo.t_plain VALUES (1, 10, 'hello', N'wörld', '2026-11-23 10:00:00.123', 12.3456, 0xDEADBEEF); COMMIT;
BEGIN TRAN x02_ins_cdc;   INSERT dbo.t_cdc   VALUES (1, 10, 'hello', N'wörld', '2026-11-23 10:00:00.123', 12.3456, 0xDEADBEEF); COMMIT;
BEGIN TRAN x03_ins_heap;  INSERT dbo.t_heap_cdc VALUES (1, 10, 'hello'); COMMIT;
BEGIN TRAN x04_ins_wide;  INSERT dbo.t_wide_cdc VALUES (1, REPLICATE('x', 7500)); COMMIT;
BEGIN TRAN x10_upd_fixed_plain; UPDATE dbo.t_plain SET a = 11 WHERE id = 1; COMMIT;
BEGIN TRAN x11_upd_fixed_cdc;   UPDATE dbo.t_cdc   SET a = 11 WHERE id = 1; COMMIT;
BEGIN TRAN x12_upd_var_plain;   UPDATE dbo.t_plain SET b = 'hello there, longer' WHERE id = 1; COMMIT;
BEGIN TRAN x13_upd_var_cdc;     UPDATE dbo.t_cdc   SET b = 'hello there, longer' WHERE id = 1; COMMIT;
BEGIN TRAN x14_upd_two_plain;   UPDATE dbo.t_plain SET a = 12, c = N'x' WHERE id = 1; COMMIT;
BEGIN TRAN x15_upd_two_cdc;     UPDATE dbo.t_cdc   SET a = 12, c = N'x' WHERE id = 1; COMMIT;
BEGIN TRAN x16_upd_heap_cdc;    UPDATE dbo.t_heap_cdc SET a = 11 WHERE id = 1; COMMIT;
BEGIN TRAN x17_upd_key_cdc;     UPDATE dbo.t_cdc   SET id = 2 WHERE id = 1; COMMIT;
BEGIN TRAN x18_upd_wide_cdc;    UPDATE dbo.t_wide_cdc SET big = REPLICATE('y', 7500) WHERE id = 1; COMMIT;
BEGIN TRAN x20_del_cdc;         DELETE dbo.t_cdc WHERE id = 2; COMMIT;
BEGIN TRAN x21_del_plain;       DELETE dbo.t_plain WHERE id = 1; COMMIT;
GO
SELECT DB_ID() AS dbid, (SELECT log_reuse_wait_desc FROM sys.databases WHERE database_id = DB_ID()) AS log_reuse_wait;
SELECT p.object_id, OBJECT_NAME(p.object_id) AS tbl, p.partition_id, p.hobt_id, au.allocation_unit_id, au.type_desc
FROM sys.partitions p JOIN sys.allocation_units au ON au.container_id = p.hobt_id
WHERE OBJECT_NAME(p.object_id) LIKE 't[_]%' AND p.index_id IN (0,1);
GO
WITH x AS (SELECT [Transaction ID] AS xid, [Transaction Name] AS xname FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_XACT' AND [Transaction Name] LIKE 'x[0-9][0-9][_]%')
SELECT x.xname, l.[Current LSN], l.Operation, l.Context, l.AllocUnitName, l.[Page ID], l.[Slot ID], l.[Offset in Row], l.[Modify Size],
       l.[Log Record Length], DATALENGTH(l.[Log Record]) AS lr_bytes, l.[Num Elements],
       l.[RowLog Contents 0], l.[RowLog Contents 1], l.[RowLog Contents 2], l.[RowLog Contents 3], l.[RowLog Contents 4], l.[RowLog Contents 5],
       l.[Log Record]
FROM fn_dblog(NULL, NULL) l JOIN x ON x.xid = l.[Transaction ID]
ORDER BY l.[Current LSN];
