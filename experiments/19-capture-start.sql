-- cdc.change_tables.start_lsn vs the commit of the enabling transaction; second capture instance; disable + enable.
IF DB_ID('lab_gap') IS NOT NULL BEGIN ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap; END
CREATE DATABASE lab_gap; ALTER DATABASE lab_gap SET RECOVERY FULL;
GO
USE lab_gap;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
SELECT 'enabled' AS step, capture_instance, source_object_id, start_lsn, create_date FROM cdc.change_tables;
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL, @capture_instance = N'second';
SELECT 'second instance' AS step, capture_instance, start_lsn FROM cdc.change_tables;
EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N't', @capture_instance=N'dbo_t';
SELECT 'first dropped' AS step, capture_instance, start_lsn FROM cdc.change_tables;
EXEC sys.sp_cdc_disable_table @source_schema=N'dbo', @source_name=N't', @capture_instance=N'all';
INSERT dbo.t VALUES (1, 1); -- lost
EXEC sys.sp_cdc_enable_table @source_schema=N'dbo', @source_name=N't', @role_name=NULL;
SELECT 'disabled + enabled' AS step, capture_instance, start_lsn FROM cdc.change_tables;
SELECT [Current LSN] AS commit_lsn, [Transaction Begin] AS begin_lsn FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_COMMIT_XACT' AND CONVERT(int, [Flag Bits]) & 4 <> 0 ORDER BY 1;
SELECT b.[Transaction Name], COUNT(*) AS repl_commands, MIN(l.[Current LSN]) AS first_lsn FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.Operation = 'LOP_BEGIN_XACT' WHERE l.Operation = 'LOP_REPL_COMMAND' GROUP BY b.[Transaction Name], b.[Transaction ID] ORDER BY 3;
GO
USE master; ALTER DATABASE lab_gap SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_gap;
