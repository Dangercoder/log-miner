-- Which permissions does the doorbell's event session need, and what does SQL Server say when they are missing?
SET NOCOUNT ON;
IF SUSER_ID('logminer_lab_low') IS NOT NULL DROP LOGIN logminer_lab_low;
DECLARE @p nvarchar(200) = N'CREATE LOGIN logminer_lab_low WITH PASSWORD = ''' + CONVERT(nvarchar(36), NEWID()) + N'aA1!'', CHECK_POLICY = OFF;';
EXEC (@p);
SELECT SERVERPROPERTY('IsIntegratedSecurityOnly') AS windows_only;
GO
DECLARE @steps TABLE (id int IDENTITY, grants nvarchar(400));
INSERT @steps VALUES (N''), (N'GRANT VIEW SERVER STATE TO logminer_lab_low;'), (N'GRANT ALTER ANY EVENT SESSION TO logminer_lab_low;'), (N'REVOKE VIEW SERVER STATE FROM logminer_lab_low; GRANT VIEW SERVER PERFORMANCE STATE TO logminer_lab_low;');
DECLARE @i int = 1, @g nvarchar(400);
WHILE @i <= (SELECT COUNT(*) FROM @steps)
BEGIN
    SELECT @g = grants FROM @steps WHERE id = @i;
    EXEC (@g);
    SELECT '--- after: ' + @g AS step;
    EXECUTE AS LOGIN = 'logminer_lab_low';
    BEGIN TRY EXEC (N'CREATE EVENT SESSION logminer_lab_perm ON SERVER ADD EVENT sqlserver.log_flush_complete (WHERE database_id > 4);'); SELECT 'create: ok' AS r; END TRY BEGIN CATCH SELECT CONCAT('create: ', ERROR_NUMBER(), ' ', ERROR_MESSAGE()) AS r; END CATCH;
    BEGIN TRY EXEC (N'ALTER EVENT SESSION logminer_lab_perm ON SERVER STATE = START;'); SELECT 'start: ok' AS r; END TRY BEGIN CATCH SELECT CONCAT('start: ', ERROR_NUMBER(), ' ', ERROR_MESSAGE()) AS r; END CATCH;
    BEGIN TRY SELECT CONCAT('targets: ok ', COUNT(*)) AS r FROM sys.dm_xe_session_targets WHERE event_session_address = 0x01; END TRY BEGIN CATCH SELECT CONCAT('targets: ', ERROR_NUMBER(), ' ', ERROR_MESSAGE()) AS r; END CATCH;
    BEGIN TRY SELECT TOP (1) CONCAT('file stats: ok ', COUNT(*)) AS r FROM sys.dm_io_virtual_file_stats(NULL, 2); END TRY BEGIN CATCH SELECT CONCAT('file stats: ', ERROR_NUMBER(), ' ', ERROR_MESSAGE()) AS r; END CATCH;
    BEGIN TRY SELECT TOP (1) CONCAT('stream: ok type ', type) AS r FROM sys.fn_MSxe_read_event_stream(N'logminer_lab_perm', 0); END TRY BEGIN CATCH SELECT CONCAT('stream: ', ERROR_NUMBER(), ' ', ERROR_MESSAGE()) AS r; END CATCH;
    BEGIN TRY EXEC (N'DROP EVENT SESSION logminer_lab_perm ON SERVER;'); SELECT 'drop: ok' AS r; END TRY BEGIN CATCH SELECT CONCAT('drop: ', ERROR_NUMBER(), ' ', ERROR_MESSAGE()) AS r; END CATCH;
    REVERT;
    SET @i += 1;
END;
GO
IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = 'logminer_lab_perm') DROP EVENT SESSION logminer_lab_perm ON SERVER;
DROP LOGIN logminer_lab_low;
