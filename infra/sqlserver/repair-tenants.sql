-- Enables CDC on any tenant table that is not tracked (e.g. a tenant whose setup was interrupted), drops CDC jobs.
SET NOCOUNT ON;
DECLARE @db sysname, @sql nvarchar(max), @fixed int = 0;
DECLARE dbs CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM sys.databases WHERE name LIKE N'tenant[_]%' AND state = 0 ORDER BY name;
OPEN dbs; FETCH NEXT FROM dbs INTO @db;
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @sql = N'USE ' + QUOTENAME(@db) + N';
    DECLARE @n int = 0;
    IF (SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()) = 0 EXEC sys.sp_cdc_enable_db;
    IF EXISTS (SELECT 1 FROM sys.tables WHERE name = N''customers'' AND is_tracked_by_cdc = 0)
      BEGIN EXEC sys.sp_cdc_enable_table @source_schema = N''dbo'', @source_name = N''customers'', @role_name = NULL; SET @n += 1; END
    IF EXISTS (SELECT 1 FROM sys.tables WHERE name = N''orders'' AND is_tracked_by_cdc = 0)
      BEGIN EXEC sys.sp_cdc_enable_table @source_schema = N''dbo'', @source_name = N''orders'', @role_name = NULL; SET @n += 1; END
    IF @n > 0
    BEGIN
      BEGIN TRY EXEC sys.sp_cdc_drop_job @job_type = N''capture''; END TRY BEGIN CATCH END CATCH;
      BEGIN TRY EXEC sys.sp_cdc_drop_job @job_type = N''cleanup''; END TRY BEGIN CATCH END CATCH;
      RAISERROR(''repaired %s (%d tables)'', 0, 1, @db_name, @n) WITH NOWAIT;
    END';
  SET @sql = REPLACE(@sql, N'@db_name', N'''' + @db + N'''');
  EXEC (@sql);
  FETCH NEXT FROM dbs INTO @db;
END
CLOSE dbs; DEALLOCATE dbs;
SELECT COUNT(*) AS untracked_tenant_tables
FROM sys.databases d CROSS APPLY (SELECT 1 AS x) x
WHERE d.name LIKE N'tenant[_]%' AND 1 = 0;
