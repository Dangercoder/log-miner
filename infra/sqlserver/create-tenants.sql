-- Creates tenant databases tenant_0001..tenant_N: TDE on, FULL recovery, CDC enabled on the
-- tables (for full row-image logging + replication log hold), CDC capture/cleanup jobs dropped.
-- Idempotent per database. Usage: lab sql create-tenants.sql  (edit @first/@last)
SET NOCOUNT ON;
DECLARE @first int = 1, @last int = 1200;
DECLARE @i int = @first, @db sysname, @sql nvarchar(max), @msg nvarchar(200);
-- A previous run may have been interrupted mid-database: drop tenants that are not fully configured.
DECLARE half CURSOR LOCAL FAST_FORWARD FOR
  SELECT d.name FROM sys.databases d
  LEFT JOIN sys.dm_database_encryption_keys k ON k.database_id = d.database_id
  WHERE d.name LIKE N'tenant[_]%' AND (d.is_cdc_enabled = 0 OR k.database_id IS NULL);
-- note: run repair-tenants.sql afterwards; it also enables CDC on any table whose setup was interrupted
OPEN half;
FETCH NEXT FROM half INTO @db;
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @msg = CONCAT(N'dropping half-configured ', @db); RAISERROR (@msg, 0, 1) WITH NOWAIT;
  SET @sql = N'ALTER DATABASE ' + QUOTENAME(@db) + N' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE ' + QUOTENAME(@db) + N';';
  EXEC (@sql);
  FETCH NEXT FROM half INTO @db;
END
CLOSE half; DEALLOCATE half;
WHILE @i <= @last
BEGIN
  SET @db = CONCAT(N'tenant_', RIGHT(CONCAT(N'0000', @i), 4));
  IF DB_ID(@db) IS NULL
  BEGIN
    SET @sql = N'CREATE DATABASE ' + QUOTENAME(@db) + N'; ALTER DATABASE ' + QUOTENAME(@db) + N' SET RECOVERY FULL;';
    EXEC (@sql);
    SET @sql = N'USE ' + QUOTENAME(@db) + N';
      CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = AES_256 ENCRYPTION BY SERVER CERTIFICATE LogMinerTdeCert;
      CREATE TABLE dbo.customers (
        id int IDENTITY NOT NULL CONSTRAINT pk_customers PRIMARY KEY,
        name nvarchar(100) NOT NULL,
        email varchar(200) NULL,
        balance decimal(18,2) NOT NULL CONSTRAINT df_customers_balance DEFAULT 0,
        updated_at datetime2(3) NOT NULL CONSTRAINT df_customers_updated DEFAULT SYSUTCDATETIME());
      CREATE TABLE dbo.orders (
        id bigint IDENTITY NOT NULL CONSTRAINT pk_orders PRIMARY KEY,
        customer_id int NOT NULL,
        amount decimal(18,2) NOT NULL,
        status tinyint NOT NULL,
        created_at datetime2(3) NOT NULL,
        note nvarchar(400) NULL,
        client_ts bigint NULL);  -- load generator stamps its clock here for end-to-end latency
      EXEC sys.sp_cdc_enable_db;
      EXEC sys.sp_cdc_enable_table @source_schema = N''dbo'', @source_name = N''customers'', @role_name = NULL;
      EXEC sys.sp_cdc_enable_table @source_schema = N''dbo'', @source_name = N''orders'', @role_name = NULL;
      EXEC sys.sp_cdc_drop_job @job_type = N''capture'';
      EXEC sys.sp_cdc_drop_job @job_type = N''cleanup'';';
    EXEC (@sql);
    SET @sql = N'ALTER DATABASE ' + QUOTENAME(@db) + N' SET ENCRYPTION ON; BACKUP DATABASE ' + QUOTENAME(@db) + N' TO DISK = ''NUL'' WITH NO_COMPRESSION;';
    EXEC (@sql);
  END
  IF @i % 50 = 0 BEGIN SET @msg = CONCAT(N'created through ', @db, N' at ', CONVERT(nvarchar(30), SYSDATETIME(), 126)); RAISERROR (@msg, 0, 1) WITH NOWAIT; END
  SET @i += 1;
END
SELECT COUNT(*) AS tenants, SUM(CASE WHEN is_encrypted = 1 THEN 1 ELSE 0 END) AS encrypted, SUM(CASE WHEN is_cdc_enabled = 1 THEN 1 ELSE 0 END) AS cdc
FROM sys.databases WHERE name LIKE N'tenant[_]%';
