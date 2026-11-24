-- Test-host tuning for ~1,200 small databases on one instance (not part of the tool).
EXEC sp_configure 'show advanced options', 1; RECONFIGURE;
EXEC sp_configure 'max server memory (MB)', 14336; RECONFIGURE;
ALTER DATABASE model SET QUERY_STORE = OFF;
DECLARE @db sysname, @sql nvarchar(max);
DECLARE dbs CURSOR LOCAL FAST_FORWARD FOR SELECT name FROM sys.databases WHERE name LIKE N'tenant[_]%' AND is_query_store_on = 1;
OPEN dbs; FETCH NEXT FROM dbs INTO @db;
WHILE @@FETCH_STATUS = 0
BEGIN
  SET @sql = N'ALTER DATABASE ' + QUOTENAME(@db) + N' SET QUERY_STORE = OFF;'; EXEC (@sql);
  FETCH NEXT FROM dbs INTO @db;
END
CLOSE dbs; DEALLOCATE dbs;
SELECT COUNT(*) AS tenants, SUM(CAST(is_query_store_on AS int)) AS qds_on FROM sys.databases WHERE name LIKE N'tenant[_]%';
