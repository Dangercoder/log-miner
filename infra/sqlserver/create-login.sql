-- A SQL login for logminer when it runs off the database host (a container in Kubernetes). Run it as a sysadmin, after setup-logminer-db.sql:
--
--   sqlcmd -S sql01 -E -C -i create-login.sql -v Login=logminer Password="<a strong password>" Databases="tenant_%" StateDatabase=logminer Sysadmin=0
--
--   Login          the login to create (if it is not there; an existing login keeps its password)
--   Password       its password (without a single quote); goes into the Kubernetes Secret as LogMiner__Sql__Password
--   Databases      LIKE pattern of the databases to capture (the service's LogMiner__Databases__0 uses * and ? instead: tenant_*)
--   StateDatabase  the state database (LogMiner__State__Database, default logminer)
--   Sysadmin       1: also sysadmin. Then the service verifies a new SQL Server build by itself before it captures (SelfTest: Require, the
--                  default): the self-test enables CDC in a scratch database, and sp_cdc_enable_db wants sysadmin (measured; dbcreator is not
--                  enough). 0: not; then set LogMiner__SelfTest=Off and run `logminer --self-test` as a sysadmin after every SQL Server update
--                  and every new logminer version (docs/kubernetes.md).
--
-- Without sysadmin it grants what capturing needs (measured; docs/design.md, "Off the database host"): fn_full_dblog asks for db_owner in master;
-- db_owner in every captured database and in the state database; VIEW SERVER STATE; ALTER ANY EVENT SESSION (SQL Server pushes log flushes;
-- without it the miner polls, p99 ~100 ms instead of ~7 ms). That is a lot of power, too: keep the password in a Secret, encrypt the connection,
-- and let only the logminer pods reach the instance.
--
-- A database created later needs the user: run the script again (it changes nothing that is already there). A sysadmin needs none of that.

IF SUSER_ID(N'$(Login)') IS NULL
    CREATE LOGIN [$(Login)] WITH PASSWORD = N'$(Password)', DEFAULT_DATABASE = master, CHECK_POLICY = ON;
GRANT VIEW SERVER STATE TO [$(Login)];
GRANT ALTER ANY EVENT SESSION TO [$(Login)];
IF $(Sysadmin) = 1 ALTER SERVER ROLE sysadmin ADD MEMBER [$(Login)];
GO

DECLARE @db sysname, @sql nvarchar(max), @n int = 0;
DECLARE dbs CURSOR LOCAL FAST_FORWARD FOR
    SELECT name FROM sys.databases
    WHERE (name = N'master' OR name = N'$(StateDatabase)' OR name LIKE N'$(Databases)') AND state_desc = N'ONLINE' AND is_read_only = 0
    ORDER BY name;
OPEN dbs;
FETCH NEXT FROM dbs INTO @db;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'USE ' + QUOTENAME(@db) + N';
IF DATABASE_PRINCIPAL_ID(N''$(Login)'') IS NULL CREATE USER [$(Login)] FOR LOGIN [$(Login)];
IF IS_ROLEMEMBER(N''db_owner'', N''$(Login)'') = 0 ALTER ROLE db_owner ADD MEMBER [$(Login)];';
    EXEC (@sql);
    SET @n += 1;
    FETCH NEXT FROM dbs INTO @db;
END
CLOSE dbs;
DEALLOCATE dbs;
IF DB_ID(N'$(StateDatabase)') IS NULL PRINT N'The state database $(StateDatabase) is not there: run setup-logminer-db.sql first, then this script again.';
PRINT CONCAT(N'$(Login): db_owner in ', @n, N' database(s) (master, the state database and those LIKE ''$(Databases)'')',
    CASE WHEN $(Sysadmin) = 1 THEN N', and sysadmin' ELSE N'; not sysadmin: set LogMiner__SelfTest=Off and run the self-test yourself' END);
GO
