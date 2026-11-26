-- What identifies a database's history? Message ids, checkpoints and the log reader (which reads by database id) rest on it.
-- Restores of one backup, over itself and as other databases; rename and swap; drop and create; snapshot revert; offline; detach.
SET NOCOUNT ON;
USE master;
DECLARE @d sysname;
DECLARE c CURSOR LOCAL FOR SELECT name FROM sys.databases WHERE name LIKE 'lm[_]id[_]%';
OPEN c; FETCH NEXT FROM c INTO @d;
WHILE @@FETCH_STATUS = 0 BEGIN IF DB_ID(@d) IS NOT NULL EXEC('DROP DATABASE [' + @d + ']'); FETCH NEXT FROM c INTO @d; END
CLOSE c; DEALLOCATE c;
GO
CREATE OR ALTER PROCEDURE #show @what varchar(60) AS
SELECT @what AS step, d.database_id AS id, d.name, LEFT(CONVERT(char(36), r.recovery_fork_guid), 8) AS fork, LEFT(CONVERT(char(36), r.first_recovery_fork_guid), 8) AS first_fork,
       LEFT(CONVERT(char(36), r.database_guid), 8) AS db_guid, LEFT(CONVERT(char(36), r.family_guid), 8) AS family, LEFT(CONVERT(char(36), d.service_broker_guid), 8) AS broker,
       CONVERT(varchar(23), d.create_date, 121) AS create_date, s.log_end_lsn
FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id = d.database_id CROSS APPLY sys.dm_db_log_stats(d.database_id) s
WHERE d.name LIKE 'lm[_]id[_]%' ORDER BY d.name;
GO
CREATE DATABASE lm_id_a;
GO
EXEC lm_id_a.sys.sp_executesql N'CREATE TABLE dbo.t (id int PRIMARY KEY, v int); INSERT dbo.t VALUES (1, 1);';
BACKUP DATABASE lm_id_a TO DISK = N'lm_id_a.bak' WITH INIT, COPY_ONLY, NO_COMPRESSION;
EXEC #show 'created, backed up';
RESTORE DATABASE lm_id_a FROM DISK = N'lm_id_a.bak' WITH REPLACE;
EXEC #show 'restored over itself';
RESTORE DATABASE lm_id_a FROM DISK = N'lm_id_a.bak' WITH REPLACE;
EXEC #show 'restored over itself again';
DECLARE @p nvarchar(400) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath'));
DECLARE @m1 nvarchar(400) = @p + N'lm_id_b.mdf', @l1 nvarchar(400) = @p + N'lm_id_b.ldf', @m2 nvarchar(400) = @p + N'lm_id_c.mdf', @l2 nvarchar(400) = @p + N'lm_id_c.ldf';
RESTORE DATABASE lm_id_b FROM DISK = N'lm_id_a.bak' WITH MOVE N'lm_id_a' TO @m1, MOVE N'lm_id_a_log' TO @l1;
RESTORE DATABASE lm_id_c FROM DISK = N'lm_id_a.bak' WITH MOVE N'lm_id_a' TO @m2, MOVE N'lm_id_a_log' TO @l2;
EXEC #show 'b and c restored from it';
GO
DECLARE @i int = 0;
EXEC lm_id_a.sys.sp_executesql N'INSERT dbo.t VALUES (2, 2)'; EXEC lm_id_b.sys.sp_executesql N'INSERT dbo.t VALUES (2, 2)'; EXEC lm_id_c.sys.sp_executesql N'INSERT dbo.t VALUES (2, 2)';
EXEC #show 'the same insert in each';
GO
ALTER DATABASE lm_id_a MODIFY NAME = lm_id_old; ALTER DATABASE lm_id_b MODIFY NAME = lm_id_a;
EXEC #show 'a renamed to old, b renamed to a';
GO
DROP DATABASE lm_id_a;
DECLARE @p nvarchar(400) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath'));
DECLARE @sql nvarchar(max) = N'CREATE DATABASE lm_id_a ON (NAME = N''x'', FILENAME = N''' + @p + N'lm_id_a2.mdf'') LOG ON (NAME = N''x_log'', FILENAME = N''' + @p + N'lm_id_a2.ldf'')';
EXEC (@sql);
EXEC #show 'a dropped and created';
GO
DECLARE @sql nvarchar(max) = N'CREATE DATABASE lm_id_s ON (NAME = N''x'', FILENAME = N''' + CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath')) + N'lm_id_s.ss'') AS SNAPSHOT OF lm_id_a';
EXEC lm_id_a.sys.sp_executesql N'CREATE TABLE dbo.t (id int PRIMARY KEY, v int); INSERT dbo.t VALUES (1, 1);';
EXEC (@sql);
EXEC lm_id_a.sys.sp_executesql N'INSERT dbo.t VALUES (2, 2)';
EXEC #show 'snapshot taken, row inserted';
RESTORE DATABASE lm_id_a FROM DATABASE_SNAPSHOT = N'lm_id_s';
EXEC #show 'reverted to the snapshot';
DROP DATABASE lm_id_s;
GO
ALTER DATABASE lm_id_c SET OFFLINE WITH ROLLBACK IMMEDIATE; ALTER DATABASE lm_id_c SET ONLINE;
EXEC #show 'c offline and online';
ALTER DATABASE lm_id_c SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
SELECT name, state_desc, user_access_desc FROM sys.databases WHERE name = 'lm_id_c';
ALTER DATABASE lm_id_c SET MULTI_USER;
EXEC sp_detach_db N'lm_id_c';
DECLARE @p nvarchar(400) = CONVERT(nvarchar(400), SERVERPROPERTY('InstanceDefaultDataPath'));
DECLARE @sql nvarchar(max) = N'CREATE DATABASE lm_id_c ON (FILENAME = N''' + @p + N'lm_id_c.mdf''), (FILENAME = N''' + @p + N'lm_id_c.ldf'') FOR ATTACH';
EXEC (@sql);
EXEC #show 'c detached and attached';
GO
DROP DATABASE lm_id_a; DROP DATABASE lm_id_c; DROP DATABASE lm_id_old;
EXEC msdb.dbo.sp_delete_database_backuphistory N'lm_id_a';
GO
