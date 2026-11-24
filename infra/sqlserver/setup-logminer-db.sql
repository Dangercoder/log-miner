-- State database for logminer (checkpoints + lease). One per instance; it is not captured (no CDC).
-- The service creates its table on start; this script creates the database and a least-privilege login mapping.
IF DB_ID(N'logminer') IS NULL
BEGIN
    CREATE DATABASE logminer;
    ALTER DATABASE logminer SET RECOVERY FULL;          -- back it up with the instance's normal regime
    ALTER DATABASE logminer SET DELAYED_DURABILITY = DISABLED;
END
GO
USE logminer;
IF OBJECT_ID(N'dbo.checkpoints', N'U') IS NULL
    CREATE TABLE dbo.checkpoints (
        server_name   sysname      NOT NULL,
        database_name sysname      NOT NULL,
        incarnation   varchar(80)  NOT NULL,
        restart_lsn   binary(10)   NOT NULL,
        commit_lsn    binary(10)   NOT NULL,
        begin_lsn     binary(10)   NOT NULL,
        updated_at    datetime2(3) NOT NULL,
        held          bit          NOT NULL CONSTRAINT df_checkpoints_held DEFAULT 0,
        resumed_at    binary(10)   NULL,
        resumed_in_gap bit         NOT NULL CONSTRAINT df_checkpoints_in_gap DEFAULT 0,
        CONSTRAINT pk_checkpoints PRIMARY KEY (server_name, database_name));
GO
SELECT server_name, database_name, incarnation, restart_lsn, commit_lsn, updated_at FROM dbo.checkpoints ORDER BY database_name;
