-- How metadata-only column defaults (sql_variant in sysseobjvalues) are stored, per type.
IF DB_ID('lab_def') IS NOT NULL BEGIN ALTER DATABASE lab_def SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_def; END
CREATE DATABASE lab_def;
GO
USE lab_def;
CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY);
INSERT dbo.t VALUES (1);
GO
ALTER TABLE dbo.t ADD c01 int NOT NULL DEFAULT 5, c02 bigint NOT NULL DEFAULT 6, c03 smallint NOT NULL DEFAULT 7, c04 tinyint NOT NULL DEFAULT 8, c05 bit NOT NULL DEFAULT 1, c06 decimal(9,2) NOT NULL DEFAULT 1.25,
 c07 money NOT NULL DEFAULT 1.5, c08 float NOT NULL DEFAULT 2.5, c09 real NOT NULL DEFAULT 2.5, c10 date NOT NULL DEFAULT '2026-11-24', c11 time(3) NOT NULL DEFAULT '12:34:56.789', c12 datetime2(3) NOT NULL DEFAULT '2026-11-24T12:34:56.789',
 c13 datetimeoffset(0) NOT NULL DEFAULT '2026-11-24T12:34:56+01:00', c14 datetime NOT NULL DEFAULT '2026-11-24T12:34:56', c15 smalldatetime NOT NULL DEFAULT '2026-11-24T12:34:00', c16 char(6) NOT NULL DEFAULT 'abc', c17 varchar(20) NOT NULL DEFAULT 'abc',
 c18 nchar(6) NOT NULL DEFAULT N'abc', c19 nvarchar(20) NOT NULL DEFAULT N'abc', c20 binary(4) NOT NULL DEFAULT 0x0102, c21 varbinary(20) NOT NULL DEFAULT 0x0102, c22 uniqueidentifier NOT NULL DEFAULT '00112233-4455-6677-8899-AABBCCDDEEFF';
GO
SELECT c.name, CONVERT(nvarchar(60), SQL_VARIANT_PROPERTY(pc.default_value, 'BaseType')) AS base, pc.default_value, pc.has_default, pc.system_type_id
FROM sys.partitions p JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id JOIN sys.columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id
WHERE p.object_id = OBJECT_ID('dbo.t') ORDER BY c.column_id;
SELECT CONVERT(int, SUBSTRING([RowLog Contents 0], 14, 1)) AS hbcol, [RowLog Contents 0] AS img FROM fn_dblog(NULL, NULL) WHERE AllocUnitName = 'sys.sysseobjvalues.clst' AND Operation = 'LOP_INSERT_ROWS' AND DATALENGTH([RowLog Contents 0]) < 80 ORDER BY [Current LSN];
GO
USE master; ALTER DATABASE lab_def SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE lab_def;
