-- sysrscols.ti (type info) as logged vs what the catalog view derives from it, for every type.
USE lab_ddl;
IF OBJECT_ID('dbo.alltypes') IS NOT NULL DROP TABLE dbo.alltypes;
GO
BEGIN TRAN x_alltypes;
CREATE TABLE dbo.alltypes (a int NOT NULL, id bigint NOT NULL, c_tinyint tinyint, c_smallint smallint, c_bit bit, c_bit2 bit, c_dec9 decimal(9,2), c_dec38 decimal(38,10), c_num numeric(5,0), c_money money, c_smallmoney smallmoney,
  c_float float, c_float24 float(24), c_real real, c_date date, c_time0 time(0), c_time3 time(3), c_time7 time(7), c_dt2_0 datetime2(0), c_dt2_7 datetime2(7), c_dto3 datetimeoffset(3), c_dt datetime, c_sdt smalldatetime,
  c_char char(10), c_char8000 char(800), c_varchar varchar(100), c_vmax varchar(max), c_nchar nchar(10), c_nvarchar nvarchar(400), c_nvmax nvarchar(max), c_sysname sysname NULL, c_bin binary(8), c_varbin varbinary(300), c_vbmax varbinary(max),
  c_guid uniqueidentifier, c_rv rowversion, c_xml xml, c_text text, c_ntext ntext, c_image image, c_variant sql_variant, c_hier hierarchyid, c_geo geography, c_geom geometry,
  CONSTRAINT pk_alltypes PRIMARY KEY (id, a));
COMMIT;
GO
SELECT c.name, l.[RowLog Contents 0] AS img, SUBSTRING(l.[RowLog Contents 0], 29, 4) AS ti, SUBSTRING(l.[RowLog Contents 0], 33, 4) AS cid, SUBSTRING(l.[RowLog Contents 0], 37, 2) AS ordkey, SUBSTRING(l.[RowLog Contents 0], 39, 2) AS maxinrow,
       SUBSTRING(l.[RowLog Contents 0], 41, 4) AS status_, SUBSTRING(l.[RowLog Contents 0], 45, 4) AS offs, SUBSTRING(l.[RowLog Contents 0], 49, 4) AS nullbit, SUBSTRING(l.[RowLog Contents 0], 53, 2) AS bitpos,
       pc.partition_column_id AS rscol, pc.system_type_id AS xt, pc.max_length AS len, pc.max_inrow_length AS mir, pc.precision AS pr, pc.scale AS sc, pc.leaf_offset AS lo, pc.internal_offset AS io, pc.leaf_null_bit AS lnb, pc.internal_null_bit AS inb,
       pc.leaf_bit_position AS lbp, pc.internal_bit_position AS ibp, pc.is_nullable AS nul, pc.key_ordinal AS ko, COLLATIONPROPERTY(pc.collation_name, 'CollationId') AS coll
FROM fn_dblog(NULL, NULL) l JOIN fn_dblog(NULL, NULL) b ON b.[Transaction ID] = l.[Transaction ID] AND b.[Transaction Name] = 'x_alltypes'
JOIN sys.partitions p ON p.object_id = OBJECT_ID('dbo.alltypes') AND p.index_id = 1
JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id AND CONVERT(binary(8), REVERSE(CONVERT(binary(8), pc.partition_id))) = SUBSTRING(l.[RowLog Contents 0], 5, 8)
     AND CONVERT(binary(4), REVERSE(CONVERT(binary(4), pc.partition_column_id))) = SUBSTRING(l.[RowLog Contents 0], 13, 4)
JOIN sys.columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id
WHERE l.AllocUnitName = 'sys.sysrscols.clst' AND l.Operation = 'LOP_INSERT_ROWS'
ORDER BY pc.partition_column_id;
