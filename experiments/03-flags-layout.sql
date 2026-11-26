USE lab_a;
-- Which header fields distinguish CDC-table records and replicated commits?
WITH x AS (SELECT [Transaction ID] AS xid, [Transaction Name] AS xname FROM fn_dblog(NULL, NULL) WHERE Operation = 'LOP_BEGIN_XACT' AND [Transaction Name] LIKE 'x[0-9][0-9][_]%')
SELECT x.xname, l.Operation, l.Context, l.[Flag Bits], l.[Tag Bits], l.[Replicated Records], l.[Xact ID], l.[Begin Time], l.[End Time], l.PartitionId, l.AllocUnitId, l.RowFlags, l.[Lock Information]
FROM fn_dblog(NULL, NULL) l JOIN x ON x.xid = l.[Transaction ID]
WHERE x.xname IN ('x01_ins_plain', 'x02_ins_cdc', 'x10_upd_fixed_plain', 'x11_upd_fixed_cdc', 'x17_upd_key_cdc')
ORDER BY l.[Current LSN];
-- Physical layout the decoder needs
SELECT OBJECT_NAME(p.object_id) AS tbl, p.partition_id, p.hobt_id, pc.partition_column_id, c.name, TYPE_NAME(pc.system_type_id) AS type,
       pc.max_inrow_length, pc.precision, pc.scale, pc.collation_name, pc.leaf_offset, pc.leaf_null_bit, pc.leaf_bit_position,
       pc.is_nullable, pc.is_dropped, pc.is_uniqueifier, pc.is_sparse, pc.is_anti_matter, pc.key_ordinal, pc.has_default, pc.default_value
FROM sys.partitions p
JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id
LEFT JOIN sys.columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id
WHERE OBJECT_NAME(p.object_id) IN ('t_cdc', 't_heap_cdc') AND p.index_id IN (0, 1)
ORDER BY tbl, pc.partition_column_id;
SELECT name, is_read_committed_snapshot_on, snapshot_isolation_state_desc, delayed_durability_desc, is_accelerated_database_recovery_on
FROM sys.databases WHERE name = 'lab_a';
