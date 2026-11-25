using System.Collections.Concurrent;
using System.Globalization;
using LogMiner.Capture;
using LogMiner.Schema;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

public enum CaptureScope
{
    /// <summary>Tables with CDC enabled (sys.tables.is_tracked_by_cdc): full row images, replication log hold.</summary>
    CdcTrackedTables,
    /// <summary>All user tables (no-CDC mode: updates are partial).</summary>
    AllUserTables,
}

/// <summary>
/// Loads the physical layouts of a database's captured tables: the current ones from the catalog, and the ones valid earlier in the log
/// by undoing the catalog's logged changes (<see cref="CatalogHistory"/>). The two reads are not one snapshot; a DDL that commits in
/// between shows up as a contradiction and the load is repeated.
/// </summary>
public static class SchemaLoader
{
    private const int Attempts = 6;

    // Runs inside the target database via [db].sys.sp_executesql so catalog views resolve there (any compatibility level).
    // An open transaction with DDL holds catalog rows that this has to wait for: not for long, others need the worker (it is tried again).
    private const string Query = """
        SET LOCK_TIMEOUT 10000;
        SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID()); -- before the catalog: DDL committed before this is in what follows
        SELECT t.object_id, SCHEMA_NAME(t.schema_id), t.name, p.partition_id, p.data_compression, p.index_id,
               pc.partition_column_id, ISNULL(c.column_id, 0), c.name, pc.system_type_id, pc.max_length, pc.max_inrow_length, pc.precision, pc.scale,
               CONVERT(int, COLLATIONPROPERTY(pc.collation_name, 'CodePage')), CONVERT(int, COLLATIONPROPERTY(pc.collation_name, 'CollationId')),
               pc.leaf_offset, pc.leaf_null_bit, pc.leaf_bit_position, pc.internal_offset, pc.internal_null_bit, pc.internal_bit_position, pc.key_ordinal,
               CONVERT(int, pc.is_replicated) + 2 * pc.is_dropped + 4 * pc.is_logged_for_replication + 8 * pc.is_descending_key + 16 * pc.is_uniqueifier
                 + 32 * pc.is_filestream + 64 * pc.is_anti_matter + 128 * (1 - pc.is_nullable) + 256 * pc.is_sparse,
               ISNULL(k.key_ordinal, 0), pc.has_default, pc.default_value, t.is_tracked_by_cdc
        FROM sys.tables t
        JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
        JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id
        LEFT JOIN sys.columns c ON c.object_id = t.object_id AND c.column_id = pc.partition_column_id AND pc.is_dropped = 0 AND pc.is_uniqueifier = 0 AND pc.is_anti_matter = 0
        OUTER APPLY (
            SELECT ic.key_ordinal FROM sys.index_columns ic
            WHERE ic.object_id = t.object_id AND ic.column_id = c.column_id AND ic.key_ordinal > 0
              AND ic.index_id = (SELECT TOP (1) i.index_id FROM sys.indexes i
                                 WHERE i.object_id = t.object_id AND (i.is_primary_key = 1 OR (i.index_id = 1 AND i.is_unique = 1))
                                 ORDER BY i.is_primary_key DESC)) k
        WHERE t.is_ms_shipped = 0
          AND (@scope = 1 OR t.is_tracked_by_cdc = 1
               OR (@objects <> N'' AND @objects LIKE N'%,' + CONVERT(nvarchar(20), t.object_id) + N',%')
               OR (@rowsets <> N'' AND t.is_replicated = 0 AND t.object_id IN (SELECT u.object_id FROM sys.partitions u WHERE @rowsets LIKE N'%,' + CONVERT(nvarchar(20), u.partition_id) + N',%')))
        ORDER BY p.partition_id, pc.partition_column_id
        OPTION (MAXDOP 1);
        SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name FROM sys.tables t
        WHERE t.is_ms_shipped = 0 AND t.is_tracked_by_cdc = 0 AND SCHEMA_NAME(t.schema_id) <> 'cdc' AND @scope = 0
        ORDER BY 1;
        SELECT p.partition_id FROM sys.tables t JOIN sys.partitions p ON p.object_id = t.object_id AND p.index_id IN (0, 1)
        WHERE t.is_tracked_by_cdc = 0 AND (t.is_replicated = 1 OR t.is_ms_shipped = 1) AND @scope = 0; -- their replicated records are transactional replication's
        IF OBJECT_ID(N'cdc.change_tables') IS NOT NULL SELECT source_object_id, start_lsn FROM cdc.change_tables WHERE start_lsn IS NOT NULL
        ELSE SELECT 0, CONVERT(binary(10), 0) WHERE 1 = 0;
        SELECT o.name, o.object_id, p.partition_id, au.allocation_unit_id
        FROM sys.all_objects o JOIN sys.partitions p ON p.object_id = o.object_id AND p.index_id = 1 JOIN sys.allocation_units au ON au.container_id = p.hobt_id AND au.type = 1
        WHERE o.schema_id = 4 AND o.name IN (N'sysrscols', N'sysrowsets', N'syscolpars', N'sysschobjs', N'sysseobjvalues');
        """;

    /// <summary>Only needed when the log does contain catalog changes: the layouts of the system tables themselves, and schema names.</summary>
    private const string FormatsQuery = """
        SELECT p.object_id, p.partition_id, pc.partition_column_id, c.name, pc.system_type_id, pc.max_length, pc.max_inrow_length, pc.precision, pc.scale,
               pc.leaf_offset, pc.leaf_null_bit, pc.leaf_bit_position, pc.is_nullable, pc.is_dropped, pc.has_default
        FROM sys.partitions p JOIN sys.system_internals_partition_columns pc ON pc.partition_id = p.partition_id
        JOIN sys.all_columns c ON c.object_id = p.object_id AND c.column_id = pc.partition_column_id
        WHERE p.index_id = 1 AND p.object_id IN (SELECT o.object_id FROM sys.all_objects o WHERE o.schema_id = 4 AND o.name IN (N'sysrscols', N'sysrowsets', N'syscolpars', N'sysschobjs', N'sysseobjvalues'))
        ORDER BY p.object_id, pc.partition_column_id;
        SELECT schema_id, name FROM sys.schemas;
        SELECT object_id FROM sys.tables WHERE is_ms_shipped = 0 AND is_replicated = 0;
        """;

    private sealed record Row(int ObjectId, string Schema, string Table, long RowsetId, DataCompression Compression, int IndexId, PhysicalColumn Physical, int ColumnId, string? Name,
                              int CodePage, int KeyOrdinal, object? Default, bool Tracked);

    private sealed record Catalog(Lsn AsOf, List<Row> Rows, List<string> Untracked, HashSet<long> NotCaptured, Dictionary<int, IReadOnlyList<Lsn>> CaptureStarts, List<SystemTable> SystemTables);

    private sealed record Formats(Dictionary<CatalogTable, TableLayout> Tables, Dictionary<int, string> Schemas, HashSet<int> UserTables);

    public static async Task<DatabaseSchema> LoadAsync(SqlConnection conn, DatabaseRef id, CaptureScope scope, Lsn? from,
                                                       IReadOnlySet<long> unresolvedRowsets, CancellationToken ct)
    {
        var (databaseId, database, _) = id;
        var formerObjects = new HashSet<int>();
        for (var attempt = 1; ; attempt++)
        {
            Catalog catalog;
            try { catalog = await ReadCatalogAsync(conn, id, scope, formerObjects, unresolvedRowsets, ct); }
            catch (SqlException ex) when (DatabaseIdentity.IsChanged(ex)) { throw new DatabaseChangedException(id, ex); }
            var log = await CatalogLogReader.ReadAsync(conn, id, from, catalog.SystemTables, ct);
            var coversFrom = from ?? default;
            var history = new LayoutHistory(coversFrom, new Dictionary<long, IReadOnlyList<LayoutVersion>>());
            if (log.Changes.Count > 0)
            {
                var formats = await ReadFormatsAsync(conn, id, ct);
                // a rowset that is gone, of a table that still exists but is not captured any more: load that table too
                var former = CatalogHistory.OwnersOf(unresolvedRowsets, log.Changes, formats.Tables)
                    .Where(o => formats.UserTables.Contains(o) && catalog.Rows.All(r => r.ObjectId != o) && formerObjects.Add(o)).ToList();
                if (former.Count > 0) { attempt--; continue; }
                try { history = CatalogHistory.Build(Snapshot(databaseId, database, catalog, formats, await CodePagesAsync(conn, catalog, ct)), log.Changes, log.Outcomes, formats.Tables, coversFrom); }
                catch (CatalogHistoryException) when (attempt < Attempts)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(25 << attempt), ct); // most likely a DDL committed between the two reads
                    continue;
                }
            }
            var layouts = Layouts(databaseId, database, catalog.Rows);
            return new DatabaseSchema(databaseId, database, layouts.Where(l => l.Captured).Select(l => l.Layout).ToArray(), catalog.AsOf, catalog.Untracked, history,
                layouts.Where(l => !l.Captured).Select(l => l.Layout).ToArray(), catalog.NotCaptured, catalog.CaptureStarts);
        }
    }

    private static List<(TableLayout Layout, bool Captured)> Layouts(int databaseId, string database, List<Row> rows) =>
        rows.GroupBy(r => r.RowsetId).Select(g =>
        {
            var f = g.First();
            return (new TableLayout(databaseId, database, f.ObjectId, f.Schema, f.Table, f.RowsetId, f.Compression,
                g.Select(r => CatalogHistory.ToColumn(r.Physical, r.Name ?? $"#col{r.Physical.RscolId}", r.ColumnId, r.CodePage, r.KeyOrdinal, r.Default)).ToArray()), f.Tracked);
        }).ToList();

    private static CatalogSnapshot Snapshot(int databaseId, string database, Catalog catalog, Formats formats, Func<int, int> codePage) => new()
    {
        DatabaseId = databaseId,
        Database = database,
        SchemaNames = formats.Schemas,
        CodePage = codePage,
        Rowsets = catalog.Rows.GroupBy(r => r.RowsetId).ToDictionary(g => g.Key, g => new CurrentRowset(g.Key, g.First().ObjectId, g.First().IndexId, g.First().Compression,
            g.Select(r => r.Physical).ToArray(), g.Where(r => r.Default is not null).ToDictionary(r => (int)r.Physical.LeafNullBit, r => r.Default))),
        Objects = catalog.Rows.GroupBy(r => r.ObjectId).ToDictionary(g => g.Key, g =>
        {
            var columns = g.Where(r => r.ColumnId > 0).DistinctBy(r => r.ColumnId).ToList();
            return new TableIdentity(g.Key, g.First().Schema, g.First().Table, columns.ToDictionary(r => r.ColumnId, r => r.Name!),
                columns.Where(r => r.KeyOrdinal > 0).ToDictionary(r => r.ColumnId, r => r.KeyOrdinal));
        }),
    };

    private static async Task<Catalog> ReadCatalogAsync(SqlConnection conn, DatabaseRef id, CaptureScope scope, HashSet<int> formerObjects, IReadOnlySet<long> rowsets, CancellationToken ct)
    {
        var database = id.Name;
        await using var cmd = DatabaseIdentity.Inside(conn, id, Query, ", @scope int, @objects nvarchar(max), @rowsets nvarchar(max)", ", @scope, @objects, @rowsets");
        cmd.CommandTimeout = 60;
        cmd.Parameters.AddWithValue("@scope", scope == CaptureScope.AllUserTables ? 1 : 0);
        cmd.Parameters.Add(new SqlParameter("@objects", System.Data.SqlDbType.NVarChar, -1) { Value = IdList(formerObjects.Select(o => (long)o)) });
        cmd.Parameters.Add(new SqlParameter("@rowsets", System.Data.SqlDbType.NVarChar, -1) { Value = IdList(rowsets) });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var asOf = await r.ReadAsync(ct) && !r.IsDBNull(0) ? Lsn.Parse(r.GetString(0)) : default;
        await r.NextResultAsync(ct);
        var rows = new List<Row>();
        while (await r.ReadAsync(ct))
        {
            var xtype = r.GetByte(9);
            var physical = PhysicalColumn.FromView(r.GetInt32(6), xtype, r.GetInt16(10), r.GetByte(12), r.GetByte(13), r.GetInt16(11), r.IsDBNull(15) ? 0 : r.GetInt32(15),
                Convert.ToInt16(r.GetValue(22), CultureInfo.InvariantCulture), r.GetInt32(23),
                (ushort)r.GetInt16(16) | r.GetInt16(19) << 16, (ushort)r.GetInt16(17) | r.GetInt16(20) << 16, (short)(r.GetByte(18) | r.GetByte(21) << 8));
            rows.Add(new Row(r.GetInt32(0), r.GetString(1), r.GetString(2), r.GetInt64(3), (DataCompression)r.GetByte(4), r.GetInt32(5), physical, r.GetInt32(7),
                r.IsDBNull(8) ? null : r.GetString(8), r.IsDBNull(14) ? 0 : r.GetInt32(14), r.GetByte(24),
                r.GetBoolean(25) && !r.IsDBNull(26) ? Default(r, 26, xtype, r.GetByte(13)) : null, scope == CaptureScope.AllUserTables || r.GetBoolean(27)));
        }
        await r.NextResultAsync(ct);
        var untracked = new List<string>();
        while (await r.ReadAsync(ct)) untracked.Add(r.GetString(0));
        await r.NextResultAsync(ct);
        var notCaptured = new HashSet<long>();
        while (await r.ReadAsync(ct)) notCaptured.Add(r.GetInt64(0));
        await r.NextResultAsync(ct);
        var starts = new List<(int ObjectId, Lsn Start)>();
        while (await r.ReadAsync(ct)) starts.Add((r.GetInt32(0), Lsn.FromBinary(r.GetFieldValue<byte[]>(1))));
        await r.NextResultAsync(ct);
        var system = new List<SystemTable>();
        while (await r.ReadAsync(ct)) system.Add(new SystemTable(TableOf(r.GetString(0)), r.GetInt32(1), r.GetInt64(2), r.GetInt64(3)));
        if (system.Count != 5) throw new CatalogHistoryException($"{database}: expected 5 system base tables, found {system.Count}; this SQL Server version is not supported");
        return new Catalog(asOf, rows, untracked, notCaptured, starts.GroupBy(s => s.ObjectId).ToDictionary(g => g.Key, g => (IReadOnlyList<Lsn>)g.Select(s => s.Start).ToArray()), system);
    }

    private static async Task<Formats> ReadFormatsAsync(SqlConnection conn, DatabaseRef id, CancellationToken ct)
    {
        var (databaseId, database, _) = id;
        await using var cmd = DatabaseIdentity.Inside(conn, id, FormatsQuery);
        cmd.CommandTimeout = 60;
        await using var r = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<(int ObjectId, long RowsetId, ColumnLayout Column)>();
        while (await r.ReadAsync(ct))
        {
            rows.Add((r.GetInt32(0), r.GetInt64(1), new ColumnLayout
            {
                PartitionColumnId = r.GetInt32(2), ColumnId = r.GetInt32(2), Name = r.GetString(3), Type = SqlTypes.FromIds(r.GetByte(4), r.GetByte(4)), MaxLength = r.GetInt16(5),
                MaxInRowLength = r.GetInt16(6), Precision = r.GetByte(7), Scale = r.GetByte(8), CodePage = 1252, LeafOffset = r.GetInt16(9), LeafNullBit = r.GetInt16(10),
                LeafBitPosition = r.GetByte(11), IsNullable = r.GetBoolean(12), IsDropped = r.GetBoolean(13),
            }));
        }
        await r.NextResultAsync(ct);
        var schemas = new Dictionary<int, string>();
        while (await r.ReadAsync(ct)) schemas[r.GetInt32(0)] = r.GetString(1);
        await r.NextResultAsync(ct);
        var userTables = new HashSet<int>();
        while (await r.ReadAsync(ct)) userTables.Add(r.GetInt32(0));
        var tables = rows.GroupBy(x => x.ObjectId).ToDictionary(g => SystemObject(g.Key),
            g => new TableLayout(databaseId, database, g.Key, "sys", SystemObject(g.Key).ToString(), g.First().RowsetId, DataCompression.None, g.Select(x => x.Column).ToArray()));
        return new Formats(tables, schemas, userTables);
    }

    private static CatalogTable TableOf(string name) => name switch
    {
        "sysrscols" => CatalogTable.RowsetColumns,
        "sysrowsets" => CatalogTable.Rowsets,
        "syscolpars" => CatalogTable.ObjectColumns,
        "sysschobjs" => CatalogTable.Objects,
        _ => CatalogTable.Defaults,
    };

    // ids of the system base tables are part of the on-disk format (the boot of every database depends on them)
    private static CatalogTable SystemObject(int objectId) => objectId switch
    {
        3 => CatalogTable.RowsetColumns,
        5 => CatalogTable.Rowsets,
        41 => CatalogTable.ObjectColumns,
        34 => CatalogTable.Objects,
        _ => CatalogTable.Defaults,
    };

    /// <summary>Collation id to code page: from the loaded columns, else (a collation that only occurs in the log) from the engine's list.</summary>
    private static async Task<Func<int, int>> CodePagesAsync(SqlConnection conn, Catalog catalog, CancellationToken ct)
    {
        var known = new Dictionary<int, int>();
        foreach (var r in catalog.Rows) if (r.Physical.CollationId != 0) known[r.Physical.CollationId] = r.CodePage;
        if (!AllCollations.TryGetValue(conn.DataSource, out var all))
        {
            all = [];
            await using var cmd = new SqlCommand("SELECT CONVERT(int, COLLATIONPROPERTY(name, 'CollationId')), CONVERT(int, COLLATIONPROPERTY(name, 'CodePage')) FROM sys.fn_helpcollations()", conn);
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct)) if (!r.IsDBNull(0)) all[r.GetInt32(0)] = r.IsDBNull(1) ? 0 : r.GetInt32(1);
            AllCollations[conn.DataSource] = all;
        }
        return id => known.TryGetValue(id, out var cp) || all.TryGetValue(id, out cp) ? cp : throw new CatalogHistoryException($"unknown collation id 0x{id:X8}");
    }

    private static readonly ConcurrentDictionary<string, Dictionary<int, int>> AllCollations = new();

    private static string IdList(IEnumerable<long> ids)
    {
        var list = string.Join(',', ids);
        return list.Length == 0 ? "" : "," + list + ",";
    }

    private static object? Default(SqlDataReader r, int i, byte xtype, byte scale)
    {
        if (xtype is 106 or 108) // exactly, also beyond System.Decimal - as the decoder gives values
        {
            if (r.GetSqlValue(i) is System.Data.SqlTypes.SqlDecimal d)
            {
                if (d.Scale != scale) d = System.Data.SqlTypes.SqlDecimal.ConvertToPrecScale(d, 38, scale);
                if (scale <= 28) try { return d.Value; } catch (OverflowException) { }
                return d.ToString();
            }
        }
        return NormalizeDefault(r.GetValue(i), xtype);
    }

    /// <summary>Normalises sql_variant defaults to the CLR types the row decoder produces for the column.</summary>
    private static object? NormalizeDefault(object v, byte xtype) => v switch
    {
        DBNull => null,
        System.Data.SqlTypes.SqlDecimal d => d.Value,
        DateTime d when xtype == 40 => DateOnly.FromDateTime(d),
        decimal m when xtype is 60 or 122 => Rows.ValueDecoder.FromScaledInteger(decimal.ToInt64(m * 10000m), 4),
        _ => v,
    };

    public static string Quote(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";
}
